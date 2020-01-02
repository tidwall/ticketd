package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/hashicorp/raft"
	raftjss "github.com/tidwall/raft-jss"
	raftwal "github.com/tidwall/raft-wal"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog"
)

const deadline = 10 * time.Second // network deadline
var log *redlog.Logger

func main() {
	nodeID := "1"         // node id
	sbindAddr := ":11001" // resp service bind addr
	rbindAddr := ":12001" // raft transport bind addr
	joinAddr := ""        // address of existing node in cluster
	dir := "data"         // data directory
	dur := "high"         // log durability

	flag.StringVar(&nodeID, "id", nodeID, "Node id")
	flag.StringVar(&sbindAddr, "saddr", sbindAddr, "Service bind address")
	flag.StringVar(&rbindAddr, "raddr", rbindAddr, "Raft bind address")
	flag.StringVar(&joinAddr, "join", joinAddr, "Join existing Raft cluster.")
	flag.StringVar(&dir, "dir", dir, "Data directory")
	flag.StringVar(&dur, "durability", dur, "Durability (high,medium,low)")
	flag.Parse()

	advertise := rbindAddr
	logOutput := os.Stderr // output for logger

	// Create a unified logger and filter the hashicorp raft logs
	log = redlog.New(logOutput)
	log.SetFilter(redlog.HashicorpRaftFilter)
	log.SetIgnoreDups(true)
	log.SetLevel(0)

	durability := raftwal.High
	switch dur {
	default:
		log.Fatalf("Invalid durability: %s", dur)
	case "low":
		durability = raftwal.Low
	case "medium":
		durability = raftwal.Medium
	case "high":
		durability = raftwal.High
	}

	// Create shared data directory
	if err := os.MkdirAll(filepath.Join(dir, nodeID), 0777); err != nil {
		log.Fatal(err)
	}

	// Open the Raft log storage, which is a write ahead log that is used to
	// keep the data in the cluster consistent.
	logs, err := raftwal.Open(filepath.Join(dir, nodeID, "log"), durability)
	if err != nil {
		log.Fatal(err)
	}
	if durability < raftwal.High {
		// Performs fsyncs in the background every 200 milliseconds when the
		// durability is not set to High. This ensures that log data will not
		// stay in memory for too long.
		go func() {
			for range time.NewTicker(time.Second / 5).C {
				logs.Sync()
			}
		}()
	}

	// Open the Raft stable storage, which is used to maintain the state of
	// the Raft cluster.
	stable, err := raftjss.Open(filepath.Join(dir, nodeID, "stable.json"))
	if err != nil {
		log.Fatal(err)
	}
	// Open the Raft snapshot storage, this is used to keep the size of the
	// log small by hanging onto a snapshot of the data a specific point.
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(dir, nodeID), 1, log)
	if err != nil {
		log.Fatal(err)
	}

	// Create the Raft network transport for communicating with other nodes in
	// the cluster.
	taddr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		log.Fatal(err)
	}
	trans, err := raft.NewTCPTransport(rbindAddr, taddr, 8, deadline, log)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new finite state machine object, which handles the ticket data
	// and applying that data to the Raft cluster.
	m := new(machine)

	// Start the Raft machine and bind it to all transport, fsm, and storage.
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(nodeID)
	conf.LogOutput = log
	ra, err := raft.NewRaft(conf, m, logs, stable, snaps, trans)
	if err != nil {
		log.Fatal(err)
	}

	// Get the current Raft cluster configuration for determining whether this
	// server needs to bootstrap a new cluster, or join/re-join an existing
	// cluster.
	cfg := ra.GetConfiguration()
	if err := cfg.Error(); err != nil {
		log.Fatalf("Could not get Raft configuration: %v", err)
	}
	servers := cfg.Configuration().Servers
	if len(servers) == 0 {
		// Empty configuration. Either bootstrap or join an existing cluster.
		if joinAddr == "" {
			// No '-join' flag provided.
			// Bootstrap new cluster.
			log.Noticef("Bootstrapping new cluster")
			var configuration raft.Configuration
			configuration.Servers = []raft.Server{
				raft.Server{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress(advertise),
				},
			}
			err = ra.BootstrapCluster(configuration).Error()
			if err != nil && err != raft.ErrCantBootstrap {
				log.Fatal(err)
			}
		} else {
			// Joining an existing cluster
			log.Noticef("Joining existing cluster at %v", joinAddr)
			conn, err := redis.Dial("tcp", joinAddr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			res, err := redis.String(conn.Do("raft.addvoter",
				nodeID, advertise))
			if err != nil {
				log.Fatal(err)
			}
			if res != "OK" {
				log.Fatalf("Expected 'OK', got '%s'", res)
			}
		}
	} else if joinAddr != "" {
		log.Debugf(
			"Ignoring Join request. Server already belongs to a cluster.")
	}

	// Start the Resp service
	startService(ra, sbindAddr)
}

// machine represents a Raft finite-state machine.
type machine struct {
	ticket uint64 // monotonically growing ticket
}

// Apply a log to the machine. There's only one possible thing that can be
// done, which is update the ticket by a delta value.
func (m *machine) Apply(l *raft.Log) interface{} {
	delta, n := binary.Uvarint(l.Data)
	if n != len(l.Data) {
		return errors.New("invalid data")
	}
	m.ticket += delta
	return m.ticket
}

// Restore from a snapshot. The only thing stored in a snapshot is the
// last known ticket in plain-text.
func (m *machine) Restore(rc io.ReadCloser) error {
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	ticket, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return err
	}
	m.ticket = ticket
	return nil
}

// Snapshot returns a snapshot with the ticket.
func (m *machine) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{m.ticket}, nil
}

// snapshot represents a Raft snapshot.
type snapshot struct {
	ticket uint64
}

// Persist the snapshot to the Raft snapshot storage. This is simply the ticket
// in plain-text.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write([]byte(strconv.FormatUint(s.ticket, 10)))
	if err != nil {
		if err2 := sink.Cancel(); err2 != nil {
			return fmt.Errorf(
				"Sink cancel failed: %v, sink write failed: %v", err2, err)
		}
		return err
	}
	return sink.Close()
}
func (s *snapshot) Release() {
	// snapshot has no resources, such as file handles, to release.
}

// cmds are is the resp command table
var cmds = map[string]func(s *service, conn redcon.Conn, cmd redcon.Command){
	// Standard commands
	"ping": cmdPING,
	"quit": cmdQUIT,

	// Ticket commands
	"ticket": cmdTICKET,

	// Various raft commands
	"raft.addvoter":      cmdRAFTADDVOTER,
	"raft.configuration": cmdRAFTCONFIGURATION,
	"raft.lastcontact":   cmdRAFTLASTCONTACT,
	"raft.leader":        cmdRAFTLEADER,
	"raft.removeserver":  cmdRAFTREMOVESERVER,
	"raft.snapshot":      cmdRAFTSNAPSHOT,
	"raft.stats":         cmdRAFTSTATS,
}

// service is an application-wide state object representing everything needed
// to manage the server
type service struct {
	raft    *raft.Raft
	ticketC chan *ticketR
}

// ticketR represents a client ticket request. It may request one or more
// tickets and the wait channel receives the starting ticket or an error.
type ticketR struct {
	count uint64           // number of needed tickets
	wait  chan interface{} // wait for starting ticket
}

func startService(ra *raft.Raft, addr string) {
	// We are up and running. Now create a server context that'll manage the
	// the clients applying commands.
	s := &service{
		raft:    ra,
		ticketC: make(chan *ticketR),
	}

	// Start the ticket applier background routine.
	go ticketApplier(s)

	// Start client resp server
	handler := func(conn redcon.Conn, cmd redcon.Command) {
		fn := cmds[strings.ToLower(string(cmd.Args[0]))]
		if fn == nil {
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		} else {
			fn(s, conn, cmd)
		}
	}
	saddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	rsvr := redcon.NewServer(saddr.String(), handler, nil, nil)
	sig := make(chan error)
	go func() {
		if <-sig == nil {
			log.Printf("Service started %v", rsvr.Addr())
		}
	}()
	if err := rsvr.ListenServeAndSignal(sig); err != nil {
		log.Fatal(err)
	}
}

// ticketApplier is a background routine that is responsible for gathering and
// applying new ticket requests.
func ticketApplier(s *service) {
	var data [10]byte   // stores uintvars
	var reqs []*ticketR // incoming ticket requests.
	for {
		var totalCount uint64
		v := <-s.ticketC
		totalCount += v.count
		reqs = append(reqs[:0], v)
		var done bool
		for !done {
			select {
			case v := <-s.ticketC:
				totalCount += v.count
				reqs = append(reqs, v)
			default:
				done = true
			}
		}
		n := binary.PutUvarint(data[:], totalCount)
		f := s.raft.Apply(data[:n], deadline)
		err := f.Error()
		if err != nil {
			// Something bad happened. Notify all the callers.
			for _, req := range reqs {
				req.wait <- err
			}
		} else {
			// Ticket update applied.
			ticket := f.Response().(uint64)
			for _, req := range reqs {
				ticket -= req.count
				req.wait <- ticket + 1
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Below are the service commands.
//
// Since this service uses the Redis protocol, also known as RESP, most any
// Redis compatible library can be used, including the `redis-cli`.
////////////////////////////////////////////////////////////////////////////////

// TICKET
func cmdTICKET(s *service, conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 1 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var req *ticketR
	v := conn.Context()
	if v == nil {
		req = &ticketR{wait: make(chan interface{})}
		conn.SetContext(req)
	} else {
		req = v.(*ticketR)
	}
	req.count = 1
	pl := conn.PeekPipeline()
	if len(pl) > 0 {
		// The request belongs to a packet that has multiple commands,
		// also known a pipelined packet. This is good because it means
		// that we can bulk Apply() to the log. But all commands must
		// be a valid TICKET command in order to do so. If not, then we
		// fall back to one at a time.
		valid := true
		for _, cmd := range pl {
			if len(cmd.Args) != 1 ||
				strings.ToLower(string(cmd.Args[0])) != "ticket" {
				valid = false
				break
			}
		}
		if valid {
			// Read the commands from the pipeline and increment the
			// request count.
			conn.ReadPipeline()
			req.count += uint64(len(pl))
		}
	}
	// We'll send each request to the central receiver which will
	// attempt to gather multiple tickets and apply them all at once.
	s.ticketC <- req
	switch v := (<-req.wait).(type) {
	case uint64:
		// received a new ticket
		for i := uint64(0); i < req.count; i++ {
			// Write as a string for compatibilty for large uint64 values.
			conn.WriteString(strconv.FormatUint(v+i, 10))
		}
	case error:
		// received an error
		for i := uint64(0); i < req.count; i++ {
			conn.WriteError("ERR " + v.Error())
		}
	}
}

// PING
func cmdPING(s *service, conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

// QUIT
func cmdQUIT(s *service, conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

// RAFT.REMOVESERVER node-id [prev-index]
func cmdRAFTREMOVESERVER(s *service, conn redcon.Conn, cmd redcon.Command) {
	var prevIndex uint64
	switch len(cmd.Args) {
	default:
		conn.WriteError("ERR wrong number of arguments")
		return
	case 3:
	case 4:
		n, err := strconv.ParseUint(string(cmd.Args[3]), 10, 64)
		if err != nil {
			conn.WriteError("ERR syntax error")
			return
		}
		prevIndex = n
	}
	err := s.raft.RemoveServer(
		raft.ServerID(string(cmd.Args[1])),
		prevIndex, 0,
	).Error()
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// RAFT.ADDVOTER node-id server-addr [prev-index]
func cmdRAFTADDVOTER(s *service, conn redcon.Conn, cmd redcon.Command) {
	var prevIndex uint64
	switch len(cmd.Args) {
	default:
		conn.WriteError("ERR wrong number of arguments")
		return
	case 3:
	case 4:
		n, err := strconv.ParseUint(string(cmd.Args[3]), 10, 64)
		if err != nil {
			conn.WriteError("ERR syntax error")
			return
		}
		prevIndex = n
	}
	err := s.raft.AddVoter(
		raft.ServerID(string(cmd.Args[1])),
		raft.ServerAddress(string(cmd.Args[2])),
		prevIndex, 0,
	).Error()
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// RAFT.LEADER
// Returns the current leader
func cmdRAFTLEADER(s *service, conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString(string(s.raft.Leader()))
}

// RAFT.STATS
func cmdRAFTSTATS(s *service, conn redcon.Conn, cmd redcon.Command) {
	m := s.raft.Stats()
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	conn.WriteArray(len(keys) * 2)
	for _, k := range keys {
		conn.WriteBulkString(k)
		conn.WriteBulkString(m[k])
	}
}

// RAFT.LASTCONTACT
func cmdRAFTLASTCONTACT(s *service, conn redcon.Conn, cmd redcon.Command) {
	conn.WriteBulkString(s.raft.LastContact().String())
}

// RAFT.SNAPSHOT
func cmdRAFTSNAPSHOT(s *service, conn redcon.Conn, cmd redcon.Command) {
	err := s.raft.Snapshot().Error()
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// RAFT.CONFIGURATION
func cmdRAFTCONFIGURATION(s *service, conn redcon.Conn, cmd redcon.Command) {
	fcfg := s.raft.GetConfiguration()
	if err := fcfg.Error(); err != nil {
		fmt.Println("ERR " + err.Error())
		return
	}
	cfg := fcfg.Configuration()
	conn.WriteArray(len(cfg.Servers))
	for _, svr := range cfg.Servers {
		conn.WriteArray(6)
		conn.WriteBulkString("id")
		conn.WriteBulkString(string(svr.ID))
		conn.WriteBulkString("address")
		conn.WriteBulkString(string(svr.Address))
		conn.WriteBulkString("suffrage")
		conn.WriteBulkString(fmt.Sprint(svr.Suffrage))
	}
}
