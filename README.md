# ticketd

A distributed service for monotonically increasing tickets.

## Features

- Generates unique tickets
- Simple API, uses the [Redis Protocol](https://redis.io/topics/protocol)
- Fault-tolerant, uses [Raft Consensus Algorithm](https://raft.github.io)

## Building

[Go](https://golang.com) is required. 

```
make
```

## Running

It's ideal to have three, five, or seven nodes in your cluster.

Let's create the first node.

```
ticketd --id 1 --saddr :11001 --raddr :12001
```

This will create a node named `1` and bind the client service address to 
`:11001` and the Raft network transport to `:12001`

Now let's create two more nodes and add them to the cluster

```
ticketd --id 2 --saddr :11002 --raddr :12002 --join :11001
ticketd --id 3 --saddr :11003 --raddr :12003 --join :11001
```

Now we have a fault-tolerant three node cluster up and running.

## Using

You can use any Redis compatible client, the `redis-cli`, telnet, or netcat.

I'll use the `redis-cli` in the example below.


Connect to the leader. This will probably be the first node you created.

```
redis-cli -p 11001
```

Send the server a `TICKET` command and receive the first ticket. 

```
redis> TICKET
1
```

From here on every `TICKET` command will guarentee to generate a value larger
than the previous `TICKET`command.

```
redis> TICKET
2
redis> TICKET
3
redis> TICKET
4
redis> TICKET
5
```

## Pipelining and Client Multiplexing

ticketd supports pipelining commands, which means you can put multiple TICKET
commands into a single network packet. This is a performance enhancement and
will multiply the speed of applying new tickets by the number of tickets per
packet.

Client multiplexing is also supported, which means the server will read as many
TICKET commands from all connected clients as possible and apply them as a 
group. This can be a big performance enhancement on a multi-core server with
lots of concurrently connected clients.

## Other Commands

```
PING
QUIT
RAFT.ADDVOTER id addr
RAFT.CONFIGURATION
RAFT.LASTCONTACT
RAFT.LEADER 
RAFT.REMOVESERVER id addr
RAFT.SNAPSHOT
RAFT.STATS
```

## Durability

By default ticketd is highly durable, ensuring every command is applied to 
disk and fsynced. You can optionally choose to one of three durability levels
(high,medium,low) when the server starts up. For example:

```
ticketd --id 1 --saddr :11001 --raddr :12001 --durability low
```


## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License
ticketd source code is available under the MIT [License](/LICENSE).
