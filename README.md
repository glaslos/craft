craft
====

craft is a [Go](http://www.golang.org) library that manages a replicated
log and can be used with a finite state machine to manage replicated state machines. It
is a library for providing [consensus](http://en.wikipedia.org/wiki/Consensus_(computer_science)).

The use cases for such a library are far-reaching as replicated state
machines are a key component of many distributed systems. They enable
building Consistent, Partition Tolerant (CP) systems, with limited
fault tolerance as well.

## Building

If you wish to build craft you'll need Go version 1.2+ installed.

Build the library with:
```
go build
```

Please check your installation with:

```
go version
```

## Documentation

**TODO: Update links.**

For complete documentation, see the associated [Godoc](http://godoc.org/github.com/hashicorp/raft).

## Getting started

craft relies on clock synchronization. To get the best performance, the synchronization error
should be smaller than the one-way delay between any two nodes in the cluster.

An example use of craft is provided [here](https://gitlab.com/feiranwang/craft_example), which is a replicated key-value store.

## Protocol

craft is based on the CRaft consensus protocol, which is based on
["Raft: In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)

A high level overview of the CRaft protocol is described below, but for details please read the full paper.
This section will first describe the Raft protocol, and then the CRaft protocol.

### Raft

Raft servers are always in one of three states: follower, candidate or leader. All
servers initially start out as a follower. In this state, servers can accept log entries
from a leader and cast votes. If no entries are received for some time, servers
self-promote to the candidate state. In the candidate state servers request votes from
their peers. If a candidate receives a quorum of votes, then it is promoted to a leader.
The leader must accept new log entries and replicate to all the other followers.
In addition, if stale reads are not acceptable, all queries must also be performed on
the leader.

Once a cluster has a leader, it is able to accept new log entries. A client can
request that a leader append a new log entry, which is an opaque binary blob to
Raft. The leader then writes the entry to durable storage and attempts to replicate
to a quorum of followers. Once the log entry is considered *committed*, it can be
*applied* to a finite state machine. The finite state machine is application specific,
and is implemented using an interface.

An obvious question relates to the unbounded nature of a replicated log. Raft provides
a mechanism by which the current state is snapshotted, and the log is compacted. Because
of the FSM abstraction, restoring the state of the FSM must result in the same state
as a replay of old logs. This allows Raft to capture the FSM state at a point in time,
and then remove all the logs that were used to reach that state. This is performed automatically
without user intervention, and prevents unbounded disk usage as well as minimizing
time spent replaying logs.

Lastly, there is the issue of updating the peer set when new servers are joining
or existing servers are leaving. As long as a quorum of servers is available, this
is not an issue as Raft provides mechanisms to dynamically update the peer set.
If a quorum of servers is unavailable, then this becomes a very challenging issue.
For example, suppose there are only 2 peers, A and B. The quorum size is also
2, meaning both servers must agree to commit a log entry. If either A or B fails,
it is now impossible to reach quorum. This means the cluster is unable to add,
or remove a server, or commit any additional log entries. This results in *unavailability*.
At this point, manual intervention would be required to remove either A or B,
and to restart the remaining server in bootstrap mode.

A Raft cluster of 3 servers can tolerate a single server failure, while a cluster
of 5 can tolerate 2 server failures. The recommended configuration is to either
run 3 or 5 raft servers. This maximizes availability without
greatly sacrificing performance.

In terms of performance, Raft is comparable to Paxos. Assuming stable leadership,
committing a log entry requires a single round trip to half of the cluster.

### CRaft
CRaft is a multi-leader extension to Raft. It tries to solve the single leader bottleneck,
and is suitable for uses where high throughput is demanded.

CRaft runs multiple groups of
Raft concurrently. Each group operates as normal Raft: it elects a leader, and manages a replicated log.
A server may be a leader for a group, and a follower for other groups at the same time.
It may also be a follower for every group. The leader server of each group can handle client requests for that group.

Each log entry contains a timestamp taken by the leader of its group.
The log entries from different groups are merged into a *merged log* based on timestamps of entries.
The merging happens locally on each server. Each server's state machine applies entries in its merged log in order.

During normal operation, a client can send requests to any leader server (request leader). 
The workflow for handling a client request is as follows:
1. The leader of a group receives a command from the client.
2. The leader creates a log entry with the command, and assigns a timestamp.
It appends the entry to its local log, and replicates it to followers.
The entry is *committed* when it is replicated on a majority of servers.
3. The entry is merged into the merged log.
4. The leader executes the command in its state machine.
5. The leader returns the execution result to the client.

CRaft provides the same safety guarantee as Raft.

An optimization for write requests is that the requests may be responded before they are executed,
but CRaft guarantees that any later reads will see the most recent writes.

