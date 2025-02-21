# MIT 6.5840: Distribute System

## Lab1 1: MapReduce

- [x] wc test
- [x] indexer test
- [x] map parallelism test
- [x] reduce parallelism test
- [x] job count test
- [x] early exit test
- [x] crash test

**Note**: all tests on MapReduce pass three thousand times.

## Lab 2: Key/Value Server

- [x] Key/value server with no network failures
- [x] Key/value server with dropped messages

## Lab 3: Raft

- Part 3A: leader election => Pass six thousand times
  - [x] initial election
  - [x] election after network failure
  - [x] multiple elections

- Part 3B: log
  - [x] basic agreement
  - [x] RPC byte count
  - [x] agreement after follower reconnects
  - [x] no agreement if too many followers disconnect
  - [x] concurrent Start()s
  - [x] rejoin of partitioned leader
  - [x] leader backs up quickly over incorrect follower logs
  - [x] RPC counts aren't too high

- Part 3C: persistence => Pass ten thousand times
  - [x] basic persistence
  - [x] more persistence
  - [x] partitioned leader and one follower crash, leader restarts
  - [x] Figure 8
  - [x] unreliable agreement
  - [x] Figure 8 (unreliable)
  - [x] churn
  - [x] unreliable churn
  
- Part 3D: log compaction => Pass ten thousand times
  - [x] snapshots basic
  - [x] install snapshots (disconnect)
  - [x] install snapshots (disconnect+unreliable)
  - [x] install snapshots (crash)
  - [x] install snapshots (unreliable+crash)
  - [x] crash and restart all servers

## Lab 4: Fault-tolerant Key/Value Service => Pass ten thousand times

- Part 4A: Key/value service without snapshots
  - [x] one client
  - [x] ops complete fast enough
  - [x] many clients
  - [x] unreliable net, many clients
  - [x] concurrent append to same key, unreliable
  - [x] progress in majority
  - [x] no progress in minority
  - [x] completion after heal
  - [x] partitions, one client
  - [x] partitions, many clients
  - [x] restarts, one client
  - [x] restarts, many clients
  - [x] unreliable net, restarts, many clients
  - [x] restarts, partitions, many clients
  - [x] unreliable net, restarts, partitions, many clients
  - [x] unreliable net, restarts, partitions, random keys, many clients

- Part 4B: Key/value service with snapshots
  - [x] InstallSnapshot RPC
  - [x] snapshot size is reasonable
  - [x] ops complete fast enough
  - [x] restarts, snapshots, one client
  - [x] restarts, snapshots, many clients
  - [x] unreliable net, snapshots, many clients
  - [x] unreliable net, restarts, snapshots, many clients
  - [x] unreliable net, restarts, partitions, snapshots, many clients
  - [x] unreliable net, restarts, partitions, snapshots, random keys, many clients

## Lab 5: Sharded Key/Value Service

- Part A: The Controller and Static Sharding
  - [x] Basic leave/join
  - [x] Historical queries
  - [x] Move
  - [x] Concurrent leave/join
  - [x] Minimal transfers after joins
  - [x] Minimal transfers after leaves
  - [x] minimal movement again
  - [x] Multi-group join/leave
  - [x] Concurrent multi leave/join
  - [x] Minimal transfers after multijoins
  - [x] Minimal transfers after multileaves
  - [x] Check Same config on servers
