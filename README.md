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

## Lab 4: Fault-tolerant Key/Value Service

- Part 4A: Key/value service without snapshots
  - [ ] one client
  - [ ] ops complete fast enough
  - [ ] many clients
  - [ ] unreliable net, many clients
  - [ ] concurrent append to same key, unreliable
  - [ ] progress in majority
  - [ ] no progress in minority
  - [ ] completion after heal
  - [ ] partitions, one client
  - [ ] partitions, many clients
  - [ ] restarts, one client
  - [ ] restarts, many clients
  - [ ] unreliable net, restarts, many clients
  - [ ] restarts, partitions, many clients
  - [ ] unreliable net, restarts, partitions, many clients
  - [ ] unreliable net, restarts, partitions, random keys, many clients