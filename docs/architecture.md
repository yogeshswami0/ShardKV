# Architecture

ShardKV is a C++17 distributed key-value node: an LSM storage engine, a Raft
consensus module, and a TCP server that multiplexes client KV traffic with
Raft RPCs.

```
Client (KvClient)
        |
        v
   TCP : listen address
        |
        +-- accept thread
        +-- one I/O thread per connection (capped by SHARD_MAX_CONNECTIONS)
        |
        v
   Bounded thread pool (SHARD_THREAD_POOL_SIZE / SHARD_REQUEST_QUEUE_SIZE)
        |
        +-- PING / GET / PUT / DELETE
        +-- RequestVote / AppendEntries
        |
        v
   RaftNode  ---- TCP RaftClient ---- peers
        |
        apply (committed entries only)
        |
        v
   StorageEngine
        WAL --> MemTable --> SSTable + Bloom --> compaction
```

## What is implemented

- **Storage:** write-ahead log with configurable sync (`group` / `always` /
  `none`), striped MemTable, SSTables, bloom filters, leveled compaction,
  process-crash recovery of the local engine.
- **Raft:** leader election, log replication, pre-vote, check-quorum, durable
  log and hard state. Verified in-process (`test_raft_cluster`).
- **TCP:** length-prefixed protocol, partial read/write, payload cap,
  connection cap, leader-aware KV, PING. Verified by `test_tcp_kv` against a
  single-node process (empty peer list, that node is its own quorum).

## What is not implemented yet

- Hash sharding in the running server (`ShardManager` is unused).
- Multi-process TCP failover tests (in-process Raft partitions exist).
- KV snapshots / Raft log truncation.
- Dynamic membership, fault-injection CLI, cluster metrics endpoint.
- The web dashboard still speaks the old gRPC stubs and does not match this
  binary protocol.

## Consistency (as of the TCP server)

Writes return after the Raft entry is committed and applied. Strongly
consistent reads go to the leader and call `confirmLeadership`. Follower
reads are rejected unless stale reads are enabled. That is weaker than a
full ReadIndex implementation that waits for the leader's commit index to
cover the client's last write across redirects, but it is what the code
does.

## Dual logs

A committed write is fsynced in the **Raft log**, then applied into the LSM
which appends a **WAL** record. The Raft log is the replicated source of
truth. The LSM WAL exists so a node can rebuild MemTable/SSTable state after
a crash without replaying the entire Raft log into the engine (snapshots,
when added, will make that cheaper). The cost is extra write amplification.
