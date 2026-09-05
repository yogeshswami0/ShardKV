# ShardKV Architecture Deep Dive

ShardKV is a high-performance, distributed Key-Value store written in modern C++17. It marries a Log-Structured Merge (LSM) tree storage engine with a Raft consensus module, allowing it to provide fault-tolerant, strongly consistent data storage across multiple nodes.

Below is a detailed breakdown of every major feature and how they interact to form the complete system.

---

## 1. C++17 & Core Infrastructure
The codebase leverages modern C++17 features to ensure safety, performance, and maintainability.
- **Modern Idioms:** Extensive use of `std::optional`, structured bindings, and smart pointers (`std::shared_ptr`, `std::unique_ptr`) to manage lifecycle and memory safely.
- **Cross-Platform Abstractions:** While originally POSIX-oriented, the codebase includes Windows-native fallback implementations (like using `ReadFile` with `OVERLAPPED` for thread-safe concurrent reads) to ensure the system is portable.

## 2. Network & Concurrency

### TCP Protocol
ShardKV uses a custom binary TCP protocol rather than HTTP or gRPC.
- **Multiplexing:** The TCP server multiplexes both client KV traffic (GET, PUT, DEL) and Raft RPC traffic (RequestVote, AppendEntries) over the same connections.
- **Length-Prefixed Framing:** To handle stream fragmentation, all messages are prefixed with their length, ensuring partial reads and writes are buffered correctly until a complete payload is formed.

### Thread Pool
To prevent a flood of network requests from overwhelming the system, ShardKV uses a bounded Thread Pool.
- **Connection Handling:** Each accepted connection gets an I/O thread.
- **Worker Pool:** Actual request processing (parsing, routing to Raft or Storage) is dispatched to a bounded worker thread pool (`SHARD_THREAD_POOL_SIZE`).
- **Backpressure:** If the request queue (`SHARD_REQUEST_QUEUE_SIZE`) fills up, the system exerts backpressure (e.g., returning `OVERLOADED` to clients) instead of running out of memory.

---

## 3. Storage Engine (LSM Tree)

The ShardKV storage engine is a Log-Structured Merge (LSM) tree, heavily optimized for high write throughput.

### Write-Ahead Log (WAL)
When a write is applied to the storage engine, it is first appended to the WAL on disk.
- **Durability:** Ensures that data is not lost if the process crashes before the in-memory data (MemTable) is flushed to disk.
- **Sync Policies:** Configurable durability (`group` commit, `always` sync, or `none`).

### MemTable
After the WAL is updated, the key-value pair is inserted into the MemTable, which is an in-memory data structure (often a SkipList or balanced tree).
- **Fast Writes:** Because writes only involve appending to the WAL and updating memory, they are extremely fast.
- **Flushing:** Once the MemTable reaches a size limit (`SHARD_MEMTABLE_SIZE_MB`), it becomes immutable and is flushed to disk as an SSTable.

### SSTables (Sorted String Tables)
Flushed MemTables become Level 0 SSTables on disk.
- **Immutability:** SSTables are never modified once written, which avoids complex file locking and enables lock-free concurrent reads.
- **Sorted Data:** Data within an SSTable is strictly sorted by key, allowing fast binary search lookups and sequential range scans.

### Bloom Filter
Looking up a key in an LSM tree might require checking multiple SSTables, which involves expensive disk I/O.
- **Probabilistic Optimization:** Each SSTable has an associated Bloom Filter in memory.
- **Avoid Disk Reads:** Before checking an SSTable on disk, ShardKV checks the Bloom Filter. If the filter says the key is *not* present, the engine skips the file entirely. If it says it *might* be present, the engine performs the disk read.

### Compaction
As more MemTables are flushed, the number of SSTables grows, which would slow down reads over time.
- **Leveled Compaction:** A background thread periodically merges smaller, overlapping SSTables into larger, non-overlapping SSTables at deeper levels.
- **Garbage Collection:** Compaction is where deleted keys (tombstones) and overwritten values are finally purged from disk, reclaiming space.

---

## 4. Raft Consensus Module

To provide high availability, ShardKV replicates data across multiple nodes using the Raft consensus algorithm.

### Leader Election
The cluster ensures that exactly one node acts as the Leader at any given time.
- **Heartbeats:** The Leader constantly sends heartbeats to Followers.
- **Election Timeouts:** If a Follower stops receiving heartbeats for a randomized period (`SHARD_ELECTION_TIMEOUT`), it transitions to a Candidate and requests votes.
- **Pre-Vote:** ShardKV implements the "Pre-Vote" extension to prevent partitioned nodes from artificially inflating the term number and disrupting the cluster when the partition heals.

### Log Replication
All client writes (PUT, DEL) must go through the Leader.
- **Two-Phase Commit:** The Leader appends the command to its Raft Log and issues an `AppendEntries` RPC to Followers.
- **Quorum:** Once a majority of nodes (quorum) acknowledge the write, the Leader considers it *committed*.
- **State Machine Application:** Only after it is committed in Raft is the write applied to the local Storage Engine (LSM).

### Crash Recovery
ShardKV is designed to survive brutal process crashes and power losses.
- **Raft Hard State:** Crucial Raft metadata (current term, voted for) is synced to disk immediately. If a node crashes, it reloads this state to ensure it doesn't violate Raft safety guarantees.
- **Dual Logs:** After a crash, the node replays its persistent Raft Log to catch up on distributed consensus, while the Storage Engine replays its local WAL to recover any unflushed MemTable data.
- **Healing:** If a node was offline while the cluster moved ahead, the Leader automatically sends the missing `AppendEntries` to bring the recovering node back in sync.
