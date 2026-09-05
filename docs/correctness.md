# Correctness notes

What was wrong, what changed, and what the evidence is. Every entry names the
test that fails against the previous behaviour, or says plainly that no such
test exists.

## Storage

### The SSTable footer overflowed its buffer

The footer serializes 64 bytes: four 8-byte offsets, two 4-byte counts, two
8-byte sequence numbers, then a 4-byte magic and a 4-byte hash count. Both the
writer and the reader used a hardcoded 60-byte stack buffer, so each overflowed
by four bytes on every table.

Only 60 bytes reached disk, which meant the bloom filter's hash count was never
persisted at all. The reader then recovered it by seeking to the last four bytes
of the file, which are the magic number, producing a filter that claimed
1,447,383,636 hash rounds.

The integration suite aborted on every run before this was fixed. The LSM flush
path had never worked in this build.

Writer and reader now share a `kSSTableFooterSize` constant, the hash count is a
real footer field, and the writer asserts the serialized size so the two sides
cannot drift again.

### Acknowledged writes were not durable

`WAL::append` called `ofstream::flush()`, which moves bytes into the operating
system's page cache and no further. No `fsync` existed anywhere in the codebase,
so acknowledged writes were lost on machine crash. `std::ofstream` never exposes
its file descriptor, so no sync was reachable without replacing it.

The WAL now writes through a raw descriptor and syncs before acknowledging. On
Apple platforms that means `F_FULLFSYNC`: plain `fsync` returns once data reaches
the drive, which is free to hold it in a volatile write cache. Linux uses
`fdatasync`.

SSTables were equally unsynced, and the WAL segment holding those writes was
deleted immediately afterwards. Both are now durable before anything is dropped.

Evidence: `test_wal.cpp` asserts a real sync is issued before the acknowledgement
and that concurrent writers batch. See "What the durability tests cannot prove"
below.

### Group commit had nothing to batch

`StorageEngine::put` took the engine lock exclusively around the WAL append, so
exactly one writer was ever inside the WAL. Group commit could not function.

Taking the lock in shared mode doubled throughput at four writers, from 270 to
530 operations per second, and cut the 99th percentile latency from 154ms to
14ms. The lock is still needed, but only to exclude the flush swap.

### A flush could delete the WAL records of a live write

`flushMemTable` called a rotate that both started a new segment and deleted the
old one, outside the engine lock. A write could land in the new MemTable while
its WAL record sat in the segment about to be deleted. Once deleted, that write
existed only in memory despite having been acknowledged.

Rotation now happens inside the same exclusive section that freezes the MemTable,
so the closed segment holds exactly the writes in the frozen table. Those
segments are deleted only after the SSTable containing them is durably installed.

Evidence: `StorageEngineTest.WritesDuringAFlushSurviveReopen`. This test passes
but was **not** confirmed to fail against the old code: the current segment
bookkeeping refuses to reproduce the interleaving on demand.

### Concurrent readers corrupted each other

`SSTableReader` held one `std::ifstream` and `readBlock` did a seek followed by a
read with no lock, while `StorageEngine::get` takes only a shared lock.
Concurrent readers interleaved those two calls on a shared file cursor and
received each other's blocks. In practice the added test hangs indefinitely
against the old reader, with threads stuck inside stream state.

Reads now use `pread`, which takes the offset as an argument and touches no
shared cursor. Reads are genuinely parallel rather than serialized behind a lock,
and because the descriptor stays open for the reader's lifetime, an in-flight
read survives compaction unlinking the file.

Evidence: `SSTableTest.ConcurrentReadersDoNotTearEachOthersReads`.

### Level 0 returned the stalest version of a key

`getAllSSTables` walked each level in insertion order and `addSSTable` appends,
so level 0 came back oldest first while readers take the first hit. A rewritten
key read back as its oldest version until compaction merged it away.

Level 0 is now ordered by the footer's maximum sequence number, descending. That
is true recency order and, unlike insertion order, survives a restart.

Evidence: `CompactionTest.Level0ReturnsNewestVersionOfAKey` and
`Level0OrderSurvivesReopen`.

### Compaction discarded concurrent flushes

`compactLevel` snapshotted its inputs under the lock, merged without it, then
reacquired and cleared the whole level. Any SSTable a flush added during the
merge window was discarded.

Exactly the merged inputs are now removed, by identity.

Evidence: `CompactionTest.FlushDuringCompactionIsNotLost`.

### Compaction resurrected deleted keys

Tombstones were dropped whenever the target level was 2 or deeper, without
checking whether a deeper level still held an older version of that key. If one
did, dropping the tombstone brought the deleted value back.

A tombstone is now dropped only when no level below the target holds anything.

Evidence: `CompactionTest.TombstoneIsNotDroppedWhileOlderVersionSurvivesBelow`.
Making this testable required the per-level size budget to become injectable:
reaching level 2 previously needed 64MB of real data, which is precisely why the
bug went unnoticed.

### The merge read everything into memory

Peak usage was the size of the entire compaction, unbounded for an LSM. A
streaming k-way merge over reader iterators now holds one block per input.

## Raft

### Acknowledgement did not mean anything

`waitForCommit` slept 50ms and returned true without ever reading the commit
index. `KVServiceImpl` then wrote to storage directly, in addition to the apply
callback, so every leader write was applied twice and the local write happened
whether or not the entry committed. A partitioned leader accepted and
acknowledged writes it could not commit.

`waitForApplied(index, term)` now blocks until that entry is applied and verifies
the slot still carries the proposed term. That check is the substance: an index
alone can be reused by a later leader, so a matching term is the only proof that
this proposal is what committed there. The apply loop is the sole writer to
storage.

Evidence: `RaftClusterTest.PartitionedLeaderDoesNotAcknowledgeWrites`, confirmed
to fail against the sleep-and-return-true behaviour.

### Term and vote were persisted only at shutdown

Raft's election safety argument assumes a vote survives a crash. `persistState`
ran only from the graceful exit path, so a node that crashed mid-term came back
having forgotten its vote and could vote a second time in that term, which
permits two leaders.

Hard state is now written at every point that mutates it, before the node acts on
the change, through a temporary file and rename.

Evidence: `RaftHardStateTest.GrantedVoteSurvivesWithoutGracefulShutdown`.

### The log accepted unvalidated lengths

`loadFrom` read a command length straight off disk into `resize`, so a torn tail
could demand an enormous allocation or abort on load. Records are now length
framed and checksummed, and loading stops at the first damaged record and keeps
the valid prefix. A partial record after a crash is expected, not exceptional.

Evidence: `RaftLogPersistenceTest.TornTailKeepsValidPrefix` and
`ImplausibleCommandLengthDoesNotAllocate`.

### Two thread lifecycle crashes

`becomeFollower` joined the heartbeat thread while holding the node mutex, and
was reached from a callback running on that very thread, making it a self join.
`becomeLeader` assigned over a possibly joinable `std::thread`, which calls
`std::terminate`.

Stepping down now only clears the state and lets the loops observe it. Joining
happens from the election thread or from `stop`, never under the mutex and never
from a thread joining itself.

### One dead peer collapsed the heartbeat cadence

Replication fanned out to every peer in parallel but waited for all of them, so a
dead peer stretched each round to its RPC deadline. A healthy follower still
received its heartbeat promptly but received the next one a deadline later.
Measured: cadence fell from 30 per second to 10 per second because a different
peer went down.

Replication is now driven by one loop per peer, each bound to the term it started
in, with the commit index recomputed after each individual response.

Evidence: `RaftClusterTest.HeartbeatCadenceIsIndependentOfAnUnreachablePeer`.
Reproducing this required making the harness faithful: it returned failure
instantly for an unreachable peer, hiding every cost of having one, whereas a
real RPC blocks until its deadline.

### The election timeout ratio was too tight

150 to 300ms against a 50ms heartbeat is a 3 to 6 times ratio where Raft wants an
order of magnitude. This was wrong independently of any threading concern. It is
now 500 to 1000ms.

### A rejoining node could depose a healthy leader

Without pre-vote, an isolated node campaigns on every timeout and climbs to an
ever higher term. Measured on the in-process cluster: term 1 to term 14 in three
seconds. On rejoining, that inflated term obliges a working leader to step down,
so one flapping node can unseat a healthy cluster indefinitely.

A candidate now runs a trial election before touching its term, asking only
whether it would win. Pre-vote messages move neither side's term, which is the
entire point.

A follower refuses pre-votes while a leader is known to be alive, whatever the
candidate's log looks like. This covers asymmetric partitions, where a node
cannot hear the leader but its peers can. Writing that rule as "have I heard from
a leader recently" is subtly wrong: it exempts the leader itself, which then
helps a rejoining node depose it.

Evidence: `IsolatedNodeDoesNotInflateItsTermWhileAway`,
`RejoiningNodeDoesNotDisturbTheLeader`, and
`AsymmetricPartitionDoesNotUnseatAHealthyLeader`, all confirmed to fail with the
mechanism disabled.

### A partitioned leader never noticed

Nothing arrives to tell a partitioned leader it has been deposed, so it lingered
as leader indefinitely. It now steps down if it has not heard from a majority
within an election timeout.

Evidence: `RaftClusterTest.PartitionedLeaderStepsDownOnItsOwn`.

## What the durability tests cannot prove

Losing an acknowledged write requires a machine crash. A process crash leaves the
page cache intact, so data that only reached the page cache still comes back.
That is exactly why a missing `fsync` can go unnoticed: it looks durable under
every test that can be written without cutting power.

The suite checks the two things it genuinely can. `ProcessCrashDoesNotLoseAcknowledgedWrites`
forks a child that calls `_exit`, skipping every destructor and buffer flush, and
confirms records left the user space buffer before the acknowledgement. The sync
count assertions confirm a real sync call is issued before the acknowledgement
and that concurrent writers share one.

Neither distinguishes a missing `fsync`. Doing that needs filesystem fault
injection such as `dm-flakey`, which these tests do not attempt.

## Known gaps

- `WritesDuringAFlushSurviveReopen` passes but was not confirmed to fail against
  the previous code.
- Snapshotting and log compaction are unimplemented. The Raft log grows without
  bound, so a long lived cluster will accumulate entries indefinitely.
- Cluster membership is fixed at startup. There is no joint consensus or
  single server membership change.
