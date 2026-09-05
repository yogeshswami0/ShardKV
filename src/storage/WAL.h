#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace shard {

/**
 * WAL entry types for recovery
 */
enum class WALEntryType : uint8_t { PUT = 0x01, DEL = 0x02 };

/**
 * Durability policy for the write-ahead log.
 *
 * GROUP  - concurrent writers batch into a single sync; every append still
 *          returns only once its own record is on stable media. Default.
 * ALWAYS - one sync per append, no batching.
 * NONE   - no sync at all; records reach the OS page cache only, so they
 *          survive a process crash but not a machine crash. Benchmarking only.
 */
enum class WALSyncMode { GROUP, ALWAYS, NONE };

WALSyncMode parseSyncMode(const std::string &name);
const char *syncModeName(WALSyncMode mode);

/**
 * Single entry in the Write-Ahead Log
 */
struct WALEntry {
  WALEntryType type;
  uint64_t sequenceNumber;
  std::string key;
  std::string value; // Empty for DELETE

  // Serialization
  std::string serialize() const;
  static bool deserialize(const std::string &data, WALEntry &entry);
};

/**
 * Write-Ahead Log for durability.
 *
 * Every mutation is written to the WAL before being applied to the MemTable.
 * On crash recovery, the WAL is replayed to restore the MemTable state.
 *
 * WAL format per entry:
 * [4 bytes: entry length][4 bytes: CRC32][entry data]
 *
 * Durability: appends return only after the record is on stable media (see
 * WALSyncMode). This uses a raw file descriptor rather than std::ofstream
 * because ofstream never exposes its descriptor, leaving no way to fsync.
 *
 * Group commit: a sync is expensive and covers every byte written so far, so
 * concurrent writers share one. The first writer to find no sync in flight
 * performs it on behalf of everyone whose record is already written; the rest
 * wait until the synced watermark passes their own record. This is the
 * LevelDB/RocksDB pattern. N writers amortize into one sync without any of
 * them returning early.
 */
class WAL {
public:
  explicit WAL(const std::string &directory);
  WAL(const std::string &directory, WALSyncMode syncMode);
  ~WAL();

  // Append operations to log. Returns once the record is durable (unless the
  // sync mode is NONE).
  void appendPut(const std::string &key, const std::string &value,
                 uint64_t seqNum);
  void appendDelete(const std::string &key, uint64_t seqNum);

  // Recovery: replay all entries from current log
  std::vector<WALEntry> recover();

  /**
   * Start a new segment and return the id of the one just closed.
   *
   * This does NOT delete anything. The caller rotates at the moment it freezes
   * the MemTable, so the closed segment holds exactly the writes in that frozen
   * table, and then deletes it only once those writes are durable in an
   * SSTable. Deleting at rotation time, which is what this used to do --
   * discarded the WAL records of any write that had landed in the *new* MemTable
   * while the flush was still running.
   */
  uint64_t rotate();

  /**
   * Delete every segment with an id at or below `segmentId`.
   *
   * Only safe once the data in those segments is durable elsewhere.
   */
  void removeSegmentsUpTo(uint64_t segmentId);

  // Force everything written so far to stable media
  void sync();

  // Get the current sequence number
  uint64_t currentSequence() const { return sequenceNumber_.load(); }

  WALSyncMode syncMode() const { return syncMode_; }

  // Number of sync calls actually issued; group commit makes this lower than
  // the append count. Exposed for tests and benchmarking.
  uint64_t syncCount() const { return syncCount_.load(); }

private:
  void append(const WALEntry &entry);
  void openNewSegment();
  void closeSegment();
  std::string currentSegmentPath() const;
  std::vector<std::string> listSegments() const;

  // Writes raw bytes to the current segment. Caller must hold mutex_.
  bool writeAll(const char *data, size_t len);

  // Ensures everything up to `target` is durable. Caller must hold `lock`;
  // the lock is released while the sync itself runs.
  void syncUpTo(std::unique_lock<std::mutex> &lock, uint64_t target);

  std::string directory_;
  int fd_ = -1;
  uint64_t segmentId_ = 0;
  std::atomic<uint64_t> sequenceNumber_{0};
  WALSyncMode syncMode_ = WALSyncMode::GROUP;

  std::mutex mutex_;
  std::condition_variable syncCond_;

  // Group commit bookkeeping, all guarded by mutex_.
  uint64_t writeSeq_ = 0;   // records written to the fd so far
  uint64_t syncedSeq_ = 0;  // records known durable
  bool syncInProgress_ = false;

  std::atomic<uint64_t> syncCount_{0};
};

} // namespace shard
