#pragma once

#include "storage/Compaction.h"
#include "storage/MemTable.h"
#include "storage/SSTableReader.h"
#include "storage/WAL.h"
#include <atomic>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

namespace shard {

/**
 * Facade for the LSM-Tree storage engine.
 *
 * Coordinates between MemTable, WAL, SSTables, and Compaction to provide
 * a unified key-value interface with ACID durability guarantees.
 *
 * Write path: WAL -> MemTable -> (flush) -> SSTable
 * Read path:  MemTable -> Immutable MemTable -> SSTables (level 0 to N)
 */
class StorageEngine {
public:
  StorageEngine(const std::string &dataDir, const std::string &walDir);
  ~StorageEngine();

  // Core operations
  void put(const std::string &key, const std::string &value);
  std::optional<std::string> get(const std::string &key);
  void remove(const std::string &key);

  // Range scan (returns all non-deleted entries in [startKey, endKey))
  std::vector<std::pair<std::string, std::string>>
  scan(const std::string &startKey, const std::string &endKey);

  // Flush MemTable to disk (normally automatic, exposed for testing/shutdown)
  void flush();

  // Recovery from WAL on startup
  void recover();

  // Force a compaction cycle
  void compact();

  // Statistics
  uint64_t currentSequence() const { return sequenceNumber_; }
  size_t memtableSize() const;

private:
  void maybeFlush();
  void flushMemTable();

  std::string dataDir_;
  std::string walDir_;

  // In-memory structures.
  // shared_ptr rather than unique_ptr so a reader can pin the table under the
  // lock, release the lock, and then do disk I/O without blocking flushes.
  std::shared_ptr<MemTable> memtable_;
  std::shared_ptr<MemTable> immutableMemtable_; // Being flushed
  std::unique_ptr<WAL> wal_;

  // On-disk structures
  std::unique_ptr<Compaction> compaction_;

  // Synchronization
  mutable std::shared_mutex mutex_;
  std::mutex flushMutex_;

  // Sequence number for MVCC ordering
  std::atomic<uint64_t> sequenceNumber_{0};

  // WAL segment closed by the in-progress flush, deletable once its data is
  // durable in an SSTable. Guarded by flushMutex_.
  uint64_t pendingWalSegment_ = 0;
};

} // namespace shard
