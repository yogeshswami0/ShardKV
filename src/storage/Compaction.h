#pragma once

#include "storage/SSTableReader.h"
#include "util/ThreadPool.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace shard {

/**
 * Leveled compaction strategy for SSTable management.
 *
 * SSTables are organized into levels with increasing size:
 * - Level 0: Recently flushed MemTables (may have overlapping keys)
 * - Level 1+: Non-overlapping SSTables, each level 10x larger than previous
 *
 * When a level exceeds its size limit, SSTables are merged into the next level,
 * removing duplicates and tombstones in the process.
 */
class Compaction {
public:
  /**
   * @param baseLevelSize Size budget for level 1, multiplied by
   *        sizeMultiplier for each level below. Injectable so tests can drive
   *        deep compactions without writing 64 MB of data, the deeper levels
   *        are exactly where tombstone collection gets interesting.
   */
  Compaction(const std::string &dataDir, int sizeMultiplier = 10,
             size_t baseLevelSize = kDefaultBaseLevelSize);
  ~Compaction();

  // Add a new SSTable (from MemTable flush)
  void addSSTable(std::shared_ptr<SSTableReader> sst, int level = 0);

  // Get all SSTables for reading (in order from newest to oldest)
  std::vector<std::shared_ptr<SSTableReader>> getAllSSTables() const;

  // Get SSTables at a specific level
  std::vector<std::shared_ptr<SSTableReader>> getLevel(int level) const;

  // Check if compaction is needed
  bool needsCompaction() const;

  // Run compaction (can be called manually or scheduled)
  void runCompaction();

  // Start background compaction thread
  void startBackgroundCompaction();
  void stopBackgroundCompaction();

  // Allocate the next SSTable path at a level. Public so MemTable flushes draw
  // from the same file-id space as compaction output.
  std::string nextSSTablePath(int level);

  // Statistics
  size_t totalSSTables() const;
  size_t levelCount() const { return levels_.size(); }

private:
  // Compact a range within a level into the next level
  void compactLevel(int level);

  // Merge multiple SSTables into one. `dropTombstones` may only be set when
  // the target is the deepest level, see the comment at the call site.
  // Returns the output path, or "" if the merge produced no entries.
  std::string
  mergeSSTablesFiles(const std::vector<std::shared_ptr<SSTableReader>> &inputs,
                     int targetLevel, bool dropTombstones);

  // Keep a level in its canonical order. Caller must hold mutex_.
  void sortLevelLocked(int level);

  // Drop exactly these tables from a level, by identity. Caller holds mutex_.
  void removeFromLevelLocked(
      int level, const std::vector<std::shared_ptr<SSTableReader>> &victims);

  // Calculate max size for a level (in bytes)
  size_t maxLevelSize(int level) const;

  std::string dataDir_;
  int sizeMultiplier_;

  // SSTables organized by level
  mutable std::mutex mutex_;
  std::vector<std::vector<std::shared_ptr<SSTableReader>>> levels_;

  // File naming
  std::atomic<uint64_t> nextFileId_{0};

  // Background compaction
  std::unique_ptr<ThreadPool> compactionPool_;
  std::atomic<bool> running_{false};

  // Only one compaction may run at a time. The background loop and a manual
  // compact() call could otherwise overlap and merge the same inputs twice.
  std::atomic<bool> compactionInProgress_{false};

  // Level 0 size threshold (before triggering compaction)
  static constexpr size_t kLevel0Threshold = 4;

public:
  static constexpr size_t kDefaultBaseLevelSize = 64 * 1024 * 1024; // 64 MB

private:
  size_t baseLevelSize_ = kDefaultBaseLevelSize;
};

} // namespace shard
