#include "storage/Compaction.h"
#include "storage/SSTableWriter.h"
#include "util/Logger.h"
#include <algorithm>
#include <filesystem>
#include <queue>
#include <thread>
#include <unordered_set>

namespace fs = std::filesystem;

namespace shard {

Compaction::Compaction(const std::string &dataDir, int sizeMultiplier,
                       size_t baseLevelSize)
    : dataDir_(dataDir), sizeMultiplier_(sizeMultiplier),
      baseLevelSize_(baseLevelSize) {
  fs::create_directories(dataDir);

  // Ensure at least one level exists
  levels_.resize(1);

  // Collect paths first and sort them. directory_iterator order is
  // unspecified, and level 0 ordering decides which version of a key wins.
  std::vector<std::string> paths;
  for (const auto &entry : fs::directory_iterator(dataDir)) {
    const auto &path = entry.path();

    // Leftover temp files are the remains of a crash mid-write. They were
    // never installed, so nothing references them.
    if (path.extension() == ".tmp") {
      LOG_INFO << "Removing abandoned SSTable temp file: " << path;
      std::error_code ec;
      fs::remove(path, ec);
      continue;
    }

    if (path.extension() == ".sst") {
      paths.push_back(path.string());
    }
  }
  std::sort(paths.begin(), paths.end());

  for (const auto &path : paths) {
    std::string filename = fs::path(path).stem().string();

    // Parse level from filename (format: level_id.sst)
    size_t underscore = filename.find('_');
    int level = 0;
    uint64_t fileId = 0;

    try {
      if (underscore != std::string::npos) {
        level = std::stoi(filename.substr(0, underscore));
        fileId = std::stoull(filename.substr(underscore + 1));
      } else {
        fileId = std::stoull(filename);
      }
    } catch (const std::exception &e) {
      LOG_WARN << "Skipping SSTable with unparseable name: " << path;
      continue;
    }

    if (level < 0) {
      LOG_WARN << "Skipping SSTable with negative level: " << path;
      continue;
    }

    // Track highest file ID
    nextFileId_ = std::max(nextFileId_.load(), fileId + 1);

    // Ensure level vector is large enough
    while (levels_.size() <= static_cast<size_t>(level)) {
      levels_.push_back({});
    }

    try {
      auto reader = std::make_shared<SSTableReader>(path);
      levels_[level].push_back(reader);
    } catch (const std::exception &e) {
      LOG_WARN << "Failed to load SSTable " << path << ": " << e.what();
    }
  }

  for (size_t i = 0; i < levels_.size(); ++i) {
    sortLevelLocked(static_cast<int>(i));
  }

  LOG_INFO << "Compaction initialized with " << totalSSTables()
           << " existing SSTables";
}

Compaction::~Compaction() { stopBackgroundCompaction(); }

void Compaction::sortLevelLocked(int level) {
  if (static_cast<size_t>(level) >= levels_.size()) {
    return;
  }

  auto &tables = levels_[level];

  if (level == 0) {
    // Level 0 tables have overlapping key ranges, so a lookup must consult
    // them newest-first and stop at the first hit. maxSequence is the true
    // recency order and, unlike insertion order, survives a restart.
    std::sort(tables.begin(), tables.end(),
              [](const std::shared_ptr<SSTableReader> &a,
                 const std::shared_ptr<SSTableReader> &b) {
                if (a->maxSequence() != b->maxSequence()) {
                  return a->maxSequence() > b->maxSequence();
                }
                return a->path() > b->path();
              });
  } else {
    // Deeper levels hold non-overlapping ranges, so key order is enough.
    std::sort(tables.begin(), tables.end(),
              [](const std::shared_ptr<SSTableReader> &a,
                 const std::shared_ptr<SSTableReader> &b) {
                if (a->firstKey() != b->firstKey()) {
                  return a->firstKey() < b->firstKey();
                }
                return a->path() < b->path();
              });
  }
}

void Compaction::removeFromLevelLocked(
    int level, const std::vector<std::shared_ptr<SSTableReader>> &victims) {
  if (static_cast<size_t>(level) >= levels_.size()) {
    return;
  }

  std::unordered_set<const SSTableReader *> victimSet;
  victimSet.reserve(victims.size());
  for (const auto &v : victims) {
    victimSet.insert(v.get());
  }

  auto &tables = levels_[level];
  tables.erase(std::remove_if(tables.begin(), tables.end(),
                              [&](const std::shared_ptr<SSTableReader> &t) {
                                return victimSet.count(t.get()) > 0;
                              }),
               tables.end());
}

void Compaction::addSSTable(std::shared_ptr<SSTableReader> sst, int level) {
  std::lock_guard<std::mutex> lock(mutex_);

  while (levels_.size() <= static_cast<size_t>(level)) {
    levels_.push_back({});
  }

  levels_[level].push_back(sst);
  sortLevelLocked(level);

  LOG_DEBUG << "Added SSTable to level " << level
            << ", total: " << levels_[level].size();
}

std::vector<std::shared_ptr<SSTableReader>> Compaction::getAllSSTables() const {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<std::shared_ptr<SSTableReader>> result;

  // Newest data first: level 0 (already ordered newest-first) then deeper
  // levels, which hold progressively older data. Callers take the first hit.
  for (const auto &level : levels_) {
    for (const auto &sst : level) {
      result.push_back(sst);
    }
  }

  return result;
}

std::vector<std::shared_ptr<SSTableReader>>
Compaction::getLevel(int level) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (level < 0 || static_cast<size_t>(level) >= levels_.size()) {
    return {};
  }

  return levels_[level];
}

bool Compaction::needsCompaction() const {
  std::lock_guard<std::mutex> lock(mutex_);

  // Check level 0 count threshold
  if (!levels_.empty() && levels_[0].size() >= kLevel0Threshold) {
    return true;
  }

  // Check size thresholds for other levels
  for (size_t i = 1; i < levels_.size(); ++i) {
    size_t levelSize = 0;
    for (const auto &sst : levels_[i]) {
      // Size is cached in the reader at open time. It used to come from
      // fs::file_size() with no error_code, which throws if the file has just
      // been unlinked, inside the background thread, killing compaction for
      // the lifetime of the process.
      levelSize += sst->fileSize();
    }

    if (levelSize > maxLevelSize(static_cast<int>(i))) {
      return true;
    }
  }

  return false;
}

void Compaction::runCompaction() {
  // Only one compaction at a time; the background loop and a manual compact()
  // could otherwise pick the same inputs and merge them twice.
  bool expected = false;
  if (!compactionInProgress_.compare_exchange_strong(expected, true)) {
    LOG_DEBUG << "Compaction already in progress, skipping";
    return;
  }

  struct Guard {
    std::atomic<bool> &flag;
    ~Guard() { flag.store(false); }
  } guard{compactionInProgress_};

  // Find which level needs compaction
  int levelToCompact = -1;

  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!levels_.empty() && levels_[0].size() >= kLevel0Threshold) {
      levelToCompact = 0;
    } else {
      for (size_t i = 1; i < levels_.size(); ++i) {
        size_t levelSize = 0;
        for (const auto &sst : levels_[i]) {
          levelSize += sst->fileSize();
        }

        if (levelSize > maxLevelSize(static_cast<int>(i))) {
          levelToCompact = static_cast<int>(i);
          break;
        }
      }
    }
  }

  if (levelToCompact >= 0) {
    compactLevel(levelToCompact);
  }
}

void Compaction::compactLevel(int level) {
  LOG_INFO << "Starting compaction of level " << level;

  std::vector<std::shared_ptr<SSTableReader>> inputs;
  std::vector<std::shared_ptr<SSTableReader>> targetLevelInputs;
  int targetLevel = level + 1;
  bool dropTombstones = false;

  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (static_cast<size_t>(level) >= levels_.size() ||
        levels_[level].empty()) {
      return;
    }

    // For level 0, compact all SSTables
    // For higher levels, pick an SSTable and merge with overlapping ones in
    // next level
    inputs = levels_[level];

    if (static_cast<size_t>(targetLevel) < levels_.size()) {
      targetLevelInputs = levels_[targetLevel];
    }

    // A tombstone may only be discarded once no deeper level can still hold an
    // older version of that key, otherwise dropping it resurrects the value.
    // The old rule (targetLevel < 2) dropped tombstones at level 2 regardless
    // of what lived below.
    dropTombstones = true;
    for (size_t i = targetLevel + 1; i < levels_.size(); ++i) {
      if (!levels_[i].empty()) {
        dropTombstones = false;
        break;
      }
    }
  }

  // Merge all inputs
  std::vector<std::shared_ptr<SSTableReader>> allInputs;
  allInputs.insert(allInputs.end(), inputs.begin(), inputs.end());
  allInputs.insert(allInputs.end(), targetLevelInputs.begin(),
                   targetLevelInputs.end());

  if (allInputs.empty())
    return;

  // Create merged SSTable. This runs without the lock held, so flushes can
  // continue to add tables to level 0 while it works.
  std::string outputPath =
      mergeSSTablesFiles(allInputs, targetLevel, dropTombstones);

  // Update level structure
  {
    std::lock_guard<std::mutex> lock(mutex_);

    while (levels_.size() <= static_cast<size_t>(targetLevel)) {
      levels_.push_back({});
    }

    // Remove exactly the tables we merged, by identity. This used to clear()
    // the whole level, which silently discarded any SSTable a concurrent flush
    // had added during the merge. That is straight data loss.
    removeFromLevelLocked(level, inputs);
    removeFromLevelLocked(targetLevel, targetLevelInputs);

    if (!outputPath.empty()) {
      try {
        auto newReader = std::make_shared<SSTableReader>(outputPath);
        levels_[targetLevel].push_back(newReader);
        sortLevelLocked(targetLevel);
      } catch (const std::exception &e) {
        // The inputs are still registered at this point only if we bail before
        // removing them, so put them back rather than losing them.
        LOG_ERROR << "Failed to open compacted SSTable: " << e.what();
        for (const auto &sst : inputs) {
          levels_[level].push_back(sst);
        }
        for (const auto &sst : targetLevelInputs) {
          levels_[targetLevel].push_back(sst);
        }
        sortLevelLocked(level);
        sortLevelLocked(targetLevel);
        return;
      }
    }
  }

  // Delete old SSTables. Readers that already hold one keep their descriptor
  // open, so an in-flight read is unaffected by the unlink.
  for (const auto &sst : allInputs) {
    std::error_code ec;
    fs::remove(sst->path(), ec);
    if (ec) {
      LOG_WARN << "Failed to delete old SSTable: " << sst->path();
    }
  }

  LOG_INFO << "Compaction of level " << level << " complete";
}

std::string Compaction::mergeSSTablesFiles(
    const std::vector<std::shared_ptr<SSTableReader>> &inputs, int targetLevel,
    bool dropTombstones) {
  if (inputs.empty())
    return "";

  std::string outputPath = nextSSTablePath(targetLevel);
  SSTableWriter writer(outputPath);

  // Streaming k-way merge. The previous version read every entry of every
  // input fully into memory first, so peak usage was the size of the whole
  // compaction, which is unbounded for an LSM. Iterators hold one block each.
  std::vector<SSTableReader::Iterator> iters;
  iters.reserve(inputs.size());
  for (const auto &sst : inputs) {
    iters.push_back(sst->begin());
  }

  struct HeapEntry {
    std::string key;
    uint64_t sequenceNumber;
    size_t sourceIdx;

    bool operator>(const HeapEntry &other) const {
      if (key != other.key) {
        return key > other.key;
      }
      // Same key: the newest version must pop first so the dedup below keeps
      // it and discards the rest.
      return sequenceNumber < other.sequenceNumber;
    }
  };

  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>>
      heap;

  auto pushFrom = [&](size_t idx) {
    if (iters[idx].valid()) {
      auto e = iters[idx].entry();
      heap.push(HeapEntry{e.key, e.sequenceNumber, idx});
    }
  };

  for (size_t i = 0; i < iters.size(); ++i) {
    pushFrom(i);
  }

  bool haveLastKey = false;
  std::string lastKey;
  uint64_t entriesWritten = 0;

  while (!heap.empty()) {
    HeapEntry top = heap.top();
    heap.pop();

    SSTableEntry entry = iters[top.sourceIdx].entry();

    // Only the first occurrence of a key survives, and the heap ordering
    // guarantees that is the newest version.
    if (!haveLastKey || entry.key != lastKey) {
      if (!entry.deleted || !dropTombstones) {
        writer.add(entry.key, entry.value, entry.sequenceNumber, entry.deleted);
        entriesWritten++;
      }
      lastKey = entry.key;
      haveLastKey = true;
    }

    iters[top.sourceIdx].next();
    pushFrom(top.sourceIdx);
  }

  if (entriesWritten == 0) {
    // Everything merged away (all tombstones at the deepest level). Installing
    // an empty SSTable would achieve nothing.
    writer.abandon();
    LOG_INFO << "Compaction produced no entries; no SSTable written";
    return "";
  }

  writer.finish();
  return outputPath;
}

std::string Compaction::nextSSTablePath(int level) {
  uint64_t fileId = nextFileId_.fetch_add(1);
  char filename[64];
  snprintf(filename, sizeof(filename), "%d_%08llu.sst", level,
           static_cast<unsigned long long>(fileId));
  return dataDir_ + "/" + filename;
}

size_t Compaction::maxLevelSize(int level) const {
  if (level == 0)
    return 0; // Level 0 uses count threshold, not size

  size_t size = baseLevelSize_;
  for (int i = 1; i < level; ++i) {
    size *= sizeMultiplier_;
  }
  return size;
}

size_t Compaction::totalSSTables() const {
  std::lock_guard<std::mutex> lock(mutex_);

  size_t count = 0;
  for (const auto &level : levels_) {
    count += level.size();
  }
  return count;
}

void Compaction::startBackgroundCompaction() {
  if (running_.exchange(true))
    return; // Already running

  compactionPool_ = std::make_unique<ThreadPool>(1);

  compactionPool_->submit([this]() {
    while (running_) {
      // An escaping exception here used to kill the compaction thread for the
      // remaining life of the process, silently.
      try {
        if (needsCompaction()) {
          runCompaction();
        }
      } catch (const std::exception &e) {
        LOG_ERROR << "Compaction cycle failed: " << e.what();
      } catch (...) {
        LOG_ERROR << "Compaction cycle failed with unknown exception";
      }

      // Wake often enough to notice a shutdown promptly.
      for (int i = 0; i < 10 && running_; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  });

  LOG_INFO << "Background compaction started";
}

void Compaction::stopBackgroundCompaction() {
  running_ = false;
  if (compactionPool_) {
    compactionPool_->shutdown();
    compactionPool_.reset();
  }
}

} // namespace shard
