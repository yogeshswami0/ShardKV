#include "storage/StorageEngine.h"
#include "config/Config.h"
#include "storage/SSTableWriter.h"
#include "util/Logger.h"
#include <filesystem>
#include <set>

namespace fs = std::filesystem;

namespace shard {

StorageEngine::StorageEngine(const std::string &dataDir,
                             const std::string &walDir)
    : dataDir_(dataDir), walDir_(walDir) {
  fs::create_directories(dataDir);
  fs::create_directories(walDir);

  auto &config = Config::instance();

  // Initialize components
  memtable_ = std::make_shared<MemTable>(config.memtableSizeBytes());
  wal_ = std::make_unique<WAL>(walDir, parseSyncMode(config.walSyncMode()));
  compaction_ =
      std::make_unique<Compaction>(dataDir, config.levelSizeMultiplier());

  // Recover from WAL
  recover();

  // Start background compaction if enabled
  if (config.compactionEnabled()) {
    compaction_->startBackgroundCompaction();
  }

  LOG_INFO << "Storage engine initialized at " << dataDir;
}

StorageEngine::~StorageEngine() {
  // Stop background tasks
  compaction_->stopBackgroundCompaction();

  // Flush any remaining data
  try {
    flush();
  } catch (const std::exception &e) {
    LOG_ERROR << "Error during shutdown flush: " << e.what();
  }
}

void StorageEngine::put(const std::string &key, const std::string &value) {
  {
    // Shared, not exclusive. The WAL and the MemTable each have their own
    // lock, so writers need not exclude each other. Holding this exclusively
    // meant only one writer could ever be inside the WAL, leaving group commit
    // nothing to batch. The lock is here solely to exclude the flush swap, so
    // that no write is in flight across a MemTable freeze and WAL rotation.
    std::shared_lock<std::shared_mutex> lock(mutex_);

    uint64_t seqNum = ++sequenceNumber_;

    // WAL first: this returns only once the record is durable, so a value
    // never becomes visible before it is recoverable.
    wal_->appendPut(key, value, seqNum);

    // MemTable entries carry their sequence number and reject older writes,
    // so WAL file order need not match sequence order.
    memtable_->put(key, value, seqNum);
  }

  maybeFlush();
}

std::optional<std::string> StorageEngine::get(const std::string &key) {
  // Pin the memtables, then drop the lock. Holding it across SSTable I/O
  // blocked every flush for the duration of a disk read.
  std::shared_ptr<MemTable> active;
  std::shared_ptr<MemTable> immutable;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    active = memtable_;
    immutable = immutableMemtable_;
  }

  // Check active MemTable first (newest data)
  auto memResult = active->get(key);
  if (memResult.has_value()) {
    if (memResult->deleted) {
      return std::nullopt; // Tombstone
    }
    return memResult->value;
  }

  // Check immutable MemTable (being flushed)
  if (immutable) {
    auto immResult = immutable->get(key);
    if (immResult.has_value()) {
      if (immResult->deleted) {
        return std::nullopt;
      }
      return immResult->value;
    }
  }

  // Check SSTables (from newest to oldest)
  auto sstables = compaction_->getAllSSTables();
  for (const auto &sst : sstables) {
    // Use bloom filter for quick rejection
    if (!sst->mightContain(key)) {
      continue;
    }

    auto sstResult = sst->get(key);
    if (sstResult.has_value()) {
      if (sstResult->deleted) {
        return std::nullopt;
      }
      return sstResult->value;
    }
  }

  return std::nullopt;
}

void StorageEngine::remove(const std::string &key) {
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    uint64_t seqNum = ++sequenceNumber_;

    // Write tombstone to WAL
    wal_->appendDelete(key, seqNum);

    // Write tombstone to MemTable
    memtable_->remove(key, seqNum);
  }

  maybeFlush();
}

std::vector<std::pair<std::string, std::string>>
StorageEngine::scan(const std::string &startKey, const std::string &endKey) {
  std::shared_ptr<MemTable> active;
  std::shared_ptr<MemTable> immutable;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    active = memtable_;
    immutable = immutableMemtable_;
  }

  // Merge results from all sources, keeping newest version of each key
  std::map<std::string, std::pair<std::string, uint64_t>> merged;
  std::set<std::string> deleted;

  // Helper to add entries, tracking newest version
  auto addEntry = [&](const std::string &key, const std::string &value,
                      uint64_t seq, bool isDel) {
    if (key < startKey || key >= endKey)
      return;

    auto it = merged.find(key);
    if (it == merged.end() || seq > it->second.second) {
      if (isDel) {
        deleted.insert(key);
        merged.erase(key);
      } else {
        deleted.erase(key);
        merged[key] = {value, seq};
      }
    }
  };

  // Scan active MemTable. This must go through forEach(), which holds the
  // table's lock, the active table has live writers rebalancing its tree.
  active->forEach([&](const std::string &key, const MemTableEntry &entry) {
    addEntry(key, entry.value, entry.sequenceNumber, entry.deleted);
  });

  // Scan immutable MemTable
  if (immutable) {
    immutable->forEach([&](const std::string &key, const MemTableEntry &entry) {
      addEntry(key, entry.value, entry.sequenceNumber, entry.deleted);
    });
  }

  // Scan SSTables
  auto sstables = compaction_->getAllSSTables();
  for (const auto &sst : sstables) {
    auto entries = sst->scan(startKey, endKey);
    for (const auto &entry : entries) {
      addEntry(entry.key, entry.value, entry.sequenceNumber, entry.deleted);
    }
  }

  // Convert to output format
  std::vector<std::pair<std::string, std::string>> result;
  result.reserve(merged.size());

  for (const auto &[key, valueSeq] : merged) {
    result.emplace_back(key, valueSeq.first);
  }

  return result;
}

void StorageEngine::flush() {
  std::lock_guard<std::mutex> flushLock(flushMutex_);

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    if (memtable_->entryCount() == 0) {
      return; // Nothing to flush
    }

    // Swap active MemTable with a new one, and start a new WAL segment in the
    // same exclusive section. Because no writer can hold the shared lock right
    // now, the segment being closed contains exactly the writes in the frozen
    // table, nothing more, nothing less.
    memtable_->freeze();
    immutableMemtable_ = std::move(memtable_);
    memtable_ =
        std::make_shared<MemTable>(Config::instance().memtableSizeBytes());

    pendingWalSegment_ = wal_->rotate();
  }

  flushMemTable();
}

void StorageEngine::maybeFlush() {
  // This read used to be entirely unlocked while flush() reassigns memtable_
  // under a unique lock, a torn read of the pointer itself.
  std::shared_ptr<MemTable> active;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    active = memtable_;
  }

  if (active->shouldFlush()) {
    flush();
  }
}

void StorageEngine::flushMemTable() {
  // Pin the frozen table under the lock and work from the local handle. The
  // iteration below used to run against the member with no lock held.
  std::shared_ptr<MemTable> frozen;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    frozen = immutableMemtable_;
  }

  if (!frozen)
    return;

  LOG_INFO << "Flushing MemTable with " << frozen->entryCount() << " entries";

  // Take the path from the compaction manager so flushes and compactions share
  // one file-id space. They previously had separate counters writing into the
  // same directory, so their names could collide and overwrite each other.
  std::string sstPath = compaction_->nextSSTablePath(0);

  // Write SSTable. finish() fsyncs and installs it via rename, so the data is
  // durable before the WAL segment holding it is dropped below.
  SSTableWriter writer(sstPath);

  auto entries = frozen->getAllEntries();
  for (auto it = entries.begin(); it != entries.end(); ++it) {
    writer.add(it->first, it->second.value, it->second.sequenceNumber,
               it->second.deleted);
  }

  writer.finish();

  // Register with compaction manager
  auto reader = std::make_shared<SSTableReader>(sstPath);
  compaction_->addSSTable(reader, 0);

  // Only now is it safe to drop the closed segments: those exact writes live
  // in a durably installed, registered SSTable.
  wal_->removeSegmentsUpTo(pendingWalSegment_);

  // Clear immutable table
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    immutableMemtable_.reset();
  }

  LOG_INFO << "MemTable flushed to " << sstPath;
}

void StorageEngine::recover() {
  LOG_INFO << "Recovering from WAL...";

  auto entries = wal_->recover();

  for (const auto &entry : entries) {
    if (entry.type == WALEntryType::PUT) {
      memtable_->put(entry.key, entry.value, entry.sequenceNumber);
    } else if (entry.type == WALEntryType::DEL) {
      memtable_->remove(entry.key, entry.sequenceNumber);
    }

    sequenceNumber_ = std::max(sequenceNumber_.load(), entry.sequenceNumber);
  }

  LOG_INFO << "Recovered " << entries.size() << " entries, sequence at "
           << sequenceNumber_.load();
}

void StorageEngine::compact() { compaction_->runCompaction(); }

size_t StorageEngine::memtableSize() const {
  std::shared_ptr<MemTable> active;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    active = memtable_;
  }
  return active->approximateSize();
}

} // namespace shard
