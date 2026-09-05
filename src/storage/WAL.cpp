#include "storage/WAL.h"
#include "util/Crc32.h"
#include "util/Durability.h"
#include "util/Logger.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include "util/Platform.h"

namespace fs = std::filesystem;

namespace shard {

WALSyncMode parseSyncMode(const std::string &name) {
  if (name == "always" || name == "ALWAYS")
    return WALSyncMode::ALWAYS;
  if (name == "none" || name == "NONE")
    return WALSyncMode::NONE;
  return WALSyncMode::GROUP;
}

const char *syncModeName(WALSyncMode mode) {
  switch (mode) {
  case WALSyncMode::ALWAYS:
    return "always";
  case WALSyncMode::NONE:
    return "none";
  case WALSyncMode::GROUP:
  default:
    return "group";
  }
}

std::string WALEntry::serialize() const {
  std::string result;

  // Type (1 byte)
  result.push_back(static_cast<char>(type));

  // Sequence number (8 bytes, big-endian)
  for (int i = 7; i >= 0; --i) {
    result.push_back(static_cast<char>((sequenceNumber >> (i * 8)) & 0xFF));
  }

  // Key length (4 bytes) + key
  uint32_t keyLen = static_cast<uint32_t>(key.size());
  for (int i = 3; i >= 0; --i) {
    result.push_back(static_cast<char>((keyLen >> (i * 8)) & 0xFF));
  }
  result.append(key);

  // Value length (4 bytes) + value
  uint32_t valLen = static_cast<uint32_t>(value.size());
  for (int i = 3; i >= 0; --i) {
    result.push_back(static_cast<char>((valLen >> (i * 8)) & 0xFF));
  }
  result.append(value);

  return result;
}

bool WALEntry::deserialize(const std::string &data, WALEntry &entry) {
  if (data.size() < 17)
    return false; // Minimum: 1 + 8 + 4 + 4

  size_t offset = 0;

  // Type
  uint8_t rawType = static_cast<uint8_t>(data[offset++]);
  if (rawType != static_cast<uint8_t>(WALEntryType::PUT) &&
      rawType != static_cast<uint8_t>(WALEntryType::DEL)) {
    return false;
  }
  entry.type = static_cast<WALEntryType>(rawType);

  // Sequence number
  entry.sequenceNumber = 0;
  for (int i = 0; i < 8; ++i) {
    entry.sequenceNumber =
        (entry.sequenceNumber << 8) | static_cast<uint8_t>(data[offset++]);
  }

  // Key
  if (offset + 4 > data.size())
    return false;
  uint32_t keyLen = 0;
  for (int i = 0; i < 4; ++i) {
    keyLen = (keyLen << 8) | static_cast<uint8_t>(data[offset++]);
  }
  if (offset + keyLen > data.size())
    return false;
  entry.key = data.substr(offset, keyLen);
  offset += keyLen;

  // Value
  if (offset + 4 > data.size())
    return false;
  uint32_t valLen = 0;
  for (int i = 0; i < 4; ++i) {
    valLen = (valLen << 8) | static_cast<uint8_t>(data[offset++]);
  }
  if (offset + valLen > data.size())
    return false;
  entry.value = data.substr(offset, valLen);

  return true;
}

WAL::WAL(const std::string &directory) : WAL(directory, WALSyncMode::GROUP) {}

WAL::WAL(const std::string &directory, WALSyncMode syncMode)
    : directory_(directory), syncMode_(syncMode) {
  // Create directory if it doesn't exist
  fs::create_directories(directory);

  // Find the latest segment
  auto segments = listSegments();
  if (!segments.empty()) {
    std::sort(segments.begin(), segments.end());
    // Extract the segment ID from the last file
    std::string last = segments.back();
    std::string filename = fs::path(last).filename().string();
    segmentId_ = std::stoull(filename.substr(0, filename.find('.')));
  }

  openNewSegment();
  LOG_INFO << "WAL initialized at " << directory << ", segment " << segmentId_
           << ", sync mode " << syncModeName(syncMode_);
}

WAL::~WAL() { closeSegment(); }

void WAL::appendPut(const std::string &key, const std::string &value,
                    uint64_t seqNum) {
  WALEntry entry;
  entry.type = WALEntryType::PUT;
  entry.sequenceNumber = seqNum;
  entry.key = key;
  entry.value = value;
  append(entry);
}

void WAL::appendDelete(const std::string &key, uint64_t seqNum) {
  WALEntry entry;
  entry.type = WALEntryType::DEL;
  entry.sequenceNumber = seqNum;
  entry.key = key;
  append(entry);
}

bool WAL::writeAll(const char *data, size_t len) {
  size_t written = 0;
  while (written < len) {
    ssize_t n = ::write(fd_, data + written, len - written);
    if (n < 0) {
      if (errno == EINTR) {
        continue; // interrupted before writing anything, retry
      }
      LOG_ERROR << "WAL write failed: " << strerror(errno);
      return false;
    }
    written += static_cast<size_t>(n);
  }
  return true;
}

void WAL::syncUpTo(std::unique_lock<std::mutex> &lock, uint64_t target) {
  if (syncMode_ == WALSyncMode::NONE) {
    return;
  }

  if (syncMode_ == WALSyncMode::ALWAYS) {
    // Sync without releasing the lock, so writers cannot coalesce. This is the
    // deliberately unbatched baseline. It exists to show what durability
    // costs per write, which is only meaningful if it really is one sync per
    // append.
    if (syncedSeq_ >= target) {
      return;
    }
    uint64_t batchTarget = writeSeq_;
    if (!durableSync(fd_)) {
      LOG_ERROR << "WAL sync failed; acknowledged writes are NOT durable";
      throw std::runtime_error("WAL sync failed");
    }
    syncCount_.fetch_add(1, std::memory_order_relaxed);
    syncedSeq_ = std::max(syncedSeq_, batchTarget);
    return;
  }

  while (syncedSeq_ < target) {
    if (syncInProgress_) {
      // Another writer is syncing. Its sync covers every byte written before
      // it started, which may already include our record. Wait and re-check.
      syncCond_.wait(lock);
      continue;
    }

    // Become the syncer on behalf of every record written so far, not just our
    // own. That batching is the whole point of group commit.
    uint64_t batchTarget = writeSeq_;
    syncInProgress_ = true;

    int fd = fd_;
    lock.unlock();
    bool ok = durableSync(fd);
    lock.lock();

    syncInProgress_ = false;

    if (ok) {
      syncCount_.fetch_add(1, std::memory_order_relaxed);
      syncedSeq_ = std::max(syncedSeq_, batchTarget);
    } else {
      LOG_ERROR << "WAL sync failed; acknowledged writes are NOT durable";
      // Wake the waiters so they can observe the failure rather than hang.
      syncCond_.notify_all();
      throw std::runtime_error("WAL sync failed");
    }

    syncCond_.notify_all();
  }
}

void WAL::append(const WALEntry &entry) {
  std::unique_lock<std::mutex> lock(mutex_);

  std::string data = entry.serialize();
  uint32_t crc = Crc32::compute(data);
  uint32_t len = static_cast<uint32_t>(data.size());

  // Write: [length][crc][data]
  char header[8];
  for (int i = 3; i >= 0; --i) {
    header[3 - i] = static_cast<char>((len >> (i * 8)) & 0xFF);
  }
  for (int i = 3; i >= 0; --i) {
    header[7 - i] = static_cast<char>((crc >> (i * 8)) & 0xFF);
  }

  // Single write so a record cannot be interleaved with another writer's.
  std::string record;
  record.reserve(8 + data.size());
  record.append(header, 8);
  record.append(data);

  if (!writeAll(record.data(), record.size())) {
    throw std::runtime_error("WAL append failed");
  }

  uint64_t mySeq = ++writeSeq_;

  uint64_t prev = sequenceNumber_.load(std::memory_order_relaxed);
  while (entry.sequenceNumber > prev &&
         !sequenceNumber_.compare_exchange_weak(prev, entry.sequenceNumber)) {
  }

  // Do not return until this record is on stable media.
  syncUpTo(lock, mySeq);
}

std::vector<WALEntry> WAL::recover() {
  std::vector<WALEntry> entries;

  auto segments = listSegments();
  std::sort(segments.begin(), segments.end());

  for (const auto &segmentPath : segments) {
    std::ifstream file(segmentPath, std::ios::binary);
    if (!file.is_open()) {
      LOG_WARN << "Could not open WAL segment: " << segmentPath;
      continue;
    }

    while (file.good() && !file.eof()) {
      // Read header
      char header[8];
      file.read(header, 8);
      if (file.gcount() != 8)
        break;

      uint32_t len = 0;
      for (int i = 0; i < 4; ++i) {
        len = (len << 8) | static_cast<uint8_t>(header[i]);
      }

      uint32_t expectedCrc = 0;
      for (int i = 0; i < 4; ++i) {
        expectedCrc = (expectedCrc << 8) | static_cast<uint8_t>(header[4 + i]);
      }

      // Read data
      std::string data(len, '\0');
      file.read(&data[0], len);
      if (static_cast<uint32_t>(file.gcount()) != len) {
        LOG_WARN << "Truncated WAL entry in " << segmentPath;
        break;
      }

      // Verify CRC
      uint32_t actualCrc = Crc32::compute(data);
      if (actualCrc != expectedCrc) {
        LOG_ERROR << "CRC mismatch in WAL entry, skipping segment";
        break;
      }

      // Deserialize entry
      WALEntry entry;
      if (WALEntry::deserialize(data, entry)) {
        entries.push_back(entry);
        uint64_t prev = sequenceNumber_.load(std::memory_order_relaxed);
        while (entry.sequenceNumber > prev &&
               !sequenceNumber_.compare_exchange_weak(prev,
                                                      entry.sequenceNumber)) {
        }
      } else {
        LOG_WARN << "Malformed WAL entry in " << segmentPath << ", stopping";
        break;
      }
    }
  }

  LOG_INFO << "Recovered " << entries.size() << " entries from WAL";
  return entries;
}

uint64_t WAL::rotate() {
  std::unique_lock<std::mutex> lock(mutex_);

  uint64_t closedSegment = segmentId_;

  // Everything written to the outgoing segment must be durable before we stop
  // writing to it.
  syncUpTo(lock, writeSeq_);

  closeSegment();

  segmentId_++;
  openNewSegment();

  LOG_DEBUG << "WAL rotated to segment " << segmentId_;
  return closedSegment;
}

void WAL::removeSegmentsUpTo(uint64_t segmentId) {
  std::lock_guard<std::mutex> lock(mutex_);

  bool removedAny = false;

  for (const auto &path : listSegments()) {
    std::string filename = fs::path(path).filename().string();
    uint64_t id = 0;
    try {
      id = std::stoull(filename.substr(0, filename.find('.')));
    } catch (const std::exception &) {
      continue;
    }

    // Never remove the segment currently being written.
    if (id > segmentId || id == segmentId_) {
      continue;
    }

    std::error_code ec;
    fs::remove(path, ec);
    if (ec) {
      LOG_WARN << "Failed to remove WAL segment " << path << ": "
               << ec.message();
    } else {
      removedAny = true;
      LOG_DEBUG << "Removed obsolete WAL segment " << path;
    }
  }

  if (removedAny) {
    // The unlinks are directory mutations.
    syncDirectory(directory_);
  }
}

void WAL::sync() {
  std::unique_lock<std::mutex> lock(mutex_);
  syncUpTo(lock, writeSeq_);
}

void WAL::openNewSegment() {
  std::string path = currentSegmentPath();

  fd_ = ::open(path.c_str(), O_WRONLY | O_BINARY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("Failed to open WAL segment: " + path + ": " +
                             strerror(errno));
  }

  // A newly created file's directory entry is not durable until the parent
  // directory is synced, otherwise a crash can lose the file entirely even
  // though its contents were written and synced.
  syncDirectory(directory_);

  // A fresh segment has nothing outstanding.
  syncedSeq_ = writeSeq_;
}

void WAL::closeSegment() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

std::string WAL::currentSegmentPath() const {
  char filename[32];
  snprintf(filename, sizeof(filename), "%08llu.wal",
           static_cast<unsigned long long>(segmentId_));
  return directory_ + "/" + filename;
}

std::vector<std::string> WAL::listSegments() const {
  std::vector<std::string> segments;

  for (const auto &entry : fs::directory_iterator(directory_)) {
    if (entry.path().extension() == ".wal") {
      segments.push_back(entry.path().string());
    }
  }

  return segments;
}

} // namespace shard
