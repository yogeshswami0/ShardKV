#include "raft/RaftLog.h"
#include "util/Crc32.h"
#include "util/Durability.h"
#include "util/Logger.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include "util/Platform.h"

namespace {

constexpr uint32_t kLogMagic = 0x5658524C; // "VXRL"
constexpr uint32_t kLogVersion = 1;
constexpr size_t kLogHeaderSize = 24; // magic + version + snapIdx + snapTerm

void put32(std::string &out, uint32_t v) {
  for (int i = 3; i >= 0; --i) {
    out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

void put64(std::string &out, uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

uint32_t get32(const char *p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  }
  return v;
}

uint64_t get64(const char *p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  }
  return v;
}

bool writeAllTo(int fd, const char *data, size_t len) {
  size_t written = 0;
  while (written < len) {
    ssize_t n = ::write(fd, data + written, len - written);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    written += static_cast<size_t>(n);
  }
  return true;
}

// A command length is read straight off disk, so it must be bounded before it
// is used to size an allocation.
constexpr uint64_t kMaxCommandSize = 64ull * 1024 * 1024;

} // namespace

namespace shard {

RaftLog::RaftLog() {}

RaftLog::~RaftLog() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

std::string RaftLog::encodeRecord(const RaftLogEntry &entry) {
  std::string payload;
  put64(payload, entry.term);
  put64(payload, entry.index);
  put64(payload, static_cast<uint64_t>(entry.command.size()));
  payload.append(entry.command);

  std::string record;
  put32(record, static_cast<uint32_t>(payload.size()));
  put32(record, Crc32::compute(payload));
  record.append(payload);
  return record;
}

bool RaftLog::appendToFileLocked(const RaftLogEntry &entry) {
  if (fd_ < 0) {
    return true; // not bound to a file (unit tests, in-memory use)
  }

  std::string record = encodeRecord(entry);
  if (!writeAllTo(fd_, record.data(), record.size())) {
    LOG_ERROR << "Failed to append Raft log record: " << strerror(errno);
    return false;
  }

  // Raft may not acknowledge an entry that is not on stable media.
  if (!durableSync(fd_)) {
    LOG_ERROR << "Failed to sync Raft log";
    return false;
  }

  durableCount_ = entries_.size();
  return true;
}

bool RaftLog::rewriteFileLocked() {
  if (fd_ < 0) {
    return true;
  }

  // Rewrite via a temp file and rename, so a crash mid-rewrite leaves the
  // previous log intact rather than a half-written one.
  std::string tmpPath = path_ + ".tmp";
  int tmp = ::open(tmpPath.c_str(), O_WRONLY | O_BINARY | O_CREAT | O_TRUNC, 0644);
  if (tmp < 0) {
    LOG_ERROR << "Failed to open temp Raft log: " << tmpPath;
    return false;
  }

  std::string buf;
  put32(buf, kLogMagic);
  put32(buf, kLogVersion);
  put64(buf, snapshotLastIndex_);
  put64(buf, snapshotLastTerm_);

  for (const auto &entry : entries_) {
    buf.append(encodeRecord(entry));
  }

  bool ok = writeAllTo(tmp, buf.data(), buf.size());
  ::close(tmp);
  
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }

  if (!ok || !atomicInstall(tmpPath, path_)) {
    LOG_ERROR << "Failed to rewrite Raft log at " << path_;
    return false;
  }

  // Re-open the installed file for subsequent appends
  fd_ = ::open(path_.c_str(), O_WRONLY | O_BINARY | O_APPEND);
  if (fd_ < 0) {
    LOG_ERROR << "Failed to reopen Raft log after rewrite: " << path_;
    return false;
  }

  durableCount_ = entries_.size();
  return true;
}

void RaftLog::openAt(const std::string &path) {
  loadFrom(path);

  std::lock_guard<std::mutex> lock(mutex_);
  path_ = path;

  if (fd_ >= 0) {
    ::close(fd_);
  }

  fd_ = ::open(path.c_str(), O_WRONLY | O_BINARY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("Failed to open Raft log: " + path + ": " +
                             strerror(errno));
  }

  // A brand new file needs its header before any record is appended.
  off_t size = ::lseek(fd_, 0, SEEK_END);
  if (size == 0) {
    std::string header;
    put32(header, kLogMagic);
    put32(header, kLogVersion);
    put64(header, snapshotLastIndex_);
    put64(header, snapshotLastTerm_);
    if (!writeAllTo(fd_, header.data(), header.size()) || !durableSync(fd_)) {
      throw std::runtime_error("Failed to write Raft log header: " + path);
    }
    syncDirectory(parentDirectory(path));
  }

  durableCount_ = entries_.size();
}

bool RaftLog::isDurable() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return fd_ < 0 || durableCount_ >= entries_.size();
}

uint64_t RaftLog::append(uint64_t term, const std::string &command) {
  std::lock_guard<std::mutex> lock(mutex_);

  uint64_t newIndex = snapshotLastIndex_ + entries_.size() + 1;
  entries_.emplace_back(term, newIndex, command);

  // Durable before the caller can act on the index. Raft's safety argument
  // assumes an appended entry survives a crash.
  if (!appendToFileLocked(entries_.back())) {
    entries_.pop_back();
    throw std::runtime_error("Failed to persist Raft log entry");
  }

  return newIndex;
}

void RaftLog::appendEntries(const std::vector<RaftLogEntry> &entries) {
  std::lock_guard<std::mutex> lock(mutex_);

  bool truncated = false;
  bool appended = false;

  for (const auto &entry : entries) {
    // Calculate position in our local vector
    if (entry.index <= snapshotLastIndex_) {
      continue; // Already in snapshot
    }

    size_t localIdx = entry.index - snapshotLastIndex_ - 1;

    if (localIdx < entries_.size()) {
      // Check for conflict
      if (entries_[localIdx].term != entry.term) {
        // Conflict - truncate from here
        entries_.resize(localIdx);
        entries_.push_back(entry);
        truncated = true;
      }
      // If terms match, entry is already there, skip
    } else if (localIdx == entries_.size()) {
      // Append new entry
      entries_.push_back(entry);
      appended = true;
    }
    // If localIdx > size, there's a gap - shouldn't happen in correct Raft
  }

  // A truncation cannot be expressed as an append, so the file is rewritten.
  // That path is rare; the common case is a pure append.
  if (truncated) {
    if (!rewriteFileLocked()) {
      throw std::runtime_error("Failed to persist truncated Raft log");
    }
  } else if (appended) {
    if (fd_ >= 0) {
      std::string buf;
      for (size_t i = durableCount_; i < entries_.size(); ++i) {
        buf.append(encodeRecord(entries_[i]));
      }
      if (!buf.empty() &&
          (!writeAllTo(fd_, buf.data(), buf.size()) || !durableSync(fd_))) {
        throw std::runtime_error("Failed to persist replicated Raft entries");
      }
      durableCount_ = entries_.size();
    }
  }
}

std::optional<RaftLogEntry> RaftLog::getEntry(uint64_t index) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (index <= snapshotLastIndex_ ||
      index > snapshotLastIndex_ + entries_.size()) {
    return std::nullopt;
  }

  size_t localIdx = index - snapshotLastIndex_ - 1;
  return entries_[localIdx];
}

std::vector<RaftLogEntry> RaftLog::getEntriesFrom(uint64_t startIndex) const {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<RaftLogEntry> result;

  if (startIndex <= snapshotLastIndex_) {
    startIndex = snapshotLastIndex_ + 1;
  }

  for (uint64_t i = startIndex; i <= snapshotLastIndex_ + entries_.size();
       ++i) {
    size_t localIdx = i - snapshotLastIndex_ - 1;
    result.push_back(entries_[localIdx]);
  }

  return result;
}

uint64_t RaftLog::getTerm(uint64_t index) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (index == 0)
    return 0;
  if (index == snapshotLastIndex_)
    return snapshotLastTerm_;
  if (index < snapshotLastIndex_ ||
      index > snapshotLastIndex_ + entries_.size()) {
    return 0;
  }

  size_t localIdx = index - snapshotLastIndex_ - 1;
  return entries_[localIdx].term;
}

uint64_t RaftLog::lastIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshotLastIndex_ + entries_.size();
}

uint64_t RaftLog::lastTerm() const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (entries_.empty()) {
    return snapshotLastTerm_;
  }
  return entries_.back().term;
}

size_t RaftLog::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

bool RaftLog::isEmpty() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.empty() && snapshotLastIndex_ == 0;
}

void RaftLog::truncateFrom(uint64_t index) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (index <= snapshotLastIndex_) {
    LOG_WARN << "Cannot truncate entries in snapshot";
    return;
  }

  size_t localIdx = index - snapshotLastIndex_ - 1;
  if (localIdx < entries_.size()) {
    entries_.resize(localIdx);
    if (!rewriteFileLocked()) {
      LOG_ERROR << "Failed to persist truncation at index " << index;
    }
    LOG_DEBUG << "Truncated log from index " << index;
  }
}

bool RaftLog::isAtLeastAsUpToDate(uint64_t candidateLastTerm,
                                  uint64_t candidateLastIndex) const {
  std::lock_guard<std::mutex> lock(mutex_);

  uint64_t myLastTerm =
      entries_.empty() ? snapshotLastTerm_ : entries_.back().term;
  uint64_t myLastIndex = snapshotLastIndex_ + entries_.size();

  // Our log is STRICTLY more up-to-date if:
  // 1. Our last term is greater, OR
  // 2. Terms are equal but our index is STRICTLY greater
  // If logs are equal (same term AND same index), we should allow voting
  if (myLastTerm != candidateLastTerm) {
    return myLastTerm > candidateLastTerm;
  }
  // Same term - only reject if OUR log is STRICTLY longer
  return myLastIndex > candidateLastIndex;
}

void RaftLog::persist(const std::string &path) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string buf;
  put32(buf, kLogMagic);
  put32(buf, kLogVersion);
  put64(buf, snapshotLastIndex_);
  put64(buf, snapshotLastTerm_);

  for (const auto &entry : entries_) {
    buf.append(encodeRecord(entry));
  }

  std::string tmpPath = path + ".tmp";
  int tmp = ::open(tmpPath.c_str(), O_WRONLY | O_BINARY | O_CREAT | O_TRUNC, 0644);
  if (tmp < 0) {
    LOG_ERROR << "Failed to open log file for persistence: " << tmpPath;
    return;
  }

  bool ok = writeAllTo(tmp, buf.data(), buf.size());
  ::close(tmp);

  if (!ok || !atomicInstall(tmpPath, path)) {
    LOG_ERROR << "Failed to persist Raft log to " << path;
    return;
  }

  durableCount_ = entries_.size();
  LOG_DEBUG << "Persisted " << entries_.size() << " log entries to " << path;
}

void RaftLog::loadFrom(const std::string &path) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    LOG_DEBUG << "No existing log file, starting fresh";
    return;
  }

  std::string data((std::istreambuf_iterator<char>(file)),
                   std::istreambuf_iterator<char>());
  file.close();

  if (data.size() < kLogHeaderSize) {
    if (!data.empty()) {
      LOG_WARN << "Raft log too short to contain a header: " << path;
    }
    return;
  }

  uint32_t magic = get32(data.data());
  uint32_t version = get32(data.data() + 4);

  if (magic != kLogMagic) {
    LOG_ERROR << "Raft log has bad magic, refusing to load: " << path;
    return;
  }
  if (version != kLogVersion) {
    LOG_ERROR << "Raft log version " << version << " is not supported: "
              << path;
    return;
  }

  snapshotLastIndex_ = get64(data.data() + 8);
  snapshotLastTerm_ = get64(data.data() + 16);

  entries_.clear();

  size_t pos = kLogHeaderSize;
  size_t recovered = 0;

  // Stop at the first damaged record and keep the valid prefix. A crash during
  // an append leaves a torn tail, which is expected rather than exceptional.
  while (pos + 8 <= data.size()) {
    uint32_t len = get32(data.data() + pos);
    uint32_t expectedCrc = get32(data.data() + pos + 4);
    pos += 8;

    if (len < 24 || pos + len > data.size()) {
      LOG_WARN << "Truncated Raft log record at offset " << pos << " in "
               << path << "; keeping " << recovered << " valid entries";
      break;
    }

    std::string payload = data.substr(pos, len);
    if (Crc32::compute(payload) != expectedCrc) {
      LOG_WARN << "Raft log CRC mismatch at offset " << pos << " in " << path
               << "; keeping " << recovered << " valid entries";
      break;
    }
    pos += len;

    RaftLogEntry entry;
    entry.term = get64(payload.data());
    entry.index = get64(payload.data() + 8);
    uint64_t cmdLen = get64(payload.data() + 16);

    // Bound the length before it sizes an allocation. This used to be read
    // straight into resize(), so a corrupt field meant a huge allocation or a
    // crash on load.
    if (cmdLen > kMaxCommandSize || 24 + cmdLen != len) {
      LOG_WARN << "Raft log record has implausible command length in " << path;
      break;
    }

    entry.command = payload.substr(24, cmdLen);
    entries_.push_back(std::move(entry));
    recovered++;
  }

  durableCount_ = entries_.size();
  LOG_INFO << "Loaded " << entries_.size() << " log entries from " << path;
}

} // namespace shard
