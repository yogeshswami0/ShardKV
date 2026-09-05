#include "storage/SSTableReader.h"
#include "util/Crc32.h"
#include "util/Logger.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include "util/Platform.h"

namespace shard {

SSTableReader::SSTableReader(const std::string &path) : path_(path) {
  fd_ = ::open(path.c_str(), O_RDONLY | O_BINARY);
  if (fd_ < 0) {
    throw std::runtime_error("Failed to open SSTable: " + path + ": " +
                             strerror(errno));
  }

  struct stat st;
  if (::fstat(fd_, &st) != 0) {
    ::close(fd_);
    fd_ = -1;
    throw std::runtime_error("Failed to stat SSTable: " + path);
  }
  fileSize_ = static_cast<uint64_t>(st.st_size);

  auto fail = [&](const char *what) {
    ::close(fd_);
    fd_ = -1;
    throw std::runtime_error(std::string(what) + ": " + path);
  };

  if (!loadFooter())
    fail("Invalid SSTable footer");
  if (!loadIndex())
    fail("Failed to load SSTable index");
  if (!loadBloomFilter())
    fail("Failed to load bloom filter");

  // Cache the first key so non-overlapping levels can be ordered without
  // touching the disk.
  if (!index_.empty()) {
    auto firstBlock = readBlock(index_[0].handle);
    if (!firstBlock.empty()) {
      firstKey_ = firstBlock.front().key;
    }
  }

  LOG_DEBUG << "Opened SSTable: " << path << " with " << footer_.numEntries
            << " entries";
}

SSTableReader::~SSTableReader() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool SSTableReader::readAt(uint64_t offset, size_t len, char *out) const {
  size_t got = 0;
  while (got < len) {
    ssize_t n = ::pread(fd_, out + got, len - got,
                        static_cast<off_t>(offset + got));
    if (n < 0) {
      if (errno == EINTR)
        continue;
      LOG_ERROR << "pread failed on " << path_ << ": " << strerror(errno);
      return false;
    }
    if (n == 0) {
      return false; // short read: hit EOF early
    }
    got += static_cast<size_t>(n);
  }
  return true;
}

bool SSTableReader::loadFooter() {
  if (fileSize_ < kSSTableFooterSize) {
    LOG_ERROR << "SSTable too small to contain a footer: " << path_;
    return false;
  }

  char footerBytes[kSSTableFooterSize];
  if (!readAt(fileSize_ - kSSTableFooterSize, kSSTableFooterSize, footerBytes)) {
    return false;
  }

  size_t pos = 0;

  auto read64 = [&]() -> uint64_t {
    uint64_t val = 0;
    for (int i = 0; i < 8; ++i) {
      val = (val << 8) | static_cast<uint8_t>(footerBytes[pos++]);
    }
    return val;
  };

  auto read32 = [&]() -> uint32_t {
    uint32_t val = 0;
    for (int i = 0; i < 4; ++i) {
      val = (val << 8) | static_cast<uint8_t>(footerBytes[pos++]);
    }
    return val;
  };

  footer_.indexHandle.offset = read64();
  footer_.indexHandle.size = read64();
  footer_.bloomHandle.offset = read64();
  footer_.bloomHandle.size = read64();
  footer_.numEntries = read32();
  footer_.numBlocks = read32();
  footer_.minSequence = read64();
  footer_.maxSequence = read64();
  footer_.magic = read32();
  footer_.bloomNumHashes = read32();

  // Validate magic number
  if (footer_.magic != 0x56455254) {
    LOG_ERROR << "Invalid SSTable magic: " << std::hex << footer_.magic;
    return false;
  }

  // Every offset in the footer is attacker-visible after a torn write, so
  // bound them against the real file size before trusting them.
  if (footer_.indexHandle.offset + footer_.indexHandle.size > fileSize_ ||
      footer_.bloomHandle.offset + footer_.bloomHandle.size > fileSize_) {
    LOG_ERROR << "SSTable footer offsets exceed file size: " << path_;
    return false;
  }

  return true;
}

bool SSTableReader::loadIndex() {
  // Read the whole index block in one shot rather than field by field.
  std::string buf(footer_.indexHandle.size, '\0');
  if (footer_.indexHandle.size == 0 ||
      !readAt(footer_.indexHandle.offset, buf.size(), &buf[0])) {
    LOG_ERROR << "Failed to read SSTable index: " << path_;
    return false;
  }

  size_t pos = 0;
  auto need = [&](size_t n) { return pos + n <= buf.size(); };

  if (!need(4))
    return false;
  uint32_t numEntries = 0;
  for (int i = 0; i < 4; ++i) {
    numEntries = (numEntries << 8) | static_cast<uint8_t>(buf[pos++]);
  }

  index_.reserve(numEntries);

  for (uint32_t i = 0; i < numEntries; ++i) {
    IndexEntry entry;

    if (!need(4)) {
      LOG_ERROR << "Truncated SSTable index: " << path_;
      return false;
    }
    uint32_t keyLen = 0;
    for (int j = 0; j < 4; ++j) {
      keyLen = (keyLen << 8) | static_cast<uint8_t>(buf[pos++]);
    }

    if (!need(keyLen)) {
      LOG_ERROR << "Truncated SSTable index key: " << path_;
      return false;
    }
    entry.lastKey = buf.substr(pos, keyLen);
    pos += keyLen;

    if (!need(16)) {
      LOG_ERROR << "Truncated SSTable index handle: " << path_;
      return false;
    }

    entry.handle.offset = 0;
    for (int j = 0; j < 8; ++j) {
      entry.handle.offset =
          (entry.handle.offset << 8) | static_cast<uint8_t>(buf[pos++]);
    }

    entry.handle.size = 0;
    for (int j = 0; j < 8; ++j) {
      entry.handle.size =
          (entry.handle.size << 8) | static_cast<uint8_t>(buf[pos++]);
    }

    if (entry.handle.offset + entry.handle.size > fileSize_) {
      LOG_ERROR << "SSTable index points past end of file: " << path_;
      return false;
    }

    index_.push_back(entry);
  }

  return true;
}

bool SSTableReader::loadBloomFilter() {
  std::vector<uint8_t> bloomData(footer_.bloomHandle.size);
  if (footer_.bloomHandle.size > 0 &&
      !readAt(footer_.bloomHandle.offset, bloomData.size(),
              reinterpret_cast<char *>(bloomData.data()))) {
    return false;
  }

  // numHashes comes from the footer we already parsed. It used to be re-read
  // from the last 4 bytes of the file, which are the magic number, yielding a
  // bloom filter that claimed 0x56455254 hash rounds.
  if (footer_.bloomNumHashes == 0) {
    LOG_ERROR << "SSTable bloom filter reports zero hash rounds: " << path_;
    return false;
  }

  bloom_ = std::make_unique<BloomFilter>(bloomData, footer_.bloomNumHashes);
  return true;
}

bool SSTableReader::mightContain(const std::string &key) const {
  return bloom_->mightContain(key);
}

std::vector<SSTableEntry>
SSTableReader::readBlock(const BlockHandle &handle) const {
  std::vector<SSTableEntry> entries;

  if (handle.size < 8) {
    return entries;
  }

  // One positional read for the whole block: header and data together, at an
  // explicit offset, so concurrent readers cannot disturb each other.
  std::string raw(handle.size, '\0');
  if (!readAt(handle.offset, raw.size(), &raw[0])) {
    LOG_ERROR << "Failed to read block at offset " << handle.offset << " in "
              << path_;
    return entries;
  }

  uint32_t size = 0;
  for (int i = 0; i < 4; ++i) {
    size = (size << 8) | static_cast<uint8_t>(raw[i]);
  }

  uint32_t expectedCrc = 0;
  for (int i = 0; i < 4; ++i) {
    expectedCrc = (expectedCrc << 8) | static_cast<uint8_t>(raw[4 + i]);
  }

  if (static_cast<size_t>(size) + 8 > raw.size()) {
    LOG_ERROR << "Block header size exceeds block extent in " << path_;
    return entries;
  }

  std::string blockData = raw.substr(8, size);

  // Verify CRC
  uint32_t actualCrc = Crc32::compute(blockData);
  if (actualCrc != expectedCrc) {
    LOG_ERROR << "Block CRC mismatch at offset " << handle.offset;
    return entries;
  }

  // Parse entries
  size_t pos = 0;
  while (pos < blockData.size()) {
    SSTableEntry entry;

    // Key length and key
    if (pos + 4 > blockData.size())
      break;
    uint32_t keyLen = 0;
    for (int i = 0; i < 4; ++i) {
      keyLen = (keyLen << 8) | static_cast<uint8_t>(blockData[pos++]);
    }

    if (pos + keyLen > blockData.size())
      break;
    entry.key = blockData.substr(pos, keyLen);
    pos += keyLen;

    // Value length and value
    if (pos + 4 > blockData.size())
      break;
    uint32_t valLen = 0;
    for (int i = 0; i < 4; ++i) {
      valLen = (valLen << 8) | static_cast<uint8_t>(blockData[pos++]);
    }

    if (pos + valLen > blockData.size())
      break;
    entry.value = blockData.substr(pos, valLen);
    pos += valLen;

    // Sequence number
    if (pos + 8 > blockData.size())
      break;
    entry.sequenceNumber = 0;
    for (int i = 0; i < 8; ++i) {
      entry.sequenceNumber =
          (entry.sequenceNumber << 8) | static_cast<uint8_t>(blockData[pos++]);
    }

    // Deleted flag
    if (pos + 1 > blockData.size())
      break;
    entry.deleted = blockData[pos++] != 0;

    entries.push_back(entry);
  }

  return entries;
}

size_t SSTableReader::findBlockForKey(const std::string &key) const {
  // Binary search on index to find the right block
  size_t left = 0;
  size_t right = index_.size();

  while (left < right) {
    size_t mid = left + (right - left) / 2;
    if (index_[mid].lastKey < key) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

std::optional<SSTableEntry> SSTableReader::get(const std::string &key) const {
  // Quick bloom filter check
  if (!bloom_->mightContain(key)) {
    return std::nullopt;
  }

  // Find the block that might contain this key
  size_t blockIdx = findBlockForKey(key);
  if (blockIdx >= index_.size()) {
    return std::nullopt;
  }

  // Read and search the block
  auto entries = readBlock(index_[blockIdx].handle);

  // Binary search within block
  size_t left = 0;
  size_t right = entries.size();

  while (left < right) {
    size_t mid = left + (right - left) / 2;
    if (entries[mid].key < key) {
      left = mid + 1;
    } else if (entries[mid].key > key) {
      right = mid;
    } else {
      return entries[mid];
    }
  }

  return std::nullopt;
}

std::vector<SSTableEntry> SSTableReader::scan(const std::string &startKey,
                                              const std::string &endKey) const {
  std::vector<SSTableEntry> results;

  size_t startBlock = findBlockForKey(startKey);

  for (size_t i = startBlock; i < index_.size(); ++i) {
    auto entries = readBlock(index_[i].handle);

    for (const auto &entry : entries) {
      if (entry.key >= endKey) {
        return results;
      }
      if (entry.key >= startKey) {
        results.push_back(entry);
      }
    }
  }

  return results;
}

// Iterator implementation
SSTableReader::Iterator::Iterator(const SSTableReader *reader, size_t blockIdx,
                                  size_t entryIdx)
    : reader_(reader), blockIdx_(blockIdx), entryIdx_(entryIdx) {
  if (blockIdx_ < reader_->index_.size()) {
    loadBlock();
    // A block can come back empty if its CRC failed. Skip forward so valid()
    // never reports true while entry() would read out of bounds.
    while (entryIdx_ >= currentBlock_.size() &&
           blockIdx_ < reader_->index_.size()) {
      blockIdx_++;
      entryIdx_ = 0;
      if (blockIdx_ < reader_->index_.size()) {
        loadBlock();
      }
    }
  }
}

bool SSTableReader::Iterator::valid() const {
  return blockIdx_ < reader_->index_.size() && entryIdx_ < currentBlock_.size();
}

void SSTableReader::Iterator::next() {
  entryIdx_++;
  while (entryIdx_ >= currentBlock_.size() &&
         blockIdx_ < reader_->index_.size()) {
    blockIdx_++;
    entryIdx_ = 0;
    if (blockIdx_ < reader_->index_.size()) {
      loadBlock();
    } else {
      currentBlock_.clear();
    }
  }
}

SSTableEntry SSTableReader::Iterator::entry() const {
  return currentBlock_[entryIdx_];
}

void SSTableReader::Iterator::loadBlock() {
  currentBlock_ = reader_->readBlock(reader_->index_[blockIdx_].handle);
}

SSTableReader::Iterator SSTableReader::begin() const {
  return Iterator(this, 0, 0);
}

SSTableReader::Iterator SSTableReader::end() const {
  return Iterator(this, index_.size(), 0);
}

} // namespace shard
