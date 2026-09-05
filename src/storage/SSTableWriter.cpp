#include "storage/SSTableWriter.h"
#include "config/Config.h"
#include "util/Crc32.h"
#include "util/Durability.h"
#include <filesystem>
#include "util/Logger.h"

namespace shard {

SSTableWriter::SSTableWriter(const std::string &path, size_t blockSize)
    : path_(path), tmpPath_(path + ".tmp"), blockSize_(blockSize) {
  // Build under a temp name and rename into place at finish(), so a crash
  // mid-write can never leave a partial file under the real name. Readers
  // either see the whole SSTable or no SSTable.
  file_.open(tmpPath_, std::ios::binary | std::ios::trunc);
  if (!file_.is_open()) {
    throw std::runtime_error("Failed to create SSTable: " + tmpPath_);
  }

  // Initialize bloom filter with estimated capacity
  double fpRate = Config::instance().bloomFpRate();
  bloom_ = std::make_unique<BloomFilter>(10000, fpRate);
}

SSTableWriter::~SSTableWriter() {
  if (!finished_ && file_.is_open()) {
    // Try to finish if not already done
    try {
      finish();
    } catch (...) {
      // Ignore errors in destructor
    }
  }
}

void SSTableWriter::add(const std::string &key, const std::string &value,
                        uint64_t seqNum, bool deleted) {
  if (finished_) {
    throw std::runtime_error("Cannot add to finished SSTable");
  }

  // Entry format: [keyLen:4][key][valueLen:4][value][seqNum:8][deleted:1]
  std::string entry;

  // Key length and key
  uint32_t keyLen = static_cast<uint32_t>(key.size());
  for (int i = 3; i >= 0; --i) {
    entry.push_back(static_cast<char>((keyLen >> (i * 8)) & 0xFF));
  }
  entry.append(key);

  // Value length and value
  uint32_t valLen = static_cast<uint32_t>(value.size());
  for (int i = 3; i >= 0; --i) {
    entry.push_back(static_cast<char>((valLen >> (i * 8)) & 0xFF));
  }
  entry.append(value);

  // Sequence number
  for (int i = 7; i >= 0; --i) {
    entry.push_back(static_cast<char>((seqNum >> (i * 8)) & 0xFF));
  }

  // Deleted flag
  entry.push_back(deleted ? 1 : 0);

  // Add to bloom filter
  bloom_->add(key);

  // Add to current block
  currentBlock_.append(entry);
  lastKeyInBlock_ = key;

  // Track statistics
  entryCount_++;
  minSeq_ = std::min(minSeq_, seqNum);
  maxSeq_ = std::max(maxSeq_, seqNum);

  // Flush block if it exceeds size threshold
  if (currentBlock_.size() >= blockSize_) {
    flushDataBlock();
  }
}

void SSTableWriter::flushDataBlock() {
  if (currentBlock_.empty())
    return;

  // Block format: [size:4][crc:4][data]
  uint32_t size = static_cast<uint32_t>(currentBlock_.size());
  uint32_t crc = Crc32::compute(currentBlock_);

  uint64_t blockOffset = static_cast<uint64_t>(file_.tellp());

  // Write header
  char header[8];
  for (int i = 3; i >= 0; --i) {
    header[3 - i] = static_cast<char>((size >> (i * 8)) & 0xFF);
  }
  for (int i = 3; i >= 0; --i) {
    header[7 - i] = static_cast<char>((crc >> (i * 8)) & 0xFF);
  }
  file_.write(header, 8);

  // Write data
  file_.write(currentBlock_.data(), currentBlock_.size());

  // Add to index
  IndexEntry indexEntry;
  indexEntry.lastKey = lastKeyInBlock_;
  indexEntry.handle.offset = blockOffset;
  indexEntry.handle.size = 8 + size; // header + data
  index_.push_back(indexEntry);

  blockCount_++;
  currentBlock_.clear();
}

void SSTableWriter::writeIndexBlock() {
  // Index format: [numEntries:4][entries...]
  // Each entry: [keyLen:4][key][offset:8][size:8]

  // Take the offset from the actual write position. It used to be derived as a
  // max over index_, which yields 0 for an empty table and writes a footer
  // pointing the index at the start of the file.
  indexOffset_ = static_cast<uint64_t>(file_.tellp());

  std::string indexData;

  uint32_t numEntries = static_cast<uint32_t>(index_.size());
  for (int i = 3; i >= 0; --i) {
    indexData.push_back(static_cast<char>((numEntries >> (i * 8)) & 0xFF));
  }

  for (const auto &entry : index_) {
    // Key
    uint32_t keyLen = static_cast<uint32_t>(entry.lastKey.size());
    for (int i = 3; i >= 0; --i) {
      indexData.push_back(static_cast<char>((keyLen >> (i * 8)) & 0xFF));
    }
    indexData.append(entry.lastKey);

    // Offset
    for (int i = 7; i >= 0; --i) {
      indexData.push_back(
          static_cast<char>((entry.handle.offset >> (i * 8)) & 0xFF));
    }

    // Size
    for (int i = 7; i >= 0; --i) {
      indexData.push_back(
          static_cast<char>((entry.handle.size >> (i * 8)) & 0xFF));
    }
  }

  file_.write(indexData.data(), indexData.size());
}

void SSTableWriter::writeBloomFilter() {
  bloomOffset_ = static_cast<uint64_t>(file_.tellp());
  auto bloomData = bloom_->serialize();
  file_.write(reinterpret_cast<const char *>(bloomData.data()),
              bloomData.size());
}

void SSTableWriter::writeFooter() {
  // Index size calculation
  size_t indexSize = 4; // numEntries
  for (const auto &entry : index_) {
    indexSize += 4 + entry.lastKey.size() + 8 + 8;
  }

  SSTableFooter footer;
  footer.indexHandle.offset = indexOffset_;
  footer.indexHandle.size = indexSize;
  footer.bloomHandle.offset = bloomOffset_;
  footer.bloomHandle.size = bloom_->sizeBytes();
  footer.numEntries = entryCount_;
  footer.numBlocks = blockCount_;
  footer.minSequence = minSeq_;
  footer.maxSequence = maxSeq_;

  footer.bloomNumHashes = static_cast<uint32_t>(bloom_->numHashes());

  // Write footer as raw bytes
  // Format: [indexOffset:8][indexSize:8][bloomOffset:8][bloomSize:8]
  //         [numEntries:4][numBlocks:4][minSeq:8][maxSeq:8][magic:4][numHashes:4]
  char footerBytes[kSSTableFooterSize];
  size_t pos = 0;

  auto write64 = [&](uint64_t val) {
    for (int i = 7; i >= 0; --i) {
      footerBytes[pos++] = static_cast<char>((val >> (i * 8)) & 0xFF);
    }
  };

  auto write32 = [&](uint32_t val) {
    for (int i = 3; i >= 0; --i) {
      footerBytes[pos++] = static_cast<char>((val >> (i * 8)) & 0xFF);
    }
  };

  write64(footer.indexHandle.offset);
  write64(footer.indexHandle.size);
  write64(footer.bloomHandle.offset);
  write64(footer.bloomHandle.size);
  write32(footer.numEntries);
  write32(footer.numBlocks);
  write64(footer.minSequence);
  write64(footer.maxSequence);
  write32(footer.magic);
  write32(footer.bloomNumHashes);

  // Guards the writer against ever silently overflowing this buffer again.
  if (pos != kSSTableFooterSize) {
    throw std::runtime_error("SSTable footer size mismatch");
  }

  file_.write(footerBytes, kSSTableFooterSize);
}

void SSTableWriter::abandon() {
  if (finished_)
    return;

  finished_ = true;
  if (file_.is_open()) {
    file_.close();
  }

  std::error_code ec;
  std::filesystem::remove(tmpPath_, ec);
}

void SSTableWriter::finish() {
  if (finished_)
    return;

  // Flush any remaining data
  flushDataBlock();

  // Write metadata sections
  writeIndexBlock();
  writeBloomFilter();
  writeFooter();

  file_.flush();
  if (!file_.good()) {
    throw std::runtime_error("SSTable write error: " + tmpPath_);
  }
  file_.close();
  finished_ = true;

  // fsync the contents, then rename into place, then fsync the directory.
  // Without this the SSTable lives only in the page cache, yet the caller
  // deletes the WAL segment that still holds these writes immediately after.
  if (!atomicInstall(tmpPath_, path_)) {
    throw std::runtime_error("Failed to durably install SSTable: " + path_);
  }

  LOG_INFO << "SSTable written: " << path_ << " with " << entryCount_
           << " entries in " << blockCount_ << " blocks";
}

} // namespace shard
