#pragma once

#include "storage/BloomFilter.h"
#include "storage/SSTableWriter.h"
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace shard {

/**
 * Reads an SSTable from disk.
 *
 * Supports point lookups (using bloom filter and binary search on index)
 * and range scans.
 *
 * Thread safety: fully concurrent. Reads go through pread(), which takes the
 * offset as an argument and so touches no shared file cursor, the previous
 * seekg()+read() pair on a shared ifstream let concurrent readers interleave
 * and receive each other's blocks. After construction all state is immutable,
 * so no locking is needed on the read path at all.
 *
 * The descriptor stays open for the reader's lifetime, which also means an
 * in-flight read stays valid after compaction unlinks the file underneath it.
 */
class SSTableReader {
public:
  explicit SSTableReader(const std::string &path);
  ~SSTableReader();

  // Not copyable: owns a file descriptor.
  SSTableReader(const SSTableReader &) = delete;
  SSTableReader &operator=(const SSTableReader &) = delete;

  // Point lookup - returns nullopt if key doesn't exist
  std::optional<SSTableEntry> get(const std::string &key) const;

  // Range scan - returns all entries with keys in [startKey, endKey)
  std::vector<SSTableEntry> scan(const std::string &startKey,
                                 const std::string &endKey) const;

  // Metadata access
  std::string path() const { return path_; }
  uint32_t entryCount() const { return footer_.numEntries; }
  uint64_t minSequence() const { return footer_.minSequence; }
  uint64_t maxSequence() const { return footer_.maxSequence; }
  uint64_t fileSize() const { return fileSize_; }

  // First key in the table, for ordering non-overlapping levels.
  const std::string &firstKey() const { return firstKey_; }

  // Check if key might exist (bloom filter check)
  bool mightContain(const std::string &key) const;

  // Iterator support for compaction
  class Iterator {
  public:
    Iterator(const SSTableReader *reader, size_t blockIdx, size_t entryIdx);

    bool valid() const;
    void next();
    SSTableEntry entry() const;

  private:
    const SSTableReader *reader_;
    size_t blockIdx_;
    size_t entryIdx_;
    std::vector<SSTableEntry> currentBlock_;

    void loadBlock();
  };

  Iterator begin() const;
  Iterator end() const;

private:
  bool loadFooter();
  bool loadIndex();
  bool loadBloomFilter();
  std::vector<SSTableEntry> readBlock(const BlockHandle &handle) const;
  size_t findBlockForKey(const std::string &key) const;

  // Positional read of exactly `len` bytes at `offset`. Stateless, so safe to
  // call concurrently on one reader.
  bool readAt(uint64_t offset, size_t len, char *out) const;

  std::string path_;
  int fd_ = -1;
  uint64_t fileSize_ = 0;
  SSTableFooter footer_;
  std::vector<IndexEntry> index_;
  std::unique_ptr<BloomFilter> bloom_;
  std::string firstKey_;
};

} // namespace shard
