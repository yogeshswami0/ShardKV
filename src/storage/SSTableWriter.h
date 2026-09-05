#pragma once

#include "storage/BloomFilter.h"
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace shard {

/**
 * SSTable block index entry - points to a data block in the file
 */
struct BlockHandle {
  uint64_t offset;
  uint64_t size;
};

/**
 * SSTable index entry - sparse index pointing to data blocks
 */
struct IndexEntry {
  std::string lastKey; // Last key in this block
  BlockHandle handle;
};

/**
 * SSTable footer - metadata at the end of the file
 */
struct SSTableFooter {
  BlockHandle indexHandle;     // Location of index block
  BlockHandle bloomHandle;     // Location of bloom filter
  uint32_t numEntries;         // Total key-value pairs
  uint32_t numBlocks;          // Number of data blocks
  uint64_t minSequence;        // Minimum sequence number
  uint64_t maxSequence;        // Maximum sequence number
  uint32_t magic = 0x56455254; // "VERT" in hex, for validation
  uint32_t bloomNumHashes = 0; // Hash rounds used by the bloom filter
};

/**
 * Serialized footer size, in bytes:
 *   indexOffset(8) + indexSize(8) + bloomOffset(8) + bloomSize(8)
 * + numEntries(4) + numBlocks(4) + minSeq(8) + maxSeq(8)
 * + magic(4) + bloomNumHashes(4)
 *
 * Writer and reader MUST agree on this. They previously both used a hardcoded
 * 60 while serializing 64 bytes, which overflowed the stack buffer on both
 * sides and truncated bloomNumHashes out of the file entirely.
 */
static constexpr size_t kSSTableFooterSize = 64;

/**
 * Single key-value entry in an SSTable
 */
struct SSTableEntry {
  std::string key;
  std::string value;
  uint64_t sequenceNumber;
  bool deleted;
};

/**
 * Writes an SSTable to disk.
 *
 * SSTable file format:
 * +------------------------+
 * | Data Block 0           |
 * | Data Block 1           |
 * | ...                    |
 * | Data Block N           |
 * +------------------------+
 * | Index Block            |
 * +------------------------+
 * | Bloom Filter           |
 * +------------------------+
 * | Footer (fixed size)    |
 * +------------------------+
 */
class SSTableWriter {
public:
  explicit SSTableWriter(const std::string &path, size_t blockSize = 4096);
  ~SSTableWriter();

  // Add entries (must be added in sorted order!)
  void add(const std::string &key, const std::string &value, uint64_t seqNum,
           bool deleted = false);

  // Finalize and close the file
  void finish();

  // Discard the table without installing it, removing the temp file. Used when
  // a compaction merge yields no entries, where installing a zero-entry
  // SSTable would serve no purpose.
  void abandon();

  // Get path of the written file
  std::string path() const { return path_; }
  uint32_t entryCount() const { return entryCount_; }

private:
  void flushDataBlock();
  void writeIndexBlock();
  void writeBloomFilter();
  void writeFooter();

  std::string path_;
  std::string tmpPath_;
  uint64_t indexOffset_ = 0;
  uint64_t bloomOffset_ = 0;
  std::ofstream file_;
  size_t blockSize_;

  // Current data block being built
  std::string currentBlock_;
  std::string lastKeyInBlock_;

  // Index and bloom filter
  std::vector<IndexEntry> index_;
  std::unique_ptr<BloomFilter> bloom_;

  // Statistics
  uint32_t entryCount_ = 0;
  uint32_t blockCount_ = 0;
  uint64_t minSeq_ = UINT64_MAX;
  uint64_t maxSeq_ = 0;

  bool finished_ = false;
};

} // namespace shard
