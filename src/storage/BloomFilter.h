#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace shard {

/**
 * Bloom filter for probabilistic set membership testing.
 * Used to avoid unnecessary disk reads for non-existent keys.
 *
 * False positives are possible (configurable rate), but false negatives are
 * not. A "maybe exists" result requires checking the SSTable, while "definitely
 * not" saves a disk read entirely.
 */
class BloomFilter {
public:
  /**
   * Create a bloom filter optimized for the expected number of elements.
   * @param expectedElements Approximate number of keys to store
   * @param falsePositiveRate Target false positive probability (0.0-1.0)
   */
  BloomFilter(size_t expectedElements, double falsePositiveRate = 0.01);

  /**
   * Reconstruct from serialized data (for SSTable loading)
   */
  explicit BloomFilter(const std::vector<uint8_t> &data, size_t numHashes);

  // Add a key to the filter (cannot be removed)
  void add(const std::string &key);

  // Check if key might exist (false = definitely not, true = maybe)
  bool mightContain(const std::string &key) const;

  // Serialization for SSTable storage
  std::vector<uint8_t> serialize() const;
  size_t numHashes() const { return numHashes_; }
  size_t sizeBytes() const { return bits_.size(); }

  // Stats for debugging
  double fillRatio() const;

private:
  std::vector<uint64_t> hash(const std::string &key) const;
  void setBit(size_t index);
  bool getBit(size_t index) const;

  std::vector<uint8_t> bits_;
  size_t numBits_;
  size_t numHashes_;
};

} // namespace shard
