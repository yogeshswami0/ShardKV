#include "storage/BloomFilter.h"
#include <cmath>
#include <cstring>

namespace shard {

// MurmurHash3 implementation for bloom filter hashing
namespace {

uint64_t murmur64(const void *key, size_t len, uint64_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t *data = static_cast<const uint64_t *>(key);
  const uint64_t *end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }

  const uint8_t *tail = reinterpret_cast<const uint8_t *>(data);
  uint64_t k = 0;

  switch (len & 7) {
  case 7:
    k ^= static_cast<uint64_t>(tail[6]) << 48;
    [[fallthrough]];
  case 6:
    k ^= static_cast<uint64_t>(tail[5]) << 40;
    [[fallthrough]];
  case 5:
    k ^= static_cast<uint64_t>(tail[4]) << 32;
    [[fallthrough]];
  case 4:
    k ^= static_cast<uint64_t>(tail[3]) << 24;
    [[fallthrough]];
  case 3:
    k ^= static_cast<uint64_t>(tail[2]) << 16;
    [[fallthrough]];
  case 2:
    k ^= static_cast<uint64_t>(tail[1]) << 8;
    [[fallthrough]];
  case 1:
    k ^= static_cast<uint64_t>(tail[0]);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

} // anonymous namespace

BloomFilter::BloomFilter(size_t expectedElements, double falsePositiveRate) {
  // Calculate optimal bit array size: m = -n * ln(p) / (ln(2)^2)
  double ln2 = std::log(2.0);
  double ln2Squared = ln2 * ln2;

  size_t m =
      static_cast<size_t>(std::ceil(-static_cast<double>(expectedElements) *
                                    std::log(falsePositiveRate) / ln2Squared));

  // Minimum size to avoid edge cases
  numBits_ = std::max(m, size_t(64));

  // Round up to byte boundary
  size_t numBytes = (numBits_ + 7) / 8;
  bits_.resize(numBytes, 0);
  numBits_ = numBytes * 8;

  // Calculate optimal number of hash functions: k = (m/n) * ln(2)
  numHashes_ = static_cast<size_t>(
      std::ceil(static_cast<double>(numBits_) / expectedElements * ln2));
  numHashes_ = std::max(numHashes_, size_t(1));
  numHashes_ = std::min(numHashes_, size_t(16)); // Cap at reasonable limit
}

BloomFilter::BloomFilter(const std::vector<uint8_t> &data, size_t numHashes)
    : bits_(data), numBits_(data.size() * 8), numHashes_(numHashes) {}

void BloomFilter::add(const std::string &key) {
  std::vector<uint64_t> hashes = hash(key);
  for (uint64_t h : hashes) {
    setBit(h % numBits_);
  }
}

bool BloomFilter::mightContain(const std::string &key) const {
  std::vector<uint64_t> hashes = hash(key);
  for (uint64_t h : hashes) {
    if (!getBit(h % numBits_)) {
      return false; // Definitely not in set
    }
  }
  return true; // Might be in set
}

std::vector<uint8_t> BloomFilter::serialize() const { return bits_; }

double BloomFilter::fillRatio() const {
  size_t setBits = 0;
  for (uint8_t byte : bits_) {
    // Count set bits using lookup table approach
    while (byte) {
      setBits += byte & 1;
      byte >>= 1;
    }
  }
  return static_cast<double>(setBits) / numBits_;
}

std::vector<uint64_t> BloomFilter::hash(const std::string &key) const {
  std::vector<uint64_t> result;
  result.reserve(numHashes_);

  // Use double hashing to generate multiple hash values efficiently
  // h_i(x) = h1(x) + i * h2(x)
  uint64_t h1 = murmur64(key.data(), key.size(), 0x9747b28c);
  uint64_t h2 = murmur64(key.data(), key.size(), 0xc6a4a793);

  for (size_t i = 0; i < numHashes_; ++i) {
    result.push_back(h1 + i * h2);
  }

  return result;
}

void BloomFilter::setBit(size_t index) {
  bits_[index / 8] |= (1 << (index % 8));
}

bool BloomFilter::getBit(size_t index) const {
  return (bits_[index / 8] & (1 << (index % 8))) != 0;
}

} // namespace shard
