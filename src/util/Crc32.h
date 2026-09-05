#pragma once

#include <cstdint>
#include <string>

namespace shard {

/**
 * CRC32 checksum for data integrity verification.
 * Used in WAL entries and SSTable blocks.
 */
class Crc32 {
public:
  Crc32();

  // Process data incrementally
  void update(const void *data, size_t length);
  void update(const std::string &data);

  // Get current checksum value
  uint32_t value() const;

  // Reset for reuse
  void reset();

  // One-shot convenience methods
  static uint32_t compute(const void *data, size_t length);
  static uint32_t compute(const std::string &data);

private:
  uint32_t crc_;
  static const uint32_t table_[256];
  static const uint32_t *initTable();
};

} // namespace shard
