#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>
#include <functional>

namespace shard {

struct MemTableEntry {
  std::string value;
  uint64_t sequenceNumber;
  bool deleted;

  MemTableEntry() : sequenceNumber(0), deleted(false) {}
  MemTableEntry(const std::string &val, uint64_t seq, bool del)
      : value(val), sequenceNumber(seq), deleted(del) {}
};

class MemTable {
public:
  explicit MemTable(size_t maxSizeBytes);

  void put(const std::string &key, const std::string &value, uint64_t seqNum);
  void remove(const std::string &key, uint64_t seqNum);
  std::optional<MemTableEntry> get(const std::string &key) const;

  bool shouldFlush() const;
  size_t entryCount() const;
  size_t approximateSize() const;

  void freeze();
  bool isFrozen() const { return frozen_.load(); }
  void clear();

  void forEach(std::function<void(const std::string &, const MemTableEntry &)> cb) const;

  // For compatibility with old code
  std::map<std::string, MemTableEntry> getAllEntries() const;

private:
  size_t getStripeIndex(const std::string &key) const;

  static constexpr size_t NUM_STRIPES = 16;
  
  struct Stripe {
    std::map<std::string, MemTableEntry> entries;
    mutable std::shared_mutex mutex;
  };

  Stripe stripes_[NUM_STRIPES];
  
  size_t maxSizeBytes_;
  std::atomic<size_t> currentSize_{0};
  std::atomic<size_t> entryCount_{0};
  std::atomic<bool> frozen_{false};
};

} // namespace shard
