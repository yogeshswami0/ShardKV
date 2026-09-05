#include "storage/MemTable.h"
#include "util/Logger.h"
#include <stdexcept>
#include <functional>

namespace shard {

MemTable::MemTable(size_t maxSizeBytes) : maxSizeBytes_(maxSizeBytes) {}

size_t MemTable::getStripeIndex(const std::string &key) const {
  return std::hash<std::string>{}(key) % NUM_STRIPES;
}

void MemTable::put(const std::string &key, const std::string &value,
                   uint64_t seqNum) {
  if (frozen_.load(std::memory_order_acquire)) {
    throw std::runtime_error("Cannot write to frozen MemTable");
  }

  size_t idx = getStripeIndex(key);
  std::unique_lock<std::shared_mutex> lock(stripes_[idx].mutex);

  auto& entries = stripes_[idx].entries;
  auto it = entries.find(key);
  if (it != entries.end()) {
    if (seqNum < it->second.sequenceNumber) {
      return;
    }
    size_t oldSize = it->second.value.size();
    size_t newSize = value.size();
    
    // We approximate size changes across atomic
    currentSize_.fetch_add(newSize, std::memory_order_relaxed);
    currentSize_.fetch_sub(oldSize, std::memory_order_relaxed);
    
    it->second = MemTableEntry(value, seqNum, false);
  } else {
    currentSize_.fetch_add(key.size() + value.size() + sizeof(MemTableEntry), std::memory_order_relaxed);
    entryCount_.fetch_add(1, std::memory_order_relaxed);
    entries[key] = MemTableEntry(value, seqNum, false);
  }
}

void MemTable::remove(const std::string &key, uint64_t seqNum) {
  if (frozen_.load(std::memory_order_acquire)) {
    throw std::runtime_error("Cannot write to frozen MemTable");
  }

  size_t idx = getStripeIndex(key);
  std::unique_lock<std::shared_mutex> lock(stripes_[idx].mutex);

  auto& entries = stripes_[idx].entries;
  auto it = entries.find(key);
  if (it != entries.end()) {
    if (seqNum < it->second.sequenceNumber) {
      return;
    }
    size_t oldSize = it->second.value.size();
    currentSize_.fetch_sub(oldSize, std::memory_order_relaxed);
    it->second = MemTableEntry("", seqNum, true);
  } else {
    currentSize_.fetch_add(key.size() + sizeof(MemTableEntry), std::memory_order_relaxed);
    entryCount_.fetch_add(1, std::memory_order_relaxed);
    entries[key] = MemTableEntry("", seqNum, true);
  }
}

std::optional<MemTableEntry> MemTable::get(const std::string &key) const {
  size_t idx = getStripeIndex(key);
  std::shared_lock<std::shared_mutex> lock(stripes_[idx].mutex);

  const auto& entries = stripes_[idx].entries;
  auto it = entries.find(key);
  if (it != entries.end()) {
    return it->second;
  }
  return std::nullopt;
}

bool MemTable::shouldFlush() const {
  return currentSize_.load(std::memory_order_relaxed) >= maxSizeBytes_;
}

size_t MemTable::entryCount() const {
  return entryCount_.load(std::memory_order_relaxed);
}

size_t MemTable::approximateSize() const {
  return currentSize_.load(std::memory_order_relaxed);
}

void MemTable::freeze() {
  frozen_.store(true, std::memory_order_release);
  LOG_DEBUG << "MemTable frozen with " << entryCount() << " entries, "
            << approximateSize() << " bytes";
}

void MemTable::clear() {
  for (size_t i = 0; i < NUM_STRIPES; ++i) {
    std::unique_lock<std::shared_mutex> lock(stripes_[i].mutex);
    stripes_[i].entries.clear();
  }
  currentSize_.store(0, std::memory_order_relaxed);
  entryCount_.store(0, std::memory_order_relaxed);
  frozen_.store(false, std::memory_order_release);
}

void MemTable::forEach(std::function<void(const std::string &, const MemTableEntry &)> cb) const {
  for (size_t i = 0; i < NUM_STRIPES; ++i) {
    std::shared_lock<std::shared_mutex> lock(stripes_[i].mutex);
    for (const auto& [k, v] : stripes_[i].entries) {
      cb(k, v);
    }
  }
}

std::map<std::string, MemTableEntry> MemTable::getAllEntries() const {
  std::map<std::string, MemTableEntry> result;
  for (size_t i = 0; i < NUM_STRIPES; ++i) {
    std::shared_lock<std::shared_mutex> lock(stripes_[i].mutex);
    result.insert(stripes_[i].entries.begin(), stripes_[i].entries.end());
  }
  return result;
}

} // namespace shard
