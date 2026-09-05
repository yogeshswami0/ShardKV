#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace shard {

/**
 * Single entry in the Raft replicated log.
 */
struct RaftLogEntry {
  uint64_t term;       // Term when entry was received
  uint64_t index;      // Position in the log (1-indexed)
  std::string command; // Serialized command for state machine

  RaftLogEntry() : term(0), index(0) {}
  RaftLogEntry(uint64_t t, uint64_t i, const std::string &cmd)
      : term(t), index(i), command(cmd) {}
};

/**
 * Replicated log for Raft consensus.
 *
 * The log stores commands that have been agreed upon by the cluster.
 * Entries are appended by the leader and replicated to followers.
 * Committed entries are applied to the state machine.
 */
class RaftLog {
public:
  RaftLog();
  ~RaftLog();

  // Append a new entry (returns the index assigned)
  uint64_t append(uint64_t term, const std::string &command);

  // Append multiple entries (for replication)
  void appendEntries(const std::vector<RaftLogEntry> &entries);

  // Get entry at a specific index (1-indexed, returns nullopt if not found)
  std::optional<RaftLogEntry> getEntry(uint64_t index) const;

  // Get entries from startIndex onward
  std::vector<RaftLogEntry> getEntriesFrom(uint64_t startIndex) const;

  // Get term of entry at index (0 if index doesn't exist)
  uint64_t getTerm(uint64_t index) const;

  // Log metadata
  uint64_t lastIndex() const;
  uint64_t lastTerm() const;
  size_t size() const;
  bool isEmpty() const;

  // Truncate log from index onward (for conflict resolution)
  // This removes entries at and after the given index
  void truncateFrom(uint64_t index);

  // Check if our log is at least as up-to-date as the candidate
  bool isAtLeastAsUpToDate(uint64_t candidateLastTerm,
                           uint64_t candidateLastIndex) const;

  /**
   * Persistence.
   *
   * Raft requires the log to be durable before an entry is acknowledged, so
   * openAt() binds the log to a file and every subsequent mutation appends to
   * it and fsyncs before returning. persist() rewrites the whole file, which
   * is only needed after a truncation.
   *
   * On-disk format:
   *   header : "VXRL" magic (4) + version (4) + snapshotIndex (8)
   *            + snapshotTerm (8)
   *   record : length (4) + CRC32 (4) + [term(8) index(8) cmdLen(8) command]
   *
   * Every record is length-framed and checksummed, so a torn tail from a crash
   * mid-append is detected and the valid prefix is kept.
   */
  void openAt(const std::string &path);
  void persist(const std::string &path);
  void loadFrom(const std::string &path);

  // True once every entry in memory is on stable media.
  bool isDurable() const;

private:
  // Appends one record to the open file and syncs. Caller holds mutex_.
  bool appendToFileLocked(const RaftLogEntry &entry);

  // Rewrites the entire file from memory and syncs. Caller holds mutex_.
  bool rewriteFileLocked();

  static std::string encodeRecord(const RaftLogEntry &entry);

  std::vector<RaftLogEntry> entries_;
  mutable std::mutex mutex_;

  std::string path_;
  int fd_ = -1;
  uint64_t durableCount_ = 0;

  // Snapshot metadata (for log compaction)
  uint64_t snapshotLastIndex_ = 0;
  uint64_t snapshotLastTerm_ = 0;
};

} // namespace shard
