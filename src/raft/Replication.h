#pragma once

#include <mutex>

#include "raft/RaftLog.h"
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>

namespace shard {

/**
 * Manages log replication from leader to followers.
 *
 * Tracks replication progress for each follower and handles
 * the AppendEntries RPC logic for maintaining log consistency.
 */
class Replication {
public:
  using SendEntriesCallback = std::function<bool(
      uint32_t peerId, uint64_t term, uint64_t prevLogIndex,
      uint64_t prevLogTerm, const std::vector<RaftLogEntry> &entries,
      uint64_t leaderCommit,
      uint64_t &matchIndex)>; // Output: follower's match index

  Replication(uint32_t nodeId, const std::vector<uint32_t> &peerIds);

  // Initialize leader state (called when becoming leader)
  void initializeLeaderState(uint64_t lastLogIndex);

  /**
   * Replicate to a single peer and update its nextIndex/matchIndex.
   *
   * The leader drives one of these per peer, independently, so a slow or dead
   * follower cannot affect how often any other follower is contacted. Sending
   * to every peer in one round and waiting for all of them meant a dead peer
   * stretched the round to its RPC deadline, collapsing the heartbeat cadence
   * seen by perfectly healthy followers.
   */
  bool replicateToPeer(uint32_t peerId, const RaftLog &log,
                       uint64_t currentTerm, uint64_t leaderCommit,
                       const SendEntriesCallback &send);

  // Handle successful replication response
  void handleSuccessLocked(uint32_t peerId, uint64_t matchIndex);

  // Handle failed replication (log inconsistency)
  void handleFailureLocked(uint32_t peerId);

  // Get the commit index (highest index replicated to majority)
  uint64_t computeCommitIndex(const RaftLog &log, uint64_t currentTerm) const;

  // Get replication status for a peer
  uint64_t getNextIndex(uint32_t peerId) const;
  uint64_t getMatchIndex(uint32_t peerId) const;

  // Send heartbeats (empty AppendEntries)

private:
  /**
   * Guards nextIndex_ and matchIndex_.
   *
   * initializeLeaderState() writes them from becomeLeader() on the election
   * thread while heartbeatLoop() reads and updates them on its own thread.
   */
  mutable std::mutex mutex_;

  uint32_t nodeId_;
  std::vector<uint32_t> peerIds_;

  // For each follower, index of next log entry to send
  std::unordered_map<uint32_t, uint64_t> nextIndex_;

  // For each follower, highest log entry known to be replicated
  std::unordered_map<uint32_t, uint64_t> matchIndex_;

  // Total cluster size for majority calculation
  int clusterSize_;
};

} // namespace shard
