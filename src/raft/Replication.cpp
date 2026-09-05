#include "raft/Replication.h"
#include <future>
#include "util/Logger.h"
#include <algorithm>

namespace shard {

Replication::Replication(uint32_t nodeId, const std::vector<uint32_t> &peerIds)
    : nodeId_(nodeId), peerIds_(peerIds),
      clusterSize_(1 + static_cast<int>(peerIds.size())) {}

void Replication::initializeLeaderState(uint64_t lastLogIndex) {
  std::lock_guard<std::mutex> lock(mutex_);
  nextIndex_.clear();
  matchIndex_.clear();

  for (uint32_t peerId : peerIds_) {
    // Initialize nextIndex to leader's last log index + 1
    nextIndex_[peerId] = lastLogIndex + 1;
    matchIndex_[peerId] = 0;
  }

  LOG_DEBUG << "Initialized leader replication state, nextIndex = "
            << (lastLogIndex + 1);
}

bool Replication::replicateToPeer(uint32_t peerId, const RaftLog &log,
                                  uint64_t currentTerm, uint64_t leaderCommit,
                                  const SendEntriesCallback &send) {
  uint64_t nextIdx;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    nextIdx = nextIndex_.count(peerId) ? nextIndex_[peerId] : 1;
  }

  uint64_t prevLogIndex = nextIdx - 1;
  uint64_t prevLogTerm = log.getTerm(prevLogIndex);

  // Entries beyond what this peer already has; empty is a plain heartbeat.
  auto entries = log.getEntriesFrom(nextIdx);

  uint64_t matchIdx = 0;
  // The RPC runs without the lock held; it can block for a full timeout.
  bool success =
      send(peerId, currentTerm, prevLogIndex, prevLogTerm, entries,
           leaderCommit, matchIdx);

  std::lock_guard<std::mutex> lock(mutex_);
  if (success) {
    handleSuccessLocked(peerId, matchIdx);
  } else {
    handleFailureLocked(peerId);
  }

  return success;
}

void Replication::handleSuccessLocked(uint32_t peerId, uint64_t matchIndex) {
  matchIndex_[peerId] = matchIndex;
  nextIndex_[peerId] = matchIndex + 1;

  LOG_DEBUG << "Peer " << peerId << " matched up to index " << matchIndex;
}

void Replication::handleFailureLocked(uint32_t peerId) {
  // Decrement nextIndex and retry
  if (nextIndex_[peerId] > 1) {
    nextIndex_[peerId]--;
    LOG_DEBUG << "Peer " << peerId
              << " log inconsistency, decrementing nextIndex to "
              << nextIndex_[peerId];
  }
}

uint64_t Replication::computeCommitIndex(const RaftLog &log,
                                         uint64_t currentTerm) const {
  // Collect all known match indexes (including leader's own)
  std::vector<uint64_t> matchIndexes;
  matchIndexes.reserve(clusterSize_);

  // Leader's match index is its last log index
  matchIndexes.push_back(log.lastIndex());

  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &[peerId, matchIdx] : matchIndex_) {
      matchIndexes.push_back(matchIdx);
    }
  }

  // Sort to find median (which is the majority threshold)
  std::sort(matchIndexes.begin(), matchIndexes.end(), std::greater<uint64_t>());

  // The index at position (clusterSize / 2) is the highest index
  // replicated to a majority
  size_t majorityPos = clusterSize_ / 2;
  if (majorityPos >= matchIndexes.size()) {
    return 0;
  }

  uint64_t majorityMatch = matchIndexes[majorityPos];

  // Only commit if the entry is from the current term
  // (Raft safety requirement)
  if (majorityMatch > 0 && log.getTerm(majorityMatch) == currentTerm) {
    return majorityMatch;
  }

  return 0;
}

uint64_t Replication::getNextIndex(uint32_t peerId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = nextIndex_.find(peerId);
  return (it != nextIndex_.end()) ? it->second : 1;
}

uint64_t Replication::getMatchIndex(uint32_t peerId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = matchIndex_.find(peerId);
  return (it != matchIndex_.end()) ? it->second : 0;
}

} // namespace shard
