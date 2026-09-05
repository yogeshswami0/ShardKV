#include "raft/Election.h"
#include <future>
#include <vector>
#include "util/Logger.h"

namespace shard {

Election::Election(uint32_t nodeId, const std::vector<uint32_t> &peerIds)
    : nodeId_(nodeId), peerIds_(peerIds), rng_(std::random_device{}()) {
  // Majority needed: (total nodes + 1) / 2
  // Total nodes = 1 (self) + peers
  votesNeeded_ = (1 + static_cast<int>(peerIds.size()) + 1) / 2;

  resetTimer();
}

void Election::resetTimer() {
  std::lock_guard<std::mutex> lock(mutex_);
  lastReset_ = std::chrono::steady_clock::now();
  currentTimeout_ = randomizeTimeoutLocked();
}

bool Election::isTimeoutElapsed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - lastReset_);
  return elapsed >= currentTimeout_;
}

std::chrono::milliseconds Election::timeUntilTimeout() const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - lastReset_);

  if (elapsed >= currentTimeout_) {
    return std::chrono::milliseconds(0);
  }

  return currentTimeout_ - elapsed;
}

void Election::noteLeaderContact() {
  std::lock_guard<std::mutex> lock(mutex_);
  lastLeaderContact_ = std::chrono::steady_clock::now();
  hadLeaderContact_ = true;
}

bool Election::recentlyHeardFromLeader() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!hadLeaderContact_) {
    return false;
  }
  auto since = std::chrono::steady_clock::now() - lastLeaderContact_;
  return since < currentTimeout_;
}

bool Election::runPreVote(uint64_t prospectiveTerm, uint64_t lastLogIndex,
                          uint64_t lastLogTerm,
                          const VoteCallback &requestVote) {
  LOG_DEBUG << "Running pre-vote for prospective term " << prospectiveTerm;

  // We would vote for ourselves in the real election, so we count as one.
  int granted = 1;

  std::vector<std::future<bool>> pending;
  pending.reserve(peerIds_.size());

  for (uint32_t peerId : peerIds_) {
    pending.push_back(std::async(std::launch::async, [&, peerId]() {
      return requestVote(peerId, prospectiveTerm, lastLogIndex, lastLogTerm,
                         /*preVote=*/true);
    }));
  }

  for (auto &future : pending) {
    if (future.get()) {
      granted++;
    }
  }

  bool wouldWin = granted >= votesNeeded_;
  if (!wouldWin) {
    LOG_DEBUG << "Pre-vote failed with " << granted << " of " << votesNeeded_
              << " needed; staying a follower without touching the term";
  }
  return wouldWin;
}

bool Election::startElection(uint64_t newTerm, uint64_t lastLogIndex,
                             uint64_t lastLogTerm,
                             const VoteCallback &requestVote) {
  resetVotes();
  votedForSelf(); // Vote for ourselves

  LOG_INFO << "Starting election for term " << newTerm;

  // Fan the vote requests out in parallel. Issuing them serially meant one
  // unreachable peer burned a full RPC timeout before the next was even tried,
  // which can exceed the election timeout itself and livelock the cluster into
  // repeated failed elections.
  std::vector<std::future<bool>> pending;
  pending.reserve(peerIds_.size());

  for (uint32_t peerId : peerIds_) {
    pending.push_back(std::async(std::launch::async, [&, peerId]() {
      return requestVote(peerId, newTerm, lastLogIndex, lastLogTerm,
                         /*preVote=*/false);
    }));
  }

  for (auto &future : pending) {
    // Every future must be collected before returning: they capture locals by
    // reference, so abandoning one would leave it reading a dead frame.
    if (future.get()) {
      recordVote(true);
    }
  }

  if (hasWonElection()) {
    LOG_INFO << "Won election for term " << newTerm << " with "
             << votesReceived_.load() << " votes";
    return true;
  }

  bool won = hasWonElection();
  if (won) {
    LOG_INFO << "Won election for term " << newTerm;
  } else {
    LOG_INFO << "Election for term " << newTerm << " failed, got "
             << votesReceived_.load() << " of " << votesNeeded_ << " needed";
  }

  return won;
}

std::pair<bool, uint64_t>
Election::handleVoteRequest(uint64_t candidateTerm, uint32_t candidateId,
                            uint64_t candidateLastLogIndex,
                            uint64_t candidateLastLogTerm, uint64_t currentTerm,
                            std::optional<uint32_t> &votedFor,
                            bool myLogIsAsUpToDate) {
  // Rule 1: If candidate's term is less than ours, reject
  if (candidateTerm < currentTerm) {
    LOG_DEBUG << "Rejecting vote for " << candidateId << ": term "
              << candidateTerm << " < " << currentTerm;
    return {false, currentTerm};
  }

  // Rule 2: If we already voted for someone else this term, reject
  if (votedFor.has_value() && votedFor.value() != candidateId) {
    LOG_DEBUG << "Rejecting vote for " << candidateId << ": already voted for "
              << votedFor.value();
    return {false, currentTerm};
  }

  // Rule 3: Check if candidate's log is at least as up-to-date as ours
  if (myLogIsAsUpToDate) {
    LOG_DEBUG << "Rejecting vote for " << candidateId
              << ": our log is more up-to-date";
    return {false, currentTerm};
  }

  // Grant vote
  votedFor = candidateId;
  resetTimer(); // Reset election timer on granting vote

  LOG_DEBUG << "Granted vote to " << candidateId << " for term "
            << candidateTerm;
  return {true, candidateTerm};
}

void Election::recordVote(bool granted) {
  if (granted) {
    votesReceived_.fetch_add(1);
  }
}

bool Election::hasWonElection() const {
  return votesReceived_.load() >= votesNeeded_;
}

void Election::resetVotes() { votesReceived_ = 0; }

void Election::setTimeoutRange(int minMs, int maxMs) {
  std::lock_guard<std::mutex> lock(mutex_);
  minTimeoutMs_ = minMs;
  maxTimeoutMs_ = maxMs;
}

std::chrono::milliseconds Election::randomizeTimeoutLocked() {
  std::uniform_int_distribution<int> dist(minTimeoutMs_, maxTimeoutMs_);
  return std::chrono::milliseconds(dist(rng_));
}

} // namespace shard
