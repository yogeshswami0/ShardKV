#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <mutex>
#include <random>
#include <string>
#include <vector>

namespace shard {

/**
 * Manages leader election for Raft consensus.
 *
 * Handles timeouts, vote requests, and state transitions during elections.
 * The election timeout is randomized to reduce split-brain scenarios.
 */
class Election {
public:
  using VoteCallback =
      std::function<bool(uint32_t peerId, uint64_t term, uint64_t lastLogIndex,
                         uint64_t lastLogTerm, bool preVote)>;

  Election(uint32_t nodeId, const std::vector<uint32_t> &peerIds);

  // Reset election timer (called when receiving valid heartbeat)
  void resetTimer();

  // Check if election timeout has elapsed
  bool isTimeoutElapsed() const;

  // Get time until next timeout (for scheduling)
  std::chrono::milliseconds timeUntilTimeout() const;

  /**
   * Run a pre-vote round for the term we would campaign in.
   *
   * No term is incremented and no vote is recorded on either side. This asks
   * only "would a majority elect me right now?". A node that cannot win --
   * because it is partitioned, or because the cluster already has a healthy
   * leader, gets a no and stays a follower, instead of bumping the term and
   * forcing that leader to step down.
   *
   * Returns true if a majority would have voted for us.
   */
  bool runPreVote(uint64_t prospectiveTerm, uint64_t lastLogIndex,
                  uint64_t lastLogTerm, const VoteCallback &requestVote);

  // Start a new election
  // Returns true if we won, false if election failed or timed out
  bool startElection(uint64_t newTerm, uint64_t lastLogIndex,
                     uint64_t lastLogTerm, const VoteCallback &requestVote);

  // Handle incoming vote request
  // Returns {voteGranted, currentTerm}
  std::pair<bool, uint64_t>
  handleVoteRequest(uint64_t candidateTerm, uint32_t candidateId,
                    uint64_t candidateLastLogIndex,
                    uint64_t candidateLastLogTerm, uint64_t currentTerm,
                    std::optional<uint32_t> &votedFor, bool myLogIsAsUpToDate);

  /**
   * True if we have heard from a leader within the last election timeout.
   *
   * A follower that is still hearing from its leader must refuse pre-votes,
   * however good the candidate's log is. Without that rule a node whose own
   * link to the leader is broken (an asymmetric partition) could still
   * collect pre-votes from healthy followers and unseat a working leader.
   */
  bool recentlyHeardFromLeader() const;

  // Record contact from a valid leader (an accepted AppendEntries).
  void noteLeaderContact();

  // Record that we voted for ourselves this term
  void votedForSelf() { votesReceived_ = 1; }

  // Record a vote received
  void recordVote(bool granted);

  // Check if we have won the election
  bool hasWonElection() const;

  // Reset votes for a new election
  void resetVotes();

  // Configuration
  void setTimeoutRange(int minMs, int maxMs);

private:
  // Caller must hold mutex_.
  std::chrono::milliseconds randomizeTimeoutLocked();

  uint32_t nodeId_;
  std::vector<uint32_t> peerIds_;

  /**
   * Guards the timer state and the RNG.
   *
   * resetTimer() is reached from at least three threads: the election loop,
   * the heartbeat thread (via becomeFollower) and any RPC handler thread
   * granting a vote or accepting a leader, while timeUntilTimeout() reads it
   * concurrently. std::mt19937 in particular is not merely torn by concurrent
   * use, its internal state is corrupted. ThreadSanitizer reports this on the
   * three-node cluster test.
   */
  mutable std::mutex mutex_;

  // Timer state
  std::chrono::steady_clock::time_point lastReset_;
  std::chrono::milliseconds currentTimeout_;

  // Last accepted contact from a leader, for the pre-vote stickiness rule.
  std::chrono::steady_clock::time_point lastLeaderContact_{};
  bool hadLeaderContact_ = false;

  // Election state
  std::atomic<int> votesReceived_{0};
  int votesNeeded_;

  // Timeout configuration
  int minTimeoutMs_ = 150;
  int maxTimeoutMs_ = 300;

  // Random number generation
  std::mt19937 rng_;
};

} // namespace shard
