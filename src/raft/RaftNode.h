#pragma once

#include "raft/Election.h"
#include "raft/RaftLog.h"
#include "raft/Replication.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace shard {

enum class RaftState { FOLLOWER, CANDIDATE, LEADER };

/**
 * Result of proposing a command to the Raft cluster.
 */
struct ProposeResult {
  bool success;
  uint64_t index;    // Log index where command was placed
  uint64_t term;     // Term when command was proposed
  std::string error; // Error message if failed
};

/**
 * Core Raft consensus state machine.
 *
 * Implements the Raft algorithm for distributed consensus:
 * - Leader election with randomized timeouts
 * - Log replication with consistency checks
 * - Safety guarantees through term and log matching
 */
class RaftNode {
public:
  // Callback types for RPC communication
  using RequestVoteRPC =
      std::function<bool(uint32_t peerId, uint64_t term, uint32_t candidateId,
                         uint64_t lastLogIndex, uint64_t lastLogTerm,
                         bool preVote, uint64_t &responseTerm,
                         bool &voteGranted)>;

  using AppendEntriesRPC = std::function<bool(
      uint32_t peerId, uint64_t term, uint32_t leaderId, uint64_t prevLogIndex,
      uint64_t prevLogTerm, const std::vector<RaftLogEntry> &entries,
      uint64_t leaderCommit, uint64_t &responseTerm, bool &success,
      uint64_t &matchIndex)>;

  using ApplyCallback =
      std::function<void(uint64_t index, const std::string &command)>;

  RaftNode(uint32_t nodeId, const std::vector<uint32_t> &peerIds);
  ~RaftNode();

  // Set RPC callbacks (must be called before start)
  void setRequestVoteRPC(RequestVoteRPC rpc) {
    requestVoteRPC_ = std::move(rpc);
  }
  void setAppendEntriesRPC(AppendEntriesRPC rpc) {
    appendEntriesRPC_ = std::move(rpc);
  }
  void setApplyCallback(ApplyCallback callback) {
    applyCallback_ = std::move(callback);
  }

  // Start the Raft node (begins election timer)
  void start();
  void stop();

  // State queries
  RaftState state() const { return state_; }
  uint64_t currentTerm() const { return currentTerm_; }
  uint32_t nodeId() const { return nodeId_; }
  std::optional<uint32_t> leaderId() const;
  bool isLeader() const { return state_ == RaftState::LEADER; }

  uint64_t commitIndex() const;
  uint64_t lastApplied() const;

  // Highest index present in this node's log (not necessarily committed).
  uint64_t logLastIndex() const { return log_.lastIndex(); }
  uint64_t logTermAt(uint64_t index) const { return log_.getTerm(index); }

  // Client interface (for proposing commands)
  ProposeResult propose(const std::string &command);

  /**
   * Block until the entry proposed at (index, term) has been applied to the
   * state machine, or the proposal is known to have failed, or the timeout
   * elapses.
   *
   * Returns true only if that specific entry was applied. If a later leader
   * overwrote the slot, the proposal did NOT commit and this returns false --
   * that check is what makes the acknowledgement meaningful, since an index
   * alone can be reused by a different term.
   */
  bool waitForApplied(uint64_t index, uint64_t term,
                      std::chrono::milliseconds timeout);

  /**
   * Confirm this node is still leader by collecting a fresh heartbeat quorum.
   *
   * A leader that has been partitioned away does not know it yet, so serving
   * reads from local state can return values a newer leader has already
   * replaced. This is the ReadIndex check that makes a linearizable read
   * possible.
   */
  bool confirmLeadership(std::chrono::milliseconds timeout);

  // RPC handlers (called by gRPC service)
  /**
   * @param preVote when set, this is a trial election. Neither side updates
   *        currentTerm or votedFor, and the vote is refused outright if we have
   *        heard from a leader within the last election timeout.
   */
  void handleRequestVote(uint64_t term, uint32_t candidateId,
                         uint64_t lastLogIndex, uint64_t lastLogTerm,
                         bool preVote, uint64_t &responseTerm,
                         bool &voteGranted);

  void handleAppendEntries(uint64_t term, uint32_t leaderId,
                           uint64_t prevLogIndex, uint64_t prevLogTerm,
                           const std::vector<RaftLogEntry> &entries,
                           uint64_t leaderCommit, uint64_t &responseTerm,
                           bool &success, uint64_t &matchIndex);

  // Persistence. loadState() also binds the log to its file, after which every
  // term change, vote and log append is made durable before it is acted on.
  void persistState(const std::string &path);
  void loadState(const std::string &path);

private:
  // State transitions
  void becomeFollower(uint64_t term);
  void becomeCandidate();
  void becomeLeader();

  // Main loop threads
  void electionLoop();

  /**
   * Drives replication to one peer, for as long as we lead this term.
   *
   * One of these runs per peer. Previously a single heartbeat thread contacted
   * every peer in one round and waited for all of them, so an unreachable peer
   * stretched the round to its RPC deadline and the cadence seen by healthy
   * followers collapsed with it.
   */
  void peerLoop(uint32_t peerId, uint64_t term);

  void applyLoop();

  // Stop and reap the per-peer threads. Must not be called from one of them.
  void joinPeerThreads();

  // Write currentTerm/votedFor to stable media. Caller must hold mutex_.
  void persistHardStateLocked();

  /**
   * CheckQuorum: step down if we have not heard from a majority recently.
   *
   * A partitioned leader has no way to notice it has been deposed, nothing
   * arrives to tell it. Left alone it stays "leader" indefinitely, which blocks
   * nothing but misreports the cluster and keeps it answering as though it were
   * authoritative. A leader that cannot reach a quorum must return to being a
   * follower on its own.
   */
  void checkQuorumOrStepDown();

  // Per-peer time of the last successful AppendEntries response.
  std::mutex contactMutex_;
  std::unordered_map<uint32_t, std::chrono::steady_clock::time_point>
      lastPeerContact_;

  // Configuration
  uint32_t nodeId_;
  std::vector<uint32_t> peerIds_;
  std::string statePath_;

  // Persistent state
  std::atomic<uint64_t> currentTerm_{0};
  std::optional<uint32_t> votedFor_;
  RaftLog log_;

  // Volatile state. commitIndex_, lastApplied_ and leaderId_ are guarded by
  // mutex_; they were previously read and written from the heartbeat thread
  // with no synchronization at all.
  std::atomic<RaftState> state_{RaftState::FOLLOWER};
  std::optional<uint32_t> leaderId_;
  uint64_t commitIndex_ = 0;
  uint64_t lastApplied_ = 0;

  // Sub-components
  std::unique_ptr<Election> election_;
  std::unique_ptr<Replication> replication_;

  // RPC callbacks
  RequestVoteRPC requestVoteRPC_;
  AppendEntriesRPC appendEntriesRPC_;
  ApplyCallback applyCallback_;

  // Thread management. The peer threads have their own mutex because joining
  // them must never happen under mutex_, and never from a peer thread itself.
  std::atomic<bool> running_{false};
  std::thread electionThread_;
  std::vector<std::thread> peerThreads_;
  std::thread applyThread_;
  std::mutex peerThreadMutex_;

  // Synchronization
  mutable std::mutex mutex_;
  std::condition_variable commitCond_;
  std::condition_variable appliedCond_;
};

} // namespace shard
