#include "raft/RaftNode.h"
#include <chrono>
#include "config/Config.h"
#include "util/Durability.h"
#include "util/Logger.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include "util/Platform.h"

namespace shard {

RaftNode::RaftNode(uint32_t nodeId, const std::vector<uint32_t> &peerIds)
    : nodeId_(nodeId), peerIds_(peerIds) {
  election_ = std::make_unique<Election>(nodeId, peerIds);
  replication_ = std::make_unique<Replication>(nodeId, peerIds);

  // Configure timeouts from config
  auto &config = Config::instance();
  election_->setTimeoutRange(config.electionTimeoutMinMs(),
                             config.electionTimeoutMaxMs());
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
  if (running_.exchange(true))
    return;

  LOG_INFO << "Starting Raft node " << nodeId_;

  // Start background threads
  electionThread_ = std::thread(&RaftNode::electionLoop, this);
  applyThread_ = std::thread(&RaftNode::applyLoop, this);
}

void RaftNode::stop() {
  if (!running_.exchange(false))
    return;

  LOG_INFO << "Stopping Raft node " << nodeId_;

  commitCond_.notify_all();
  appliedCond_.notify_all();

  if (electionThread_.joinable())
    electionThread_.join();

  joinPeerThreads();

  if (applyThread_.joinable())
    applyThread_.join();
}

void RaftNode::becomeFollower(uint64_t term) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (term > currentTerm_) {
    currentTerm_ = term;
    votedFor_.reset();
    persistHardStateLocked();
  }

  state_ = RaftState::FOLLOWER;
  election_->resetTimer();

  // Do NOT join the peer threads here. This function is reached from
  // sendCallback, which runs ON one of them. Joining was therefore a self-join
  // (resource_deadlock_would_occur), done while holding mutex_.
  // Clearing state_ is enough: each peer loop tests it every iteration and
  // returns. The join happens in becomeLeader() or stop().

  LOG_INFO << "Node " << nodeId_ << " became follower for term "
           << currentTerm_.load();
}

void RaftNode::becomeCandidate() {
  std::lock_guard<std::mutex> lock(mutex_);

  currentTerm_++;
  state_ = RaftState::CANDIDATE;
  votedFor_ = nodeId_;
  leaderId_.reset();

  // The new term and the self-vote must be durable before we solicit votes,
  // or a crash-and-restart could produce a second vote in this same term.
  persistHardStateLocked();

  election_->resetTimer();

  LOG_INFO << "Node " << nodeId_ << " became candidate for term "
           << currentTerm_.load();
}

void RaftNode::becomeLeader() {
  {
    std::lock_guard<std::mutex> lock(mutex_);

    state_ = RaftState::LEADER;
    leaderId_ = nodeId_;

    // Initialize leader-specific state
    replication_->initializeLeaderState(log_.lastIndex());
  }

  LOG_INFO << "Node " << nodeId_ << " became LEADER for term "
           << currentTerm_.load();

  // Reap any threads left over from a previous leadership before starting new
  // ones, assigning over a still-joinable std::thread calls std::terminate.
  // This runs on the election thread, never on a peer thread, so it is safe.
  joinPeerThreads();

  uint64_t term = currentTerm_.load();

  std::lock_guard<std::mutex> lock(peerThreadMutex_);
  peerThreads_.reserve(peerIds_.size());
  for (uint32_t peerId : peerIds_) {
    peerThreads_.emplace_back(&RaftNode::peerLoop, this, peerId, term);
  }
}

void RaftNode::electionLoop() {
  while (running_) {
    auto timeout = election_->timeUntilTimeout();

    if (timeout.count() > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    // Timeout elapsed
    if (state_ == RaftState::LEADER) {
      // Leaders do not campaign, but they must notice if they have lost the
      // cluster underneath them.
      checkQuorumOrStepDown();
      election_->resetTimer();
      continue;
    }

    uint64_t lastLogIndex = log_.lastIndex();
    uint64_t lastLogTerm = log_.lastTerm();
    uint64_t prospectiveTerm = currentTerm_.load() + 1;

    auto voteCallback = [this](uint32_t peerId, uint64_t electionTerm,
                               uint64_t lastIdx, uint64_t lastTerm,
                               bool preVote) -> bool {
      if (!requestVoteRPC_)
        return false;

      uint64_t responseTerm = 0;
      bool granted = false;

      bool ok = requestVoteRPC_(peerId, electionTerm, nodeId_, lastIdx,
                                lastTerm, preVote, responseTerm, granted);

      // A pre-vote reply must never move our term. Reacting to it would
      // reintroduce exactly the disruption pre-vote exists to prevent.
      if (ok && !preVote && responseTerm > currentTerm_) {
        becomeFollower(responseTerm);
        return false;
      }

      return ok && granted;
    };

    // Trial election first. If we could not win, stay a follower and leave the
    // term alone, a partitioned or rejoining node must not be able to unseat
    // a healthy leader simply by campaigning.
    if (!election_->runPreVote(prospectiveTerm, lastLogIndex, lastLogTerm,
                               voteCallback)) {
      election_->resetTimer();
      continue;
    }

    // The pre-vote says a majority would elect us, so campaign for real.
    becomeCandidate();

    uint64_t term = currentTerm_;
    lastLogIndex = log_.lastIndex();
    lastLogTerm = log_.lastTerm();

    bool won =
        election_->startElection(term, lastLogIndex, lastLogTerm, voteCallback);

    if (won && state_ == RaftState::CANDIDATE && currentTerm_ == term) {
      becomeLeader();
    }
  }
}

void RaftNode::checkQuorumOrStepDown() {
  if (peerIds_.empty()) {
    return; // a single node is its own quorum
  }

  auto electionTimeout =
      std::chrono::milliseconds(Config::instance().electionTimeoutMinMs());
  auto now = std::chrono::steady_clock::now();

  size_t reachable = 1; // ourselves
  {
    std::lock_guard<std::mutex> lock(contactMutex_);
    for (const auto &[peerId, when] : lastPeerContact_) {
      if (now - when < electionTimeout) {
        reachable++;
      }
    }
  }

  size_t needed = (peerIds_.size() + 1) / 2 + 1;
  if (reachable < needed) {
    LOG_WARN << "Stepping down: heard from " << reachable << " of " << needed
             << " needed within the election timeout";
    becomeFollower(currentTerm_.load());
  }
}

void RaftNode::peerLoop(uint32_t peerId, uint64_t term) {
  auto &config = Config::instance();
  auto heartbeatInterval = std::chrono::milliseconds(config.heartbeatMs());

  auto sendCallback = [this, term](uint32_t p, uint64_t t, uint64_t prevIdx,
                                   uint64_t prevTerm,
                                   const std::vector<RaftLogEntry> &entries,
                                   uint64_t leaderCommit,
                                   uint64_t &matchIdx) -> bool {
    if (!appendEntriesRPC_)
      return false;

    uint64_t responseTerm = 0;
    bool success = false;

    bool ok = appendEntriesRPC_(p, t, nodeId_, prevIdx, prevTerm, entries,
                                leaderCommit, responseTerm, success, matchIdx);

    if (ok && responseTerm > currentTerm_) {
      becomeFollower(responseTerm);
      return false;
    }

    if (ok) {
      // Any response at all proves the peer is reachable, which is what
      // CheckQuorum needs to know.
      std::lock_guard<std::mutex> lock(contactMutex_);
      lastPeerContact_[p] = std::chrono::steady_clock::now();
    }

    return ok && success;
  };

  // Bound to the term this loop was started for, so a stale loop from a
  // previous leadership cannot keep replicating.
  while (running_ && state_ == RaftState::LEADER && currentTerm_ == term) {
    auto roundStart = std::chrono::steady_clock::now();

    uint64_t commit;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      commit = commitIndex_;
    }

    replication_->replicateToPeer(peerId, log_, term, commit, sendCallback);

    // Any peer's progress can be what carries an entry to a majority, so the
    // commit index is recomputed after each response rather than once per
    // round over all peers.
    uint64_t newCommit = replication_->computeCommitIndex(log_, term);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (newCommit > commitIndex_) {
        commitIndex_ = newCommit;
        commitCond_.notify_all();
      }
    }

    // Sleep only the remainder. This peer's cadence depends on this peer
    // alone, a different peer being slow or dead cannot stretch it.
    auto elapsed = std::chrono::steady_clock::now() - roundStart;
    if (elapsed < heartbeatInterval) {
      std::this_thread::sleep_for(heartbeatInterval - elapsed);
    }
  }
}

void RaftNode::joinPeerThreads() {
  std::lock_guard<std::mutex> lock(peerThreadMutex_);
  for (auto &t : peerThreads_) {
    // Never join ourselves: this is reachable from becomeFollower, which a
    // peer thread calls when it sees a higher term.
    if (t.joinable() && t.get_id() != std::this_thread::get_id()) {
      t.join();
    }
  }
  peerThreads_.clear();
}

void RaftNode::applyLoop() {
  while (running_) {
    std::vector<std::pair<uint64_t, std::string>> batch;
    uint64_t highestIndex = 0;

    {
      std::unique_lock<std::mutex> lock(mutex_);

      commitCond_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return !running_ || lastApplied_ < commitIndex_;
      });

      if (!running_)
        break;

      // Collect the committed-but-unapplied entries under the lock, then
      // release it before touching the state machine.
      for (uint64_t idx = lastApplied_ + 1; idx <= commitIndex_; ++idx) {
        auto entry = log_.getEntry(idx);
        if (!entry.has_value()) {
          LOG_ERROR << "Missing log entry at index " << idx;
          break;
        }
        batch.emplace_back(idx, entry->command);
        highestIndex = idx;
      }
    }

    if (batch.empty()) {
      continue;
    }

    // Applied outside mutex_. This callback writes to the storage engine --
    // doing that while holding the Raft lock stalled every RPC handler for the
    // duration of a disk write, and any re-entry into RaftNode would deadlock.
    for (const auto &item : batch) {
      if (applyCallback_) {
        applyCallback_(item.first, item.second);
      }
      LOG_DEBUG << "Applied entry at index " << item.first;
    }

    {
      // lastApplied_ only advances once the state machine has actually
      // finished, so a waiter that sees it can safely read the result.
      std::lock_guard<std::mutex> lock(mutex_);
      lastApplied_ = std::max(lastApplied_, highestIndex);
    }
    appliedCond_.notify_all();
  }
}

std::optional<uint32_t> RaftNode::leaderId() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return leaderId_;
}

uint64_t RaftNode::commitIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return commitIndex_;
}

uint64_t RaftNode::lastApplied() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return lastApplied_;
}

bool RaftNode::waitForApplied(uint64_t index, uint64_t term,
                              std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  std::unique_lock<std::mutex> lock(mutex_);

  while (running_) {
    if (lastApplied_ >= index) {
      // Applied, but an index alone proves nothing, because a later leader
      // can reuse the same slot for a different entry. Only a matching term
      // means *our* proposal is what got committed there.
      return log_.getTerm(index) == term;
    }

    uint64_t termAt = log_.getTerm(index);
    if (termAt != 0 && termAt != term) {
      // The slot now holds a different entry, so our proposal was overwritten
      // by a newer leader and will never commit.
      LOG_WARN << "Proposal at index " << index << " term " << term
               << " was overwritten by term " << termAt;
      return false;
    }

    if (appliedCond_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return lastApplied_ >= index && log_.getTerm(index) == term;
    }
  }

  return false;
}

bool RaftNode::confirmLeadership(std::chrono::milliseconds timeout) {
  if (state_ != RaftState::LEADER) {
    return false;
  }

  uint64_t term = currentTerm_.load();

  // A single-node cluster is trivially its own quorum.
  if (peerIds_.empty()) {
    return state_ == RaftState::LEADER && currentTerm_.load() == term;
  }

  if (!appendEntriesRPC_) {
    return false;
  }

  uint64_t commit = commitIndex();
  size_t confirmations = 1; // ourselves
  size_t needed = (peerIds_.size() + 1) / 2 + 1;

  auto deadline = std::chrono::steady_clock::now() + timeout;

  for (uint32_t peerId : peerIds_) {
    if (std::chrono::steady_clock::now() > deadline) {
      break;
    }

    uint64_t responseTerm = 0;
    bool success = false;
    uint64_t matchIndex = 0;
    std::vector<RaftLogEntry> empty;

    uint64_t prevIdx = log_.lastIndex();
    bool ok = appendEntriesRPC_(peerId, term, nodeId_, prevIdx,
                                log_.getTerm(prevIdx), empty, commit,
                                responseTerm, success, matchIndex);

    if (ok && responseTerm > term) {
      becomeFollower(responseTerm);
      return false;
    }

    if (ok && success) {
      confirmations++;
      if (confirmations >= needed) {
        // Still leader as of a moment ago, confirmed by a majority.
        return state_ == RaftState::LEADER && currentTerm_.load() == term;
      }
    }
  }

  return false;
}

ProposeResult RaftNode::propose(const std::string &command) {
  ProposeResult result;

  std::lock_guard<std::mutex> lock(mutex_);

  // Checked under the lock. The test used to happen before acquiring it, so a
  // node could lose leadership between the check and the append.
  if (state_ != RaftState::LEADER) {
    result.success = false;
    result.error = "Not leader";
    if (leaderId_.has_value()) {
      result.error += ", leader is node " + std::to_string(leaderId_.value());
    }
    return result;
  }

  uint64_t term = currentTerm_;
  uint64_t index = log_.append(term, command);

  result.success = true;
  result.index = index;
  result.term = term;

  // A lone node is a majority of one. Without peers there is no peerLoop to
  // recompute commitIndex, so a proposed current-term entry would sit
  // uncommitted forever.
  if (peerIds_.empty()) {
    uint64_t newCommit = replication_->computeCommitIndex(log_, term);
    if (newCommit > commitIndex_) {
      commitIndex_ = newCommit;
      commitCond_.notify_all();
    }
  }

  LOG_DEBUG << "Proposed command at index " << index << " term " << term;

  return result;
}

void RaftNode::handleRequestVote(uint64_t term, uint32_t candidateId,
                                 uint64_t lastLogIndex, uint64_t lastLogTerm,
                                 bool preVote, uint64_t &responseTerm,
                                 bool &voteGranted) {
  // A pre-vote is answered without mutating anything. It is a question, not a
  // ballot: no term bump, no votedFor, no persistence. Treating it as a real
  // vote would defeat the entire mechanism, since the term bump is precisely
  // the disruption being avoided.
  if (preVote) {
    std::lock_guard<std::mutex> lock(mutex_);
    responseTerm = currentTerm_;

    if (term <= currentTerm_) {
      voteGranted = false;
      return;
    }

    // Leader stickiness: refuse while a leader is known to be alive, however
    // good the candidate's log is. Otherwise a node whose own link to the
    // leader is broken could still gather pre-votes from healthy followers and
    // unseat a leader that is working fine for everyone else.
    //
    // "A leader is alive" includes us being that leader. Asking only whether we
    // have *heard from* a leader silently exempts the leader itself, which then
    // cheerfully helps a rejoining node depose it. A leader that has genuinely
    // lost its quorum steps down first (CheckQuorum) and can grant afterwards.
    if (state_ == RaftState::LEADER || election_->recentlyHeardFromLeader()) {
      LOG_DEBUG << "Refusing pre-vote to " << candidateId
                << ": still hearing from the current leader";
      voteGranted = false;
      return;
    }

    voteGranted = !log_.isAtLeastAsUpToDate(lastLogTerm, lastLogIndex);
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // If term > currentTerm, step down
  bool stateChanged = false;
  if (term > currentTerm_) {
    currentTerm_ = term;
    state_ = RaftState::FOLLOWER;
    votedFor_.reset();
    stateChanged = true;
  }

  responseTerm = currentTerm_;

  // Check if candidate's log is at least as up-to-date as ours
  // isAtLeastAsUpToDate returns true if OUR log is at least as up-to-date as
  // candidate's So if true, our log is better/equal -> we should NOT grant vote
  // based on this
  bool ourLogIsBetter = log_.isAtLeastAsUpToDate(lastLogTerm, lastLogIndex);

  auto [granted, newTerm] =
      election_->handleVoteRequest(term, candidateId, lastLogIndex, lastLogTerm,
                                   currentTerm_, votedFor_, ourLogIsBetter);

  voteGranted = granted;

  if (granted) {
    stateChanged = true;
    LOG_INFO << "Granted vote to node " << candidateId << " for term " << term;
    election_->resetTimer();
  }

  // Durable before the reply goes out. Answering first and persisting later
  // would let a crash erase a vote the candidate has already counted.
  if (stateChanged) {
    persistHardStateLocked();
  }
}

void RaftNode::handleAppendEntries(uint64_t term, uint32_t leaderId,
                                   uint64_t prevLogIndex, uint64_t prevLogTerm,
                                   const std::vector<RaftLogEntry> &entries,
                                   uint64_t leaderCommit,
                                   uint64_t &responseTerm, bool &success,
                                   uint64_t &matchIndex) {
  std::lock_guard<std::mutex> lock(mutex_);

  responseTerm = currentTerm_;
  success = false;
  matchIndex = 0;

  // Rule 1: Reply false if term < currentTerm
  if (term < currentTerm_) {
    return;
  }

  // Valid leader - reset election timer and update state
  election_->resetTimer();
  election_->noteLeaderContact();

  if (term > currentTerm_ || state_ != RaftState::FOLLOWER) {
    currentTerm_ = term;
    state_ = RaftState::FOLLOWER;
    votedFor_.reset();
    // Durable before we acknowledge this leader.
    persistHardStateLocked();
  }

  leaderId_ = leaderId;
  responseTerm = currentTerm_;

  // Rule 2: Check log consistency at prevLogIndex
  if (prevLogIndex > 0) {
    uint64_t localTerm = log_.getTerm(prevLogIndex);
    if (localTerm != prevLogTerm) {
      // Log inconsistency
      LOG_DEBUG << "Log inconsistency at index " << prevLogIndex
                << ": expected term " << prevLogTerm << ", got " << localTerm;
      return;
    }
  }

  // Rule 3 & 4: Append new entries (handled by appendEntries)
  if (!entries.empty()) {
    log_.appendEntries(entries);
  }

  // Rule 5: Update commit index
  if (leaderCommit > commitIndex_) {
    uint64_t lastNew = entries.empty() ? prevLogIndex : entries.back().index;
    commitIndex_ = std::min(leaderCommit, lastNew);
    commitCond_.notify_all();
  }

  success = true;
  matchIndex = log_.lastIndex();
}

void RaftNode::persistHardStateLocked() {
  // Raft requires currentTerm and votedFor to be durable BEFORE the node
  // replies to any RPC that changed them. Persisting only at graceful shutdown
  //, which is what this used to do, lets a crashed node forget its vote
  // and vote a second time in the same term, which breaks election safety.
  if (statePath_.empty()) {
    return;
  }

  std::string buf;
  for (int i = 7; i >= 0; --i) {
    buf.push_back(static_cast<char>((currentTerm_.load() >> (i * 8)) & 0xFF));
  }
  uint32_t voted = votedFor_.value_or(UINT32_MAX);
  for (int i = 3; i >= 0; --i) {
    buf.push_back(static_cast<char>((voted >> (i * 8)) & 0xFF));
  }

  std::string tmpPath = statePath_ + ".tmp";
  int fd = ::open(tmpPath.c_str(), O_WRONLY | O_BINARY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    LOG_ERROR << "Failed to open temp state file: " << tmpPath;
    return;
  }

  bool ok = true;
  size_t written = 0;
  while (written < buf.size()) {
    ssize_t n = ::write(fd, buf.data() + written, buf.size() - written);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      ok = false;
      break;
    }
    written += static_cast<size_t>(n);
  }
  ::close(fd);

  // Temp file + rename, so a crash mid-write cannot leave a half-updated term.
  if (!ok || !atomicInstall(tmpPath, statePath_)) {
    LOG_ERROR << "Failed to persist Raft hard state to " << statePath_;
  }
}

void RaftNode::persistState(const std::string &path) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    statePath_ = path;
    persistHardStateLocked();
  }

  // Also persist log
  log_.persist(path + ".log");
}

void RaftNode::loadState(const std::string &path) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    statePath_ = path;
  }

  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    LOG_DEBUG << "No existing state file, starting fresh";
  } else {
    std::string data((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    file.close();

    if (data.size() >= 12) {
      uint64_t term = 0;
      for (int i = 0; i < 8; ++i) {
        term = (term << 8) | static_cast<uint8_t>(data[i]);
      }
      uint32_t voted = 0;
      for (int i = 0; i < 4; ++i) {
        voted = (voted << 8) | static_cast<uint8_t>(data[8 + i]);
      }

      std::lock_guard<std::mutex> lock(mutex_);
      currentTerm_ = term;
      if (voted != UINT32_MAX) {
        votedFor_ = voted;
      } else {
        votedFor_.reset();
      }
    } else {
      LOG_WARN << "Raft state file too short, ignoring: " << path;
    }
  }

  // Bind the log to its file so subsequent appends are durable.
  log_.openAt(path + ".log");

  LOG_INFO << "Loaded state: term=" << currentTerm_.load() << ", votedFor="
           << (votedFor_.has_value() ? std::to_string(votedFor_.value())
                                     : "none");
}

} // namespace shard
