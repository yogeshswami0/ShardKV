#include "raft/Command.h"
#include "raft/RaftNode.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;
using namespace shard;
using namespace std::chrono_literals;

/**
 * A three-node Raft cluster wired up in-process.
 *
 * The RPC seams are std::function, so the whole cluster runs without gRPC,
 * sockets or containers, which is exactly what makes leader election and
 * commit behaviour testable at all. Peers can be partitioned by flipping a
 * flag, so failover is deterministic rather than timing-dependent.
 */
class RaftClusterTest : public ::testing::Test {
protected:
  struct Node {
    std::shared_ptr<RaftNode> raft;
    std::vector<std::string> applied;
    std::mutex appliedMutex;
    std::atomic<bool> reachable{true};

    // AppendEntries calls delivered TO this node. Used to measure the heartbeat
    // cadence a follower actually observes.
    std::atomic<int> appendsReceived{0};

    // Pre-vote requests delivered TO this node.
    std::atomic<int> preVotesReceived{0};
  };

  // How long an RPC to an unreachable peer blocks before failing.
  //
  // A real gRPC call to a node that is down does not fail instantly: the
  // packets are dropped and the call blocks until its deadline. Returning
  // false immediately, which this harness used to do, hides every cost of
  // having a dead peer, which is exactly the thing under test here.
  static constexpr auto kSimulatedRpcTimeout = std::chrono::milliseconds(100);

  void SetUp() override {
    testDir = "/tmp/shard_cluster_test_" +
              std::to_string(
                  std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(testDir);

    _putenv_s("SHARD_ELECTION_TIMEOUT_MIN_MS", "150");
    _putenv_s("SHARD_ELECTION_TIMEOUT_MAX_MS", "300");
    _putenv_s("SHARD_HEARTBEAT_MS", "30");

    for (uint32_t id = 1; id <= 3; ++id) {
      nodes_[id] = std::make_unique<Node>();
    }

    for (uint32_t id = 1; id <= 3; ++id) {
      std::vector<uint32_t> peers;
      for (uint32_t p = 1; p <= 3; ++p) {
        if (p != id)
          peers.push_back(p);
      }

      auto node = std::make_shared<RaftNode>(id, peers);
      nodes_[id]->raft = node;

      node->setApplyCallback([this, id](uint64_t, const std::string &cmd) {
        auto parsed = Command::decode(cmd);
        if (!parsed.has_value())
          return;
        std::lock_guard<std::mutex> lock(nodes_[id]->appliedMutex);
        nodes_[id]->applied.push_back(parsed->key);
      });

      node->setRequestVoteRPC([this, id](uint32_t peerId, uint64_t term,
                                         uint32_t candidateId,
                                         uint64_t lastLogIndex,
                                         uint64_t lastLogTerm, bool preVote,
                                         uint64_t &responseTerm,
                                         bool &voteGranted) {
        if (!linkUp(id, peerId)) {
          std::this_thread::sleep_for(kSimulatedRpcTimeout);
          return false;
        }
        if (preVote) {
          nodes_[peerId]->preVotesReceived.fetch_add(1);
        }
        nodes_[peerId]->raft->handleRequestVote(term, candidateId, lastLogIndex,
                                                lastLogTerm, preVote,
                                                responseTerm, voteGranted);
        return true;
      });

      node->setAppendEntriesRPC(
          [this, id](uint32_t peerId, uint64_t term, uint32_t leaderId,
                     uint64_t prevLogIndex, uint64_t prevLogTerm,
                     const std::vector<RaftLogEntry> &entries,
                     uint64_t leaderCommit, uint64_t &responseTerm,
                     bool &success, uint64_t &matchIndex) {
            if (!linkUp(id, peerId)) {
              std::this_thread::sleep_for(kSimulatedRpcTimeout);
              return false;
            }
            nodes_[peerId]->appendsReceived.fetch_add(1);
            nodes_[peerId]->raft->handleAppendEntries(
                term, leaderId, prevLogIndex, prevLogTerm, entries,
                leaderCommit, responseTerm, success, matchIndex);
            return true;
          });

      node->loadState(testDir + "/node" + std::to_string(id));
    }

    for (auto &[id, node] : nodes_) {
      node->raft->start();
    }
  }

  void TearDown() override {
    for (auto &[id, node] : nodes_) {
      node->raft->stop();
    }
    nodes_.clear();
    fs::remove_all(testDir);
  }

  bool linkUp(uint32_t from, uint32_t to) const {
    auto f = nodes_.find(from);
    auto t = nodes_.find(to);
    if (f == nodes_.end() || t == nodes_.end())
      return false;

    if (!f->second->reachable.load() || !t->second->reachable.load())
      return false;

    // Directional cuts model asymmetric partitions, where traffic flows one way
    // but not the other. Symmetric reachability alone cannot express the case
    // leader stickiness exists for.
    std::lock_guard<std::mutex> lock(cutLinksMutex_);
    return cutLinks_.count({from, to}) == 0;
  }

  // Sever traffic from `from` to `to`, leaving the reverse direction intact.
  void cutLink(uint32_t from, uint32_t to) {
    std::lock_guard<std::mutex> lock(cutLinksMutex_);
    cutLinks_.insert({from, to});
  }

  void restoreLink(uint32_t from, uint32_t to) {
    std::lock_guard<std::mutex> lock(cutLinksMutex_);
    cutLinks_.erase({from, to});
  }

  // Waits for exactly one leader to emerge and returns its id, or 0.
  uint32_t waitForLeader(std::chrono::milliseconds timeout = 5s) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      uint32_t leader = 0;
      int count = 0;
      for (auto &[id, node] : nodes_) {
        if (node->reachable.load() && node->raft->isLeader()) {
          leader = id;
          count++;
        }
      }
      if (count == 1)
        return leader;
      std::this_thread::sleep_for(20ms);
    }
    return 0;
  }

  std::string testDir;
  std::map<uint32_t, std::unique_ptr<Node>> nodes_;

  struct LinkHash {
    size_t operator()(const std::pair<uint32_t, uint32_t> &p) const {
      return (static_cast<size_t>(p.first) << 32) ^ p.second;
    }
  };

  mutable std::mutex cutLinksMutex_;
  std::unordered_set<std::pair<uint32_t, uint32_t>, LinkHash> cutLinks_;
};

TEST_F(RaftClusterTest, ElectsExactlyOneLeader) {
  uint32_t leader = waitForLeader();
  ASSERT_NE(leader, 0u) << "no leader elected";

  int leaders = 0;
  for (auto &[id, node] : nodes_) {
    if (node->raft->isLeader())
      leaders++;
  }
  EXPECT_EQ(leaders, 1) << "split brain: more than one leader";
}

// The core regression test for the commit wait.
//
// waitForCommit() used to sleep 50ms and return true without ever consulting
// the commit index. Here the entry must genuinely reach a majority and be
// applied before the wait returns.
TEST_F(RaftClusterTest, WaitForAppliedReturnsOnlyAfterMajorityApplies) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  auto &leader = nodes_[leaderId]->raft;

  auto result = leader->propose(Command::put("alpha", "1").encode());
  ASSERT_TRUE(result.success);

  ASSERT_TRUE(leader->waitForApplied(result.index, result.term, 5s))
      << "entry was never applied";

  // The leader has applied it...
  {
    std::lock_guard<std::mutex> lock(nodes_[leaderId]->appliedMutex);
    EXPECT_EQ(nodes_[leaderId]->applied.size(), 1u);
    EXPECT_EQ(nodes_[leaderId]->applied[0], "alpha");
  }

  // ...and the leader considers it committed.
  EXPECT_GE(leader->commitIndex(), result.index);

  // Commitment means a majority holds the entry in its LOG. Followers only
  // learn the advanced commit index on the next heartbeat, so asserting on
  // their commitIndex here would be checking the wrong property.
  int replicasWithEntry = 0;
  for (auto &[id, node] : nodes_) {
    if (node->raft->logLastIndex() >= result.index &&
        node->raft->logTermAt(result.index) == result.term) {
      replicasWithEntry++;
    }
  }
  EXPECT_GE(replicasWithEntry, 2)
      << "entry was acknowledged without reaching a majority of logs";
}

TEST_F(RaftClusterTest, CommittedEntriesReachEveryFollower) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  auto &leader = nodes_[leaderId]->raft;

  for (int i = 0; i < 5; ++i) {
    auto result =
        leader->propose(Command::put("k" + std::to_string(i), "v").encode());
    ASSERT_TRUE(result.success);
    ASSERT_TRUE(leader->waitForApplied(result.index, result.term, 5s));
  }

  // Followers apply asynchronously; give the heartbeats a moment to carry the
  // commit index across.
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline) {
    bool allCaughtUp = true;
    for (auto &[id, node] : nodes_) {
      std::lock_guard<std::mutex> lock(node->appliedMutex);
      if (node->applied.size() < 5)
        allCaughtUp = false;
    }
    if (allCaughtUp)
      break;
    std::this_thread::sleep_for(20ms);
  }

  for (auto &[id, node] : nodes_) {
    std::lock_guard<std::mutex> lock(node->appliedMutex);
    EXPECT_EQ(node->applied.size(), 5u)
        << "node " << id << " did not apply every committed entry";
  }
}

// Every node must apply a given index exactly once. The old code had the
// leader write to storage directly in addition to the apply callback, so each
// leader write landed twice.
TEST_F(RaftClusterTest, EntriesAreAppliedExactlyOnce) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  auto &leader = nodes_[leaderId]->raft;

  const int kWrites = 10;
  for (int i = 0; i < kWrites; ++i) {
    auto result =
        leader->propose(Command::put("key" + std::to_string(i), "v").encode());
    ASSERT_TRUE(result.success);
    ASSERT_TRUE(leader->waitForApplied(result.index, result.term, 5s));
  }

  std::lock_guard<std::mutex> lock(nodes_[leaderId]->appliedMutex);
  EXPECT_EQ(nodes_[leaderId]->applied.size(), static_cast<size_t>(kWrites))
      << "leader applied entries more than once";
}

TEST_F(RaftClusterTest, ClusterReelectsAfterLeaderIsPartitioned) {
  uint32_t oldLeader = waitForLeader();
  ASSERT_NE(oldLeader, 0u);

  // Cut the leader off from both peers.
  nodes_[oldLeader]->reachable.store(false);

  uint32_t newLeader = waitForLeader(10s);
  ASSERT_NE(newLeader, 0u) << "surviving nodes failed to elect a new leader";
  EXPECT_NE(newLeader, oldLeader);

  // The new leader can still make progress with the remaining majority.
  auto &leader = nodes_[newLeader]->raft;
  auto result = leader->propose(Command::put("after_failover", "1").encode());
  ASSERT_TRUE(result.success);
  EXPECT_TRUE(leader->waitForApplied(result.index, result.term, 5s))
      << "new leader could not commit with a majority";
}

// A partitioned leader cannot commit, so a write proposed to it must NOT be
// acknowledged. This is precisely the case the old sleep-and-return-true made
// indistinguishable from success.
TEST_F(RaftClusterTest, PartitionedLeaderDoesNotAcknowledgeWrites) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  nodes_[leaderId]->reachable.store(false);

  auto &leader = nodes_[leaderId]->raft;
  auto result = leader->propose(Command::put("doomed", "1").encode());

  if (result.success) {
    EXPECT_FALSE(leader->waitForApplied(result.index, result.term, 1500ms))
        << "a partitioned leader acknowledged a write it could not commit";
  }
}

TEST_F(RaftClusterTest, LeadershipConfirmationFailsWhenPartitioned) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  EXPECT_TRUE(nodes_[leaderId]->raft->confirmLeadership(1s))
      << "a healthy leader could not confirm its quorum";

  nodes_[leaderId]->reachable.store(false);

  EXPECT_FALSE(nodes_[leaderId]->raft->confirmLeadership(500ms))
      << "a partitioned leader confirmed leadership it no longer holds";
}

// becomeFollower() used to join the heartbeat thread from inside that same
// thread, and becomeLeader() assigned over a still-joinable std::thread. Both
// are fatal. Repeated leadership churn exercises those transitions.
TEST_F(RaftClusterTest, RepeatedLeadershipChurnDoesNotCrash) {
  for (int round = 0; round < 3; ++round) {
    uint32_t leaderId = waitForLeader(10s);
    ASSERT_NE(leaderId, 0u) << "no leader in round " << round;

    nodes_[leaderId]->reachable.store(false);
    std::this_thread::sleep_for(400ms);
    nodes_[leaderId]->reachable.store(true);
    std::this_thread::sleep_for(200ms);
  }

  EXPECT_NE(waitForLeader(10s), 0u) << "cluster did not stabilise";
}


// ---------------------------------------------------------------------------
// Leader stability with a degraded peer
// ---------------------------------------------------------------------------

// Measures the mechanism directly rather than waiting for flakiness to show up.
//
// A leader's heartbeat round is issued to every peer in parallel but waits for
// all of them, so one unreachable peer stretches the whole round to that peer's
// RPC deadline. The healthy follower still gets its heartbeat promptly, but it
// gets the NEXT one a deadline later, its observed cadence collapses even
// though nothing is wrong with it.
TEST_F(RaftClusterTest, HeartbeatCadenceIsIndependentOfAnUnreachablePeer) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  std::vector<uint32_t> followers;
  for (auto &[id, node] : nodes_) {
    if (id != leaderId)
      followers.push_back(id);
  }
  ASSERT_EQ(followers.size(), 2u);

  const uint32_t observed = followers[0];
  const uint32_t victim = followers[1];
  const auto kWindow = 1000ms;

  // Baseline: how many heartbeats the observed follower gets when all is well.
  nodes_[observed]->appendsReceived.store(0);
  std::this_thread::sleep_for(kWindow);
  int baseline = nodes_[observed]->appendsReceived.load();
  ASSERT_GT(baseline, 5) << "no heartbeats observed even in the healthy case";

  // Now take the OTHER follower away. Nothing about the observed follower or
  // its link to the leader has changed.
  nodes_[victim]->reachable.store(false);
  std::this_thread::sleep_for(200ms); // let a round or two turn over

  nodes_[observed]->appendsReceived.store(0);
  std::this_thread::sleep_for(kWindow);
  int degraded = nodes_[observed]->appendsReceived.load();

  // Some drop is tolerable; a collapse is not. The leader must not let one
  // dead peer dictate how often it talks to a healthy one.
  EXPECT_GE(degraded, baseline * 7 / 10)
      << "heartbeat cadence to a healthy follower fell from " << baseline
      << "/s to " << degraded << "/s because a different peer went down";
}

// The user-visible consequence: losing a follower must not cost the cluster its
// leader. Killing a follower leaves a quorum intact, so there is no legitimate
// reason for the term to advance at all.
TEST_F(RaftClusterTest, LosingAFollowerDoesNotTriggerReelection) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  uint32_t victim = 0;
  for (auto &[id, node] : nodes_) {
    if (id != leaderId) {
      victim = id;
      break;
    }
  }
  ASSERT_NE(victim, 0u);

  nodes_[victim]->reachable.store(false);
  std::this_thread::sleep_for(300ms); // settle

  ASSERT_TRUE(nodes_[leaderId]->raft->isLeader())
      << "leader stepped down when a follower became unreachable";

  uint64_t stableTerm = nodes_[leaderId]->raft->currentTerm();

  // Watch for a few seconds. Every re-election bumps the term, so a constant
  // term is exactly "no elections happened".
  auto deadline = std::chrono::steady_clock::now() + 3s;
  while (std::chrono::steady_clock::now() < deadline) {
    ASSERT_EQ(nodes_[leaderId]->raft->currentTerm(), stableTerm)
        << "term advanced while a quorum was still intact, the surviving "
           "follower timed out waiting for heartbeats";
    ASSERT_TRUE(nodes_[leaderId]->raft->isLeader())
        << "leader lost leadership with a quorum still available";
    std::this_thread::sleep_for(50ms);
  }
}


// ---------------------------------------------------------------------------
// Pre-vote and CheckQuorum
//
// Both come from the Raft dissertation and are standard in production
// implementations (etcd, CockroachDB, TiKV all enable them). They address
// liveness problems the core algorithm has under partial partitions.
// ---------------------------------------------------------------------------

// The disruptive-server problem pre-vote exists to solve.
//
// A node that is partitioned keeps timing out and campaigning, incrementing its
// term each round. When it rejoins, its inflated term forces the healthy leader
// to step down, even though that leader was serving a perfectly good quorum
// the whole time. One flapping node can unseat a working cluster indefinitely.
//
// With pre-vote it never gets that far: it cannot win a trial election while
// isolated, so its term never moves.
TEST_F(RaftClusterTest, IsolatedNodeDoesNotInflateItsTermWhileAway) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  uint32_t victim = 0;
  for (auto &[id, node] : nodes_) {
    if (id != leaderId) {
      victim = id;
      break;
    }
  }
  ASSERT_NE(victim, 0u);

  uint64_t termBefore = nodes_[victim]->raft->currentTerm();

  // Isolate it long enough for several election timeouts to fire.
  nodes_[victim]->reachable.store(false);
  std::this_thread::sleep_for(3s);

  uint64_t termAfter = nodes_[victim]->raft->currentTerm();

  EXPECT_EQ(termAfter, termBefore)
      << "an isolated node inflated its term from " << termBefore << " to "
      << termAfter << " while away; on rejoining it would depose a healthy "
         "leader";
}

TEST_F(RaftClusterTest, RejoiningNodeDoesNotDisturbTheLeader) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  uint32_t victim = 0;
  for (auto &[id, node] : nodes_) {
    if (id != leaderId) {
      victim = id;
      break;
    }
  }

  uint64_t leaderTerm = nodes_[leaderId]->raft->currentTerm();

  // Away long enough to campaign repeatedly, then back.
  nodes_[victim]->reachable.store(false);
  std::this_thread::sleep_for(2s);
  nodes_[victim]->reachable.store(true);
  std::this_thread::sleep_for(1s);

  EXPECT_TRUE(nodes_[leaderId]->raft->isLeader())
      << "the leader was deposed by a node rejoining the cluster";
  EXPECT_EQ(nodes_[leaderId]->raft->currentTerm(), leaderTerm)
      << "a rejoining node forced a term change";
}

// Leader stickiness under an asymmetric partition, which is the case it exists
// for.
//
// One follower stops receiving the leader's heartbeats while every other link
// stays up. It times out and campaigns, and its log is perfectly current, so a
// plain up-to-dateness check would grant it a vote. The other follower is still
// hearing from the leader, so it must refuse anyway. Otherwise a node with one
// broken link can unseat a leader that is serving everyone else correctly.
TEST_F(RaftClusterTest, AsymmetricPartitionDoesNotUnseatAHealthyLeader) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  std::vector<uint32_t> followers;
  for (auto &[id, node] : nodes_) {
    if (id != leaderId)
      followers.push_back(id);
  }
  ASSERT_EQ(followers.size(), 2u);

  const uint32_t deafened = followers[0];
  uint64_t stableTerm = nodes_[leaderId]->raft->currentTerm();

  // Replicate an entry first so every log is current. The deafened follower
  // must be refused on stickiness grounds, not because its log is behind.
  auto result =
      nodes_[leaderId]->raft->propose(Command::put("shared", "1").encode());
  ASSERT_TRUE(result.success);
  ASSERT_TRUE(
      nodes_[leaderId]->raft->waitForApplied(result.index, result.term, 5s));
  std::this_thread::sleep_for(300ms);

  // Cut only leader to deafened. The reverse direction, and both links to the
  // other follower, stay up.
  cutLink(leaderId, deafened);
  nodes_[followers[1]]->preVotesReceived.store(0);

  std::this_thread::sleep_for(3s);

  EXPECT_GT(nodes_[followers[1]]->preVotesReceived.load(), 0)
      << "the deafened follower never campaigned, so the rule was never "
         "exercised";

  EXPECT_TRUE(nodes_[leaderId]->raft->isLeader())
      << "a follower with one broken link unseated a leader that was serving "
         "the rest of the cluster";
  EXPECT_EQ(nodes_[leaderId]->raft->currentTerm(), stableTerm)
      << "an asymmetric partition forced a term change";

  restoreLink(leaderId, deafened);
}

// CheckQuorum: a partitioned leader has nothing arriving to tell it that it has
// been deposed, so it must notice on its own and step down. Otherwise it
// lingers as "leader" indefinitely and misreports the cluster.
TEST_F(RaftClusterTest, PartitionedLeaderStepsDownOnItsOwn) {
  uint32_t leaderId = waitForLeader();
  ASSERT_NE(leaderId, 0u);

  nodes_[leaderId]->reachable.store(false);

  // Give it several election timeouts to observe that nobody is answering.
  auto deadline = std::chrono::steady_clock::now() + 6s;
  bool steppedDown = false;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!nodes_[leaderId]->raft->isLeader()) {
      steppedDown = true;
      break;
    }
    std::this_thread::sleep_for(50ms);
  }

  EXPECT_TRUE(steppedDown)
      << "a leader that could not reach any peer still believed it was leader";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
