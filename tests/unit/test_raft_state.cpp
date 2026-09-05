#include "raft/RaftNode.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

namespace fs = std::filesystem;
using namespace shard;

// Raft's election-safety argument rests on currentTerm and votedFor surviving a
// crash. persistState() previously ran only at graceful shutdown (from main()'s
// exit path), so a node that crashed mid-term forgot its vote and could vote a
// second time in that same term, which permits two leaders.
class RaftHardStateTest : public ::testing::Test {
protected:
  void SetUp() override {
    testDir = "/tmp/shard_raftstate_test_" +
              std::to_string(
                  std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(testDir);
    statePath = testDir + "/raft_state";
  }

  void TearDown() override { fs::remove_all(testDir); }

  std::string testDir;
  std::string statePath;
};

TEST_F(RaftHardStateTest, GrantedVoteSurvivesWithoutGracefulShutdown) {
  const uint64_t kTerm = 7;
  const uint32_t kCandidate = 2;

  {
    RaftNode node(1, {2, 3});
    node.loadState(statePath);

    uint64_t responseTerm = 0;
    bool granted = false;
    node.handleRequestVote(kTerm, kCandidate, 0, 0, /*preVote=*/false,
                              responseTerm, granted);

    ASSERT_TRUE(granted) << "precondition: the vote is granted";
    EXPECT_EQ(node.currentTerm(), kTerm);
    // No persistState() call here, the vote must already be durable.
  }

  // A fresh node reading the same directory, as after a restart.
  RaftNode restarted(1, {2, 3});
  restarted.loadState(statePath);

  EXPECT_EQ(restarted.currentTerm(), kTerm) << "term was lost across restart";

  // The vote is remembered, so a *different* candidate in the same term is
  // refused. Without durable votedFor this would be granted, allowing a second
  // leader for term 7.
  uint64_t responseTerm = 0;
  bool granted = true;
  restarted.handleRequestVote(kTerm, /*candidateId=*/3, 0, 0,
                              /*preVote=*/false, responseTerm, granted);

  EXPECT_FALSE(granted)
      << "node double-voted in term " << kTerm << " after restart";
}

TEST_F(RaftHardStateTest, SameCandidateMayRetryInTheSameTerm) {
  {
    RaftNode node(1, {2, 3});
    node.loadState(statePath);
    uint64_t responseTerm = 0;
    bool granted = false;
    node.handleRequestVote(4, 2, 0, 0, /*preVote=*/false,
                              responseTerm, granted);
    ASSERT_TRUE(granted);
  }

  RaftNode restarted(1, {2, 3});
  restarted.loadState(statePath);

  // A retry from the candidate we already voted for is idempotent, not a
  // double vote.
  uint64_t responseTerm = 0;
  bool granted = false;
  restarted.handleRequestVote(4, 2, 0, 0, /*preVote=*/false,
                              responseTerm, granted);
  EXPECT_TRUE(granted);
}

TEST_F(RaftHardStateTest, TermFromAppendEntriesIsDurable) {
  {
    RaftNode node(1, {2, 3});
    node.loadState(statePath);

    uint64_t responseTerm = 0;
    bool success = false;
    uint64_t matchIndex = 0;
    node.handleAppendEntries(9, /*leaderId=*/2, 0, 0, {}, 0, responseTerm,
                             success, matchIndex);
    ASSERT_TRUE(success);
  }

  RaftNode restarted(1, {2, 3});
  restarted.loadState(statePath);
  EXPECT_EQ(restarted.currentTerm(), 9u);

  // Having already reached term 9, a stale candidate at term 8 is refused.
  uint64_t responseTerm = 0;
  bool granted = true;
  restarted.handleRequestVote(8, 3, 0, 0, /*preVote=*/false,
                              responseTerm, granted);
  EXPECT_FALSE(granted) << "accepted a vote request from a stale term";
}

TEST_F(RaftHardStateTest, ReplicatedEntriesSurviveRestart) {
  {
    RaftNode node(1, {2, 3});
    node.loadState(statePath);

    std::vector<RaftLogEntry> entries = {RaftLogEntry(3, 1, "PUT|1|a|1|x"),
                                         RaftLogEntry(3, 2, "PUT|1|b|1|y")};
    uint64_t responseTerm = 0;
    bool success = false;
    uint64_t matchIndex = 0;
    node.handleAppendEntries(3, 2, 0, 0, entries, 0, responseTerm, success,
                             matchIndex);
    ASSERT_TRUE(success);
    EXPECT_EQ(matchIndex, 2u);
  }

  RaftNode restarted(1, {2, 3});
  restarted.loadState(statePath);

  // The follower's log is intact, so it can still vote based on its real
  // up-to-dateness rather than looking empty.
  uint64_t responseTerm = 0;
  bool granted = true;
  restarted.handleRequestVote(4, /*candidateId=*/3, /*lastLogIndex=*/0,
                              /*lastLogTerm=*/0, /*preVote=*/false,
                              responseTerm, granted);
  EXPECT_FALSE(granted)
      << "voted for a candidate with a shorter log; our log was lost";
}

TEST_F(RaftHardStateTest, MissingStateFileStartsClean) {
  RaftNode node(1, {2, 3});
  EXPECT_NO_THROW(node.loadState(testDir + "/does_not_exist"));
  EXPECT_EQ(node.currentTerm(), 0u);
}

TEST_F(RaftHardStateTest, TruncatedStateFileIsIgnored) {
  {
    RaftNode node(1, {2, 3});
    node.loadState(statePath);
    uint64_t responseTerm = 0;
    bool granted = false;
    node.handleRequestVote(5, 2, 0, 0, /*preVote=*/false,
                              responseTerm, granted);
  }

  fs::resize_file(statePath, 3);

  RaftNode restarted(1, {2, 3});
  EXPECT_NO_THROW(restarted.loadState(statePath));
  EXPECT_EQ(restarted.currentTerm(), 0u);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
