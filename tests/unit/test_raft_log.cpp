#include "raft/RaftLog.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

using namespace shard;

class RaftLogTest : public ::testing::Test {
protected:
  void SetUp() override { log = std::make_unique<RaftLog>(); }

  std::unique_ptr<RaftLog> log;
};

TEST_F(RaftLogTest, AppendAndGet) {
  uint64_t idx1 = log->append(1, "cmd1");
  uint64_t idx2 = log->append(1, "cmd2");
  uint64_t idx3 = log->append(2, "cmd3");

  EXPECT_EQ(idx1, 1);
  EXPECT_EQ(idx2, 2);
  EXPECT_EQ(idx3, 3);

  auto entry1 = log->getEntry(1);
  ASSERT_TRUE(entry1.has_value());
  EXPECT_EQ(entry1->term, 1);
  EXPECT_EQ(entry1->command, "cmd1");

  auto entry3 = log->getEntry(3);
  ASSERT_TRUE(entry3.has_value());
  EXPECT_EQ(entry3->term, 2);
}

TEST_F(RaftLogTest, LastIndexAndTerm) {
  EXPECT_EQ(log->lastIndex(), 0);
  EXPECT_EQ(log->lastTerm(), 0);

  log->append(1, "cmd1");
  EXPECT_EQ(log->lastIndex(), 1);
  EXPECT_EQ(log->lastTerm(), 1);

  log->append(2, "cmd2");
  EXPECT_EQ(log->lastIndex(), 2);
  EXPECT_EQ(log->lastTerm(), 2);
}

TEST_F(RaftLogTest, GetEntriesFrom) {
  log->append(1, "cmd1");
  log->append(1, "cmd2");
  log->append(2, "cmd3");
  log->append(2, "cmd4");

  auto entries = log->getEntriesFrom(2);

  ASSERT_EQ(entries.size(), 3);
  EXPECT_EQ(entries[0].index, 2);
  EXPECT_EQ(entries[1].index, 3);
  EXPECT_EQ(entries[2].index, 4);
}

TEST_F(RaftLogTest, TruncateFrom) {
  log->append(1, "cmd1");
  log->append(1, "cmd2");
  log->append(2, "cmd3");
  log->append(2, "cmd4");

  log->truncateFrom(3);

  EXPECT_EQ(log->lastIndex(), 2);
  EXPECT_FALSE(log->getEntry(3).has_value());
  EXPECT_FALSE(log->getEntry(4).has_value());
}

TEST_F(RaftLogTest, AppendEntries) {
  log->append(1, "cmd1");

  std::vector<RaftLogEntry> entries;
  entries.push_back(RaftLogEntry(1, 2, "cmd2"));
  entries.push_back(RaftLogEntry(2, 3, "cmd3"));

  log->appendEntries(entries);

  EXPECT_EQ(log->lastIndex(), 3);

  auto entry2 = log->getEntry(2);
  ASSERT_TRUE(entry2.has_value());
  EXPECT_EQ(entry2->command, "cmd2");
}

TEST_F(RaftLogTest, AppendEntriesConflict) {
  log->append(1, "cmd1");
  log->append(1, "cmd2");
  log->append(1, "cmd3"); // This will conflict

  // Leader sends entries with different term at index 3
  std::vector<RaftLogEntry> entries;
  entries.push_back(RaftLogEntry(2, 3, "new_cmd3")); // Different term
  entries.push_back(RaftLogEntry(2, 4, "cmd4"));

  log->appendEntries(entries);

  // Entry at index 3 should be replaced
  auto entry3 = log->getEntry(3);
  ASSERT_TRUE(entry3.has_value());
  EXPECT_EQ(entry3->term, 2);
  EXPECT_EQ(entry3->command, "new_cmd3");

  EXPECT_EQ(log->lastIndex(), 4);
}

TEST_F(RaftLogTest, IsAtLeastAsUpToDate) {
  log->append(1, "cmd1");
  log->append(2, "cmd2");

  // Our log: term 2, index 2

  // Candidate with older term
  EXPECT_TRUE(log->isAtLeastAsUpToDate(1, 3));

  // Candidate with same term but shorter log
  EXPECT_TRUE(log->isAtLeastAsUpToDate(2, 1));

  // Candidate with newer term
  EXPECT_FALSE(log->isAtLeastAsUpToDate(3, 1));

  // Candidate with same term and longer log
  EXPECT_FALSE(log->isAtLeastAsUpToDate(2, 3));
}

TEST_F(RaftLogTest, GetTerm) {
  log->append(1, "cmd1");
  log->append(2, "cmd2");
  log->append(2, "cmd3");

  EXPECT_EQ(log->getTerm(0), 0);
  EXPECT_EQ(log->getTerm(1), 1);
  EXPECT_EQ(log->getTerm(2), 2);
  EXPECT_EQ(log->getTerm(3), 2);
  EXPECT_EQ(log->getTerm(100), 0); // Non-existent
}


// ---------------------------------------------------------------------------
// Durable persistence
//
// Raft's safety argument assumes currentTerm, votedFor and the log survive a
// crash. The log previously rewrote itself in full on each persist() call, ran
// only at graceful shutdown, and read command lengths straight off disk into
// resize() with no validation.
// ---------------------------------------------------------------------------

class RaftLogPersistenceTest : public ::testing::Test {
protected:
  void SetUp() override {
    testDir = "/tmp/shard_raftlog_test_" +
              std::to_string(
                  std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(testDir);
    logPath = testDir + "/raft.log";
  }

  void TearDown() override { fs::remove_all(testDir); }

  std::string testDir;
  std::string logPath;
};

TEST_F(RaftLogPersistenceTest, AppendsAreDurableWithoutAnExplicitPersist) {
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, "first");
    log.append(1, "second");
    log.append(2, "third");
    EXPECT_TRUE(log.isDurable());
    // Deliberately no persist() call: openAt() binds the log so each append is
    // already on stable media.
  }

  RaftLog reopened;
  reopened.openAt(logPath);

  EXPECT_EQ(reopened.lastIndex(), 3u);
  EXPECT_EQ(reopened.lastTerm(), 2u);

  auto entry = reopened.getEntry(2);
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->command, "second");
  EXPECT_EQ(entry->term, 1u);
}

TEST_F(RaftLogPersistenceTest, ReplicatedEntriesSurviveReopen) {
  {
    RaftLog log;
    log.openAt(logPath);
    std::vector<RaftLogEntry> entries = {
        RaftLogEntry(1, 1, "a"),
        RaftLogEntry(1, 2, "b"),
        RaftLogEntry(2, 3, "c"),
    };
    log.appendEntries(entries);
  }

  RaftLog reopened;
  reopened.openAt(logPath);
  EXPECT_EQ(reopened.lastIndex(), 3u);
  EXPECT_EQ(reopened.getEntry(3)->command, "c");
}

TEST_F(RaftLogPersistenceTest, TruncationIsPersisted) {
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, "keep1");
    log.append(1, "keep2");
    log.append(1, "drop1");
    log.append(1, "drop2");
    log.truncateFrom(3);
    EXPECT_EQ(log.lastIndex(), 2u);
  }

  RaftLog reopened;
  reopened.openAt(logPath);
  EXPECT_EQ(reopened.lastIndex(), 2u)
      << "truncation was not persisted; dropped entries came back";
  EXPECT_EQ(reopened.getEntry(2)->command, "keep2");
}

TEST_F(RaftLogPersistenceTest, ConflictingAppendTruncatesDurably) {
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, "a");
    log.append(1, "b");
    log.append(1, "c");

    // A new leader overwrites index 2 onward with a higher term.
    std::vector<RaftLogEntry> conflicting = {RaftLogEntry(5, 2, "b_prime")};
    log.appendEntries(conflicting);
    EXPECT_EQ(log.lastIndex(), 2u);
  }

  RaftLog reopened;
  reopened.openAt(logPath);
  EXPECT_EQ(reopened.lastIndex(), 2u);
  EXPECT_EQ(reopened.getEntry(2)->command, "b_prime");
  EXPECT_EQ(reopened.getEntry(2)->term, 5u);
}

TEST_F(RaftLogPersistenceTest, LargeCommandsRoundTrip) {
  std::string large(500000, 'z');
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, large);
  }

  RaftLog reopened;
  reopened.openAt(logPath);
  ASSERT_EQ(reopened.lastIndex(), 1u);
  EXPECT_EQ(reopened.getEntry(1)->command, large);
}

// A crash during an append leaves a partial record. That is expected, not
// exceptional: the valid prefix must load and the torn tail must be dropped.
TEST_F(RaftLogPersistenceTest, TornTailKeepsValidPrefix) {
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, "good1");
    log.append(1, "good2");
    log.append(1, "good3");
  }

  // Lop off part of the last record.
  auto size = fs::file_size(logPath);
  fs::resize_file(logPath, size - 6);

  RaftLog reopened;
  reopened.openAt(logPath);

  EXPECT_EQ(reopened.lastIndex(), 2u) << "expected the two intact entries";
  EXPECT_EQ(reopened.getEntry(1)->command, "good1");
  EXPECT_EQ(reopened.getEntry(2)->command, "good2");
}

TEST_F(RaftLogPersistenceTest, CorruptRecordIsRejectedByChecksum) {
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, "first");
    log.append(1, "second");
  }

  // Flip a byte inside the first record's payload, past the 24-byte header.
  {
    std::fstream f(logPath, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(40);
    char c = '!';
    f.write(&c, 1);
  }

  RaftLog reopened;
  reopened.openAt(logPath);

  // The CRC mismatch stops the load rather than admitting corrupt data.
  EXPECT_EQ(reopened.lastIndex(), 0u);
}

// The regression test for the unvalidated length field. A garbage cmdLen was
// fed straight into resize(), so a corrupt file could demand an enormous
// allocation or abort the process on load.
TEST_F(RaftLogPersistenceTest, ImplausibleCommandLengthDoesNotAllocate) {
  {
    RaftLog log;
    log.openAt(logPath);
    log.append(1, "small");
  }

  // Rewrite the first record's cmdLen field as a huge value. The CRC will no
  // longer match either, so this must be rejected on both counts rather than
  // trusted.
  {
    std::fstream f(logPath, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(24 + 8 + 16); // header + record header + term + index
    char huge[8] = {0x7F, (char)0xFF, (char)0xFF, (char)0xFF,
                    (char)0xFF, (char)0xFF, (char)0xFF, (char)0xFF};
    f.write(huge, 8);
  }

  RaftLog reopened;
  EXPECT_NO_THROW(reopened.openAt(logPath));
  EXPECT_EQ(reopened.lastIndex(), 0u);
}

TEST_F(RaftLogPersistenceTest, GarbageHeaderIsRejected) {
  {
    std::ofstream f(logPath, std::ios::binary);
    f << "this is not a raft log at all, not even close";
  }

  RaftLog log;
  EXPECT_NO_THROW(log.openAt(logPath));
  EXPECT_EQ(log.lastIndex(), 0u);
}

TEST_F(RaftLogPersistenceTest, EmptyFileLoadsAsEmptyLog) {
  { std::ofstream f(logPath, std::ios::binary); }

  RaftLog log;
  EXPECT_NO_THROW(log.openAt(logPath));
  EXPECT_EQ(log.lastIndex(), 0u);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
