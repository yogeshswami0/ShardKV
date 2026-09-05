#include "storage/WAL.h"
#include <atomic>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace shard;

class WALTest : public ::testing::Test {
protected:
  void SetUp() override {
    testDir = "/tmp/shard_wal_test_" +
              std::to_string(
                  std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(testDir);
  }

  void TearDown() override { fs::remove_all(testDir); }

  std::string testDir;
};

TEST_F(WALTest, AppendAndRecover) {
  {
    WAL wal(testDir);
    wal.appendPut("key1", "value1", 1);
    wal.appendPut("key2", "value2", 2);
    wal.appendDelete("key3", 3);
  }

  // Recover from WAL
  WAL wal2(testDir);
  auto entries = wal2.recover();

  ASSERT_EQ(entries.size(), 3);

  EXPECT_EQ(entries[0].type, WALEntryType::PUT);
  EXPECT_EQ(entries[0].key, "key1");
  EXPECT_EQ(entries[0].value, "value1");
  EXPECT_EQ(entries[0].sequenceNumber, 1);

  EXPECT_EQ(entries[1].type, WALEntryType::PUT);
  EXPECT_EQ(entries[1].key, "key2");
  EXPECT_EQ(entries[1].value, "value2");

  EXPECT_EQ(entries[2].type, WALEntryType::DEL);
  EXPECT_EQ(entries[2].key, "key3");
}

// rotate() starts a new segment but deliberately keeps the old one. Deleting
// at rotation time discarded the WAL records of any write that had already
// landed in the new MemTable while the flush was still running, an
// acknowledged write with no remaining record anywhere on disk.
TEST_F(WALTest, RotateStartsNewSegmentWithoutDiscardingTheOldOne) {
  WAL wal(testDir);

  wal.appendPut("key1", "value1", 1);
  uint64_t closed = wal.rotate();
  wal.appendPut("key2", "value2", 2);

  WAL wal2(testDir);
  auto entries = wal2.recover();

  ASSERT_EQ(entries.size(), 2u) << "rotation must not drop the closed segment";
  EXPECT_EQ(entries[0].key, "key1");
  EXPECT_EQ(entries[1].key, "key2");
  EXPECT_EQ(closed, 0u);
}

TEST_F(WALTest, SegmentsAreDroppedOnlyWhenExplicitlyDiscarded) {
  WAL wal(testDir);

  wal.appendPut("old", "value", 1);
  uint64_t closed = wal.rotate();
  wal.appendPut("new", "value", 2);

  // Simulates the flush completing: the closed segment's data is now durable
  // in an SSTable, so it may go.
  wal.removeSegmentsUpTo(closed);

  WAL wal2(testDir);
  auto entries = wal2.recover();

  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].key, "new");
}

TEST_F(WALTest, DiscardNeverRemovesTheActiveSegment) {
  WAL wal(testDir);

  wal.appendPut("live", "value", 1);

  // An over-broad request must still leave the segment being written alone.
  wal.removeSegmentsUpTo(9999);
  wal.appendPut("also_live", "value", 2);

  WAL wal2(testDir);
  auto entries = wal2.recover();
  EXPECT_EQ(entries.size(), 2u) << "the active segment was deleted underneath";
}

TEST_F(WALTest, SequenceNumber) {
  WAL wal(testDir);

  wal.appendPut("key1", "value1", 100);
  wal.appendPut("key2", "value2", 200);

  EXPECT_GE(wal.currentSequence(), 200);
}

TEST_F(WALTest, EmptyRecovery) {
  WAL wal(testDir);
  auto entries = wal.recover();

  EXPECT_TRUE(entries.empty());
}

TEST_F(WALTest, LargeValue) {
  WAL wal(testDir);

  std::string largeValue(100000, 'x'); // 100 KB
  wal.appendPut("large_key", largeValue, 1);

  WAL wal2(testDir);
  auto entries = wal2.recover();

  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].value.size(), 100000);
  EXPECT_EQ(entries[0].value, largeValue);
}

TEST_F(WALTest, CorruptedEntry) {
  {
    WAL wal(testDir);
    wal.appendPut("key1", "value1", 1);
  }

  // Corrupt the WAL file
  auto files = fs::directory_iterator(testDir);
  for (const auto &entry : files) {
    if (entry.path().extension() == ".wal") {
      std::fstream file(entry.path(),
                        std::ios::in | std::ios::out | std::ios::binary);
      file.seekp(10); // Corrupt middle of entry
      file.write("CORRUPT", 7);
    }
  }

  // Recovery should stop at corrupted entry
  WAL wal2(testDir);
  auto entries = wal2.recover();

  // May recover 0 entries due to CRC check
  EXPECT_LE(entries.size(), 1);
}


// ---------------------------------------------------------------------------
// Durability
//
// A note on what these can and cannot prove. Losing acknowledged writes needs a
// *machine* crash: a process crash leaves the OS page cache intact, so data that
// only reached the page cache still survives. That is exactly why the original
// bug went unnoticed, ofstream::flush() pushes to the page cache, which looks
// durable under every test you can write without cutting power.
//
// So we verify durability from two directions:
//   1. ProcessCrash*  , records leave the user-space buffer before the ack.
//   2. *Sync*         , a real sync call is issued before the ack, and group
//                         commit batches those calls across concurrent writers.
// Genuine power-loss testing needs filesystem fault injection (dm-flakey and
// similar), which is out of reach on this platform.
// ---------------------------------------------------------------------------

TEST_F(WALTest, ProcessCrashDoesNotLoseAcknowledgedWrites) {
  pid_t pid = fork();
  ASSERT_GE(pid, 0) << "fork failed";

  if (pid == 0) {
    // Child: append, then die without unwinding. _exit skips destructors,
    // atexit handlers and any buffered-stream flush, so anything still sitting
    // in a user-space buffer is lost here.
    WAL wal(testDir);
    wal.appendPut("durable1", "value1", 1);
    wal.appendPut("durable2", "value2", 2);
    wal.appendDelete("durable3", 3);
    _exit(0);
  }

  int status = 0;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));

  WAL wal(testDir);
  auto entries = wal.recover();

  ASSERT_EQ(entries.size(), 3u);
  EXPECT_EQ(entries[0].key, "durable1");
  EXPECT_EQ(entries[0].value, "value1");
  EXPECT_EQ(entries[1].key, "durable2");
  EXPECT_EQ(entries[2].type, WALEntryType::DEL);
}

TEST_F(WALTest, GroupModeIssuesRealSyncs) {
  WAL wal(testDir, WALSyncMode::GROUP);
  EXPECT_EQ(wal.syncCount(), 0u);

  wal.appendPut("k", "v", 1);

  // A single writer with nothing to batch against must still sync before the
  // append returns.
  EXPECT_GE(wal.syncCount(), 1u);
}

TEST_F(WALTest, NoneModeSkipsSyncEntirely) {
  WAL wal(testDir, WALSyncMode::NONE);

  for (int i = 0; i < 50; ++i) {
    wal.appendPut("k" + std::to_string(i), "v", i + 1);
  }

  EXPECT_EQ(wal.syncCount(), 0u);

  // The records are still written and readable; only the durability guarantee
  // is dropped.
  WAL wal2(testDir, WALSyncMode::NONE);
  EXPECT_EQ(wal2.recover().size(), 50u);
}

TEST_F(WALTest, AlwaysModeSyncsEveryAppend) {
  WAL wal(testDir, WALSyncMode::ALWAYS);

  const int kAppends = 20;
  for (int i = 0; i < kAppends; ++i) {
    wal.appendPut("k" + std::to_string(i), "v", i + 1);
  }

  // Serial appends in ALWAYS mode have nothing to batch with.
  EXPECT_EQ(wal.syncCount(), static_cast<uint64_t>(kAppends));
}

TEST_F(WALTest, GroupCommitBatchesConcurrentWriters) {
  WAL wal(testDir, WALSyncMode::GROUP);

  const int kThreads = 8;
  const int kPerThread = 50;
  const int kTotal = kThreads * kPerThread;

  std::atomic<uint64_t> seq{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&wal, &seq, t]() {
      for (int i = 0; i < kPerThread; ++i) {
        uint64_t s = ++seq;
        wal.appendPut("t" + std::to_string(t) + "_k" + std::to_string(i),
                      "value", s);
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  // Every record is durable and present...
  WAL wal2(testDir, WALSyncMode::GROUP);
  EXPECT_EQ(wal2.recover().size(), static_cast<size_t>(kTotal));

  // ...but concurrent writers shared syncs rather than each paying for one.
  // That amortization is the entire reason durability stays affordable here.
  EXPECT_GE(wal.syncCount(), 1u);
  EXPECT_LT(wal.syncCount(), static_cast<uint64_t>(kTotal));
}

TEST_F(WALTest, SyncModeParsing) {
  EXPECT_EQ(parseSyncMode("group"), WALSyncMode::GROUP);
  EXPECT_EQ(parseSyncMode("always"), WALSyncMode::ALWAYS);
  EXPECT_EQ(parseSyncMode("none"), WALSyncMode::NONE);
  // Anything unrecognised falls back to the durable default rather than
  // silently disabling durability.
  EXPECT_EQ(parseSyncMode("nonsense"), WALSyncMode::GROUP);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
