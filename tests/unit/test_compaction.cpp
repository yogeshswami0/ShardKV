#include "storage/Compaction.h"
#include "storage/SSTableReader.h"
#include "storage/SSTableWriter.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace shard;

class CompactionTest : public ::testing::Test {
protected:
  void SetUp() override {
    testDir = "/tmp/shard_compaction_test_" +
              std::to_string(
                  std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(testDir);
  }

  void TearDown() override { fs::remove_all(testDir); }

  // Builds an SSTable at `level` containing the given entries.
  std::shared_ptr<SSTableReader> makeTable(
      Compaction &c, int level,
      const std::vector<std::tuple<std::string, std::string, uint64_t, bool>>
          &entries) {
    std::string path = c.nextSSTablePath(level);
    SSTableWriter writer(path);
    for (const auto &[key, value, seq, deleted] : entries) {
      writer.add(key, value, seq, deleted);
    }
    writer.finish();
    return std::make_shared<SSTableReader>(path);
  }

  // Reads a key the way StorageEngine does: walk getAllSSTables() in order and
  // take the first hit.
  std::optional<std::string> lookup(Compaction &c, const std::string &key) {
    for (const auto &sst : c.getAllSSTables()) {
      if (!sst->mightContain(key))
        continue;
      auto entry = sst->get(key);
      if (entry.has_value()) {
        if (entry->deleted)
          return std::nullopt;
        return entry->value;
      }
    }
    return std::nullopt;
  }

  std::string testDir;
};

// Regression test for level-0 read order.
//
// getAllSSTables() walked each level in insertion order and addSSTable()
// appended, so level 0 came back oldest-first. A reader taking the first hit
// therefore got the *stalest* version of a re-written key, not merely a
// possibly-stale one.
TEST_F(CompactionTest, Level0ReturnsNewestVersionOfAKey) {
  Compaction c(testDir, 10);

  // Three flushes of the same key, oldest first, exactly as a running engine
  // would produce them.
  c.addSSTable(makeTable(c, 0, {{"key", "v1", 1, false}}), 0);
  c.addSSTable(makeTable(c, 0, {{"key", "v2", 2, false}}), 0);
  c.addSSTable(makeTable(c, 0, {{"key", "v3", 3, false}}), 0);

  auto value = lookup(c, "key");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), "v3") << "level 0 returned a stale version";
}

TEST_F(CompactionTest, Level0OrderSurvivesReopen) {
  {
    Compaction c(testDir, 10);
    c.addSSTable(makeTable(c, 0, {{"key", "old", 1, false}}), 0);
    c.addSSTable(makeTable(c, 0, {{"key", "new", 2, false}}), 0);
  }

  // Reopened from disk, where insertion order is gone and only the sequence
  // numbers in the footers can establish recency.
  Compaction reopened(testDir, 10);
  auto value = lookup(reopened, "key");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), "new") << "recency order lost across restart";
}

TEST_F(CompactionTest, NewerTombstoneHidesOlderValue) {
  Compaction c(testDir, 10);

  c.addSSTable(makeTable(c, 0, {{"key", "value", 1, false}}), 0);
  c.addSSTable(makeTable(c, 0, {{"key", "", 2, true}}), 0);

  EXPECT_FALSE(lookup(c, "key").has_value()) << "deleted key still readable";
}

// Regression test for the compaction data-loss race.
//
// compactLevel() snapshotted its inputs under the lock, merged without it,
// then re-acquired and called levels_[level].clear(), discarding any table a
// concurrent flush had added in between.
TEST_F(CompactionTest, FlushDuringCompactionIsNotLost) {
  Compaction c(testDir, 10);

  // Enough level-0 tables to trigger a compaction, each with distinct keys.
  for (int i = 0; i < 6; ++i) {
    c.addSSTable(makeTable(c, 0,
                           {{"existing_" + std::to_string(i),
                             "value_" + std::to_string(i), uint64_t(i + 1),
                             false}}),
                 0);
  }

  std::atomic<bool> compactionDone{false};

  // Compact on one thread while another keeps flushing into level 0.
  std::thread compactor([&]() {
    c.runCompaction();
    compactionDone.store(true);
  });

  std::vector<std::string> lateKeys;
  std::thread flusher([&]() {
    for (int i = 0; i < 20 && !compactionDone.load(); ++i) {
      std::string key = "late_" + std::to_string(i);
      c.addSSTable(makeTable(c, 0, {{key, "late_value", uint64_t(100 + i),
                                     false}}),
                   0);
      lateKeys.push_back(key);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  compactor.join();
  flusher.join();

  // Everything present before the compaction must still be readable...
  for (int i = 0; i < 6; ++i) {
    auto value = lookup(c, "existing_" + std::to_string(i));
    EXPECT_TRUE(value.has_value())
        << "compaction lost pre-existing key existing_" << i;
  }

  // ...and so must everything flushed while it ran.
  for (const auto &key : lateKeys) {
    auto value = lookup(c, key);
    EXPECT_TRUE(value.has_value())
        << "compaction discarded " << key << ", flushed mid-merge";
  }
}

// Regression test for tombstone garbage collection.
//
// The old rule dropped tombstones whenever the target level was >= 2, without
// checking whether a deeper level still held an older version of that key. If
// one did, dropping the tombstone brought the deleted value back to life.
TEST_F(CompactionTest, TombstoneIsNotDroppedWhileOlderVersionSurvivesBelow) {
  // A tiny level budget so level 1 overflows immediately and the merge lands
  // at level 2, which is where the old rule started discarding tombstones.
  Compaction c(testDir, 10, /*baseLevelSize=*/1024);

  // An old value parked deep, and a tombstone for it higher up.
  c.addSSTable(makeTable(c, 3, {{"ghost", "should_stay_deleted", 1, false}}), 3);
  c.addSSTable(makeTable(c, 1, {{"ghost", "", 5, true}}), 1);

  EXPECT_FALSE(lookup(c, "ghost").has_value()) << "precondition: key is deleted";

  // Merge level 1 into level 2. Level 3 still holds the old value, so the
  // tombstone must be preserved or the delete is undone.
  c.runCompaction();

  EXPECT_FALSE(lookup(c, "ghost").has_value())
      << "compaction resurrected a deleted key";
}

TEST_F(CompactionTest, TombstoneIsDroppedAtTheDeepestLevel) {
  Compaction c(testDir, 10);

  // Nothing below level 1, so a tombstone merged into level 1 can go.
  c.addSSTable(makeTable(c, 0, {{"a", "value", 1, false}}), 0);
  c.addSSTable(makeTable(c, 0, {{"gone", "", 2, true}}), 0);
  c.addSSTable(makeTable(c, 0, {{"b", "value", 3, false}}), 0);
  c.addSSTable(makeTable(c, 0, {{"c", "value", 4, false}}), 0);

  c.runCompaction();

  EXPECT_FALSE(lookup(c, "gone").has_value());
  EXPECT_TRUE(lookup(c, "a").has_value());
  EXPECT_TRUE(lookup(c, "b").has_value());
  EXPECT_TRUE(lookup(c, "c").has_value());
}

TEST_F(CompactionTest, CompactionKeepsNewestVersionOfDuplicateKeys) {
  Compaction c(testDir, 10);

  for (int i = 1; i <= 5; ++i) {
    c.addSSTable(
        makeTable(c, 0, {{"dup", "v" + std::to_string(i), uint64_t(i), false}}),
        0);
  }

  c.runCompaction();

  auto value = lookup(c, "dup");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), "v5") << "compaction kept the wrong version";
}

TEST_F(CompactionTest, MergeOfOnlyTombstonesWritesNoTable) {
  Compaction c(testDir, 10);

  for (int i = 0; i < 5; ++i) {
    c.addSSTable(
        makeTable(c, 0, {{"dead_" + std::to_string(i), "", uint64_t(i + 1),
                          true}}),
        0);
  }

  c.runCompaction();

  // Every entry merged away, so no output table should have been installed.
  EXPECT_EQ(c.totalSSTables(), 0u);

  // And no stray temp file was left behind.
  for (const auto &entry : fs::directory_iterator(testDir)) {
    EXPECT_NE(entry.path().extension(), ".tmp");
  }
}

TEST_F(CompactionTest, AbandonedTempFilesAreCleanedUpOnOpen) {
  {
    std::ofstream(testDir + "/0_00000099.sst.tmp", std::ios::binary)
        << "garbage";
  }

  Compaction c(testDir, 10);

  EXPECT_FALSE(fs::exists(testDir + "/0_00000099.sst.tmp"))
      << "crash leftover not cleaned up";
}

TEST_F(CompactionTest, ConcurrentReadsDuringCompactionSeeConsistentData) {
  Compaction c(testDir, 10);

  const int kKeys = 40;
  for (int i = 0; i < kKeys; ++i) {
    c.addSSTable(makeTable(c, 0,
                           {{"key_" + std::to_string(i),
                             "value_" + std::to_string(i), uint64_t(i + 1),
                             false}}),
                 0);
  }

  std::atomic<bool> stop{false};
  std::atomic<int> missing{0};
  std::atomic<int> wrong{0};

  std::vector<std::thread> readers;
  for (int t = 0; t < 4; ++t) {
    readers.emplace_back([&]() {
      while (!stop.load()) {
        for (int i = 0; i < kKeys; ++i) {
          auto value = lookup(c, "key_" + std::to_string(i));
          if (!value.has_value()) {
            missing.fetch_add(1);
          } else if (value.value() != "value_" + std::to_string(i)) {
            wrong.fetch_add(1);
          }
        }
      }
    });
  }

  c.runCompaction();
  stop.store(true);
  for (auto &r : readers) {
    r.join();
  }

  EXPECT_EQ(missing.load(), 0) << "a key vanished during compaction";
  EXPECT_EQ(wrong.load(), 0) << "a read returned the wrong value";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
