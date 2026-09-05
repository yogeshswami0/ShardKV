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

class SSTableTest : public ::testing::Test {
protected:
  void SetUp() override {
    testDir = "/tmp/shard_sstable_test_" +
              std::to_string(
                  std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(testDir);
    sstPath = testDir + "/0_00000001.sst";
  }

  void TearDown() override { fs::remove_all(testDir); }

  // Writes kKeys entries where key_<i> maps to a value derived from i, so any
  // mismatch identifies exactly which read went wrong.
  void writeTable(int numKeys) {
    SSTableWriter writer(sstPath);
    for (int i = 0; i < numKeys; ++i) {
      writer.add(keyFor(i), valueFor(i), static_cast<uint64_t>(i + 1), false);
    }
    writer.finish();
  }

  static std::string keyFor(int i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%06d", i);
    return buf;
  }

  static std::string valueFor(int i) {
    // Long enough to span several blocks, so lookups actually hit distinct
    // offsets rather than all landing in one cached block.
    return "value_" + std::to_string(i) + std::string(200, 'x');
  }

  std::string testDir;
  std::string sstPath;
};

TEST_F(SSTableTest, RoundTripSingleThreaded) {
  writeTable(500);

  SSTableReader reader(sstPath);
  EXPECT_EQ(reader.entryCount(), 500u);

  for (int i = 0; i < 500; ++i) {
    auto entry = reader.get(keyFor(i));
    ASSERT_TRUE(entry.has_value()) << "missing " << keyFor(i);
    EXPECT_EQ(entry->value, valueFor(i));
    EXPECT_FALSE(entry->deleted);
  }

  EXPECT_FALSE(reader.get("key_999999").has_value());
}

// The regression test for the shared read cursor.
//
// readBlock() used to seekg() then read() on one ifstream member shared by
// every caller. Two threads interleaving those two calls would have one of them
// read from the other's offset, returning a valid-looking block for the wrong
// key range. pread() takes the offset as an argument, so there is no shared
// cursor to race on.
TEST_F(SSTableTest, ConcurrentReadersDoNotTearEachOthersReads) {
  const int kKeys = 500;
  writeTable(kKeys);

  SSTableReader reader(sstPath);

  const int kThreads = 8;
  const int kIterations = 200;
  std::atomic<int> mismatches{0};
  std::atomic<int> missing{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int iter = 0; iter < kIterations; ++iter) {
        // Each thread walks a different stride so they contend on different
        // blocks at the same moment.
        int i = (t * 61 + iter * 7) % kKeys;
        auto entry = reader.get(keyFor(i));
        if (!entry.has_value()) {
          missing.fetch_add(1);
        } else if (entry->value != valueFor(i) || entry->key != keyFor(i)) {
          mismatches.fetch_add(1);
        }
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(missing.load(), 0) << "concurrent reads lost entries";
  EXPECT_EQ(mismatches.load(), 0) << "concurrent reads returned wrong values";
}

TEST_F(SSTableTest, ConcurrentScansAgree) {
  const int kKeys = 300;
  writeTable(kKeys);

  SSTableReader reader(sstPath);

  const int kThreads = 6;
  std::atomic<int> bad{0};
  std::vector<std::thread> threads;

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&]() {
      for (int iter = 0; iter < 40; ++iter) {
        auto results = reader.scan(keyFor(10), keyFor(60));
        if (results.size() != 50) {
          bad.fetch_add(1);
          continue;
        }
        for (size_t j = 0; j < results.size(); ++j) {
          if (results[j].key != keyFor(static_cast<int>(10 + j))) {
            bad.fetch_add(1);
            break;
          }
        }
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(bad.load(), 0) << "concurrent scans disagreed";
}

#ifndef _WIN32
// An SSTableReader keeps its descriptor open, so compaction unlinking the file
// must not disturb a reader already holding it.
TEST_F(SSTableTest, ReadsSurviveFileUnlink) {
  writeTable(200);

  SSTableReader reader(sstPath);
  ASSERT_TRUE(fs::remove(sstPath));

  for (int i = 0; i < 200; ++i) {
    auto entry = reader.get(keyFor(i));
    ASSERT_TRUE(entry.has_value()) << "lost " << keyFor(i) << " after unlink";
    EXPECT_EQ(entry->value, valueFor(i));
  }
}
#endif

TEST_F(SSTableTest, IteratorVisitsEveryEntryInOrder) {
  const int kKeys = 250;
  writeTable(kKeys);

  SSTableReader reader(sstPath);

  int count = 0;
  std::string previous;
  for (auto it = reader.begin(); it.valid(); it.next()) {
    auto e = it.entry();
    EXPECT_EQ(e.key, keyFor(count));
    EXPECT_EQ(e.value, valueFor(count));
    if (!previous.empty()) {
      EXPECT_LT(previous, e.key) << "iterator returned keys out of order";
    }
    previous = e.key;
    count++;
  }

  EXPECT_EQ(count, kKeys);
}

TEST_F(SSTableTest, TombstonesRoundTrip) {
  {
    SSTableWriter writer(sstPath);
    writer.add("alive", "value", 1, false);
    writer.add("dead", "", 2, true);
    writer.finish();
  }

  SSTableReader reader(sstPath);

  auto alive = reader.get("alive");
  ASSERT_TRUE(alive.has_value());
  EXPECT_FALSE(alive->deleted);

  auto dead = reader.get("dead");
  ASSERT_TRUE(dead.has_value());
  EXPECT_TRUE(dead->deleted);
}

// A truncated file must be rejected at open rather than producing a reader that
// returns garbage.
TEST_F(SSTableTest, TruncatedFileIsRejected) {
  writeTable(100);

  auto size = fs::file_size(sstPath);
  fs::resize_file(sstPath, size / 2);

  EXPECT_THROW({ SSTableReader reader(sstPath); }, std::runtime_error);
}

TEST_F(SSTableTest, EmptyFileIsRejected) {
  std::ofstream(sstPath, std::ios::binary).close();
  EXPECT_THROW({ SSTableReader reader(sstPath); }, std::runtime_error);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
