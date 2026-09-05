#include "storage/StorageEngine.h"
#include <filesystem>
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace shard;

class StorageEngineTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create unique test directories
    auto timestamp =
        std::chrono::system_clock::now().time_since_epoch().count();
    dataDir = "shard_test_data_" + std::to_string(timestamp);
    walDir = "shard_test_wal_" + std::to_string(timestamp);

    fs::create_directories(dataDir);
    fs::create_directories(walDir);

    // Set environment variables for config
    _putenv_s("SHARD_DATA_DIR", dataDir.c_str());
    _putenv_s("SHARD_WAL_DIR", walDir.c_str());
    _putenv_s("SHARD_MEMTABLE_SIZE_MB", "1");       // 1 MB for testing
    _putenv_s("SHARD_COMPACTION_ENABLED", "false"); // Disable for tests
  }

  void TearDown() override {
    fs::remove_all(dataDir);
    fs::remove_all(walDir);
  }

  std::string dataDir;
  std::string walDir;
};

TEST_F(StorageEngineTest, BasicPutGet) {
  StorageEngine engine(dataDir, walDir);

  engine.put("key1", "value1");
  engine.put("key2", "value2");

  auto val1 = engine.get("key1");
  ASSERT_TRUE(val1.has_value());
  EXPECT_EQ(val1.value(), "value1");

  auto val2 = engine.get("key2");
  ASSERT_TRUE(val2.has_value());
  EXPECT_EQ(val2.value(), "value2");
}

TEST_F(StorageEngineTest, UpdateKey) {
  StorageEngine engine(dataDir, walDir);

  engine.put("key", "value1");
  engine.put("key", "value2");

  auto val = engine.get("key");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(val.value(), "value2");
}

TEST_F(StorageEngineTest, DeleteKey) {
  StorageEngine engine(dataDir, walDir);

  engine.put("key", "value");
  EXPECT_TRUE(engine.get("key").has_value());

  engine.remove("key");
  EXPECT_FALSE(engine.get("key").has_value());
}

TEST_F(StorageEngineTest, NonExistentKey) {
  StorageEngine engine(dataDir, walDir);

  auto val = engine.get("nonexistent");
  EXPECT_FALSE(val.has_value());
}

TEST_F(StorageEngineTest, Scan) {
  StorageEngine engine(dataDir, walDir);

  engine.put("a", "1");
  engine.put("b", "2");
  engine.put("c", "3");
  engine.put("d", "4");

  auto results = engine.scan("b", "d");

  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].first, "b");
  EXPECT_EQ(results[0].second, "2");
  EXPECT_EQ(results[1].first, "c");
  EXPECT_EQ(results[1].second, "3");
}

TEST_F(StorageEngineTest, FlushAndRecover) {
  {
    StorageEngine engine(dataDir, walDir);
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    engine.flush(); // Force flush to SSTable
  }

  // Reopen and verify data persisted
  StorageEngine engine2(dataDir, walDir);

  auto val1 = engine2.get("key1");
  ASSERT_TRUE(val1.has_value());
  EXPECT_EQ(val1.value(), "value1");

  auto val2 = engine2.get("key2");
  ASSERT_TRUE(val2.has_value());
  EXPECT_EQ(val2.value(), "value2");
}

TEST_F(StorageEngineTest, WALRecovery) {
  {
    StorageEngine engine(dataDir, walDir);
    engine.put("key", "value");
    // Don't flush - data only in WAL
  }

  // Reopen - should recover from WAL
  StorageEngine engine2(dataDir, walDir);

  auto val = engine2.get("key");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(val.value(), "value");
}

TEST_F(StorageEngineTest, ConcurrentWrites) {
  StorageEngine engine(dataDir, walDir);

  std::vector<std::thread> threads;
  const int numThreads = 4;
  const int numOps = 100;

  for (int t = 0; t < numThreads; ++t) {
    threads.emplace_back([&engine, t, numOps]() {
      for (int i = 0; i < numOps; ++i) {
        std::string key = "key_" + std::to_string(t) + "_" + std::to_string(i);
        engine.put(key, "value");
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  // Verify all keys written
  for (int t = 0; t < numThreads; ++t) {
    for (int i = 0; i < numOps; ++i) {
      std::string key = "key_" + std::to_string(t) + "_" + std::to_string(i);
      auto val = engine.get(key);
      EXPECT_TRUE(val.has_value()) << "Missing key: " << key;
    }
  }
}


// Mixed read/write/scan traffic against one engine. The point is not just that
// it returns correct answers but that it does so with no data race, run this
// under -DSHARD_SANITIZE=thread, where the previously unlocked reads of
// memtable_ in maybeFlush() and flushMemTable() are reported.
TEST_F(StorageEngineTest, ConcurrentReadWriteScanIsRaceFree) {
  _putenv_s("SHARD_MEMTABLE_SIZE_MB", "1");
  StorageEngine engine(dataDir, walDir);

  const int kWriters = 4;
  const int kReaders = 4;
  const int kOpsPerThread = 150;

  std::atomic<bool> stop{false};
  std::atomic<int> wrongValues{0};

  auto keyFor = [](int t, int i) {
    return "k_" + std::to_string(t) + "_" + std::to_string(i);
  };
  auto valueFor = [](int t, int i) {
    return "v_" + std::to_string(t) + "_" + std::to_string(i);
  };

  std::vector<std::thread> threads;

  for (int t = 0; t < kWriters; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        engine.put(keyFor(t, i), valueFor(t, i));
      }
    });
  }

  // Readers run concurrently with the writers and with the flushes those
  // writes trigger. A key that has been written must never read back as a
  // different key's value.
  for (int r = 0; r < kReaders; ++r) {
    threads.emplace_back([&, r]() {
      while (!stop.load()) {
        for (int t = 0; t < kWriters; ++t) {
          int i = r * 13 % kOpsPerThread;
          auto val = engine.get(keyFor(t, i));
          if (val.has_value() && val.value() != valueFor(t, i)) {
            wrongValues.fetch_add(1);
          }
        }
        engine.scan("k_", "k_z");
      }
    });
  }

  for (int t = 0; t < kWriters; ++t) {
    threads[t].join();
  }
  stop.store(true);
  for (size_t i = kWriters; i < threads.size(); ++i) {
    threads[i].join();
  }

  EXPECT_EQ(wrongValues.load(), 0) << "a read returned another key's value";

  // Every write must be readable once the writers are done.
  for (int t = 0; t < kWriters; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      auto val = engine.get(keyFor(t, i));
      ASSERT_TRUE(val.has_value()) << "lost " << keyFor(t, i);
      EXPECT_EQ(val.value(), valueFor(t, i));
    }
  }
}

// Writes must survive a reopen, which is the whole point of the WAL.
TEST_F(StorageEngineTest, DataSurvivesReopen) {
  {
    StorageEngine engine(dataDir, walDir);
    for (int i = 0; i < 100; ++i) {
      engine.put("persist_" + std::to_string(i), "value_" + std::to_string(i));
    }
    engine.remove("persist_5");
  }

  StorageEngine reopened(dataDir, walDir);
  for (int i = 0; i < 100; ++i) {
    auto val = reopened.get("persist_" + std::to_string(i));
    if (i == 5) {
      EXPECT_FALSE(val.has_value()) << "deleted key came back";
    } else {
      ASSERT_TRUE(val.has_value()) << "lost persist_" << i;
      EXPECT_EQ(val.value(), "value_" + std::to_string(i));
    }
  }
}


// A write that lands while a flush is in progress goes into the NEW MemTable,
// but its WAL record was written to the segment the flush is about to retire.
// Deleting that segment at rotation time, rather than after the SSTable is
// durable, left such a write acknowledged with no record anywhere on disk.
TEST_F(StorageEngineTest, WritesDuringAFlushSurviveReopen) {
  _putenv_s("SHARD_MEMTABLE_SIZE_MB", "1");

  std::vector<std::string> keys;
  {
    StorageEngine engine(dataDir, walDir);

    std::atomic<bool> stop{false};

    // Keep writing while flushes are triggered underneath.
    std::thread writer([&]() {
      int i = 0;
      while (!stop.load()) {
        engine.put("concurrent_" + std::to_string(i), "value");
        i++;
        if (i > 400)
          break;
      }
    });

    for (int f = 0; f < 4; ++f) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      engine.flush();
    }

    stop.store(true);
    writer.join();

    for (int i = 0; i < 200; ++i) {
      std::string key = "concurrent_" + std::to_string(i);
      if (engine.get(key).has_value()) {
        keys.push_back(key);
      }
    }
    ASSERT_FALSE(keys.empty()) << "precondition: some writes landed";
  }

  // Everything the engine acknowledged must still be there after a reopen,
  // which replays whatever WAL segments were not discarded.
  StorageEngine reopened(dataDir, walDir);
  for (const auto &key : keys) {
    EXPECT_TRUE(reopened.get(key).has_value())
        << "lost " << key << " across flush + reopen";
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
