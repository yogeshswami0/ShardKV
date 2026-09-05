#include "storage/StorageEngine.h"
#include "storage/WAL.h"
#include "config/Config.h"
#include "util/Platform.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <thread>

namespace fs = std::filesystem;

class CrashRecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    testDir = fs::temp_directory_path() / "shard_crash_test";
    dataDir = testDir / "data";
    walDir = testDir / "wal";
    
    fs::remove_all(testDir);
    fs::create_directories(dataDir);
    fs::create_directories(walDir);
  }

  void TearDown() override {
    fs::remove_all(testDir);
  }

  fs::path testDir;
  fs::path dataDir;
  fs::path walDir;
};

TEST_F(CrashRecoveryTest, NormalRecovery) {
  {
    shard::StorageEngine engine(dataDir.string(), walDir.string());
    for (int i = 0; i < 1000; ++i) {
      engine.put("key" + std::to_string(i), "value" + std::to_string(i));
    }
  } // engine destroyed, mimicking clean shutdown or flush (actually flush is called in dtor)
  
  {
    shard::StorageEngine engine(dataDir.string(), walDir.string());
    for (int i = 0; i < 1000; ++i) {
      auto val = engine.get("key" + std::to_string(i));
      ASSERT_TRUE(val.has_value());
      EXPECT_EQ(*val, "value" + std::to_string(i));
    }
  }
}

TEST_F(CrashRecoveryTest, TruncatedWALRecovery) {
  {
    shard::WAL wal(walDir.string(), shard::WALSyncMode::GROUP);
    wal.appendPut("key0", "value0", 1);
    wal.appendPut("key1", "value1", 2);
  }
  
  fs::path walFile;
  for (const auto& entry : fs::directory_iterator(walDir)) {
    if (entry.path().extension() == ".wal") {
      if (fs::file_size(entry.path()) > 0) {
          walFile = entry.path();
      }
    }
  }
  ASSERT_FALSE(walFile.empty());
  
  // Truncate the last 5 bytes to simulate a torn write on the second entry
  auto size = fs::file_size(walFile);
  if (size > 10) {
    fs::resize_file(walFile, size - 5);
  }
  
  {
    shard::StorageEngine engine(dataDir.string(), walDir.string());
    auto val = engine.get("key0");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "value0");
    // key1 might be lost due to truncation, but it shouldn't crash
  }
}
