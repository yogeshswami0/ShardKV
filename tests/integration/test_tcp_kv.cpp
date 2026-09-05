#include "config/Config.h"
#include "raft/RaftNode.h"
#include "rpc/KvClient.h"
#include "rpc/Server.h"
#include "storage/StorageEngine.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace shard;
using namespace std::chrono_literals;

namespace {

void setEnv(const char *name, const char *value) {
#ifdef _WIN32
  _putenv_s(name, value);
#else
  setenv(name, value, 1);
#endif
}

struct EnvBeforeConfig {
  EnvBeforeConfig() {
    setEnv("SHARD_ELECTION_TIMEOUT_MIN_MS", "50");
    setEnv("SHARD_ELECTION_TIMEOUT_MAX_MS", "100");
    setEnv("SHARD_HEARTBEAT_MS", "20");
    setEnv("SHARD_RPC_TIMEOUT_MS", "200");
    setEnv("SHARD_COMMIT_TIMEOUT_MS", "3000");
    setEnv("SHARD_THREAD_POOL_SIZE", "4");
    setEnv("SHARD_REQUEST_QUEUE_SIZE", "64");
    setEnv("SHARD_ALLOW_STALE_READS", "false");
    setEnv("SHARD_COMPACTION_ENABLED", "false");
    setEnv("SHARD_WAL_SYNC", "none");
  }
} envBeforeConfig;

} // namespace

class TcpKvTest : public ::testing::Test {
protected:
  void SetUp() override {
    auto stamp = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    testDir = (fs::temp_directory_path() / ("shard_tcp_" + stamp)).string();
    fs::create_directories(testDir);

    storage = std::make_shared<StorageEngine>(testDir + "/data",
                                              testDir + "/wal");
    raft = std::make_shared<RaftNode>(1, std::vector<uint32_t>{});
    raft->loadState(testDir + "/raft_state");
    raft->start();

    server = std::make_unique<Server>(storage, raft);
    server->start("127.0.0.1:0");

    ASSERT_NE(server->listenPort(), 0);
    address = "127.0.0.1:" + std::to_string(server->listenPort());

    ASSERT_TRUE(waitForLeader(3000));
    client.setTimeout(4000);
    ASSERT_TRUE(client.connect(address));
  }

  void TearDown() override {
    client.disconnect();
    if (server) {
      server->stop();
    }
    if (raft) {
      raft->stop();
    }
    server.reset();
    raft.reset();
    storage.reset();
    std::error_code ec;
    fs::remove_all(testDir, ec);
  }

  bool waitForLeader(int timeoutMs) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
      if (raft->isLeader()) {
        KvClient probe;
        probe.setTimeout(500);
        if (probe.connect(address)) {
          auto p = probe.ping();
          if (p.ok && p.leaderId == 1) {
            return true;
          }
        }
      }
      std::this_thread::sleep_for(20ms);
    }
    return false;
  }

  std::string testDir;
  std::string address;
  std::shared_ptr<StorageEngine> storage;
  std::shared_ptr<RaftNode> raft;
  std::unique_ptr<Server> server;
  KvClient client;
};

TEST_F(TcpKvTest, PingPutGetDelete) {
  auto ping = client.ping();
  ASSERT_TRUE(ping.ok);
  EXPECT_EQ(ping.nodeId, 1u);
  EXPECT_EQ(ping.leaderId, 1u);

  auto put = client.put("user123", "hello");
  EXPECT_EQ(put.status, StatusCode::OK) << statusName(put.status);
  EXPECT_EQ(put.leaderId, 1u);

  auto get = client.get("user123");
  EXPECT_EQ(get.status, StatusCode::OK) << statusName(get.status);
  EXPECT_TRUE(get.found);
  EXPECT_EQ(get.value, "hello");

  auto missing = client.get("no-such-key");
  EXPECT_EQ(missing.status, StatusCode::OK);
  EXPECT_FALSE(missing.found);

  auto del = client.del("user123");
  EXPECT_EQ(del.status, StatusCode::OK);

  auto gone = client.get("user123");
  EXPECT_EQ(gone.status, StatusCode::OK);
  EXPECT_FALSE(gone.found);
}

TEST_F(TcpKvTest, ConcurrentPutsAreVisible) {
  constexpr int kWriters = 8;
  constexpr int kPerWriter = 20;
  std::vector<std::thread> threads;
  std::atomic<int> ok{0};

  for (int t = 0; t < kWriters; ++t) {
    threads.emplace_back([&, t]() {
      KvClient c;
      c.setTimeout(4000);
      ASSERT_TRUE(c.connect(address));
      for (int i = 0; i < kPerWriter; ++i) {
        std::string key = "k-" + std::to_string(t) + "-" + std::to_string(i);
        auto put = c.put(key, "v");
        if (put.status == StatusCode::OK) {
          ok++;
        }
      }
    });
  }
  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(ok.load(), kWriters * kPerWriter);

  auto sample = client.get("k-0-0");
  EXPECT_EQ(sample.status, StatusCode::OK);
  EXPECT_TRUE(sample.found);
  EXPECT_EQ(sample.value, "v");
}
