#include "config/Config.h"
#include "storage/StorageEngine.h"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace shard;

struct BenchmarkResult {
  double opsPerSecond;
  double avgLatencyUs;
  double p50LatencyUs;
  double p99LatencyUs;
};

std::vector<double> latencies;

BenchmarkResult runWriteBenchmark(StorageEngine &engine, int numOps,
                                  int numThreads) {
  std::atomic<int> opsCompleted{0};
  std::vector<std::thread> threads;
  std::vector<std::vector<double>> threadLatencies(numThreads);

  auto start = std::chrono::high_resolution_clock::now();

  for (int t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, t]() {
      int perThread = numOps / numThreads;

      for (int i = 0; i < perThread; ++i) {
        // Deterministic, distinct keys: the dataset written is exactly the
        // dataset the read benchmark then looks up.
        std::string key = "bench_key_" + std::to_string(t * perThread + i);
        std::string value = std::string(100, 'x'); // 100 byte value

        auto opStart = std::chrono::high_resolution_clock::now();
        engine.put(key, value);
        auto opEnd = std::chrono::high_resolution_clock::now();

        double latency =
            std::chrono::duration<double, std::micro>(opEnd - opStart).count();
        threadLatencies[t].push_back(latency);

        opsCompleted++;
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  double duration = std::chrono::duration<double>(end - start).count();

  // Merge and sort latencies
  latencies.clear();
  for (const auto &tl : threadLatencies) {
    latencies.insert(latencies.end(), tl.begin(), tl.end());
  }
  std::sort(latencies.begin(), latencies.end());

  BenchmarkResult result;
  result.opsPerSecond = opsCompleted / duration;
  result.avgLatencyUs = 0;
  for (double l : latencies)
    result.avgLatencyUs += l;
  result.avgLatencyUs /= latencies.size();
  result.p50LatencyUs = latencies[latencies.size() / 2];
  result.p99LatencyUs = latencies[static_cast<size_t>(latencies.size() * 0.99)];

  return result;
}

BenchmarkResult runReadBenchmark(StorageEngine &engine, int numOps,
                                 int numThreads) {
  std::atomic<int> opsCompleted{0};
  std::vector<std::thread> threads;
  std::vector<std::vector<double>> threadLatencies(numThreads);

  auto start = std::chrono::high_resolution_clock::now();

  for (int t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 rng(t);
      // Draw only from keys that exist. The original drew from a space ten
      // times larger than the dataset, so ~90% of "reads" were bloom-filter
      // rejections of keys that had never been written, which is what made
      // the published read throughput look the way it did.
      std::uniform_int_distribution<int> dist(0, numOps - 1);

      for (int i = 0; i < numOps / numThreads; ++i) {
        std::string key = "bench_key_" + std::to_string(dist(rng));

        auto opStart = std::chrono::high_resolution_clock::now();
        engine.get(key);
        auto opEnd = std::chrono::high_resolution_clock::now();

        double latency =
            std::chrono::duration<double, std::micro>(opEnd - opStart).count();
        threadLatencies[t].push_back(latency);

        opsCompleted++;
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  double duration = std::chrono::duration<double>(end - start).count();

  // Merge and sort latencies
  latencies.clear();
  for (const auto &tl : threadLatencies) {
    latencies.insert(latencies.end(), tl.begin(), tl.end());
  }
  std::sort(latencies.begin(), latencies.end());

  BenchmarkResult result;
  result.opsPerSecond = opsCompleted / duration;
  result.avgLatencyUs = 0;
  for (double l : latencies)
    result.avgLatencyUs += l;
  result.avgLatencyUs /= latencies.size();
  result.p50LatencyUs = latencies[latencies.size() / 2];
  result.p99LatencyUs = latencies[static_cast<size_t>(latencies.size() * 0.99)];

  return result;
}

int main(int argc, char *argv[]) {
  int numOps = 100000;
  int numThreads = 4;
  int duration = 30;
  std::string syncMode = "group";

  // Parse command line args
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--ops" && i + 1 < argc) {
      numOps = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      numThreads = std::stoi(argv[++i]);
    } else if (arg == "--duration" && i + 1 < argc) {
      duration = std::stoi(argv[++i]);
    } else if (arg == "--sync" && i + 1 < argc) {
      syncMode = argv[++i];
    }
  }

  // Setup
  std::string dataDir = "/tmp/shard_bench_data";
  std::string walDir = "/tmp/shard_bench_wal";

  fs::remove_all(dataDir);
  fs::remove_all(walDir);
  fs::create_directories(dataDir);
  fs::create_directories(walDir);

  _putenv_s("SHARD_MEMTABLE_SIZE_MB", "64");
  _putenv_s("SHARD_COMPACTION_ENABLED", "true");
  _putenv_s("SHARD_WAL_SYNC", syncMode.c_str());

  std::cout << "=== ShardKV Throughput Benchmark ===" << std::endl;
  std::cout << "Operations: " << numOps << std::endl;
  std::cout << "Threads:    " << numThreads << std::endl;
  std::cout << "WAL sync:   " << syncMode;
  if (syncMode == "none") {
    std::cout << "  (NOT durable: page cache only)";
  }
  std::cout << std::endl << std::endl;

  StorageEngine engine(dataDir, walDir);

  // Write benchmark
  std::cout << "Running write benchmark..." << std::endl;
  auto writeResult = runWriteBenchmark(engine, numOps, numThreads);

  std::cout << "Write Results:" << std::endl;
  std::cout << "  Throughput: " << writeResult.opsPerSecond << " ops/sec"
            << std::endl;
  std::cout << "  Avg Latency: " << writeResult.avgLatencyUs << " us"
            << std::endl;
  std::cout << "  P50 Latency: " << writeResult.p50LatencyUs << " us"
            << std::endl;
  std::cout << "  P99 Latency: " << writeResult.p99LatencyUs << " us"
            << std::endl;
  std::cout << std::endl;

  // Read benchmark
  std::cout << "Running read benchmark..." << std::endl;
  auto readResult = runReadBenchmark(engine, numOps, numThreads);

  std::cout << "Read Results:" << std::endl;
  std::cout << "  Throughput: " << readResult.opsPerSecond << " ops/sec"
            << std::endl;
  std::cout << "  Avg Latency: " << readResult.avgLatencyUs << " us"
            << std::endl;
  std::cout << "  P50 Latency: " << readResult.p50LatencyUs << " us"
            << std::endl;
  std::cout << "  P99 Latency: " << readResult.p99LatencyUs << " us"
            << std::endl;

  // Cleanup
  fs::remove_all(dataDir);
  fs::remove_all(walDir);

  return 0;
}
