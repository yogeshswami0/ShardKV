#include "config/Config.h"
#include <cstdlib>
#include <sstream>

namespace shard {

Config &Config::instance() {
  static Config instance;
  return instance;
}

Config::Config() {
  // Node identity
  nodeId_ = static_cast<uint32_t>(getEnvInt("SHARD_NODE_ID", 1));
  listenAddr_ = getEnv("SHARD_LISTEN_ADDR", "0.0.0.0:7070");

  std::string peersStr = getEnv("SHARD_PEERS", "");
  peers_ = parseCommaSeparated(peersStr);

  // Storage paths
  dataDir_ = getEnv("SHARD_DATA_DIR", "/var/shard/data");
  walDir_ = getEnv("SHARD_WAL_DIR", "/var/shard/wal");

  // MemTable
  memtableSizeMB_ =
      static_cast<size_t>(getEnvInt("SHARD_MEMTABLE_SIZE_MB", 64));

  // Bloom filter
  bloomFpRate_ = getEnvDouble("SHARD_BLOOM_FP_RATE", 0.01);

  // Durable by default. "none" trades crash safety for throughput and is
  // intended for benchmarking only.
  walSyncMode_ = getEnv("SHARD_WAL_SYNC", "group");
  commitTimeoutMs_ = getEnvInt("SHARD_COMMIT_TIMEOUT_MS", 5000);

  // Linearizable by default; stale reads are opt-in.
  allowStaleReads_ = getEnvBool("SHARD_ALLOW_STALE_READS", false);

  // Compaction
  compactionEnabled_ = getEnvBool("SHARD_COMPACTION_ENABLED", true);
  levelSizeMultiplier_ = getEnvInt("SHARD_LEVEL_SIZE_MULTIPLIER", 10);

  // Raft timeouts
  // Raft wants the election timeout an order of magnitude above the heartbeat
  // interval, so ordinary jitter never looks like a dead leader. The previous
  // 150-300ms against a 50ms heartbeat was a 3-6x ratio, tight enough that a
  // scheduling hiccup could unseat a healthy leader.
  electionTimeoutMinMs_ = getEnvInt("SHARD_ELECTION_TIMEOUT_MIN_MS", 500);
  electionTimeoutMaxMs_ = getEnvInt("SHARD_ELECTION_TIMEOUT_MAX_MS", 1000);
  heartbeatMs_ = getEnvInt("SHARD_HEARTBEAT_MS", 50);
  rpcTimeoutMs_ = getEnvInt("SHARD_RPC_TIMEOUT_MS", 100);

  threadPoolSize_ =
      static_cast<size_t>(getEnvInt("SHARD_THREAD_POOL_SIZE", 4));
  requestQueueSize_ =
      static_cast<size_t>(getEnvInt("SHARD_REQUEST_QUEUE_SIZE", 1000));
  maxConnections_ =
      static_cast<size_t>(getEnvInt("SHARD_MAX_CONNECTIONS", 256));
  maxPayloadBytes_ = static_cast<uint32_t>(
      getEnvInt("SHARD_MAX_PAYLOAD_BYTES", 16 * 1024 * 1024));

  // Logging
  logLevel_ = getEnv("SHARD_LOG_LEVEL", "INFO");
}

std::string Config::getEnv(const char *name, const char *defaultVal) {
  const char *val = std::getenv(name);
  return val ? std::string(val) : std::string(defaultVal);
}

int Config::getEnvInt(const char *name, int defaultVal) {
  const char *val = std::getenv(name);
  if (!val)
    return defaultVal;

  try {
    return std::stoi(val);
  } catch (...) {
    return defaultVal;
  }
}

double Config::getEnvDouble(const char *name, double defaultVal) {
  const char *val = std::getenv(name);
  if (!val)
    return defaultVal;

  try {
    return std::stod(val);
  } catch (...) {
    return defaultVal;
  }
}

bool Config::getEnvBool(const char *name, bool defaultVal) {
  const char *val = std::getenv(name);
  if (!val)
    return defaultVal;

  std::string s(val);
  return (s == "true" || s == "1" || s == "yes" || s == "on");
}

std::vector<std::string> Config::parseCommaSeparated(const std::string &input) {
  std::vector<std::string> result;
  if (input.empty())
    return result;

  std::stringstream ss(input);
  std::string item;

  while (std::getline(ss, item, ',')) {
    // Trim whitespace
    size_t start = item.find_first_not_of(" \t");
    size_t end = item.find_last_not_of(" \t");
    if (start != std::string::npos) {
      result.push_back(item.substr(start, end - start + 1));
    }
  }

  return result;
}

} // namespace shard
