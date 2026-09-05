#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace shard {

/**
 * Singleton configuration manager that loads all settings from environment variables.
 * Falls back to sensible defaults when variables aren't set.
 */
class Config {
public:
    static Config& instance();

    // Prevent copying
    Config(const Config&) = delete;
    Config& operator=(const Config&) = delete;

    // Node identity
    uint32_t nodeId() const { return nodeId_; }
    std::string listenAddr() const { return listenAddr_; }
    std::vector<std::string> peers() const { return peers_; }

    // Storage paths
    std::string dataDir() const { return dataDir_; }
    std::string walDir() const { return walDir_; }

    // MemTable settings
    size_t memtableSizeBytes() const { return memtableSizeMB_ * 1024 * 1024; }
    size_t memtableSizeMB() const { return memtableSizeMB_; }

    // Bloom filter
    double bloomFpRate() const { return bloomFpRate_; }

    // WAL durability policy: "group" (default), "always", or "none"
    std::string walSyncMode() const { return walSyncMode_; }

    // Compaction
    bool compactionEnabled() const { return compactionEnabled_; }
    int levelSizeMultiplier() const { return levelSizeMultiplier_; }

    // How long a client write waits for its entry to be committed and applied
    int commitTimeoutMs() const { return commitTimeoutMs_; }

    // When true, reads are served from local state without confirming
    // leadership. Faster, but no longer linearizable.
    bool allowStaleReads() const { return allowStaleReads_; }

    // Raft timeouts
    int electionTimeoutMinMs() const { return electionTimeoutMinMs_; }
    int electionTimeoutMaxMs() const { return electionTimeoutMaxMs_; }
    int heartbeatMs() const { return heartbeatMs_; }
    int rpcTimeoutMs() const { return rpcTimeoutMs_; }

    // Request handling
    size_t threadPoolSize() const { return threadPoolSize_; }
    size_t requestQueueSize() const { return requestQueueSize_; }
    size_t maxConnections() const { return maxConnections_; }
    uint32_t maxPayloadBytes() const { return maxPayloadBytes_; }

    // Logging
    std::string logLevel() const { return logLevel_; }

private:
    Config();
    
    std::string getEnv(const char* name, const char* defaultVal);
    int getEnvInt(const char* name, int defaultVal);
    double getEnvDouble(const char* name, double defaultVal);
    bool getEnvBool(const char* name, bool defaultVal);
    std::vector<std::string> parseCommaSeparated(const std::string& input);

    uint32_t nodeId_;
    std::string listenAddr_;
    std::vector<std::string> peers_;
    std::string dataDir_;
    std::string walDir_;
    size_t memtableSizeMB_;
    double bloomFpRate_;
    std::string walSyncMode_;
    bool compactionEnabled_;
    int levelSizeMultiplier_;
    int electionTimeoutMinMs_;
    int electionTimeoutMaxMs_;
    int heartbeatMs_;
    int rpcTimeoutMs_;
    int commitTimeoutMs_;
    bool allowStaleReads_;
    size_t threadPoolSize_;
    size_t requestQueueSize_;
    size_t maxConnections_;
    uint32_t maxPayloadBytes_;
    std::string logLevel_;
};

} // namespace shard
