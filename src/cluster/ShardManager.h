#pragma once

#include "raft/RaftNode.h"
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>

namespace shard {

class ShardManager {
public:
  ShardManager(uint32_t numShards, uint32_t nodeId, const std::vector<uint32_t>& peerIds, const std::string& dataDir);
  ~ShardManager();

  void start();
  void stop();

  std::shared_ptr<RaftNode> getShard(uint32_t shardId) const;

  uint32_t getShardId(const std::string& key) const;
  uint32_t getNumShards() const { return numShards_; }

  const std::unordered_map<uint32_t, std::shared_ptr<RaftNode>>& getAllShards() const {
    return shards_;
  }

private:
  uint32_t numShards_;
  std::unordered_map<uint32_t, std::shared_ptr<RaftNode>> shards_;
};

} // namespace shard
