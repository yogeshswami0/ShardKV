#include "cluster/ShardManager.h"
#include <functional>

namespace shard {

ShardManager::ShardManager(uint32_t numShards, uint32_t nodeId, const std::vector<uint32_t>& peerIds, const std::string& dataDir)
    : numShards_(numShards) {
  for (uint32_t i = 0; i < numShards_; ++i) {
    auto raft = std::make_shared<RaftNode>(nodeId, peerIds);
    raft->loadState(dataDir + "/raft_state_" + std::to_string(i));
    shards_[i] = raft;
  }
}

ShardManager::~ShardManager() {
  stop();
}

void ShardManager::start() {
  for (auto& [id, raft] : shards_) {
    raft->start();
  }
}

void ShardManager::stop() {
  for (auto& [id, raft] : shards_) {
    raft->stop();
  }
}

std::shared_ptr<RaftNode> ShardManager::getShard(uint32_t shardId) const {
  auto it = shards_.find(shardId);
  if (it != shards_.end()) {
    return it->second;
  }
  return nullptr;
}

uint32_t ShardManager::getShardId(const std::string& key) const {
  return std::hash<std::string>{}(key) % numShards_;
}

} // namespace shard
