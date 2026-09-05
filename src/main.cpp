#include "config/Config.h"
#include "raft/RaftNode.h"
#include "rpc/RaftClient.h"
#include "rpc/Server.h"
#include "storage/StorageEngine.h"
#include "util/Logger.h"

#include <csignal>
#include <iostream>
#include <memory>
#include <regex>

using namespace shard;

// Global for signal handling
static std::unique_ptr<Server> gServer;
static std::shared_ptr<RaftNode> gRaft;
static std::shared_ptr<RaftClient> gRaftClient;

void signalHandler(int signal) {
  LOG_INFO << "Received signal " << signal << ", shutting down...";

  if (gRaft) {
    gRaft->stop();
  }

  if (gServer) {
    gServer->stop();
  }
}

// Parse peer addresses and extract peer IDs
// Expected format: "id:host:port,id:host:port,..." or "host:port,host:port,..."
std::vector<std::pair<uint32_t, std::string>>
parsePeers(const std::vector<std::string> &peers, uint32_t ownNodeId) {
  std::vector<std::pair<uint32_t, std::string>> result;
  uint32_t autoId = 1;

  for (const auto &peer : peers) {
    // Try to parse "id:host:port" format
    std::regex fullFormat("^(\\d+):(.+:\\d+)$");
    std::smatch match;

    if (std::regex_match(peer, match, fullFormat)) {
      uint32_t peerId = std::stoul(match[1].str());
      std::string address = match[2].str();
      if (peerId != ownNodeId) {
        result.emplace_back(peerId, address);
      }
    } else {
      // Assume "host:port" format, auto-assign ID
      while (autoId == ownNodeId) {
        autoId++;
      }
      result.emplace_back(autoId++, peer);
    }
  }

  return result;
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  // Initialize logging
  auto &config = Config::instance();
  Logger::instance().setLevel(config.logLevel());

  LOG_INFO << "===========================================";
  LOG_INFO << "  ShardKV - Distributed Key-Value Store";
  LOG_INFO << "===========================================";
  LOG_INFO << "Node ID: " << config.nodeId();
  LOG_INFO << "Listen:  " << config.listenAddr();
  LOG_INFO << "Data:    " << config.dataDir();
  LOG_INFO << "WAL:     " << config.walDir();

  // Setup signal handlers
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  try {
    // Initialize storage engine
    auto storage =
        std::make_shared<StorageEngine>(config.dataDir(), config.walDir());

    // Parse peers and create RaftClient
    auto parsedPeers = parsePeers(config.peers(), config.nodeId());
    gRaftClient = std::make_shared<RaftClient>();

    std::vector<uint32_t> peerIds;
    for (const auto &[peerId, address] : parsedPeers) {
      gRaftClient->addPeer(peerId, address);
      peerIds.push_back(peerId);
      LOG_INFO << "Added peer: " << peerId << " @ " << address;
    }

    // Initialize Raft node
    gRaft = std::make_shared<RaftNode>(config.nodeId(), peerIds);

    // Setup RequestVote RPC callback
    gRaft->setRequestVoteRPC([](uint32_t peerId, uint64_t term,
                                uint32_t candidateId, uint64_t lastLogIndex,
                                uint64_t lastLogTerm, bool preVote,
                                uint64_t &responseTerm,
                                bool &voteGranted) -> bool {
      return gRaftClient->requestVote(peerId, term, candidateId, lastLogIndex,
                                      lastLogTerm, preVote, responseTerm,
                                      voteGranted);
    });

    // Setup AppendEntries RPC callback
    gRaft->setAppendEntriesRPC([](uint32_t peerId, uint64_t term,
                                  uint32_t leaderId, uint64_t prevLogIndex,
                                  uint64_t prevLogTerm,
                                  const std::vector<RaftLogEntry> &entries,
                                  uint64_t leaderCommit, uint64_t &responseTerm,
                                  bool &success, uint64_t &matchIndex) -> bool {
      return gRaftClient->appendEntries(peerId, term, leaderId, prevLogIndex,
                                        prevLogTerm, entries, leaderCommit,
                                        responseTerm, success, matchIndex);
    });

    std::string statePath = config.dataDir() + "/raft_state";
    gRaft->loadState(statePath);

    // Start Raft node
    gRaft->start();

    // Initialize and start the TCP server (client KV and Raft RPCs)
    gServer = std::make_unique<Server>(storage, gRaft);
    gServer->start(config.listenAddr());

    LOG_INFO << "ShardKV is running. Press Ctrl+C to stop.";

    // Wait for server
    gServer->wait();

    // Persist state on shutdown
    gRaft->persistState(statePath);

  } catch (const std::exception &e) {
    LOG_ERROR << "Fatal error: " << e.what();
    return 1;
  }

  LOG_INFO << "ShardKV shutdown complete.";
  return 0;
}
