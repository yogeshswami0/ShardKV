#pragma once

#include "raft/RaftLog.h"
#include "net/TcpClient.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace shard {

class RaftClient {
public:
  RaftClient();
  ~RaftClient();

  void addPeer(uint32_t peerId, const std::string &address);
  void removePeer(uint32_t peerId);

  bool requestVote(uint32_t peerId, uint64_t term, uint32_t candidateId,
                   uint64_t lastLogIndex, uint64_t lastLogTerm, bool preVote,
                   uint64_t &responseTerm, bool &voteGranted);

  bool appendEntries(uint32_t peerId, uint64_t term, uint32_t leaderId,
                     uint64_t prevLogIndex, uint64_t prevLogTerm,
                     const std::vector<RaftLogEntry> &entries,
                     uint64_t leaderCommit, uint64_t &responseTerm,
                     bool &success, uint64_t &matchIndex);

  std::vector<uint32_t> getPeerIds() const;

private:
  struct PeerConnection {
    std::string address;
    std::unique_ptr<TcpClient> client;
    std::mutex mutex; // Protects access to client
  };

  std::shared_ptr<PeerConnection> getConnection(uint32_t peerId) const;

  mutable std::mutex mutex_;
  std::unordered_map<uint32_t, std::shared_ptr<PeerConnection>> peers_;
};

} // namespace shard
