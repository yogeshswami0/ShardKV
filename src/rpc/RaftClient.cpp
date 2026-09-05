#include "rpc/RaftClient.h"
#include "config/Config.h"
#include "util/Logger.h"

namespace shard {

RaftClient::RaftClient() {}

RaftClient::~RaftClient() {}

void RaftClient::addPeer(uint32_t peerId, const std::string &address) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto peer = std::make_shared<PeerConnection>();
  peer->address = address;
  peer->client = std::make_unique<TcpClient>();
  peers_[peerId] = peer;
}

void RaftClient::removePeer(uint32_t peerId) {
  std::lock_guard<std::mutex> lock(mutex_);
  peers_.erase(peerId);
}

std::shared_ptr<RaftClient::PeerConnection> RaftClient::getConnection(uint32_t peerId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = peers_.find(peerId);
  if (it != peers_.end()) {
    return it->second;
  }
  return nullptr;
}

bool RaftClient::requestVote(uint32_t peerId, uint64_t term, uint32_t candidateId,
                             uint64_t lastLogIndex, uint64_t lastLogTerm, bool preVote,
                             uint64_t &responseTerm, bool &voteGranted) {
  auto peer = getConnection(peerId);
  if (!peer) return false;

  std::lock_guard<std::mutex> lock(peer->mutex);

  if (!peer->client->isConnected()) {
    if (!peer->client->connect(peer->address)) {
      return false;
    }
    peer->client->setTimeout(Config::instance().rpcTimeoutMs());
  }

  RaftVoteReq req{0, peerId, term, candidateId, lastLogIndex, lastLogTerm, preVote};
  std::vector<uint8_t> payload = serializeRaftVoteReq(req);
  MessageHeader reqHeader{(uint32_t)payload.size(), (uint32_t)MessageType::RAFT_VOTE_REQ};

  MessageHeader respHeader;
  std::vector<uint8_t> respPayload;
  
  if (!peer->client->sendRequest(reqHeader, payload, respHeader, respPayload)) {
    peer->client->disconnect(); // Reconnect on next try
    return false;
  }

  if (respHeader.type != (uint32_t)MessageType::RAFT_VOTE_RESP) {
    peer->client->disconnect();
    return false;
  }

  Buffer buf(respPayload);
  try {
    RaftVoteResp resp = deserializeRaftVoteResp(buf);
    responseTerm = resp.term;
    voteGranted = resp.voteGranted;
    return true;
  } catch (...) {
    peer->client->disconnect();
    return false;
  }
}

bool RaftClient::appendEntries(uint32_t peerId, uint64_t term, uint32_t leaderId,
                               uint64_t prevLogIndex, uint64_t prevLogTerm,
                               const std::vector<RaftLogEntry> &entries,
                               uint64_t leaderCommit, uint64_t &responseTerm,
                               bool &success, uint64_t &matchIndex) {
  auto peer = getConnection(peerId);
  if (!peer) return false;

  std::lock_guard<std::mutex> lock(peer->mutex);

  if (!peer->client->isConnected()) {
    if (!peer->client->connect(peer->address)) {
      return false;
    }
    peer->client->setTimeout(Config::instance().rpcTimeoutMs());
  }

  RaftAppendReq req{0, peerId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit};
  std::vector<uint8_t> payload = serializeRaftAppendReq(req);
  MessageHeader reqHeader{(uint32_t)payload.size(), (uint32_t)MessageType::RAFT_APPEND_REQ};

  MessageHeader respHeader;
  std::vector<uint8_t> respPayload;
  
  if (!peer->client->sendRequest(reqHeader, payload, respHeader, respPayload)) {
    peer->client->disconnect();
    return false;
  }

  if (respHeader.type != (uint32_t)MessageType::RAFT_APPEND_RESP) {
    peer->client->disconnect();
    return false;
  }

  Buffer buf(respPayload);
  try {
    RaftAppendResp resp = deserializeRaftAppendResp(buf);
    responseTerm = resp.term;
    success = resp.success;
    matchIndex = resp.matchIndex;
    return true;
  } catch (...) {
    peer->client->disconnect();
    return false;
  }
}

std::vector<uint32_t> RaftClient::getPeerIds() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<uint32_t> ids;
  ids.reserve(peers_.size());
  for (const auto &pair : peers_) {
    ids.push_back(pair.first);
  }
  return ids;
}

} // namespace shard
