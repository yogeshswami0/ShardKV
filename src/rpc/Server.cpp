#include "rpc/Server.h"
#include "config/Config.h"
#include "raft/Command.h"
#include "util/Logger.h"

#include <chrono>

namespace shard {

Server::Server(std::shared_ptr<StorageEngine> storage,
               std::shared_ptr<RaftNode> raft)
    : storage_(std::move(storage)), raft_(std::move(raft)) {
  auto &config = Config::instance();
  threadPool_ = std::make_shared<ThreadPool>(config.threadPoolSize(),
                                             config.requestQueueSize());

  tcpServer_ = std::make_unique<TcpServer>(
      threadPool_,
      [this](SOCKET clientSock, const MessageHeader &header, Buffer &payload) {
        this->handleMessage(clientSock, header, payload);
      },
      config.maxPayloadBytes(), config.maxConnections());

  // Apply is the only writer to the engine. Leader and followers both mutate
  // here, after the entry is committed.
  raft_->setApplyCallback([this](uint64_t index, const std::string &command) {
    auto parsed = Command::decode(command);
    if (!parsed.has_value()) {
      LOG_ERROR << "Skipping malformed command at index " << index;
      return;
    }
    if (parsed->type == Command::Type::PUT) {
      storage_->put(parsed->key, parsed->value);
    } else {
      storage_->remove(parsed->key);
    }
  });
}

Server::~Server() { stop(); }

void Server::start(const std::string &address) {
  stopped_ = false;
  tcpServer_->start(address);
}

void Server::stop() {
  if (tcpServer_) {
    tcpServer_->stop();
  }
  if (threadPool_) {
    threadPool_->shutdown();
  }
  {
    std::lock_guard<std::mutex> lock(waitMutex_);
    stopped_ = true;
  }
  waitCv_.notify_all();
}

void Server::wait() {
  std::unique_lock<std::mutex> lock(waitMutex_);
  waitCv_.wait(lock, [this] { return stopped_.load(); });
}

uint16_t Server::listenPort() const {
  return tcpServer_ ? tcpServer_->listenPort() : 0;
}

void Server::reply(SOCKET clientSock, MessageType type,
                   const std::vector<uint8_t> &payload) {
  tcpServer_->sendResponse(clientSock, encodeFrame(type, payload));
}

uint32_t Server::knownLeaderId() const {
  if (!raft_)
    return 0;
  auto leader = raft_->leaderId();
  return leader.value_or(0);
}

StatusCode Server::proposeAndWait(const std::string &command) {
  if (!raft_->isLeader()) {
    return StatusCode::NOT_LEADER;
  }

  auto res = raft_->propose(command);
  if (!res.success) {
    return raft_->isLeader() ? StatusCode::ERROR : StatusCode::NOT_LEADER;
  }

  int timeoutMs = Config::instance().commitTimeoutMs();
  if (!raft_->waitForApplied(res.index, res.term,
                             std::chrono::milliseconds(timeoutMs))) {
    return StatusCode::TIMEOUT;
  }
  return StatusCode::OK;
}

void Server::handleMessage(SOCKET clientSock, const MessageHeader &header,
                           Buffer &payload) {
  try {
    switch (static_cast<MessageType>(header.type)) {
    case MessageType::PING_REQ: {
      PingResp resp;
      resp.nodeId = raft_ ? raft_->nodeId() : 0;
      resp.leaderId = knownLeaderId();
      resp.term = raft_ ? raft_->currentTerm() : 0;
      reply(clientSock, MessageType::PING_RESP, serializePingResp(resp));
      break;
    }
    case MessageType::KV_GET_REQ: {
      auto req = deserializeKVGetReq(payload);
      KVGetResp resp;
      resp.leaderId = knownLeaderId();
      resp.found = false;

      auto &config = Config::instance();
      if (!config.allowStaleReads()) {
        if (!raft_->isLeader()) {
          resp.status = static_cast<uint32_t>(StatusCode::NOT_LEADER);
          reply(clientSock, MessageType::KV_GET_RESP, serializeKVGetResp(resp));
          break;
        }
        if (!raft_->confirmLeadership(
                std::chrono::milliseconds(config.rpcTimeoutMs()))) {
          resp.status = static_cast<uint32_t>(StatusCode::TIMEOUT);
          resp.leaderId = knownLeaderId();
          reply(clientSock, MessageType::KV_GET_RESP, serializeKVGetResp(resp));
          break;
        }
      }

      auto val = storage_->get(req.key);
      resp.status = static_cast<uint32_t>(StatusCode::OK);
      if (val) {
        resp.found = true;
        resp.value = *val;
      }
      reply(clientSock, MessageType::KV_GET_RESP, serializeKVGetResp(resp));
      break;
    }
    case MessageType::KV_PUT_REQ: {
      auto req = deserializeKVPutReq(payload);
      KVPutResp resp;
      resp.leaderId = knownLeaderId();
      resp.status = static_cast<uint32_t>(
          proposeAndWait(Command::put(req.key, req.value).encode()));
      resp.leaderId = knownLeaderId();
      reply(clientSock, MessageType::KV_PUT_RESP, serializeKVPutResp(resp));
      break;
    }
    case MessageType::KV_DELETE_REQ: {
      auto req = deserializeKVDeleteReq(payload);
      KVDeleteResp resp;
      resp.leaderId = knownLeaderId();
      resp.status = static_cast<uint32_t>(
          proposeAndWait(Command::del(req.key).encode()));
      resp.leaderId = knownLeaderId();
      reply(clientSock, MessageType::KV_DELETE_RESP,
            serializeKVDeleteResp(resp));
      break;
    }
    case MessageType::RAFT_VOTE_REQ: {
      auto req = deserializeRaftVoteReq(payload);
      RaftVoteResp resp;
      resp.shardId = req.shardId;
      resp.term = 0;
      resp.voteGranted = false;

      if (raft_) {
        raft_->handleRequestVote(req.term, req.candidateId, req.lastLogIndex,
                                 req.lastLogTerm, req.preVote, resp.term,
                                 resp.voteGranted);
      }

      reply(clientSock, MessageType::RAFT_VOTE_RESP,
            serializeRaftVoteResp(resp));
      break;
    }
    case MessageType::RAFT_APPEND_REQ: {
      auto req = deserializeRaftAppendReq(payload);
      RaftAppendResp resp;
      resp.shardId = req.shardId;
      resp.term = 0;
      resp.success = false;
      resp.matchIndex = 0;

      if (raft_) {
        raft_->handleAppendEntries(req.term, req.leaderId, req.prevLogIndex,
                                   req.prevLogTerm, req.entries,
                                   req.leaderCommit, resp.term, resp.success,
                                   resp.matchIndex);
      }

      reply(clientSock, MessageType::RAFT_APPEND_RESP,
            serializeRaftAppendResp(resp));
      break;
    }
    default:
      LOG_ERROR << "Unknown message type: " << header.type;
      break;
    }
  } catch (const std::exception &e) {
    LOG_ERROR << "Error handling message: " << e.what();
  }
}

} // namespace shard
