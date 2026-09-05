#include "net/TcpServer.h"
#include "util/Logger.h"
#include <future>
#include <stdexcept>
#include <regex>

namespace shard {

TcpServer::TcpServer(std::shared_ptr<ThreadPool> threadPool,
                     MessageHandler handler, uint32_t maxPayloadBytes,
                     size_t maxConnections)
    : threadPool_(std::move(threadPool)), handler_(std::move(handler)),
      maxPayloadBytes_(maxPayloadBytes), maxConnections_(maxConnections) {
  initSockets();
}

TcpServer::~TcpServer() {
  stop();
  cleanupSockets();
}

void TcpServer::start(const std::string &address) {
  if (running_.exchange(true))
    return;

  std::regex addrRegex("^(.+):(\\d+)$");
  std::smatch match;
  if (!std::regex_match(address, match, addrRegex)) {
    running_ = false;
    throw std::runtime_error("Invalid address format. Expected host:port");
  }

  std::string host = match[1].str();
  int port = std::stoi(match[2].str());

  listenSocket_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listenSocket_ == INVALID_SOCKET) {
    running_ = false;
    throw std::runtime_error("Failed to create socket");
  }

  int opt = 1;
  setsockopt(listenSocket_, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt,
             sizeof(opt));

  sockaddr_in serverAddr{};
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &serverAddr.sin_addr) != 1) {
    closesocket(listenSocket_);
    listenSocket_ = INVALID_SOCKET;
    running_ = false;
    throw std::runtime_error("Invalid listen address");
  }

  if (bind(listenSocket_, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) ==
      SOCKET_ERROR) {
    closesocket(listenSocket_);
    listenSocket_ = INVALID_SOCKET;
    running_ = false;
    throw std::runtime_error("Bind failed");
  }

  if (listen(listenSocket_, SOMAXCONN) == SOCKET_ERROR) {
    closesocket(listenSocket_);
    listenSocket_ = INVALID_SOCKET;
    running_ = false;
    throw std::runtime_error("Listen failed");
  }

  sockaddr_in bound{};
  socklen_t boundLen = sizeof(bound);
  if (getsockname(listenSocket_, (struct sockaddr *)&bound, &boundLen) == 0) {
    listenPort_ = ntohs(bound.sin_port);
  } else {
    listenPort_ = static_cast<uint16_t>(port);
  }

  LOG_INFO << "Server listening on " << host << ":" << listenPort_;
  acceptThread_ = std::thread(&TcpServer::acceptLoop, this);
}

void TcpServer::stop() {
  if (!running_.exchange(false))
    return;

  SOCKET listen = listenSocket_;
  listenSocket_ = INVALID_SOCKET;
  if (listen != INVALID_SOCKET) {
#ifdef _WIN32
    shutdown(listen, SD_BOTH);
#else
    shutdown(listen, SHUT_RDWR);
#endif
    closesocket(listen);
  }

  std::vector<SOCKET> clients;
  {
    std::lock_guard<std::mutex> lock(clientsMutex_);
    clients.reserve(clientWriteMutexes_.size());
    for (const auto &pair : clientWriteMutexes_) {
      clients.push_back(pair.first);
    }
  }
  for (SOCKET sock : clients) {
#ifdef _WIN32
    shutdown(sock, SD_BOTH);
#else
    shutdown(sock, SHUT_RDWR);
#endif
  }

  if (acceptThread_.joinable()) {
    acceptThread_.join();
  }

  std::vector<std::thread> toJoin;
  {
    std::lock_guard<std::mutex> lock(clientsMutex_);
    toJoin.swap(clientThreads_);
  }
  for (auto &t : toJoin) {
    if (t.joinable())
      t.join();
  }

  std::lock_guard<std::mutex> lock(clientsMutex_);
  clientWriteMutexes_.clear();
}

void TcpServer::acceptLoop() {
  while (running_) {
    sockaddr_in clientAddr{};
    socklen_t clientLen = sizeof(clientAddr);
    SOCKET clientSock =
        accept(listenSocket_, (struct sockaddr *)&clientAddr, &clientLen);

    if (clientSock == INVALID_SOCKET) {
      if (running_)
        LOG_ERROR << "Accept failed";
      continue;
    }

    bool reject = false;
    {
      std::lock_guard<std::mutex> lock(clientsMutex_);
      if (clientWriteMutexes_.size() >= maxConnections_) {
        reject = true;
      } else {
        clientWriteMutexes_[clientSock] = std::make_shared<std::mutex>();
        clientThreads_.emplace_back(&TcpServer::connectionLoop, this,
                                    clientSock);
      }
    }
    if (reject) {
      LOG_WARN << "Rejecting connection: at max " << maxConnections_;
      closesocket(clientSock);
    }
  }
}

void TcpServer::connectionLoop(SOCKET clientSock) {
  while (running_) {
    uint32_t headerData[2];
    int bytesRead = recv(clientSock, (char *)headerData, 8, 0);
    if (bytesRead <= 0) {
      break;
    }

    int totalRead = bytesRead;
    while (totalRead < 8 && running_) {
      bytesRead = recv(clientSock, (char *)headerData + totalRead, 8 - totalRead,
                       0);
      if (bytesRead <= 0)
        break;
      totalRead += bytesRead;
    }
    if (totalRead < 8)
      break;

    MessageHeader header;
    header.length = ntohl(headerData[0]);
    header.type = ntohl(headerData[1]);

    if (header.length > maxPayloadBytes_) {
      LOG_ERROR << "Rejecting frame of " << header.length
                << " bytes (max " << maxPayloadBytes_ << ")";
      break;
    }

    std::vector<uint8_t> payload(header.length);
    totalRead = 0;
    while (totalRead < (int)header.length && running_) {
      bytesRead = recv(clientSock, (char *)payload.data() + totalRead,
                       header.length - totalRead, 0);
      if (bytesRead <= 0)
        break;
      totalRead += bytesRead;
    }
    if (totalRead < (int)header.length)
      break;

    auto payloadPtr = std::make_shared<std::vector<uint8_t>>(std::move(payload));
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();

    bool queued = threadPool_->trySubmit(
        [this, clientSock, header, payloadPtr, done]() {
          try {
            Buffer buf(*payloadPtr);
            handler_(clientSock, header, buf);
          } catch (const std::exception &e) {
            LOG_ERROR << "Handler threw: " << e.what();
          }
          done->set_value();
        });

    if (!queued) {
      auto type = static_cast<MessageType>(header.type);
      if (type == MessageType::KV_GET_REQ) {
        KVGetResp resp{static_cast<uint32_t>(StatusCode::OVERLOADED), 0, false,
                       ""};
        sendResponse(clientSock,
                     encodeFrame(MessageType::KV_GET_RESP,
                                 serializeKVGetResp(resp)));
      } else if (type == MessageType::KV_PUT_REQ) {
        KVPutResp resp{static_cast<uint32_t>(StatusCode::OVERLOADED), 0};
        sendResponse(clientSock,
                     encodeFrame(MessageType::KV_PUT_RESP,
                                 serializeKVPutResp(resp)));
      } else if (type == MessageType::KV_DELETE_REQ) {
        KVDeleteResp resp{static_cast<uint32_t>(StatusCode::OVERLOADED), 0};
        sendResponse(clientSock,
                     encodeFrame(MessageType::KV_DELETE_RESP,
                                 serializeKVDeleteResp(resp)));
      }
      continue;
    }

    // Wait so requests on one connection stay in order and the next recv
    // does not race a still-running handler on the same socket.
    future.wait();
  }

  closesocket(clientSock);
  {
    std::lock_guard<std::mutex> lock(clientsMutex_);
    clientWriteMutexes_.erase(clientSock);
  }
}

void TcpServer::sendResponse(SOCKET clientSock,
                             const std::vector<uint8_t> &data) {
  std::shared_ptr<std::mutex> writeMutex;
  {
    std::lock_guard<std::mutex> lock(clientsMutex_);
    auto it = clientWriteMutexes_.find(clientSock);
    if (it != clientWriteMutexes_.end()) {
      writeMutex = it->second;
    } else {
      return;
    }
  }

  std::lock_guard<std::mutex> lock(*writeMutex);
  int totalSent = 0;
  int toSend = static_cast<int>(data.size());
  while (totalSent < toSend) {
    int sent = send(clientSock, (const char *)data.data() + totalSent,
                    toSend - totalSent, 0);
    if (sent == SOCKET_ERROR) {
      break;
    }
    totalSent += sent;
  }
}

} // namespace shard
