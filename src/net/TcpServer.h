#pragma once

#include "net/Protocol.h"
#include "net/SocketCommon.h"
#include "util/ThreadPool.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace shard {

using MessageHandler =
    std::function<void(SOCKET clientSock, const MessageHeader &header,
                       Buffer &payload)>;

class TcpServer {
public:
  TcpServer(std::shared_ptr<ThreadPool> threadPool, MessageHandler handler,
            uint32_t maxPayloadBytes = kDefaultMaxPayloadBytes,
            size_t maxConnections = 256);
  ~TcpServer();

  // Start listening on address (e.g. "0.0.0.0:8080"). Port 0 is allowed.
  void start(const std::string &address);

  void stop();

  bool running() const { return running_.load(); }
  uint16_t listenPort() const { return listenPort_; }

  void sendResponse(SOCKET clientSock, const std::vector<uint8_t> &data);

private:
  struct ClientSession {
    SOCKET sock = INVALID_SOCKET;
    std::thread thread;
    std::shared_ptr<std::mutex> writeMutex;
  };

  void acceptLoop();
  void connectionLoop(SOCKET clientSock);

  std::shared_ptr<ThreadPool> threadPool_;
  MessageHandler handler_;
  uint32_t maxPayloadBytes_;
  size_t maxConnections_;

  SOCKET listenSocket_ = INVALID_SOCKET;
  std::atomic<bool> running_{false};
  std::thread acceptThread_;
  uint16_t listenPort_ = 0;

  mutable std::mutex clientsMutex_;
  std::unordered_map<SOCKET, std::shared_ptr<std::mutex>> clientWriteMutexes_;
  std::vector<std::thread> clientThreads_;
};

} // namespace shard
