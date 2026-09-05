#pragma once

#include "net/TcpServer.h"
#include "raft/RaftNode.h"
#include "storage/StorageEngine.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

namespace shard {

class Server {
public:
  Server(std::shared_ptr<StorageEngine> storage,
         std::shared_ptr<RaftNode> raft);
  ~Server();

  void start(const std::string &address);
  void stop();
  void wait();

  uint16_t listenPort() const;

private:
  void handleMessage(SOCKET clientSock, const MessageHeader &header,
                     Buffer &payload);
  void reply(SOCKET clientSock, MessageType type,
             const std::vector<uint8_t> &payload);
  uint32_t knownLeaderId() const;
  StatusCode proposeAndWait(const std::string &command);

  std::shared_ptr<StorageEngine> storage_;
  std::shared_ptr<RaftNode> raft_;
  std::shared_ptr<ThreadPool> threadPool_;
  std::unique_ptr<TcpServer> tcpServer_;

  std::mutex waitMutex_;
  std::condition_variable waitCv_;
  std::atomic<bool> stopped_{true};
};

} // namespace shard
