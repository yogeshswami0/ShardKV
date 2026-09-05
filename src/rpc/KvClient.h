#pragma once

#include "net/Protocol.h"
#include "net/TcpClient.h"
#include <mutex>
#include <optional>
#include <string>

namespace shard {

struct KvGetResult {
  StatusCode status = StatusCode::ERROR;
  uint32_t leaderId = 0;
  bool found = false;
  std::string value;
};

struct KvWriteResult {
  StatusCode status = StatusCode::ERROR;
  uint32_t leaderId = 0;
};

struct KvPingResult {
  bool ok = false;
  uint32_t nodeId = 0;
  uint32_t leaderId = 0;
  uint64_t term = 0;
};

class KvClient {
public:
  KvClient();
  ~KvClient();

  bool connect(const std::string &address);
  void disconnect();
  bool isConnected() const;

  void setTimeout(int ms);

  KvPingResult ping();
  KvGetResult get(const std::string &key);
  KvWriteResult put(const std::string &key, const std::string &value);
  KvWriteResult del(const std::string &key);

private:
  bool rpc(MessageType reqType, const std::vector<uint8_t> &payload,
           MessageType expectedResp, Buffer &out);

  TcpClient client_;
  mutable std::mutex mutex_;
};

} // namespace shard
