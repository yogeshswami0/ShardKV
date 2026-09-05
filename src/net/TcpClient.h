#pragma once

#include "net/Protocol.h"
#include "net/SocketCommon.h"
#include <string>
#include <vector>

namespace shard {

class TcpClient {
public:
  TcpClient();
  ~TcpClient();

  bool connect(const std::string &address);
  void disconnect();
  bool isConnected() const { return sock_ != INVALID_SOCKET; }

  bool sendRequest(const MessageHeader &reqHeader,
                   const std::vector<uint8_t> &reqPayload,
                   MessageHeader &respHeader, std::vector<uint8_t> &respPayload);

  void setTimeout(int ms);
  void setMaxPayloadBytes(uint32_t bytes) { maxPayloadBytes_ = bytes; }

private:
  bool sendAll(const char *data, int len);
  bool recvAll(char *data, int len);

  SOCKET sock_ = INVALID_SOCKET;
  int timeoutMs_ = 500;
  uint32_t maxPayloadBytes_ = kDefaultMaxPayloadBytes;
};

} // namespace shard
