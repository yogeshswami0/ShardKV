#include "net/TcpClient.h"
#include "util/Logger.h"
#include <regex>

#ifndef _WIN32
#include <netinet/tcp.h>
#include <sys/time.h>
#endif

namespace shard {

TcpClient::TcpClient() { initSockets(); }

TcpClient::~TcpClient() {
  disconnect();
  cleanupSockets();
}

bool TcpClient::connect(const std::string &address) {
  disconnect();

  std::regex addrRegex("^(.+):(\\d+)$");
  std::smatch match;
  if (!std::regex_match(address, match, addrRegex)) {
    return false;
  }

  std::string host = match[1].str();
  int port = std::stoi(match[2].str());

  sock_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sock_ == INVALID_SOCKET)
    return false;

  sockaddr_in serverAddr{};
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &serverAddr.sin_addr) != 1) {
    disconnect();
    return false;
  }

  if (::connect(sock_, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) ==
      SOCKET_ERROR) {
    disconnect();
    return false;
  }

  int nodelay = 1;
  setsockopt(sock_, IPPROTO_TCP, TCP_NODELAY, (const char *)&nodelay,
             sizeof(nodelay));

  setTimeout(timeoutMs_);
  return true;
}

void TcpClient::disconnect() {
  if (sock_ != INVALID_SOCKET) {
#ifdef _WIN32
    shutdown(sock_, SD_BOTH);
#else
    shutdown(sock_, SHUT_RDWR);
#endif
    closesocket(sock_);
    sock_ = INVALID_SOCKET;
  }
}

void TcpClient::setTimeout(int ms) {
  timeoutMs_ = ms;
  if (sock_ == INVALID_SOCKET)
    return;

#ifdef _WIN32
  DWORD timeout = static_cast<DWORD>(ms);
  setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout,
             sizeof(timeout));
  setsockopt(sock_, SOL_SOCKET, SO_SNDTIMEO, (const char *)&timeout,
             sizeof(timeout));
#else
  struct timeval tv;
  tv.tv_sec = ms / 1000;
  tv.tv_usec = (ms % 1000) * 1000;
  setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
  setsockopt(sock_, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));
#endif
}

bool TcpClient::sendAll(const char *data, int len) {
  int totalSent = 0;
  while (totalSent < len) {
    int sent = send(sock_, data + totalSent, len - totalSent, 0);
    if (sent <= 0)
      return false;
    totalSent += sent;
  }
  return true;
}

bool TcpClient::recvAll(char *data, int len) {
  int totalRead = 0;
  while (totalRead < len) {
    int r = recv(sock_, data + totalRead, len - totalRead, 0);
    if (r <= 0)
      return false;
    totalRead += r;
  }
  return true;
}

bool TcpClient::sendRequest(const MessageHeader &reqHeader,
                            const std::vector<uint8_t> &reqPayload,
                            MessageHeader &respHeader,
                            std::vector<uint8_t> &respPayload) {
  if (sock_ == INVALID_SOCKET)
    return false;

  uint32_t headerData[2] = {htonl(reqHeader.length), htonl(reqHeader.type)};
  if (!sendAll((const char *)headerData, 8))
    return false;
  if (!reqPayload.empty() &&
      !sendAll((const char *)reqPayload.data(),
               static_cast<int>(reqPayload.size())))
    return false;

  uint32_t respHeaderData[2];
  if (!recvAll((char *)respHeaderData, 8))
    return false;

  respHeader.length = ntohl(respHeaderData[0]);
  respHeader.type = ntohl(respHeaderData[1]);

  if (respHeader.length > maxPayloadBytes_)
    return false;

  respPayload.resize(respHeader.length);
  if (respHeader.length > 0 &&
      !recvAll((char *)respPayload.data(),
               static_cast<int>(respHeader.length)))
    return false;

  return true;
}

} // namespace shard
