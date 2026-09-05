#pragma once

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#ifdef ERROR
#undef ERROR
#endif
#ifdef DELETE
#undef DELETE
#endif
using socklen_t = int;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#define closesocket close
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
using SOCKET = int;
#endif

#include <mutex>

namespace shard {

#ifdef _WIN32
inline int &socketRefCount() {
  static int count = 0;
  return count;
}

inline std::mutex &socketInitMutex() {
  static std::mutex m;
  return m;
}
#endif

inline void initSockets() {
#ifdef _WIN32
  std::lock_guard<std::mutex> lock(socketInitMutex());
  if (socketRefCount()++ == 0) {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
  }
#endif
}

inline void cleanupSockets() {
#ifdef _WIN32
  std::lock_guard<std::mutex> lock(socketInitMutex());
  if (socketRefCount() > 0 && --socketRefCount() == 0) {
    WSACleanup();
  }
#endif
}

} // namespace shard
