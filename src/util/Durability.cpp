#include "util/Durability.h"
#include "util/Logger.h"
#include <filesystem>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include "util/Platform.h"

namespace shard {

bool durableSync(int fd) {
  if (fd < 0) {
    return false;
  }

#if defined(__APPLE__)
  // fsync() on macOS returns once the data reaches the drive, which may still
  // park it in a volatile write cache. F_FULLFSYNC forces it through.
  if (::fcntl(fd, F_FULLFSYNC, 0) == 0) {
    return true;
  }
  // Some filesystems (and most network mounts) reject F_FULLFSYNC with ENOTSUP.
  // Fall back rather than silently reporting failure.
  if (errno != ENOTSUP && errno != EINVAL) {
    LOG_ERROR << "F_FULLFSYNC failed: " << std::string(strerror(errno));
    return false;
  }
  return ::fsync(fd) == 0;
#elif defined(__linux__)
  return ::fdatasync(fd) == 0;
#else
  return ::fsync(fd) == 0;
#endif
}

bool syncDirectory(const std::string &dirPath) {
#ifdef _WIN32
  return true; // Directory fsync not needed/supported easily on Windows via open()
#else
  int dirFd = ::open(dirPath.c_str(), O_RDONLY | O_BINARY);
  if (dirFd < 0) {
    LOG_ERROR << "Failed to open directory for sync: " << dirPath;
    return false;
  }
  bool ok = (::fsync(dirFd) == 0);
  if (!ok) {
    LOG_ERROR << "Failed to sync directory: " << dirPath;
  }
  ::close(dirFd);
  return ok;
#endif
}

std::string parentDirectory(const std::string &path) {
  size_t pos = path.find_last_of('/');
  if (pos == std::string::npos) {
    return ".";
  }
  if (pos == 0) {
    return "/";
  }
  return path.substr(0, pos);
}

bool atomicInstall(const std::string &tmpPath, const std::string &finalPath) {
  int fd = ::open(tmpPath.c_str(), O_RDWR | O_BINARY);
  if (fd < 0) {
    LOG_ERROR << "Failed to open temp file for sync: " << tmpPath;
    return false;
  }

  bool ok = durableSync(fd);
  ::close(fd);

  if (!ok) {
    LOG_ERROR << "Failed to sync temp file: " << tmpPath;
    return false;
  }

  std::error_code ec;
  std::filesystem::rename(tmpPath, finalPath, ec);
  if (ec) {
    LOG_ERROR << "Failed to rename " << tmpPath << " -> " << finalPath << ": " << ec.message();
    return false;
  }

  return syncDirectory(parentDirectory(finalPath));
}

} // namespace shard
