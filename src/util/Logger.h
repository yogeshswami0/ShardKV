#pragma once

#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

namespace shard {

enum class LogLevel { DEBUG = 0, INFO = 1, WARN = 2, ERR = 3 };

/**
 * Thread-safe logger with configurable log levels.
 * Uses RAII pattern for automatic newline and flushing.
 */
class Logger {
public:
  static Logger &instance();

  void setLevel(LogLevel level) { currentLevel_ = level; }
  void setLevel(const std::string &level);
  LogLevel level() const { return currentLevel_; }

  // Log entry builder - automatically flushes on destruction
  class LogEntry {
  public:
    LogEntry(LogLevel level, const char *file, int line);
    ~LogEntry();

    template <typename T> LogEntry &operator<<(const T &value) {
      stream_ << value;
      return *this;
    }

    bool shouldLog() const { return shouldLog_; }

  private:
    std::ostringstream stream_;
    LogLevel level_;
    bool shouldLog_;
  };

  friend class LogEntry;

private:
  Logger();

  std::string timestamp();
  const char *levelString(LogLevel level);

  LogLevel currentLevel_ = LogLevel::INFO;
  std::mutex mutex_;
};

// Convenience macros that include file and line info
#define LOG_DEBUG                                                              \
  shard::Logger::LogEntry(shard::LogLevel::DEBUG, __FILE__, __LINE__)
#define LOG_INFO                                                               \
  shard::Logger::LogEntry(shard::LogLevel::INFO, __FILE__, __LINE__)
#define LOG_WARN                                                               \
  shard::Logger::LogEntry(shard::LogLevel::WARN, __FILE__, __LINE__)
#define LOG_ERROR                                                              \
  shard::Logger::LogEntry(shard::LogLevel::ERR, __FILE__, __LINE__)

} // namespace shard
