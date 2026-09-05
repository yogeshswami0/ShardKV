#include "util/Logger.h"
#include "util/Platform.h"
#include <algorithm>
#include <ctime>

namespace shard {

Logger &Logger::instance() {
  static Logger instance;
  return instance;
}

Logger::Logger() {
  // Level will be set from config during startup
}

void Logger::setLevel(const std::string &level) {
  std::string upper = level;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

  if (upper == "DEBUG")
    currentLevel_ = LogLevel::DEBUG;
  else if (upper == "INFO")
    currentLevel_ = LogLevel::INFO;
  else if (upper == "WARN" || upper == "WARNING")
    currentLevel_ = LogLevel::WARN;
  else if (upper == "ERROR")
    currentLevel_ = LogLevel::ERR;
}

std::string Logger::timestamp() {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::tm tm_buf;
  localtime_r(&time, &tm_buf);

  std::ostringstream oss;
  oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
  oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
  return oss.str();
}

const char *Logger::levelString(LogLevel level) {
  switch (level) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO ";
  case LogLevel::WARN:
    return "WARN ";
  case LogLevel::ERR:
    return "ERROR";
  default:
    return "?????";
  }
}

Logger::LogEntry::LogEntry(LogLevel level, const char *file, int line)
    : level_(level), shouldLog_(level >= Logger::instance().currentLevel_) {
  if (shouldLog_) {
    // Extract just the filename from the full path
    std::string filepath(file);
    size_t pos = filepath.find_last_of("/\\");
    std::string filename =
        (pos != std::string::npos) ? filepath.substr(pos + 1) : filepath;

    stream_ << "[" << Logger::instance().timestamp() << "] "
            << "[" << Logger::instance().levelString(level) << "] "
            << "[" << filename << ":" << line << "] ";
  }
}

Logger::LogEntry::~LogEntry() {
  if (shouldLog_) {
    std::lock_guard<std::mutex> lock(Logger::instance().mutex_);
    std::cerr << stream_.str() << std::endl;
  }
}

} // namespace shard
