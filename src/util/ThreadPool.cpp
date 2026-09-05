#include "util/ThreadPool.h"
#include "util/Logger.h"
#include <exception>

namespace shard {

ThreadPool::ThreadPool(size_t numThreads, size_t maxQueueSize) : maxQueueSize_(maxQueueSize) {
  workers_.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    workers_.emplace_back(&ThreadPool::workerLoop, this);
  }
}

ThreadPool::~ThreadPool() { shutdown(); }

void ThreadPool::workerLoop() {
  while (true) {
    std::function<void()> task;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this] { return shutdown_ || !tasks_.empty(); });

      if (shutdown_ && tasks_.empty()) {
        return;
      }

      task = std::move(tasks_.front());
      tasks_.pop();
      if (maxQueueSize_ > 0) {
        not_full_cv_.notify_one();
      }
    }

    try {
      task();
    } catch (const std::exception &e) {
      LOG_ERROR << "Thread pool task threw: " << e.what();
    } catch (...) {
      LOG_ERROR << "Thread pool task threw an unknown exception";
    }
  }
}

bool ThreadPool::trySubmit(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
      return false;
    }
    if (maxQueueSize_ > 0 && tasks_.size() >= maxQueueSize_) {
      return false;
    }
    tasks_.emplace(std::move(task));
  }
  condition_.notify_one();
  return true;
}

size_t ThreadPool::pendingTasks() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return tasks_.size();
}

void ThreadPool::shutdown() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_)
      return;
    shutdown_ = true;
  }

  condition_.notify_all();
  not_full_cv_.notify_all();

  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

} // namespace shard
