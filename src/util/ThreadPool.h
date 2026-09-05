#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace shard {

/**
 * Fixed-size thread pool for background tasks like compaction.
 * Supports graceful shutdown and task queuing.
 */
class ThreadPool {
public:
  explicit ThreadPool(size_t numThreads, size_t maxQueueSize = 0);
  ~ThreadPool();

  // Prevent copying
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  // Submit a task and get a future for the result. Blocks if the queue is
  // bounded and full (backpressure).
  template <typename Func, typename... Args>
  auto submit(Func &&func, Args &&...args)
      -> std::future<typename std::invoke_result<Func, Args...>::type>;

  // Non-blocking submit. Returns false if the pool is shut down or the
  // bounded queue is full, so the caller can reject the request instead of
  // stalling an I/O thread.
  bool trySubmit(std::function<void()> task);

  // Queue size for monitoring
  size_t pendingTasks() const;

  // Graceful shutdown
  void shutdown();
  bool isShutdown() const { return shutdown_; }

private:
  void workerLoop();

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  size_t maxQueueSize_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::condition_variable not_full_cv_;
  std::atomic<bool> shutdown_{false};
};

template <typename Func, typename... Args>
auto ThreadPool::submit(Func &&func, Args &&...args)
    -> std::future<typename std::invoke_result<Func, Args...>::type> {
  using ReturnType = typename std::invoke_result<Func, Args...>::type;

  auto task = std::make_shared<std::packaged_task<ReturnType()>>(
      std::bind(std::forward<Func>(func), std::forward<Args>(args)...));

  std::future<ReturnType> result = task->get_future();

  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (maxQueueSize_ > 0) {
      not_full_cv_.wait(lock, [this]() {
        return shutdown_ || tasks_.size() < maxQueueSize_;
      });
    }
    
    if (shutdown_) {
      throw std::runtime_error("Cannot submit to shutdown thread pool");
    }
    tasks_.emplace([task]() { (*task)(); });
  }

  condition_.notify_one();
  return result;
}

} // namespace shard
