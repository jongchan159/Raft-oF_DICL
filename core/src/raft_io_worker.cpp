#include "raft_io_worker.h"

#include <chrono>
#include <exception>
#include <iostream>
#include <sstream>

namespace nvmeof_raft {

namespace {

/* spin 루프 한 바퀴의 쉼. 같은 코어의 다른 하이퍼스레드에 자원을 양보한다. */
inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#endif
}

void log_job_exception(const char *what) {
    std::ostringstream os;
    os << "[WARN] IoWorker: job threw (" << what << ") -- continuing with the next job";
    std::cerr << os.str() << std::endl;
}

} /* anonymous namespace */

IoWorker::~IoWorker() {
    stop();
}

void IoWorker::start(int spin_us, size_t max_queue) {
    std::lock_guard<std::mutex> lk(mu_);
    if (running_ || th_.joinable()) {
        return;
    }
    spin_us_ = spin_us;
    max_queue_ = max_queue;
    stopping_.store(false);
    running_ = true;
    th_ = std::thread([this]() { loop(); });
}

void IoWorker::stop() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        running_ = false;   /* 이제부터 submit은 false */
        if (!th_.joinable()) {
            return;
        }
        stopping_.store(true);
    }
    cv_.notify_all();
    th_.join();
}

bool IoWorker::submit(std::function<void()> job) {
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!running_ || queue_.size() >= max_queue_) {
            return false;
        }
        queue_.push_back(std::move(job));
        queued_.store(queue_.size(), std::memory_order_release);
    }
    cv_.notify_one();
    return true;
}

size_t IoWorker::queued() const {
    std::lock_guard<std::mutex> lk(mu_);
    return queue_.size();
}

void IoWorker::loop() {
    bool just_ran = false;   /* 직전에 일을 했으면 잠들기 전에 spin한다 */
    for (;;) {
        if (just_ran && spin_us_ > 0) {
            auto end = std::chrono::steady_clock::now() + std::chrono::microseconds(spin_us_);
            while (queued_.load(std::memory_order_acquire) == 0 &&
                   !stopping_.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < end) {
                cpu_relax();
            }
        }

        std::function<void()> job;
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [this]() { return !queue_.empty() || stopping_.load(); });
            if (queue_.empty()) {
                return;   /* stop: 받아 둔 일을 다 돌렸다 */
            }
            job = std::move(queue_.front());
            queue_.pop_front();
            queued_.store(queue_.size(), std::memory_order_release);
        }

        /* 큐 락을 놓은 채로 돌린다 (헤더 주석의 락 규칙) */
        try {
            job();
        } catch (const std::exception &e) {
            log_job_exception(e.what());
        } catch (...) {
            log_job_exception("unknown exception");
        }
        just_ran = true;
    }
}

} /* namespace nvmeof_raft */
