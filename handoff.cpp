#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>
#include <algorithm>
using clk = std::chrono::steady_clock;
int main(int argc, char** argv) {
    int T = argc > 1 ? atoi(argv[1]) : 19;
    int rounds = argc > 2 ? atoi(argv[2]) : 2000;
    std::mutex m;
    std::atomic<int> arrived{0}, done{0}, go_round{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> th;
    for (int i = 0; i < T; i++) th.emplace_back([&] {
        int seen = 0;
        while (true) {
            while (go_round.load(std::memory_order_acquire) == seen) {
                if (stop.load()) return;
                std::this_thread::yield();
            }
            seen = go_round.load();
            arrived.fetch_add(1);
            m.lock();                       // 메인이 쥔 락을 기다리며 잠든다
            done.fetch_add(1);
            m.unlock();
        }
    });
    std::vector<double> per;
    for (int r = 0; r < rounds; r++) {
        m.lock();
        arrived = 0; done = 0;
        go_round.fetch_add(1, std::memory_order_release);
        while (arrived.load() < T) std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::microseconds(300));   // 전원이 잠들 시간
        auto t0 = clk::now();
        m.unlock();
        while (done.load() < T) std::this_thread::yield();
        double us = std::chrono::duration<double, std::micro>(clk::now() - t0).count();
        if (r >= rounds / 10) per.push_back(us / T);
    }
    stop = true;
    for (auto& t : th) t.join();
    std::sort(per.begin(), per.end());
    printf("T=%d  획득당 시간(us)  중앙 %.2f  p10 %.2f  p90 %.2f  (nproc=%u)\n", T,
           per[per.size()/2], per[per.size()/10], per[per.size()*9/10], std::thread::hardware_concurrency());
}