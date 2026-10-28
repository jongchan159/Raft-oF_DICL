/* ============================================================
 * IoWorker 단위 테스트 (core/include/raft_io_worker.h, DECISIONS.md D20).
 *
 * push 모드 리더는 팔로워 볼륨에 쓰는 일을 대상마다 이 워커에 맡기고
 * 기다리지 않는다. 그래서 다음이 깨지면 안 된다:
 *   - 넣은 순서대로 하나씩 돈다 (대상마다 push 완료가 인덱스 순서로 와야
 *     끝난 구간이 연속으로 남는다)
 *   - 호출자 스레드가 아니라 워커 스레드에서 돈다 (persist가 기다리지 않는다)
 *   - 일 하나가 예외를 던져도 워커는 다음 일을 계속한다
 *   - 큐 상한을 넘으면 넣지 않는다 (멈춘 대상이 끝없이 쌓이지 않게)
 *   - stop은 받아 둔 일을 끝까지 돌린다
 * 기다릴 때는 조건이 설 때까지 짧게 확인한다 (상한 1초) -- 시간에 기대지
 * 않는다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_io_worker.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <ostream>   /* doctest가 std::thread::id를 operator<<로 찍는다 */
#include <stdexcept>
#include <thread>
#include <vector>

using namespace nvmeof_raft;

namespace {

/* pred가 참이 될 때까지 최대 1초 기다린다. 참이 되면 true. */
template <typename Pred>
bool wait_until(Pred pred) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return pred();
}

}  /* anonymous namespace */

TEST_CASE("IoWorker: 넣은 순서대로 하나씩 돈다") {
    IoWorker w;
    w.start(0, 16);

    std::mutex mu;
    std::vector<int> order;
    for (int i = 1; i <= 5; i++) {
        CHECK(w.submit([&mu, &order, i]() {
            std::lock_guard<std::mutex> lk(mu);
            order.push_back(i);
        }));
    }
    CHECK(wait_until([&]() {
        std::lock_guard<std::mutex> lk(mu);
        return order.size() == 5;
    }));
    std::lock_guard<std::mutex> lk(mu);
    CHECK(order == std::vector<int>{1, 2, 3, 4, 5});
}

TEST_CASE("IoWorker: 호출자를 막지 않고 워커 스레드에서 돈다") {
    IoWorker w;
    w.start(0, 16);

    /* 일은 호출자가 플래그를 세울 때까지 기다린다. submit이 호출자 스레드에서
     * 일을 돌렸다면 플래그가 1초 안에 서지 않아 seen이 false로 끝난다. */
    std::atomic<bool> go{false};
    std::atomic<bool> seen{false};
    std::atomic<bool> finished{false};
    std::thread::id job_thread;
    CHECK(w.submit([&]() {
        job_thread = std::this_thread::get_id();
        seen = wait_until([&]() { return go.load(); });
        finished = true;
    }));
    go = true;
    CHECK(wait_until([&]() { return finished.load(); }));
    CHECK(seen.load());
    CHECK(job_thread != std::this_thread::get_id());
}

TEST_CASE("IoWorker: 일이 예외를 던져도 다음 일을 처리한다") {
    IoWorker w;
    w.start(0, 16);

    std::atomic<bool> second_ran{false};
    CHECK(w.submit([]() { throw std::runtime_error("boom"); }));
    CHECK(w.submit([&]() { second_ran = true; }));
    CHECK(wait_until([&]() { return second_ran.load(); }));
}

TEST_CASE("IoWorker: 큐 상한을 넘으면 넣지 않는다") {
    IoWorker w;
    w.start(0, 2);

    /* 첫 일이 큐에서 빠져 실행 중일 때 큐를 채운다 (실행 중인 일은 세지 않는다) */
    std::atomic<bool> running{false};
    std::atomic<bool> release{false};
    std::atomic<int> done_count{0};
    CHECK(w.submit([&]() {
        running = true;
        wait_until([&]() { return release.load(); });
        done_count++;
    }));
    CHECK(wait_until([&]() { return running.load(); }));

    CHECK(w.submit([&]() { done_count++; }));
    CHECK(w.submit([&]() { done_count++; }));
    CHECK(w.queued() == 2);
    CHECK(w.submit([&]() { done_count++; }) == false);   /* 넘쳤다 */

    release = true;
    CHECK(wait_until([&]() { return done_count.load() == 3; }));
}

TEST_CASE("IoWorker: stop은 받아 둔 일을 끝까지 돌리고, 그 뒤 submit은 false다") {
    IoWorker w;
    w.start(0, 16);

    std::atomic<int> done_count{0};
    for (int i = 0; i < 5; i++) {
        CHECK(w.submit([&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            done_count++;
        }));
    }
    w.stop();
    CHECK(done_count.load() == 5);

    CHECK(w.submit([]() {}) == false);
    w.stop();   /* 두 번 불러도 된다 */
}

TEST_CASE("IoWorker: start 전 submit은 false이고, stop 없이 소멸해도 된다") {
    std::atomic<bool> ran{false};
    {
        IoWorker w;
        CHECK(w.submit([&]() { ran = true; }) == false);
        w.start(0, 16);
        CHECK(w.submit([&]() { ran = true; }));
    }   /* 소멸자가 stop -> 받아 둔 일을 돌리고 join */
    CHECK(ran.load());
}

TEST_CASE("IoWorker: spin 유무와 상관없이 잠든 뒤에도 깨어난다") {
    for (int spin_us : {0, 1000}) {
        CAPTURE(spin_us);
        IoWorker w;
        w.start(spin_us, 16);

        std::atomic<int> done_count{0};
        CHECK(w.submit([&]() { done_count++; }));
        CHECK(wait_until([&]() { return done_count.load() == 1; }));

        /* spin 예산보다 오래 쉬어 워커가 cv로 잠들게 한 뒤 다시 넣는다 */
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        CHECK(w.submit([&]() { done_count++; }));
        CHECK(wait_until([&]() { return done_count.load() == 2; }));
    }
}
