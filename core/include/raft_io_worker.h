#ifndef RAFT_IO_WORKER_H
#define RAFT_IO_WORKER_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

namespace nvmeof_raft {

/* ============================================================
 * IoWorker -- 넣은 순서대로 일을 하나씩 자기 스레드에서 돌리는 상시 워커.
 *
 * push 모드(DECISIONS.md D20)에서 리더는 팔로워 볼륨에 쓰는 일을 대상마다
 * 이 워커 하나에 맡긴다. 그래서 persist(Lock A)가 push가 끝나기를 기다리지
 * 않는다. 워커는 Server도 mu도 모른다 -- 무엇을 쓰고, 끝나서 무엇을
 * 기록하고 누구를 깨울지는 넘겨받은 job 함수가 다 한다.
 *
 * 큐가 필요한 이유: Lock A가 push를 기다리지 않으므로 같은 대상에게 가는
 * push가 겹칠 수 있다(동시 클라이언트, 또는 느린 팔로워). 줄을 세우면
 * Lock A는 막히지 않고, 완료는 넣은 순서대로 오고, 쌓이는 양은 max_queue로
 * 제한된다 -- 넘치면 submit이 false를 돌려주고 호출자는 그 대상을 pull로
 * 돌린다.
 *
 * 락 규칙: 워커는 큐에서 job을 꺼낸 뒤 **자기 큐 락을 놓고** 실행한다.
 * push job은 끝에서 Server::mu를 잡는데, persist는 Server::mu를 쥔 채
 * submit으로 큐 락을 잡는다. 큐 락을 쥔 채 job을 돌리면 두 스레드가 서로
 * 상대가 쥔 락을 기다리며 멈춘다.
 *
 * 대기: 일을 마친 뒤 spin_us 동안은 큐 길이를 돌며 기다린다(깨우기 지연
 * 없이 다음 일을 받는다). 그래도 없으면 cv로 잠든다. cv 깨우기 지연이
 * NVMe-oF 쓰기 한 번과 같은 규모라서 둔 것이다
 * (raft_rdma_transport.cpp의 kSpinClientUs와 같은 방식). 한 번도 일이 오지
 * 않은 워커는 spin하지 않는다.
 * ============================================================ */
class IoWorker {
public:
    IoWorker() = default;
    ~IoWorker();

    IoWorker(const IoWorker &) = delete;
    IoWorker &operator=(const IoWorker &) = delete;

    /* 스레드를 띄운다. 이미 돌고 있으면 아무것도 하지 않는다. */
    void start(int spin_us, size_t max_queue);

    /* 새 일을 받지 않고, 받아 둔 일은 끝까지 돌린 뒤 스레드를 join한다.
     * 두 번 불러도 된다. 여러 스레드에서 동시에 부르지는 않는다. */
    void stop();

    /* 일을 큐 끝에 넣는다. 워커가 돌고 있지 않거나(start 전, stop 뒤)
     * 큐에 이미 max_queue개가 있으면 넣지 않고 false. 실행 중인 일은 큐에
     * 세지 않는다. job이 던진 예외는 워커가 잡아 로그만 남긴다. */
    bool submit(std::function<void()> job);

    /* 큐에 남은 일 수 (실행 중인 것 제외). 진단과 테스트용. */
    size_t queued() const;

private:
    void loop();

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    std::atomic<size_t> queued_{0};       /* spin 중에 락 없이 보는 큐 길이 */
    std::atomic<bool> stopping_{false};
    bool running_ = false;                /* start ~ stop 사이. submit 허용 */
    int spin_us_ = 0;
    size_t max_queue_ = 0;
    std::thread th_;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_IO_WORKER_H */
