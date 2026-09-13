/* ============================================================
 * raft_apply.cpp -- 클라이언트 진입점 (raft.go의 Apply 대응)
 *
 * 나머지 조각(persist_circular, append_entries, advance_commit_index,
 * apply_pending)을 순서대로 엮는 것이 Apply의 일이다:
 *
 *   Lock A ---------------------------------------------------------
 *     리더 확인 -> 링 여유 확인(can_write_all, 없으면 ring_not_full 대기)
 *     -> Entry 생성/log push_back/tail_log_index 전진
 *     -> persist_circular(true, n)  [O_DIRECT 쓰기 + fdatasync,
 *                                    log_slot_map에 슬롯 기록]
 *   unlock ---------------------------------------------------------
 *     append_entries()  -> 팔로워마다 스레드, PBA 메타만 전송
 *     마지막 엔트리의 committed 신호 대기 (advance_commit_index가 signal)
 *     -> apply_pending이 상태머신에 반영하고 result_sink 호출
 *
 * 원본에는 같은 본문을 구간별로 분해해 계측하는 apply_timed가 함께 있었다.
 * 이 버전에는 없다 -- ApplyTimings/ReplSink와 그 배선 전부가 빠졌다.
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <chrono>

namespace nvmeof_raft {

namespace {
/* Apply 한 번이 만든 엔트리들의 결과를 모으는 상자.
 * result_sink는 apply_pending 안에서 s.mu를 잡은 채로 호출되므로
 * (원본 Go도 채널 send를 s.mu 안에서 한다) 절대 블록하면 안 된다 --
 * 여기서는 뮤텍스로 감싼 push만 한다. */
struct ApplyCollector {
    std::mutex mu;
    std::condition_variable cv;
    std::vector<uint8_t> last_result;
    std::string error;
    size_t received = 0;
    size_t expected = 0;
};

/* 원본 backpressure: "reject early with a retry-after instead of
 * blocking the caller indefinitely in Apply()" */
constexpr int kBusyRetryAfterMs = 5;

/* 링이 꽉 찼을 때 ring_not_full을 기다리는 상한.
 * 무한 대기하면 리더가 강등돼도 클라이언트가 영원히 매달리므로,
 * 타임아웃 후 busy로 돌려보낸다. */
constexpr int kRingWaitMs = 200;

/* 커밋 신호를 기다릴 때의 CV 폴링 주기. 신호를 놓쳐도 이 주기로 다시
 * 조건을 확인한다. */
constexpr int kCommitPollMs = 20;

/* 마지막 엔트리의 apply 결과(collector)를 기다리는 상한.
 * advance_commit_index -> apply_pending은 별도 스레드라 짧은 지연이 있다. */
constexpr int kResultWaitMs = 200;

} /* anonymous namespace */

ApplyResult Server::apply(const std::vector<std::vector<uint8_t>> &commands,
                           bool *out_busy) {
    ApplyResult out;
    if (out_busy != nullptr) {
        *out_busy = false;
    }
    if (commands.empty()) {
        return out;   /* 빈 요청은 성공 처리 (원본과 동일) */
    }

    auto collector = std::make_shared<ApplyCollector>();
    std::shared_ptr<EntryCommitSignal> last_signal;
    size_t n = commands.size();
    collector->expected = n;

    /* ---- Lock A ---- */
    std::unique_lock<std::mutex> lk(mu);

    if (raft.state != ServerState::Leader) {
        out.error = "not leader";
        return out;
    }

    /* 링 여유 확인. can_write_all이 false면 slot GC가 앞쪽을 비워줄
     * 때까지 기다린다 (do_slot_gc -> ring_not_full.notify_all).
     * 상한을 넘으면 busy로 돌려보낸다 -- 원본의 RingBusy 조기 거절과
     * 같은 backpressure 정책. */
    if (!can_write_all(commands)) {
        bool ok = ring_not_full.wait_for(
            lk, std::chrono::milliseconds(kRingWaitMs),
            [&]() { return done.load() ||
                            raft.state != ServerState::Leader ||
                            can_write_all(commands); });
        if (!ok || done.load()) {    // !ok - timeout
            if (out_busy != nullptr) {
                *out_busy = true;
            }
            out.error = "ring buffer full";
            return out;
        }
        if (raft.state != ServerState::Leader) {
            out.error = "not leader";
            return out;
        }
    }

    /* 엔트리 생성 + 로그 append */
    for (size_t i = 0; i < n; i++) {
        Entry e;
        e.term = raft.current_term;
        e.command = commands[i];
        bool is_last = (i + 1 == n);
        e.result_sink = [collector, is_last](ApplyResult r) {
            {
                std::lock_guard<std::mutex> clk(collector->mu);     // {...}가 끝나면 자동 파괴
                collector->received++;
                if (!r.error.empty() && collector->error.empty()) {
                    collector->error = r.error;
                }
                if (is_last) {
                    collector->last_result = std::move(r.result);
                }
            }
            /* notify는 락 밖에서. 이 sink는 apply_pending이 s.mu를 잡은
             * 채로 부르므로 절대 블록하면 안 된다. */
            collector->cv.notify_all();
        };
        if (is_last) {
            last_signal = e.committed;
        }
        raft.log.push_back(std::move(e));
        ring.tail_log_index++;
    }

    /* persist_circular: O_DIRECT로 device에 엔트리를 쓰고 log_slot_map에
     * 슬롯을 기록한다. */
    persist_circular(true, static_cast<int>(n));

    lk.unlock();   /* ---- Lock A 끝 ---- */

    /* ---- 복제 ---- */
    append_entries();

    /* 마지막 엔트리가 커밋될 때까지 대기.
     * advance_commit_index가 signal_committed()를 호출한다 (메인 루프에서
     * 주기적으로 돌고, AE 성공 시 heartbeat_timeout을 즉시 만료시켜
     * 다음 라운드를 앞당긴다).
     *
     * 주의: last_signal->mu를 잡은 상태에서 s.mu를 잡으면 안 된다.
     * advance_commit_index는 s.mu를 잡은 채로 signal_committed()가
     * committed->mu를 잡으므로(s.mu -> committed->mu), 여기서
     * committed->mu -> s.mu 순서로 잡으면 락 순서 역전으로 데드락이
     * 난다. 그래서 신호 확인과 리더십 확인을 분리한다. */
    for (;;) {
        bool signaled = false;
        {
            std::unique_lock<std::mutex> clk(last_signal->mu);
            if (!last_signal->signaled) {
                last_signal->cv.wait_for(clk, std::chrono::milliseconds(kCommitPollMs));
            }
            signaled = last_signal->signaled;
        }
        if (signaled) {
            break;
        }
        /* 여기서는 어떤 락도 안 잡고 있다 */
        if (done.load()) {
            out.error = "server shutting down";
            return out;
        }
        if (!is_leader()) {
            out.error = "leadership lost before commit";
            return out;
        }
    }

    /* apply_pending이 상태머신에 반영하고 result_sink를 호출한다.
     * committed 신호는 advance_commit_index가, apply는 apply 워커가
     * 하므로 둘 사이에 짧은 지연이 있을 수 있다 -- 마지막 엔트리의
     * 결과가 도착할 때까지만 짧게 기다린다.
     * [수정-6] 조건변수로 기다린다. 락을 쥔 채 sleep하면 s.mu까지 함께
     * 묶여 노드 전체가 정지한다 (224ms -> 2.9ms) -- DECISIONS.md D6 */
    {
        auto deadline = clock_type::now() + std::chrono::milliseconds(kResultWaitMs);
        std::unique_lock<std::mutex> clk(collector->mu);
        collector->cv.wait_until(clk, deadline,
            [&]() { return collector->received >= n; });
    }

    {
        std::lock_guard<std::mutex> clk(collector->mu);
        out.result = collector->last_result;
        if (!collector->error.empty()) {
            out.error = collector->error;
        }
    }

    return out;
}

/* net/의 클라이언트 핸들러가 busy 응답에 실어 보낼 재시도 간격.
 * (선언: net/include/raft_tcp_server.h) */
int apply_busy_retry_after_ms() { return kBusyRetryAfterMs; }

} /* namespace nvmeof_raft */
