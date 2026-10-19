/* ============================================================
 * raft_apply.cpp -- 클라이언트 진입점 (raft.go의 Apply/ApplyTimed 대응)
 *
 * 이 파일이 없어서 지금까지 "리더 로그에 엔트리를 넣는 경로"가 아예
 * 비어 있었다. 나머지 조각(persist_circular, append_entries,
 * advance_commit_index, apply_pending)은 전부 구현돼 있었고, 그것들을
 * 순서대로 엮는 것이 Apply의 일이다:
 *
 *   Lock A ---------------------------------------------------------
 *     리더 확인 -> 링 여유 확인(can_write_all, 없으면 ring_not_full 대기)
 *     -> Entry 생성/log push_back/tail_log_index 전진
 *     -> persist_circular(true, n)  [O_DIRECT 쓰기 + fdatasync,
 *                                    log_slot_map에 슬롯 기록]
 *   unlock ---------------------------------------------------------
 *     append_entries(sink)  -> 팔로워마다 스레드, PBA 메타만 전송
 *     마지막 엔트리의 committed 신호 대기 (advance_commit_index가 signal)
 *     -> apply_pending이 상태머신에 반영하고 result_sink 호출
 *
 * ApplyTimings의 항등식은 raft_timings.h 주석 그대로 따른다:
 *   Total ~= LHandler + LPersist + AENet + FHandler + ReplNet
 *            + StorageIO + QuorumWait
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <chrono>
#include <algorithm>
#include <atomic>
#include <cstdio>

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

struct GcReq {
    const std::vector<std::vector<uint8_t>> *commands;
    std::shared_ptr<ApplyCollector> collector;
    std::shared_ptr<EntryCommitSignal> last_signal;

    bool done = false;
    bool busy = false;
    std::string error;
    
    int64_t nvme_ns = 0;      // 대표가 채움 (묶음 전체의 persist 시간)
    int64_t a_held_ns = 0;    // 대표가 채움
};

static void commit_batch_locked(Server *s, std::unique_lock<std::mutex> &lk, 
                                std::vector<GcReq *> &batch, clock_type::time_point t_a_held) {
    auto fail = [&](const std::string &err, bool busy) {
        for (GcReq *r : batch) { 
            r->error = err;
            r->busy = busy;
            r->done = true;
        }
    };

    if (s->raft.state != ServerState::Leader) {
        fail("not leader", false);
        return;
    }

    if (batch.size() > 1) {
        std::vector<size_t> sizes;
        for (GcReq *r : batch) {
            for (const auto &c : *r->commands) { sizes.push_back(c.size()); }
        }
        if (!s->can_write_sizes(sizes)) {
            {
                std::lock_guard<std::mutex> gcl(s->gc_mu);
                for (size_t i = batch.size(); i-- > 1; ) {
                    s->gc_queue.push_front(batch[i]);
                }
            }
            batch.resize(1);
        }
    }

    const auto &cmds0 = *batch[0]->commands;
    if (!s->can_write_all(cmds0)) {
        bool ok  = s->ring_not_full.wait_for(
            lk, std::chrono::milliseconds(kRingWaitMs),
            [&]() { return s->done.load() || 
                           s->raft.state != ServerState::Leader ||
                           s->can_write_all(cmds0); 
            }
        );

        if (!ok || s->done.load()) { 
            fail("ring buffer full", true);
            return;
        }

        if (s->raft.state != ServerState::Leader) {
            fail("not leader", false);
            return;
        }
    }

    const uint64_t saved_tail_log_index = s->ring.tail_log_index;
    const uint64_t saved_tail_slot = s->ring.tail_slot;

    size_t total_n = 0;
    for (GcReq *r : batch) {
        const auto &cmds = *r->commands;
        const size_t n = cmds.size();
        auto collector = r->collector;
        for (size_t i = 0; i < n; i++) {
            Entry e;
            e.term = s->raft.current_term;
            e.command = cmds[i];
            bool is_last = (i + 1 == n);
            e.result_sink = [collector, is_last](ApplyResult res) {
                {
                    std::lock_guard<std::mutex> clk(collector->mu);
                    collector->received++;
                    if (!res.error.empty() && collector->error.empty()) {
                        collector->error = res.error;
                    }

                    if (is_last) {
                        collector->last_result = std::move(res.result);
                    }
                }
                collector->cv.notify_all();
            };
            if (is_last) { r->last_signal = e.committed; }
            s->raft.log.push_back(std::move(e));
            s->ring.tail_log_index++;
        }
        total_n += n;
    }

    int64_t nvme_ns = 0;
    try {
        nvme_ns = s->persist_circular(true, static_cast<int>(total_n));
    } catch (const std::exception &e) {
        for (size_t i = 0; i < total_n && s->raft.log.size() > 1; i++) {
            s->raft.log.pop_back();
            s->ring.tail_log_index--;
        }
        s->ring.tail_slot = saved_tail_slot;                        // 보강: 복원
        for (uint64_t idx = saved_tail_log_index;
            idx < saved_tail_log_index + total_n; idx++) {
            s->ring.log_slot_map.erase(idx);                         // 보강: 낡은 기록 삭제
        }
        fail(std::string("persist failed: ") + e.what(), false);
        return;
    }

    const int64_t a_held_ns = elapsed_ns(t_a_held);
    for (GcReq *r : batch) { 
        r->nvme_ns = nvme_ns;
        r->a_held_ns = a_held_ns;
        r->done = true;
    }

    static uint64_t g_batches = 0, g_reqs = 0, g_entries = 0;
    static int64_t  g_held_ns = 0, g_persist_ns = 0;
    static auto     g_t0 = clock_type::now();
    g_batches++; g_reqs += batch.size(); g_entries += total_n;
    g_held_ns += a_held_ns; g_persist_ns += nvme_ns;
    {
        auto now = clock_type::now();
        int64_t el = std::chrono::duration_cast<std::chrono::nanoseconds>(now - g_t0).count();
        if (el >= 5'000'000'000LL) {
            std::fprintf(stderr,
                "[LOCKA] batches/s=%.0f entries/batch=%.1f reqs/batch=%.1f "
                "held/batch=%.0fus persist/batch=%.0fus held/entry=%.1fus util=%.0f%%\n",
                g_batches * 1e9 / el, (double)g_entries / g_batches, (double)g_reqs / g_batches,
                g_held_ns / 1e3 / g_batches, g_persist_ns / 1e3 / g_batches,
                g_held_ns / 1e3 / g_entries, 100.0 * g_held_ns / el);
            g_batches = g_reqs = g_entries = 0; g_held_ns = g_persist_ns = 0; g_t0 = now;
        }
    }
}

/* ============================================================
 * apply_internal: apply와 apply_timed의 공통 본문.
 * timings != nullptr이면 ReplSink를 붙여 구간을 분해한다.
 * ============================================================ */
static ApplyResult apply_internal(Server *s,
                                   const std::vector<std::vector<uint8_t>> &commands,
                                   ApplyTimings *timings,
                                   bool *out_busy) {
    ApplyResult out;
    if (out_busy != nullptr) {
        *out_busy = false;
    }
    if (commands.empty()) {
        return out;   /* 빈 요청은 성공 처리 (원본과 동일) */
    }

    /* ReplSink는 timed 경로에서만 할당 (원본: "non-timed Apply passes
     * nil and goroutines skip the push") */
    /* [수정-7] ReplSink는 반드시 힙에 둔다 (스택에 두면 AE 워커가 죽은
     * 프레임을 건드린다) -- DECISIONS.md D7 */
    std::shared_ptr<ReplSink> sink =
        (timings != nullptr) ? std::make_shared<ReplSink>(s->raft.cluster.size() - 1) : nullptr;

    auto collector = std::make_shared<ApplyCollector>();
    std::shared_ptr<EntryCommitSignal> last_signal;
    size_t n = commands.size();
    collector->expected = n;

    // [Group Commit]
    GcReq req;
    req.commands = &commands;
    req.collector = collector;
    {
        std::lock_guard<std::mutex> gcl(s->gc_mu);
        s->gc_queue.push_back(&req);
    }
    // [Group Commit]


    /* ---- Lock A ---- */
    auto t_lock_a = clock_type::now();
    std::unique_lock<std::mutex> lk(s->mu);
    int64_t mutex_a = elapsed_ns(t_lock_a);
    if (sink != nullptr) {
        sink->set_mutex_a(mutex_a);
    }
    auto t_a_held = clock_type::now();

    // [Group Commit]
    std::vector<GcReq *> batch;
    if (!req.done) {
        batch.push_back(&req);
        {
            std::lock_guard<std::mutex> gcl(s->gc_mu);
            auto it = std::find(s->gc_queue.begin(), s->gc_queue.end(), &req);
            if (it != s->gc_queue.end()) {
                s->gc_queue.erase(it);
            }

            auto bytes_of = [](const GcReq &r) {
                size_t b = 0;
                for (const auto &c : *r.commands) {
                    b += c.size();
                }
                return b;
            };

            size_t bytes = bytes_of(req);
            while (!s->gc_queue.empty() && batch.size() < s->gc_max_reqs) {
                GcReq *nx = s->gc_queue.front();
                size_t nb = bytes_of(*nx);
                if (bytes + nb > s->gc_max_bytes) {
                    break;
                }
                batch.push_back(nx);
                bytes += nb;
                s->gc_queue.pop_front();
            }
        }
        commit_batch_locked(s, lk, batch, t_a_held);
    }
    

    if (!req.error.empty()) {
        if (req.busy && out_busy != nullptr) {
            *out_busy = true;
        }
        out.error = req.error;
        return out;
    }
    last_signal = req.last_signal;
    const int64_t nvme_ns = req.nvme_ns;
    const int64_t a_held_ns = req.a_held_ns;

    lk.unlock();
    // [Group Commit]

    /* ---- 복제 ---- */
    auto t_repl = clock_type::now();
    s->append_entries(sink);   /* shared_ptr 복사가 워커에게 전달된다 */

    int64_t commit_wait_ns = 0;
    {
        auto t_cw = clock_type::now();
        /* 주의: last_signal->mu를 잡은 상태에서 s.mu를 잡으면 안 된다.
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
            if (s->done.load()) {
                out.error = "server shutting down";
                return out;
            }
            if (!s->is_leader()) {
                out.error = "leadership lost before commit";
                return out;
            }
        }
        commit_wait_ns = elapsed_ns(t_cw);
    }
    int64_t replicate_ns = elapsed_ns(t_repl);

    /* apply_pending이 상태머신에 반영하고 result_sink를 호출한다.
     * committed 신호는 advance_commit_index가, apply는 apply 워커가
     * 하므로 둘 사이에 짧은 지연이 있을 수 있다 -- 마지막 엔트리의
     * 결과가 도착할 때까지만 짧게 기다린다. */
    /* [수정-6] 조건변수로 기다린다. 락을 쥔 채 sleep하면 s.mu까지 함께
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

    if (timings == nullptr) {
        return out;
    }

    /* 항등식 계산은 순수 산술이므로 분리해 뒀다 (위 derive_apply_timings) */
    ApplyWalls walls;
    walls.nvme_ns = nvme_ns;
    walls.a_held_ns = a_held_ns;
    walls.replicate_ns = replicate_ns;
    walls.mutex_a_ns = mutex_a;
    walls.commit_wait_ns = commit_wait_ns;
    derive_apply_timings(walls, sink.get(), s->prof, *timings);

    return out;
}

ApplyResult Server::apply(const std::vector<std::vector<uint8_t>> &commands,
                           bool *out_busy) {
    return apply_internal(this, commands, nullptr, out_busy);
}

ApplyResult Server::apply_timed(const std::vector<std::vector<uint8_t>> &commands,
                                 ApplyTimings *out_timings, bool *out_busy) {
    ApplyTimings local;
    ApplyTimings *target = (out_timings != nullptr) ? out_timings : &local;
    auto t0 = clock_type::now();
    ApplyResult r = apply_internal(this, commands, target, out_busy);
    target->total_ns = elapsed_ns(t0);
    return r;
}

/* busy 응답에 붙일 retry-after (net/ 쪽 RPC 핸들러가 사용) */
int apply_busy_retry_after_ms() { return kBusyRetryAfterMs; }

} /* namespace nvmeof_raft */
