/* ============================================================
 * raft_read.cpp -- ReadIndex 읽기 (Ongaro 학위논문 §6.4) [수정-17]
 *
 * 읽기 전용 명령(KVS 의 GET)을 **로그에 넣지 않고** 리더 상태머신에서
 * 실행한다. 논문의 다섯 단계와 이 파일의 대응:
 *
 *   1) 현 term 에 커밋한 엔트리가 있어야 한다
 *        -> read_index 를 term_start_index(become_leader 의 no-op)까지 올린다
 *   2) commitIndex 를 readIndex 로 기록         -> Server::read 진입
 *   3) heartbeat 1라운드, 과반 ack 로 리더십 확인 -> ReadIndexHeartbeat 라운드
 *   4) applied >= readIndex 까지 대기           -> last_applied > read_index
 *   5) 상태머신에서 읽고 반환                    -> mu 밖에서 statemachine->apply
 *
 * 3) 의 heartbeat 는 **AppendEntries 와 분리된 메시지**다 (etcd/raft 의
 * MsgHeartbeat 이 MsgApp 과 별개인 것과 같다). 팔로워당 AE 워커가 하나이고
 * AE 커넥션도 하나라서, 빈 AE 로 확인하면 진행 중인 데이터 AE(팔로워의
 * blockcopy 포함)가 끝날 때까지 줄을 선다 -- GET 이 PUT 의 로그 비용을
 * 간접적으로 지불한다. 그래서 전용 워커 + 전용 커넥션으로 보낸다.
 *
 * 배치: 라운드가 진행 중일 때 들어온 읽기는 워커의 pending 플래그로
 * 합쳐져, 다음 heartbeat 하나가 그 시점의 최신 read_round 를 싣고 한꺼번에
 * 확인한다 (DARE 의 "배치당 한 번의 리더십 확인" 과 같다).
 *
 * mu 경합: 이 파일이 mu 를 잡는 구간은 전부 O(1) 이다 (스냅샷, ack 반영,
 * 깨어났을 때의 조건 확인). 상태머신 호출과 결과 복사는 mu 밖이다.
 * ============================================================ */
#include "raft_server.h"
#include "raft_basics.h"

#include <algorithm>
#include <chrono>

namespace nvmeof_raft {

namespace {

/* 리더십 확인을 기다리는 동안 이 간격마다 heartbeat 를 다시 찌른다
 * (전송 실패 / 응답 유실 대비). */
constexpr int kReadRekickMs = 20;

} /* namespace */

/* ============================================================
 * 팔로워 측: ReadIndexHeartbeat 처리
 *
 * term 처리는 handle_append_entries_request 와 똑같다
 * (raft_handle_append_entries.cpp) -- 진짜 heartbeat 이므로 같은 term 이면
 * election timer 도 리셋한다. 로그 일관성 검사, 링, blockcopy 는 없다.
 * ============================================================ */
void Server::handle_read_index_heartbeat(const ReadIndexHeartbeatRequest &req,
                                         ReadIndexHeartbeatResponse &rsp) {
    std::lock_guard<std::mutex> lk(mu);

    update_term(req.rpc.term);
    if (req.rpc.term == raft.current_term && raft.state == ServerState::Candidate) {
        raft.state = ServerState::Follower;
    }

    rsp.rpc.term = raft.current_term;
    rsp.round = req.round;
    rsp.ok = false;

    if (raft.state != ServerState::Follower) {
        return;
    }
    if (req.rpc.term < raft.current_term) {
        return;
    }
    reset_election_timeout();
    rsp.ok = true;
}

/* ============================================================
 * 리더 측: 팔로워별 heartbeat 워커
 * ============================================================ */
void Server::kick_read_hb_workers() {
    for (auto &w : workers.read_hb_workers) {
        if (!w) {
            continue;   /* 자기 자신 자리 */
        }
        {
            std::lock_guard<std::mutex> wl(w->mu);
            w->pending = true;
        }
        w->cv.notify_one();
    }
}

void Server::start_read_hb_workers() {
    workers.read_hb_workers.resize(raft.cluster.size());
    for (size_t i = 0; i < raft.cluster.size(); i++) {
        if (static_cast<int>(i) == raft.cluster_index || workers.read_hb_workers[i]) {
            continue;
        }
        auto w = std::make_unique<ReadIndexHbWorkerState>();
        auto *wp = w.get();
        wp->th = std::thread([this, fi = static_cast<int>(i), wp]() {
            read_hb_loop(fi, wp);
        });
        workers.read_hb_workers[i] = std::move(w);
    }
}

void Server::read_hb_loop(int fi, ReadIndexHbWorkerState *w) {
    while (!done.load()) {
        {
            std::unique_lock<std::mutex> wl(w->mu);
            w->cv.wait(wl, [&] { return done.load() || w->pending; });
            if (done.load()) {
                break;
            }
            w->pending = false;
        }

        ReadIndexHeartbeatRequest req;
        {
            std::lock_guard<std::mutex> lk(mu);
            if (raft.state != ServerState::Leader) {
                continue;
            }
            /* 보내는 시점의 최신 라운드를 싣는다 -- 그 사이에 들어온 읽기도
             * 이 응답 하나로 확인된다 */
            req.rpc.term = raft.current_term;
            req.leader_id = raft.id;
            req.round = raft.read_round;
            if (raft.cluster[static_cast<size_t>(fi)].read_ack_round >= req.round) {
                continue;   /* 이 팔로워는 이미 이 라운드를 확인해 줬다 */
            }
        }

        ReadIndexHeartbeatResponse rsp;
        bool ok = (transport != nullptr) && transport->read_index_heartbeat(fi, req, rsp);
        if (!ok) {
            continue;   /* Server::read 가 kReadRekickMs 마다 다시 찌른다 */
        }

        std::lock_guard<std::mutex> lk(mu);
        apply_read_hb_response(fi, req, rsp);
    }
}

void Server::apply_read_hb_response(int fi, const ReadIndexHeartbeatRequest &req,
                                    const ReadIndexHeartbeatResponse &rsp) {
    if (update_term(rsp.rpc.term)) {
        return;   /* 더 높은 term 을 봤다 -- 강등 (update_term 이 read_cv 를 깨운다) */
    }
    /* 보낸 뒤에 term 이 바뀌었거나 리더가 아니게 됐으면 그 ack 는 무효다 */
    if (raft.state != ServerState::Leader || raft.current_term != req.rpc.term || !rsp.ok) {
        return;
    }
    ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
    /* m.read_ack_round = "이 팔로워가 지금까지 확인해 준 최대 read 라운드"
       req.round        = 방금 그 heartbeat 가 실어 보낸 라운드 */
    /* 새로운(더 높은) 라운드를 확인받았다 → 지각/중복 응답 → */
    if (req.round > m.read_ack_round) {
        m.read_ack_round = req.round;
        try_advance_read_confirmed_locked();
    }
}

void Server::try_advance_read_confirmed_locked() {
    const size_t n = raft.cluster.size();
    if (n == 0) {
        return;
    }
    std::vector<uint64_t> acks;
    acks.reserve(n);
    for (size_t i = 0; i < n; i++) {
        /* 리더 자신은 요청한 최신 라운드를 항상 인정한다 */
        acks.push_back(static_cast<int>(i) == raft.cluster_index
                           ? raft.read_round
                           : raft.cluster[i].read_ack_round);
    }
    const size_t majority = n / 2 + 1;
    std::nth_element(acks.begin(), acks.begin() + static_cast<std::ptrdiff_t>(majority - 1),
                     acks.end(), std::greater<uint64_t>());
    const uint64_t r = acks[majority - 1];   /* 과반이 도달한 최대 라운드 */
    if (r > raft.read_confirmed_round) {
        raft.read_confirmed_round = r;
        read_cv.notify_all();
    }
}

/* ============================================================
 * Server::read
 * ============================================================ */
ReadResult Server::read(const std::vector<std::vector<uint8_t>> &queries,
                        ReadTimings *out_timings) {
    const auto t0 = clock_type::now();
    clock_type::time_point t_confirmed = t0;
    clock_type::time_point t_applied = t0;
    ReadResult out;

    auto finish = [&](clock_type::time_point t_end) {
        if (out_timings != nullptr) {
            out_timings->confirm_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_confirmed - t0).count();
            out_timings->apply_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_applied - t_confirmed).count();
            out_timings->sm_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_end - t_applied).count();
            out_timings->total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_end - t0).count();
        }
        return out;
    };

    std::unique_lock<std::mutex> lk(mu);
    if (raft.state != ServerState::Leader) {
        out.error = "not leader";
        lk.unlock();
        t_confirmed = t_applied = clock_type::now();
        return finish(t_applied);
    }
    if (queries.empty()) {
        /* 로그에 남지 않는 리더 탐색 (raft_client 의 kvs op 들이 쓴다) */
        lk.unlock();
        t_confirmed = t_applied = clock_type::now();
        return finish(t_applied);
    }

    /* 1) 2) */
    const uint64_t term0 = raft.current_term;
    const uint64_t read_index = std::max(raft.commit_index, raft.term_start_index);
    const auto deadline = t0 + std::chrono::milliseconds(read_timeout_ms);

    /* 대기 중 빠져나가야 하는 조건. 비어 있으면 계속 기다린다. */
    auto abort_reason = [&]() -> const char * {
        if (done.load()) {
            return "stopping";
        }
        if (raft.state != ServerState::Leader || raft.current_term != term0) {
            return "leadership lost";
        }
        return nullptr;
    };
    auto fail = [&](const char *why, bool busy) {
        out.error = why;
        out.busy = busy;
        lk.unlock();
        t_applied = clock_type::now();
        if (t_confirmed == t0) {
            t_confirmed = t_applied;
        }
        return finish(t_applied);
    };

    /* 3) 리더십 확인. 단일 노드면 자기 자신이 과반이다. */
    if (raft.cluster.size() > 1) {
        const uint64_t round = ++raft.read_round;
        lk.unlock();
        kick_read_hb_workers();
        lk.lock();
        while (raft.read_confirmed_round < round) {
            if (const char *why = abort_reason()) {
                return fail(why, false);
            }
            const auto now = clock_type::now();
            if (now >= deadline) {
                return fail("read quorum timeout", true);
            }
            const auto slice = std::min(deadline, now + std::chrono::milliseconds(kReadRekickMs));
            if (read_cv.wait_until(lk, slice) == std::cv_status::timeout &&
                raft.read_confirmed_round < round && abort_reason() == nullptr) {
                lk.unlock();
                kick_read_hb_workers();
                lk.lock();
            }
        }
    }
    t_confirmed = clock_type::now();

    /* 4) read_index 까지 apply. read_index 는 이미 커밋된 지점이므로
     * 아직 커밋되지 않은 PUT 은 기다리지 않는다 (메모리 apply 만). */
    if (raft.last_applied <= read_index) {
        read_waiters++;
        while (raft.last_applied <= read_index) {
            if (const char *why = abort_reason()) {
                read_waiters--;
                return fail(why, false);
            }
            if (clock_type::now() >= deadline) {
                read_waiters--;
                return fail("read apply timeout", true);
            }
            read_cv.wait_until(lk, deadline);
        }
        read_waiters--;
    }
    lk.unlock();
    t_applied = clock_type::now();

    /* 5) mu 밖에서 상태머신 호출 -- 상태머신은 스스로 thread-safe 하다
     * (raft_server.h 의 락 규약). read_index 이후 엔트리가 더 적용된
     * 상태를 읽을 수 있지만 그것도 커밋된 엔트리이므로 linearizable 하다. */
    out.results.reserve(queries.size());
    for (const auto &q : queries) {
        out.results.push_back(statemachine->apply(q));
    }
    return finish(clock_type::now());
}

} /* namespace nvmeof_raft */
