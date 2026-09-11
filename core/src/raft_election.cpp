#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <random>

namespace nvmeof_raft {

/* 전송은 Server::transport(core/include/raft_transport.h)로만 한다. */

/* ============================================================
 * resetElectionTimeout (raft.go 원본, 로직 그대로 포팅)
 * "Election timeout must be >> heartbeat interval to tolerate lock
 *  contention from concurrent Apply + persistCircular I/O.
 *  Range: [5x heartbeat, 10x heartbeat] = [1500ms, 3000ms] at 300ms HB."
 * ============================================================ */
void Server::reset_election_timeout() {
    /* Go: rand.Intn(HeartbeatMs*10) + HeartbeatMs*20, 결과는
     * [HeartbeatMs*20, HeartbeatMs*30) 범위 -- 원본 주석의 "5x~10x"는
     * 실제로는 이 코드 기준 20x~30x에 해당함. 원본 수치 그대로 포팅.
     *
     * [수정-4] std::rand()를 쓰지 말 것. 시드가 프로세스마다 같으면 세
     * 노드의 timeout이 매 라운드 동일해져 선거가 끝나지 않는다
     * -- DECISIONS.md D4 */
    static std::mt19937_64 rng(std::random_device{}());

    /* election timeout = heartbeat_ms * [20, 30) 구간의 균등 난수.
     * 원본 raft.go의 수치를 그대로 옮긴 것이다 -- 값을 바꾸지 말 것.
     * (이 함수 위 주석이 "5x~10x"라고 서술하는 것은 원본 문서의 표현이고
     *  실제 코드는 예나 지금이나 20x~30x다.) */
    constexpr int kElectionTimeoutMinMult = 20;
    constexpr int kElectionTimeoutJitterMult = 10;   /* [min, min+jitter) */

    int interval_ms = 0;
    if (raft.heartbeat_ms > 0) {
        std::uniform_int_distribution<int> dist(
            0, raft.heartbeat_ms * kElectionTimeoutJitterMult - 1);
        interval_ms = dist(rng) + raft.heartbeat_ms * kElectionTimeoutMinMult;
    }
    raft.election_timeout = clock_type::now() + std::chrono::milliseconds(interval_ms);
}

/* ============================================================
 * timeout (raft.go 원본, 로직 그대로 포팅)
 *
 * request_vote()는 이제 스레드만 띄우고 즉시 리턴하므로(병렬화 완료),
 * mu를 직접 잡지 않아 재귀 락 문제는 없다. 다만 lk.unlock()을 미리
 * 해두는 게 여전히 맞다 -- 안 풀면 방금 띄운 request_vote_worker
 * 스레드들이 timeout()이 mu를 놓을 때까지 불필요하게 블록된다.
 * ============================================================ */
void Server::timeout() {
    std::unique_lock<std::mutex> lk(mu);

    if (clock_type::now() > raft.election_timeout) {
        raft.state = ServerState::Candidate;
        raft.current_term++;
        for (size_t i = 0; i < raft.cluster.size(); i++) {
            if (static_cast<int>(i) == raft.cluster_index) {
                raft.cluster[i].voted_for = raft.id;
            } else {
                raft.cluster[i].voted_for = 0;
            }
        }
        reset_election_timeout();
        persist_circular(false, 0);
        lk.unlock();      /* request_vote_worker들이 불필요하게 블록되지 않도록 미리 해제 */
        request_vote();
    }
}

/* ============================================================
 * requestVote (raft.go 원본, 로직 그대로 포팅)
 *
 * 병렬화 완료: append_entries와 동일한 패턴 -- 팔로워마다 스레드를
 * 띄우고 fire-and-forget으로 즉시 리턴.
 * ============================================================ */
void Server::request_vote() {
    for (size_t i = 0; i < raft.cluster.size(); i++) {
        if (static_cast<int>(i) == raft.cluster_index) {
            continue;
        }
        int peer_index = static_cast<int>(i);
        spawn_replication_thread([this, peer_index]() {
            request_vote_worker(peer_index);
        });
    }
}

void Server::request_vote_worker(int peer_index) {
    size_t i = static_cast<size_t>(peer_index);

    mu.lock();
    uint64_t last_log_index = ring.tail_log_index - 1;
    uint64_t last_log_term = 0;
    if (raft.log.size() > 1) {
        last_log_term = raft.log.back().term;
    }
    RequestVoteRequest req;
    req.rpc.term = raft.current_term;
    req.candidate_id = raft.id;
    req.last_log_index = last_log_index;
    req.last_log_term = last_log_term;
    mu.unlock();

    RequestVoteResponse rsp;
    /* transport가 없으면 전송 실패와 동일하게 다룬다 */
    if (transport == nullptr || !transport->request_vote(peer_index, req, rsp)) {
        return;
    }

    std::lock_guard<std::mutex> lk(mu);
    if (update_term(rsp.rpc.term)) {
        return;
    }
    if (rsp.rpc.term != req.rpc.term) {
        return;
    }
    if (rsp.vote_granted) {
        raft.cluster[i].voted_for = raft.id;
    }
}

/* ============================================================
 * HandleRequestVoteRequest (raft.go 원본, 로직 그대로 포팅)
 * ============================================================ */
void Server::handle_request_vote_request(const RequestVoteRequest &req,
                                          RequestVoteResponse &rsp) {
    std::lock_guard<std::mutex> lk(mu);

    update_term(req.rpc.term);
    rsp.vote_granted = false;
    rsp.rpc.term = raft.current_term;

    if (req.rpc.term < raft.current_term) {
        return;
    }

    uint64_t last_log_term = 0;
    if (raft.log.size() > 1) {
        last_log_term = raft.log.back().term;
    }
    uint64_t log_len = ring.tail_log_index - 1;

    bool log_ok = req.last_log_term > last_log_term ||
                  (req.last_log_term == last_log_term && req.last_log_index >= log_len);
    bool grant = req.rpc.term == raft.current_term && log_ok &&
                 (get_voted_for() == 0 || get_voted_for() == req.candidate_id);

    if (grant) {
        set_voted_for(req.candidate_id);
        rsp.vote_granted = true;
        reset_election_timeout();
        persist_circular(false, 0);
    }
}

/* ============================================================
 * becomeLeader (raft.go 원본, 로직 그대로 포팅)
 * ============================================================ */
void Server::become_leader() {
    std::lock_guard<std::mutex> lk(mu);

    int quorum = static_cast<int>(raft.cluster.size()) / 2 + 1;
    for (size_t i = 0; i < raft.cluster.size(); i++) {
        if (raft.cluster[i].voted_for == raft.id && quorum > 0) {
            quorum--;
        }
    }

    if (quorum == 0) {
        raft.state = ServerState::Leader;

        /* "Preserve logSlotMap across the role transition... Rebuild
         *  slotStates from the surviving map so canWriteAll still
         *  respects occupancy." */
        rebuild_slot_states_from_map();
        /* 원본 그대로 0으로 리셋. gc_has_run=false로 함께 리셋해서
         * "GC 미실행" 상태를 명확히 구분 (raft_server.h의 RingLog 필드 주석 참고) */
        ring.gc_up_to = 0;
        ring.gc_has_run = false;

        /* "Load any deferred entries (follower path: Command was nil).
         *  As leader we need Commands populated for state machine apply
         *  and for building EntryMeta for replication." */
        int loaded = 0;
        for (size_t i = 1; i < raft.log.size(); i++) {
            Entry &e = raft.log[i];
            if (e.command.empty() && e.ring_slot != 0) {
                try {
                    ReadEntryResult r = read_entry_direct(e.ring_slot);
                    e.command = std::move(r.entry.command);
                    e.ring_slot = 0;
                    loaded++;
                } catch (const std::exception &) {
                    /* 원본: 실패 시 경고 로그만 남기고 계속 진행 */
                    continue;
                }
            }
        }
        (void)loaded;

        /* "Apply any committed but unapplied entries now that Commands
         *  are loaded." */
        uint64_t oldest = oldest_log_index();
        while (raft.last_applied >= oldest &&
               raft.last_applied < ring.tail_log_index &&
               raft.last_applied <= raft.commit_index) {
            Entry &log_entry = raft.log[log_slice(raft.last_applied)];
            if (!log_entry.command.empty()) {
                statemachine->apply(log_entry.command);
            }
            raft.last_applied++;
        }

        /* "No-op entry (Raft paper Section 8)" */
        Entry noop;
        noop.term = raft.current_term;
        raft.log.push_back(std::move(noop));
        ring.tail_log_index++;
        persist_circular(true, 1);

        for (size_t i = 0; i < raft.cluster.size(); i++) {
            raft.cluster[i].next_index = ring.tail_log_index - 1;
            raft.cluster[i].match_index = 0;
            /* voted_for는 여기서 리셋하지 않는다 (원본 raft.go 그대로).
             * 한때 "메인 루프가 매 바퀴 become_leader를 부르니 리셋이
             * 필요하다"고 판단해 넣었었지만, 그건 main_loop이 상태 분기를
             * 안 하던 시절의 증상이었다. 원본처럼 become_leader는
             * candidate 상태에서만 호출되고 승격 즉시 state가 Leader가
             * 되므로 재진입 자체가 없다 (raft_lifecycle.cpp main_loop
             * 주석 참고). 다음 선거에서는 timeout()이 전부 0으로
             * 되돌린다. */
        }
        raft.heartbeat_timeout = clock_type::now();
    }
}

/* ============================================================
 * heartbeat (raft.go 원본, 로직 그대로 포팅)
 * ============================================================ */
void Server::heartbeat() {
    mu.lock();
    if (clock_type::now() < raft.heartbeat_timeout) {
        mu.unlock();
        return;
    }
    raft.heartbeat_timeout = clock_type::now() + std::chrono::milliseconds(raft.heartbeat_ms);
    /* 리더의 election timeout을 여기서 갱신하지 않는다 (원본 그대로).
     * 한때 "트래픽 없는 리더가 스스로 강등된다"고 보고 갱신을 넣었지만,
     * 그것도 main_loop이 상태 분기를 안 하던 시절의 증상이었다. 원본처럼
     * 리더는 timeout()을 아예 호출하지 않으므로 election_timeout이
     * 만료돼도 강등되지 않는다. */
    mu.unlock();
    append_entries(nullptr);
}

/* ============================================================
 * update_term (raft.go 원본 updateTerm)
 *
 * "msg.Term > currentTerm이면 강등하고 persist한다."
 * term/state/voted_for를 한 번에 바꾸는 **선거 상태 전이**이므로 이 파일에
 * 있다 (예전에는 raft_handle_append_entries.cpp에 있었다 -- 팔로워 핸들러가
 * 첫 호출자였다는 이유뿐이고, 지금은 handle_request_vote_request와
 * append_entries_worker도 부른다).
 *
 * 호출자가 mu를 보유해야 한다. 반환: 강등이 일어났으면 true.
 * ============================================================ */
bool Server::update_term(uint64_t msg_term) {
    if (msg_term > raft.current_term) {
        raft.current_term = msg_term;
        raft.state = ServerState::Follower;
        set_voted_for(0);
        reset_election_timeout();
        persist_circular(false, 0);
        return true;
    }
    return false;
}

} /* namespace nvmeof_raft */