#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <algorithm>
#include <chrono>

namespace nvmeof_raft {

namespace {

/* 링 점유율이 이 퍼센트를 넘으면 slot GC를 깨운다.
 * 예전에는 `used_slots * 10 > ring_slots * 7` 로 적혀 있어 70%라는 정책이
 * 정수 산술 속에 숨어 있었다. */
constexpr uint64_t kSlotGcTriggerPercent = 70;

/* advance_commit_index 호출 횟수 기준으로, 점유율과 무관하게 GC를 깨우는 주기. */
constexpr uint64_t kSlotGcFloorTicks = 100;

/* apply_pending이 한 번에 상태머신에 반영하는 엔트리 수 상한. */
constexpr int kApplyBatchLimit = 64;

} /* anonymous namespace */

/* ============================================================
 * advanceCommitIndex (raft.go 원본, 로직 그대로 포팅)
 *
 * "(1) Leader: raise commitIndex based on quorum. Use sorted matchIndex
 *  to find the median in O(n log n)... Per Raft §5.4.2: only commit
 *  entries from the current term to avoid the 'ghost commit' problem
 *  where a pre-election entry gets committed without a current-term
 *  entry confirming leadership."
 *
 * Go의 defer-unlock은 함수 끝의 명시적 unlock으로 재현했다.
 * ============================================================ */
void Server::advance_commit_index() {
    mu.lock();

    /* (1) Leader: quorum 기반 commitIndex 전진 */
    if (raft.state == ServerState::Leader) {
        uint64_t last_log_index = ring.tail_log_index - 1;

        std::vector<uint64_t> matches(raft.cluster.size());
        for (size_t j = 0; j < raft.cluster.size(); j++) {
            if (static_cast<int>(j) == raft.cluster_index) {
                matches[j] = last_log_index;
            } else {
                matches[j] = raft.cluster[j].match_index;
            }
        }
        /* 원본: 내림차순 정렬 후 matches[len/2]가 과반수 도달 최소값 */
        std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());
        uint64_t majority_idx = matches[matches.size() / 2];

        if (majority_idx > raft.commit_index) {
            /* §5.4.2: majorityIdx의 엔트리가 current term일 때만 전진.
             * "prevents committing entries from prior terms without a
             *  current-term log entry to anchor them."
             * become_leader의 no-op 엔트리와 한 쌍이다 -- 그쪽을 지우면
             * 이 조건 때문에 이전 term 엔트리가 커밋되지 않는다. */
            if (majority_idx < ring.tail_log_index) {
                uint64_t entry_term = raft.log[log_slice(majority_idx)].term;
                if (entry_term == raft.current_term) {
                    uint64_t prev_commit = raft.commit_index;
                    raft.commit_index = majority_idx;

                    /* "Signal committed for entries newly committed.
                     *  Non-blocking cap=1 sends; safe under s.mu." */
                    uint64_t oldest_c = oldest_log_index();
                    for (uint64_t idx = prev_commit + 1; idx <= majority_idx; idx++) {
                        if (idx >= oldest_c && idx < ring.tail_log_index) {
                            Entry &e = raft.log[log_slice(idx)];
                            if (e.committed) {
                                /* Go의 non-blocking cap=1 send(select+default)를
                                 * bool 플래그 signal로 재현 -- 이미 signaled면
                                 * 그냥 넘어감(default 분기와 동일 의도) */
                                e.signal_committed();
                            }
                        }
                    }
                }
            }
        }

        /* Slot GC worker 깨우기: ring 사용률 70% 초과 또는 100회마다
         * (floor). Non-blocking cap=1 채널 -> bool 플래그로 재현 */
        workers.slot_gc_tick++;
        uint64_t used_slots = 0;
        if (!ring.log_slot_map.empty()) {
            if (ring.tail_slot >= ring.head_slot) {
                used_slots = ring.tail_slot - ring.head_slot;
            } else {
                used_slots = ring.ring_slots - ring.head_slot + ring.tail_slot;
            }
        }
        if (used_slots * 100 > ring.ring_slots * kSlotGcTriggerPercent ||
            workers.slot_gc_tick % kSlotGcFloorTicks == 0) {
            std::lock_guard<std::mutex> gc_lk(workers.slot_gc_notify_mu);
            workers.slot_gc_notify_pending = true;
            workers.slot_gc_notify_cv.notify_one();
        }
    }

    /* Apply는 applyWorker가 비동기로 수행. 깨우기만 함. */
    if (raft.state == ServerState::Leader) {
        std::lock_guard<std::mutex> apply_lk(workers.apply_notify_mu);
        workers.apply_notify_pending = true;
        workers.apply_notify_cv.notify_one();
    }

    mu.unlock();   /* Go의 defer 위치와 동일 (함수 끝에서 항상 unlock) */
}

/* ============================================================
 * applyPending (raft.go 원본, 로직 그대로 포팅)
 *
 * "drains committed entries through the state machine. Runs in the
 *  applyWorker goroutine. Acquires s.mu in bounded batches so other
 *  callers... can interleave mutex use."
 * ============================================================ */
void Server::apply_pending() {
    std::lock_guard<std::mutex> lk(mu);
    if (raft.state != ServerState::Leader) {
        return;
    }

    int applied = 0;
    uint64_t oldest = oldest_log_index();

    while (raft.last_applied >= oldest &&
           raft.last_applied < ring.tail_log_index &&
           raft.last_applied <= raft.commit_index) {

        Entry &log_entry = raft.log[log_slice(raft.last_applied)];
        if (!log_entry.command.empty()) {
            ApplyResult res = statemachine->apply(log_entry.command);
            if (log_entry.result_sink) {
                log_entry.result_sink(std::move(res));
            }
        }
        log_entry.cmd_len = log_entry.command.size();
        log_entry.command.clear();
        log_entry.result_sink = nullptr;
        log_entry.committed = nullptr;   /* Go: logEntry.committed = nil */
        log_entry.ring_slot = 0;
        raft.last_applied++;
        applied++;

        if (applied >= kApplyBatchLimit) {
            /* 배치 상한에 닿았다. 원본은 여기서 reset_election_timeout()을
             * 부른 뒤 **락을 놓고 다시 잡아** 다른 대기자에게 양보한다.
             *
             * 이 포팅은 양보하지 않는다: 이 함수가 std::lock_guard로 mu를
             * 잡고 있어 명시적 unlock이 불가능하기 때문이다. 즉 아래
             * reset_election_timeout()만 실행되고 **의도한 락 양보는 전혀
             * 일어나지 않는다.** 고치려면 이 함수를 unique_lock 기반으로
             * 바꿔야 하는데 그건 동작 변경이라 이번 리팩토링 범위 밖이다
             * -- DECISIONS.md U3. */
            reset_election_timeout();
            if (raft.state != ServerState::Leader && raft.state != ServerState::Follower) {
                return;
            }
            oldest = oldest_log_index();
            applied = 0;
        }
    }
}

/* ============================================================
 * doSlotGC (raft.go 원본, 로직 그대로 포팅)
 *
 * "frees logSlotMap entries for all log indices up to minMatch (the
 *  minimum matchIndex across all followers). Must be called with s.mu
 *  held. No-op on non-leaders."
 * ============================================================ */
void Server::do_slot_gc() {
    if (raft.state != ServerState::Leader) {
        return;
    }
    uint64_t last_log_index = ring.tail_log_index - 1;
    uint64_t min_match = last_log_index;
    for (size_t j = 0; j < raft.cluster.size(); j++) {
        if (static_cast<int>(j) == raft.cluster_index) {
            continue;
        }
        if (raft.cluster[j].match_index < min_match) {
            min_match = raft.cluster[j].match_index;
        }
    }

    bool freed = false;
    for (uint64_t idx = ring.gc_up_to + 1; idx <= min_match; idx++) {
        auto it = ring.log_slot_map.find(idx);
        if (it != ring.log_slot_map.end()) {
            ring.log_slot_map.erase(it);
            freed = true;
        }
    }
    ring.gc_up_to = min_match;
    ring.gc_has_run = true;

    if (freed) {
        recompute_head_slot();
        ring_not_full.notify_all();   /* Go의 Broadcast() 대응 */
    }

    /* 슬롯을 해제한 것과 같은 기준으로 in-memory log 벡터도 잘라낸다
     * (아래 trim_log_locked 주석 참고). */
    trim_log_locked(min_match);
}

/* ============================================================
 * trim_log_locked
 *
 * Server::log는 원본에서도 push_back만 하고 앞쪽을 잘라내지 않아서,
 * 링버퍼(32GiB)가 한 바퀴 도는 동안 in-memory 벡터는 계속 자란다
 * (엔트리당 ~100B * 수천만 = 수 GB). apply_pending이 command만 비우고
 * 벡터 원소는 남긴다.
 *
 * 잘라내는 기준은 do_slot_gc가 슬롯을 해제하는 기준과 동일하게 잡되,
 * 두 가지 하한을 지킨다:
 *   1) min_match 자체는 남긴다. append_entries_worker가 가장 느린
 *      팔로워에게 보낼 prev_log_index가 정확히 min_match이므로,
 *      이걸 지우면 prev_log_term을 못 채워 팔로워가 계속 거절한다.
 *   2) last_applied 이상은 남긴다. apply_pending의 루프 조건이
 *      last_applied >= oldest_log_index()라서, 아직 apply되지 않은
 *      엔트리를 지우면 영원히 apply되지 않는다.
 *
 * oldest_log_index() = tail_log_index - (log.size()-1) 이고
 * log_slice()가 그 값을 기준으로 절대 인덱스를 변환하므로, 앞쪽을
 * 지우면 두 함수가 자동으로 새 시작점을 가리킨다 -- 즉 이 트리밍은
 * 기존 인덱스 시맨틱 안에서 동작한다. log[0] sentinel은 유지한다.
 *
 * s.mu를 잡은 상태에서 호출해야 한다.
 * ============================================================ */
void Server::trim_log_locked(uint64_t min_match) {
    if (ring.log_trim_threshold == 0 || raft.log.size() <= 1) {
        return;   /* 비활성 (원본 동작) */
    }

    uint64_t trim_upto = std::min(min_match, raft.last_applied);
    uint64_t oldest = oldest_log_index();
    if (trim_upto <= oldest) {
        return;
    }

    uint64_t removable = trim_upto - oldest;   /* [oldest, trim_upto) 개수 */
    if (removable < ring.log_trim_threshold) {
        return;   /* 자주 부르면 O(n) memmove가 오히려 비싸므로 배치로 */
    }
    if (removable > raft.log.size() - 1) {
        removable = raft.log.size() - 1;   /* sentinel 보호 (방어적) */
    }

    /* log[0]은 sentinel, 실제 엔트리는 log[1]부터.
     * 절대 인덱스 oldest .. oldest+removable-1 = slice 1 .. removable */
    raft.log.erase(raft.log.begin() + 1, raft.log.begin() + 1 + static_cast<long>(removable));
}

/* reset_election_timeout()의 실제 구현은 raft_election.cpp에 있다
 * (election 로직 포팅 완료로 이 자리의 스텁은 제거됨 -- 중복 정의로
 * 인한 링크 오류 방지). */

} /* namespace nvmeof_raft */