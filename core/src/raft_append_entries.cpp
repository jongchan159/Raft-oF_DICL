/* ============================================================
 * raft_append_entries.cpp -- 복제 송신 (리더 측)
 *
 * 팔로워마다 스레드를 하나 띄워 append_entries_worker를 돌린다. 워커가
 * 하는 일은 "무엇을 보낼지 정하고(Lock B) -> RPC -> 기록을 갱신한다
 * (Lock C)" 세 토막이고, 그 사이 RPC는 **락을 놓은 상태**에서 한다.
 *
 * 보내는 것은 **PBA 메타데이터뿐이다.** 명령 바이트는 이 메시지에 실리지
 * 않고, 팔로워의 스토리지 노드가 leader_pba_src에서 직접 읽어 간다
 * (core/src/raft_handle_append_entries.cpp의 do_pba_copy).
 *
 * 원본 대비 빠진 것:
 *   - 구간 계측 전부 (ReplSample / ReplSink / ProfilingSink)
 *   - Leader-Side 복제 정책 (DARE 비교용)
 *   - 라운드당 바이트 상한 clamp
 *   - conflict_term/conflict_index 기반 fast backoff -> 단순 1 감소
 *   - [SKIP PBA] 구조화 진단
 * 남긴 것: 링 wrap clamp와 extent clamp. 둘 다 예외 처리가 아니라
 * **PBA 복사가 성립하기 위한 조건**이라 빠지면 조용히 틀린 바이트를 옮긴다.
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <chrono>
#include <cstdio>

namespace nvmeof_raft {

void Server::append_entries() {
    for (size_t i = 0; i < raft.cluster.size(); i++) {
        int fi = static_cast<int>(i);
        if (fi == raft.cluster_index) {
            continue;   /* "Don't need to send message to self" */
        }
        spawn_replication_thread([this, fi]() {
            append_entries_worker(fi);
        });
    }
}

/* append_entries_worker: 팔로워 한 명분 처리 로직 */
void Server::append_entries_worker(int fi) {
    /* ---- Lock B: 무엇을 보낼지 정한다 ---- */
    mu.lock();

    uint64_t next = raft.cluster[static_cast<size_t>(fi)].next_index;
    uint64_t last = ring.tail_log_index - 1;
    uint64_t oldest = oldest_log_index();

    /* "follower가 leader보다 앞서있을 때 -> last+1로 변경 (예외처리)" */
    if (next > last + 1) {
        next = last + 1;
        raft.cluster[static_cast<size_t>(fi)].next_index = next;
    }
    // /* "Clamp: if next fell below what we have in memory, reset to oldest 
    //  * -> follower에게 보내야 할 entry가 ring에서 해제됐을 때
    //  * oldest로 옮겨서 살아있는 가장 오래된 entry부터 전송" */
    // if (next < oldest) {
    //     next = oldest;
    //     raft.cluster[static_cast<size_t>(fi)].next_index = next;
    // }

    /* Raft Consistency check */
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    if (next > 0) {
        prev_log_index = next - 1;
        if (prev_log_index >= oldest && prev_log_index < ring.tail_log_index) {
            prev_log_term = raft.log[log_slice(prev_log_index)].term;
        }
    }

    uint64_t len_entries = 0;
    if (last >= next) {
        /* "Skip if a prior data-bearing RPC is still in-flight...
         * Heartbeats (0 entries) still go through." */
        if (raft.cluster[static_cast<size_t>(fi)].inflight) {
            mu.unlock();
            return;
        }
        len_entries = last - next + 1;
    }

    uint64_t total_slots = 0;
    uint64_t slots_per_entry = 0;
    uint64_t start_slot = 0;

    /* ---- 첫 엔트리의 슬롯 조회 ----
     * "Get start slot from first entry's slot map. If missing (or
     *  already GC'd), the slot was freed -- PBA copy would read
     *  stale data. Send heartbeat only." */
    if (len_entries > 0) {
        /* gc_up_to fast path: 이미 GC된 인덱스면 조회 스킵.
         * gc_has_run이 false면(GC를 아직 한 번도 안 돌린 상태)
         * 이 fast path 자체를 건너뛴다 -- 안 그러면 next=0,
         * gc_up_to=0일 때 0<=0이 참이 되어 GC된 적 없는 index 0을
         * "이미 GC됨"으로 오판한다. */
        auto it = ring.log_slot_map.find(next);
        if (ring.gc_has_run && next <= ring.gc_up_to) {
            len_entries = 0;
        } else if (it != ring.log_slot_map.end()) {
            start_slot = it->second.start;
        } else {
            len_entries = 0;
        }
    }

    if (len_entries > 0) {
        /* 엔트리별 슬롯 수를 누적한다 */
        for (uint64_t k = 0; k < len_entries; k++) {
            total_slots += slots_for_log_index(next + k);
        }
        slots_per_entry = (total_slots + len_entries - 1) / len_entries;

        /* "Limit batch at ring wrap-around for contiguous FIEMAP"
         * 링 끝을 넘어가는 배치는 물리적으로 연속이 아니므로 한 번의
         * PBA 복사로 옮길 수 없다. 끝까지만 보내고 나머지는 다음 라운드. */
        uint64_t slots_until_end = ring.ring_slots - start_slot;
        if (total_slots > slots_until_end) {
            total_slots = 0;
            len_entries = 0;
            for (uint64_t k = 0; ; k++) {
                uint64_t log_idx = next + k;
                if (log_idx > last) {
                    break;
                }
                uint64_t needed = slots_for_log_index(log_idx);
                if (total_slots + needed > slots_until_end) {
                    break;
                }
                total_slots += needed;
                len_entries++;
            }
            if (len_entries > 0) {
                slots_per_entry = (total_slots + len_entries - 1) / len_entries;
            }
        }
    }

    /* ---- PBA 조회 ---- */
    uint64_t leader_pba_src = 0;
    uint64_t log_block_length = 0;
    if (len_entries > 0) {
        PbaRangeResult r = leader_pba_for_range(start_slot, total_slots);
        leader_pba_src = r.pba_src;
        /* "actualBytes가 핵심, extent cache가 반환하는 실제로 연속된
         *  물리 블록의 바이트 수. 요청한 범위 전체가 하나의 extent
         *  안에 있으면 actualBytes == totalSlots*SECTOR_SIZE, extent
         *  경계에 걸리면 actualBytes < totalSlots*SECTOR_SIZE"
         *
         * "Clamp batch if FIEMAP extent doesn't cover the full range."
         * AppendEntries는 (PBA, 길이) 한 쌍만 싣는다 -- extent 경계를
         * 넘는 배치는 한 번에 복사할 수 없으므로 경계에서 자른다. */
        uint64_t actual_slots = r.nbytes / SECTOR_SIZE;
        if (actual_slots < total_slots) {
            len_entries = 0;
            uint64_t counted = 0;
            for (uint64_t k = 0; ; k++) {
                uint64_t log_idx = next + k;
                if (log_idx > last) {
                    break;
                }
                uint64_t needed = slots_for_log_index(log_idx);
                if (counted + needed > actual_slots) {
                    break;
                }
                counted += needed;
                len_entries++;
            }
            total_slots = counted;  // total_slots clamping 
        }
        log_block_length = total_slots; // 굳이 필요한가? 그런데 오버헤드도 없을듯
    }

    bool has_entries = len_entries > 0;
    if (has_entries) {
        raft.cluster[static_cast<size_t>(fi)].inflight = true;
    }

    /* ---- 엔트리 메타 빌드 ----
     * "Send lightweight entry metadata so follower can build its
     *  in-memory log without readback. Command payload is on the
     *  device via PBA copy; follower loads it lazily at apply time." */
    std::vector<EntryMeta> metas;
    if (len_entries > 0) {
        metas.resize(len_entries);
        for (uint64_t k = 0; k < len_entries; k++) {
            Entry &e = raft.log[log_slice(next + k)];
            uint64_t cl = e.command.size();
            if (e.command.empty() && e.ring_slot != 0) {
                cl = e.cmd_len;   /* deferred entry: use stored cmdLen */
            }
            metas[k] = EntryMeta{e.term, cl};
        }
    }

    AppendEntriesRequest req;
    req.rpc.term = raft.current_term;
    req.leader_id = raft.cluster[static_cast<size_t>(raft.cluster_index)].id;
    req.prev_log_index = prev_log_index;
    req.prev_log_term = prev_log_term;
    req.leader_commit = raft.commit_index;
    req.leader_pba_src = leader_pba_src;
    req.log_block_length = log_block_length;
    req.num_entries = len_entries;
    req.slots_per_entry = slots_per_entry;
    req.start_slot = start_slot;
    req.leader_dev_index = raft.cluster_index;
    req.entry_metas = std::move(metas);

    mu.unlock();   /* "lockB Finish" */

    /* ---- RPC (락 밖) ---- */
    AppendEntriesResponse rsp;
    /* transport가 없으면(주입 안 된 하네스) 전송 실패와 동일하게 다룬다 */
    bool ok = (transport != nullptr) && transport->append_entries(fi, req, rsp);

    /* ---- Lock C: 응답 반영 ---- */
    mu.lock();

    if (has_entries) {
        raft.cluster[static_cast<size_t>(fi)].inflight = false;
    }

    if (!ok) {
        mu.unlock();
        return;
    }

    /* 응답의 term이 더 높으면 이 라운드를 버린다.
     *
     * 주의: 원본 raft.go는 여기서 s.updateTerm(rsp.RPCMessage)를 불러
     * **팔로워로 강등까지** 한다. 이 포팅은 강등하지 않고 그냥 중단한다.
     * 실질 차이는 크지 않다 -- 그 팔로워가 더 높은 term의 리더를 알고
     * 있다면 그 리더가 곧 우리에게 AppendEntries를 보내고
     * handle_append_entries_request가 update_term으로 강등시킨다. */
    if (rsp.rpc.term > raft.current_term) {
        mu.unlock();
        return;
    }
    if (rsp.rpc.term != req.rpc.term && raft.state == ServerState::Leader) {
        mu.unlock();
        return;
    }

    if (rsp.success) {
        apply_ae_success(fi, next, len_entries, has_entries);
    } else if (raft.cluster[static_cast<size_t>(fi)].next_index == next && next > 1) {
        /* 팔로워가 거절했다. 정상 경로에서는 일어나지 않지만(리더 하나,
         * 로그 분기 없음), 일어났을 때 그 팔로워가 영원히 멈추지 않도록
         * next_index를 1만 되감는다. 원본은 응답에 실려 온
         * conflict_term/conflict_index로 한 번에 건너뛴다 -- 그 빠른
         * 되감기는 로그가 실제로 갈라지는 상황을 위한 것이라 뺐다.
         * next_index가 그대로일 때만 되감는다 (다른 워커가 이미
         * 전진시켰으면 stale 응답이다). */
        raft.cluster[static_cast<size_t>(fi)].next_index = next - 1;
    }

    mu.unlock();
}

/* ============================================================
 * apply_ae_success
 *
 * append_entries_worker가 RPC 응답을 받은 뒤 Lock C 안에서 하는 기록
 * 갱신. 파일 I/O도 네트워크도 만지지 않는 순수 산술이라 단위 테스트가
 * 붙는다 (tests/test_ae_bookkeeping.cpp).
 * **호출자가 mu를 보유**해야 한다.
 * ============================================================ */
void Server::apply_ae_success(int fi, uint64_t next, uint64_t len_entries,
                               bool has_entries) {

        /* 수정: 원본 공식(prev_log_index + len_entries + 1)은
         * prev_log_index=0이 "index 0 자체"인지 "이전 엔트리 없음
         * (1-based의 관례적 0)"인지 구분 못 해 0-based 시스템에서
         * off-by-one이 남. next는 이 배치의 첫 엔트리 index 그
         * 자체이므로(모호함 없음), new_next = next + len_entries가
         * 정확하다. 실제 3노드 e2e 테스트에서 next=0일 때
         * (팔로워가 아직 아무 엔트리도 없을 때) 이 어긋남이 실제로
         * 재현되어 확인됨. */
        uint64_t new_next = std::max(next + len_entries, uint64_t{1});
        if (new_next > raft.cluster[static_cast<size_t>(fi)].next_index) {
            raft.cluster[static_cast<size_t>(fi)].next_index = new_next;
        }
        raft.cluster[static_cast<size_t>(fi)].match_index =
            raft.cluster[static_cast<size_t>(fi)].next_index - 1;

        /* "If this follower still has pending entries... schedule the
         *  next heartbeat immediately" -- heartbeat_timeout을 즉시
         * 만료시켜 다음 루프에서 바로 재전송되게 함 */
        if (has_entries && raft.state == ServerState::Leader &&
            ring.tail_log_index - 1 >= raft.cluster[static_cast<size_t>(fi)].next_index) {
            raft.heartbeat_timeout = std::chrono::steady_clock::time_point{};
        }
}

} /* namespace nvmeof_raft */
