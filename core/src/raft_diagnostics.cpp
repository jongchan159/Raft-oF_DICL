#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <chrono>
#include <iostream>
#include <sstream>
#include <string>

/* ============================================================
 * 진단 / 관측 코드.
 *
 * 복제 로직도 링 관리 로직도 아니고, **문제가 났을 때 상태를 사람이 읽을
 * 수 있게 찍는 것**만 한다. 예전에는 log_skip_pba_diag가
 * raft_append_entries.cpp(복제 로직)에, 슬롯맵 추적 링이
 * raft_ring_helpers.cpp(링 산술)에 각각 섞여 있었다.
 *
 * 이 파일의 코드는 정상 경로의 정확성에 영향을 주지 않는다. 다만
 * log_skip_pba_diag는 **팔로워가 영구히 못 따라잡는 상태를 로그에 남기는
 * 유일한 수단**이므로 지우면 안 된다 (HANDOFF §10).
 * ============================================================ */

namespace nvmeof_raft {

/* traceSlotMap (raft.go 원본)
 * "records a logSlotMap mutation in the diagnostic ring. Inserts are
 *  gated by slotMapTraceInserts to avoid spamming the ring on the hot
 *  path; deletes are always recorded because they're rarer and are the
 *  prime suspect for the [SKIP PBA] warning. Caller must hold s.mu." */
void Server::trace_slot_map(char op, uint64_t idx) {
    if (op == 'I' && !ring.slot_map_trace_inserts) {
        return;
    }
    ring.slot_map_trace_ring[static_cast<size_t>(ring.slot_map_trace_cursor)] = SlotMapTraceEvt{op, idx};
    ring.slot_map_trace_cursor = (ring.slot_map_trace_cursor + 1) %
                             static_cast<int>(ring.kSlotMapTraceRingSize);
}

/* findSlotMapTrace: 진단 링에서 특정 idx의 마지막 연산을 찾음
 * (raft.go 원본에 언급만 있고 본문은 못 봤음 -- 로직 추정,
 *  [SKIP PBA] 경고 메시지 생성부에서 호출되는 걸로 봐서 최신 것 우선 검색) */
char Server::find_slot_map_trace(uint64_t idx) const {
    for (size_t i = 0; i < ring.kSlotMapTraceRingSize; i++) {
        size_t pos = static_cast<size_t>(
            (ring.slot_map_trace_cursor - 1 - static_cast<int>(i) +
             2 * static_cast<int>(ring.kSlotMapTraceRingSize)) %
            static_cast<int>(ring.kSlotMapTraceRingSize));
        if (ring.slot_map_trace_ring[pos].idx == idx && ring.slot_map_trace_ring[pos].op != 0) {
            return ring.slot_map_trace_ring[pos].op;
        }
    }
    return 0;   /* "absent" -- 원본의 default case */
}

/* ============================================================
 * logSkipPBADiag (raft.go 원본, 메시지 포맷까지 그대로 포팅)
 *
 * log_slot_map[next]가 없어서 PBA 복제를 포기하고 하트비트만 보내는
 * 순간의 스냅샷. 원본 주석:
 *   "Captures everything needed to classify whether (a) tier-1 freeing
 *    dropped an index that the floor guard should have protected,
 *    (b) the index was never inserted on this leader, or (c) the
 *    in-memory log itself is missing the entry."
 * "Caller must hold s.mu."
 *
 * 원본은 매번 무조건 출력한다. 하트비트 주기(기본 100ms)로 계속 걸리면
 * 팔로워당 초당 10줄이 나와서 장시간 벤치 로그를 덮으므로, **같은
 * (follower, next) 조합은 1초에 한 번만** 출력한다. 그 외 내용은 원본과
 * 동일하다.
 * ============================================================ */
void Server::log_skip_pba_diag(int fi, uint64_t next, uint64_t last,
                                uint64_t prev_log_index) {
    /* 스로틀: (follower, next)가 같으면 1초에 한 번 */
    {
        auto now = clock_type::now();
        ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
        if (m.skip_pba_last_next == next &&
            now - m.skip_pba_last_log < std::chrono::seconds(1)) {
            return;
        }
        m.skip_pba_last_next = next;
        m.skip_pba_last_log = now;
    }

    /* "Recompute minMatch as advanceCommitIndex would, so we can see
     *  whether next <= minMatch (which would mean the floor guard was
     *  bypassed)." */
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

    /* "Range of indices currently present in logSlotMap." */
    uint64_t sm_min = UINT64_MAX, sm_max = 0;
    for (const auto &kv : ring.log_slot_map) {
        if (kv.first < sm_min) sm_min = kv.first;
        if (kv.first > sm_max) sm_max = kv.first;
    }
    if (ring.log_slot_map.empty()) {
        sm_min = 0;
        sm_max = 0;
    }

    std::ostringstream peers;
    for (size_t j = 0; j < raft.cluster.size(); j++) {
        if (static_cast<int>(j) == raft.cluster_index) {
            continue;
        }
        peers << " p" << raft.cluster[j].id
              << "{m=" << raft.cluster[j].match_index
              << " n=" << raft.cluster[j].next_index
              << " inflight=" << (raft.cluster[j].inflight ? "true" : "false") << "}";
    }

    /* "Was next recently freed or inserted?" */
    const char *trace_str = "absent";
    switch (find_slot_map_trace(next)) {
    case 'I': trace_str = "INSERT (last op was insert -- freed by an unrecorded path?)"; break;
    case 'F': trace_str = "FREE (tier-1 freed it; floor guard bypassed)"; break;
    default: break;
    }

    /* "Does the in-memory log still have the entry?" */
    std::string log_state = "out-of-range";
    uint64_t oldest = oldest_log_index();
    if (next >= oldest && next < ring.tail_log_index) {
        const Entry &e = raft.log[log_slice(next)];
        log_state = "present term=" + std::to_string(e.term) +
                    " cmdLen=" + std::to_string(e.cmd_len);
    }

    bool floor_bypassed = next <= min_match;

    std::ostringstream os;
    os << "[WARN] [SKIP PBA] missing logSlotMap[" << next << "]"
       << " follower=" << raft.cluster[static_cast<size_t>(fi)].id
       << " term=" << raft.current_term
       << " | next=" << next << " last=" << last << " prev=" << prev_log_index
       << " oldest=" << oldest << " tail=" << ring.tail_log_index
       << " commit=" << raft.commit_index << " applied=" << raft.last_applied
       << " | minMatch_now=" << min_match
       << " floorBypassed=" << (floor_bypassed ? "true" : "false")
       << " |" << peers.str()
       << " | slotMap len=" << ring.log_slot_map.size()
       << " range=[" << sm_min << ".." << sm_max << "]"
       << " | trace=" << trace_str
       << " | log=" << log_state;
    std::cerr << os.str() << std::endl;
}

} /* namespace nvmeof_raft */
