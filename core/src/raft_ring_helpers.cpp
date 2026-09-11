#include "raft_server.h"
#include "raft_constants.h"

namespace nvmeof_raft {

/* ============================================================
 * slotsForEntry (raft.go 원본)
 * "returns ceil((ENTRY_META_SIZE + cmdLen) / SECTOR_SZIE).
 *  Metadata (ENTRY_META_SIZE bytes) is packed into the first bytes of
 *  slot 0 and the command follows immediately after, so an entry with
 *  4064B command fits in exactly 8 slots = 4096B = one 4KiB page."
 * ============================================================ */
uint64_t slots_for_entry(size_t cmd_len) {
    return (ENTRY_META_SIZE + cmd_len + SECTOR_SIZE - 1) / SECTOR_SIZE;
}

/* Entry::slots() (raft.go 원본)
 * "returns the number of ring slots this entry occupies. Handles
 *  deferred entries (nil Command) by using the stored cmdLen." */
uint64_t entry_slots(const Entry &e) {
    if (!e.command.empty()) {
        return slots_for_entry(e.command.size());
    }
    return slots_for_entry(static_cast<size_t>(e.cmd_len));
}

/* slotOffset (raft.go 원본)
 * "returns file byte offset for a given slot number." */
int64_t slot_offset(uint64_t slot) {
    return static_cast<int64_t>(RING_OFFSET) +
           static_cast<int64_t>(slot) * static_cast<int64_t>(SECTOR_SIZE);
}

/* oldestLogIndex (raft.go 원본)
 * "returns the absolute index of the oldest entry in s.log."
 * 원본: real := uint64(len(s.log) - 1) // exclude sentinel
 * s.log[0]이 sentinel이므로 실제 엔트리 수는 size()-1 */
uint64_t Server::oldest_log_index() const {
    uint64_t real = raft.log.empty() ? 0 : static_cast<uint64_t>(raft.log.size() - 1);
    if (real == 0) {
        return ring.tail_log_index;
    }
    return ring.tail_log_index - real;
}

/* is_leader (raft.go util.go 대응)
 * **스스로 mu를 잡는다** -- 락을 이미 들고 있는 곳에서 부르면 데드락이다.
 * 지금 호출부는 net/의 클라이언트 RPC 핸들러들뿐이다.
 * (예전에는 raft_commit.cpp에 있었다 -- commit 로직과 무관한 util이다.) */
bool Server::is_leader() {
    std::lock_guard<std::mutex> lk(mu);
    return raft.state == ServerState::Leader;
}

/* logSlice (raft.go 원본)
 * "converts absolute logIdx to s.log slice index." */
uint64_t Server::log_slice(uint64_t log_idx) const {
    return 1 + (log_idx - oldest_log_index());
}

/* slots_for_log_index: 로그 인덱스 -> 슬롯 수.
 * log_slot_map은 리더가 실제로 배치한 (start, num_slots)를 들고 있으므로
 * 그게 있으면 그 값이 정답이다. 없으면(팔로워이거나 아직 배치 전) 엔트리의
 * cmd_len으로 계산한다 -- deferred 엔트리도 cmd_len을 갖고 있어 같은 값이
 * 나온다 (entry_slots 참고). 호출자가 mu를 보유해야 한다. */
uint64_t Server::slots_for_log_index(uint64_t log_idx) const {
    auto it = ring.log_slot_map.find(log_idx);
    if (it != ring.log_slot_map.end()) {
        return it->second.num_slots;
    }
    const Entry &e = raft.log[log_slice(log_idx)];
    return entry_slots(e);
}

/* ============================================================
 * Slot state management (leader-only, raft.go 원본)
 * ============================================================ */

/* canWriteAll (raft.go 원본)
 * "checks if all commands can be written to the ring. Uses headSlot
 *  (circular buffer model) instead of per-slot slotStates scan." */
bool Server::can_write_all(const std::vector<std::vector<uint8_t>> &commands) const {
    if (ring.log_slot_map.empty()) {
        return true;
    }
    uint64_t slot = ring.tail_slot;
    bool wrapped = ring.tail_slot < ring.head_slot;   /* already in wrapped state */
    for (const auto &cmd : commands) {
        uint64_t needed = slots_for_entry(cmd.size());
        if (slot + needed > ring.ring_slots) {
            slot = 0;
            wrapped = true;
        }
        if (wrapped && slot + needed > ring.head_slot) {
            return false;
        }
        slot += needed;
    }
    return true;
}

/* recomputeHeadSlot (raft.go 원본)
 * "updates s.headSlot to the start slot of the entry that would first be
 *  encountered by new allocations advancing forward from tailSlot. Must
 *  be called after any logSlotMap deletion (GC) or on leader init." */
void Server::recompute_head_slot() {
    if (ring.log_slot_map.empty()) {
        ring.head_slot = ring.tail_slot;
        return;
    }
    uint64_t min_ahead = ~uint64_t(0);   /* min start slot >= tailSlot */
    uint64_t min_wrap = ~uint64_t(0);    /* min start slot < tailSlot (entries after a wrap) */
    for (const auto &kv : ring.log_slot_map) {
        const SlotRecord &sr = kv.second;
        if (sr.start >= ring.tail_slot) {
            if (sr.start < min_ahead) {
                min_ahead = sr.start;
            }
        } else {
            if (sr.start < min_wrap) {
                min_wrap = sr.start;
            }
        }
    }
    if (min_ahead != ~uint64_t(0)) {
        ring.head_slot = min_ahead;
    } else {
        ring.head_slot = min_wrap;
    }
}

/* initSlotStates (raft.go 원본)
 * "wipes logSlotMap and resets headSlot. Used on startup." */
void Server::init_slot_states() {
    ring.log_slot_map.clear();
    ring.head_slot = 0;
    ring.gc_up_to = 0;
    ring.gc_has_run = false;
}

/* rebuildSlotStatesFromMap (raft.go 원본)
 * "sets all logSlotMap entries to SlotUsing and recomputes headSlot.
 *  Used on becomeLeader to preserve live slot mappings." */
void Server::rebuild_slot_states_from_map() {
    for (auto &kv : ring.log_slot_map) {
        kv.second.state = LogEntryState::Using;
    }
    recompute_head_slot();
}

/* RingBusy (raft.go 원본)는 이 포팅에서 제거했다.
 * 호출부가 하나도 없었고(전수 grep 확인), mu를 잡고 log_slot_map을 O(n)
 * 스캔하면서 링 점유 임계값(99%)을 자기 리터럴로 들고 있었다. 실제
 * 백프레셔는 apply_internal의 can_write_all + ring_not_full 대기(200ms
 * 타임아웃 후 busy 반환)가 담당한다 -- 즉 서로 다른 두 백프레셔 설계가
 * 공존하고 있었고, 살아 있는 쪽만 남긴 것이다.
 * 원본 주석: "returns true if the ring buffer is too full to accept new
 * client requests. Used by RPC handlers to reject early with a
 * backpressure signal." -- RPC 핸들러가 실제로는 쓰지 않았다. */

} /* namespace nvmeof_raft */
