#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"
#include "cached_fd.h"   /* CachedFD 역참조 */
#include <cstring>

namespace nvmeof_raft {

/* ============================================================
 * leaderPBAForRange (raft.go 원본, 로직 그대로 포팅)
 * "resolves the physical block address for a contiguous range of ring
 *  buffer slots starting at startSlot for totalSlots slots. Uses FIEMAP
 *  to map the metadata file offset to a device PBA. Must be called with
 *  s.mu held."
 * ============================================================ */
Server::PbaRangeResult Server::leader_pba_for_range(uint64_t start_slot, uint64_t total_slots) {
    if (total_slots == 0) {
        return {0, 0};
    }

    /* 링 슬롯 -> 파일 논리 오프셋. 이 식이 네 곳에 손으로 복사돼 있었다.
     * slot_offset()이 단일 출처다 (raft_ring_helpers.cpp). */
    int64_t logical_off = slot_offset(start_slot);

    uint64_t raw_bytes = total_slots * SECTOR_SIZE;
    uint64_t nbytes = align_up(raw_bytes, PAGE_SIZE);

    PBASegment seg = io.cached_fd->get_pba(logical_off, nbytes);
    if (seg.pba == 0) {
        throw std::runtime_error(
            "leader_pba_for_range: PBA=0 at start_slot=" + std::to_string(start_slot) +
            " (hole in ring file)");
    }

    /* "Return actual available bytes -- the caller's EXTENT CLAMP logic
     * already handles partial extents by reducing the batch size." */
    if (seg.len > 0 && seg.len < nbytes) {
        nbytes = align_up(seg.len, PAGE_SIZE);
    }

    return {seg.pba, nbytes};
}

/* ============================================================
 * readEntryDirect (raft.go 원본, 로직 그대로 포팅)
 * "reads one entry from the device using O_DIRECT, bypassing page cache
 *  entirely. Used after doPBACopy on follower." -- becomeLeader가
 * deferred entry(Command=nil)를 실제로 로드할 때도 사용.
 * ============================================================ */
Server::ReadEntryResult Server::read_entry_direct(uint64_t header_slot) {
    int64_t logical_off = slot_offset(header_slot);
    PBASegment seg = io.cached_fd->get_pba(logical_off, SECTOR_SIZE);
    uint64_t raw_pba = seg.pba;

    /* "Align PBA down to 4KB boundary for O_DIRECT" */
    uint64_t aligned_pba = raw_pba & ~(PAGE_SIZE - 1);
    int64_t offset_in_page = static_cast<int64_t>(raw_pba - aligned_pba);

    std::vector<uint8_t> buf = io.cached_fd->read(aligned_pba, PAGE_SIZE);

    /* "Parse header at the correct offset within the 4KB block" */
    const uint8_t *header = buf.data() + offset_in_page;
    uint64_t term = get_u64_le(header + entry_hdr::kOffTerm);
    uint64_t cmd_len = get_u64_le(header + entry_hdr::kOffCmdLen);
    uint64_t num_slots = get_u64_le(header + entry_hdr::kOffNumSlots);

    std::vector<uint8_t> cmd(cmd_len);
    if (cmd_len > 0) {
        size_t copied = 0;

        /* 슬롯 0에도 명령 바이트가 들어 있다 -- 메타 32B 뒤의 480B다
         * (core/raft_basics.h의 entry_hdr::kCmdBytesInFirstSlot).
         * [수정-13] 이 480바이트를 건너뛰면 1슬롯 엔트리는 명령이 전부 0이 되고
         * 여러 슬롯 엔트리는 480B씩 밀린다 -- DECISIONS.md D13.
         * selftest T1이 경계값으로 이 경로를 직접 검증한다. */
        {
            size_t chunk = static_cast<size_t>(SECTOR_SIZE - ENTRY_META_SIZE);
            if (chunk > cmd_len) {
                chunk = static_cast<size_t>(cmd_len);
            }
            std::memcpy(cmd.data(), header + ENTRY_META_SIZE, chunk);
            copied = chunk;
        }

        for (uint64_t i = 1; i < num_slots && copied < cmd_len; i++) {
            uint64_t p_slot = (header_slot + i) % ring.ring_slots;
            int64_t p_off = slot_offset(p_slot);
            PBASegment p_seg = io.cached_fd->get_pba(p_off, SECTOR_SIZE);
            uint64_t p_raw_pba = p_seg.pba;
            uint64_t p_aligned_pba = p_raw_pba & ~(PAGE_SIZE - 1);
            int64_t p_off_in_page = static_cast<int64_t>(p_raw_pba - p_aligned_pba);

            std::vector<uint8_t> p_buf = io.cached_fd->read(p_aligned_pba, PAGE_SIZE);
            size_t chunk = SECTOR_SIZE;
            if (copied + chunk > cmd_len) {
                chunk = cmd_len - copied;
            }
            std::memcpy(cmd.data() + copied, p_buf.data() + p_off_in_page, chunk);
            copied += chunk;
        }
    }

    Entry e;
    e.term = term;
    e.command = std::move(cmd);
    return {std::move(e), (header_slot + num_slots) % ring.ring_slots};
}

} /* namespace nvmeof_raft */