#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"
#include "cached_fd.h"   /* CachedFD 역참조 */

#include <chrono>
#include <cstring>
#include <cstdio>

namespace nvmeof_raft {

/* ============================================================
 * persistCircular (raft.go 원본, 로직 그대로 포팅)
 *
 * "writes Raft metadata and new log entries directly to the NVMe-oF
 *  storage device, bypassing the filesystem layer entirely. Log entries
 *  are written via DirectWrite at the FIEMAP-resolved device PBA."
 * ============================================================ */
void Server::persist_circular(bool write_log, int n_new_entries) {
    if (n_new_entries == 0 && write_log) {
        n_new_entries = static_cast<int>(raft.log.size()) - 1;
    }

    if (write_log && n_new_entries > 0) {
        int start = static_cast<int>(raft.log.size()) - n_new_entries;
        if (start < 1) {
            start = 1;
        }

        size_t est_size = 0;
        for (int k = start; k < static_cast<int>(raft.log.size()); k++) {
            est_size += static_cast<size_t>(
                slots_for_entry(raft.log[static_cast<size_t>(k)].command.size())) * SECTOR_SIZE;
        }
        if (ring.run_buf.capacity() < est_size) {
            /* 원본: makeAlignedBlock(estSize)[:0]. AlignedBuffer는
             * posix_memalign 메모리를 복사 없이 그대로 소유하므로
             * O_DIRECT 정렬이 항상 유지됨 (std::vector 버그 수정) */
            ring.run_buf.resize(est_size, SECTOR_SIZE);
            ring.run_buf.clear();   /* capacity는 유지, size만 0 (Go의 [:0] 슬라이싱과 동일 의도) */
        }

        AlignedBuffer &run_buffer = ring.run_buf;   /* 원본 runBuf 별칭, s.runBuf[:0] 대응 */
        run_buffer.clear();
        int64_t run_off = 0;

        /* flushRun (원본 클로저 -> 람다)
         * "write the buffered runBuf to the metadata file at the current
         *  runOff via O_DIRECT WriteAtFile... One pwrite per flushRun
         *  call regardless of extent count" */
        auto flush_run = [&]() {
            if (run_buffer.empty()) {
                return;
            }
            io.cached_fd->write_at_file(run_buffer.data(), run_buffer.size(), run_off);
            run_off += static_cast<int64_t>(run_buffer.size());
            run_buffer.clear();
        };

        /* FIEMAP extent layout: contiguous fast-path bypass + per-entry
         * extent hint (원본 주석 그대로) */
        bool contiguous = io.cached_fd->is_contiguous();
        int ext_hint = 0;

        for (int i = start; i < static_cast<int>(raft.log.size()); i++) {
            uint64_t log_idx = ring.tail_log_index - static_cast<uint64_t>(raft.log.size() - static_cast<size_t>(i));
            const Entry &e = raft.log[static_cast<size_t>(i)];
            uint64_t needed = slots_for_entry(e.command.size());

            if (ring.tail_slot + needed > ring.ring_slots) {
                flush_run();
                ring.tail_slot = 0;
            }

            /* Per-entry extent skip (원본 주석 그대로)
             * "WriteAtFile can straddle FIEMAP extents on the persist
             *  side (kernel handles it), but the replication side
             *  cannot: each WritePBA carries one (PBA, length) tuple
             *  per source range, so an entry that spans two extents
             *  cannot be PBA-copied atomically." */
            if (!contiguous) {
                int64_t entry_off = slot_offset(ring.tail_slot);
                uint64_t entry_bytes = needed * SECTOR_SIZE;
                auto [rem, new_hint] = io.cached_fd->remaining_at(entry_off, ext_hint);
                ext_hint = new_hint;
                if (rem < entry_bytes) {
                    flush_run();
                    uint64_t skip_slots = rem / SECTOR_SIZE;
                    ring.tail_slot += skip_slots;
                    if (ring.tail_slot + needed > ring.ring_slots) {
                        ring.tail_slot = 0;
                    }
                    /* 건너뛴 뒤의 extent 힌트를 다시 잡는다. rem2는 쓰지
                     * 않지만 remaining_at은 힌트 갱신을 위해 불러야 한다. */
                    auto [rem2, hint2] = io.cached_fd->remaining_at(slot_offset(ring.tail_slot), ext_hint);
                    (void)rem2;
                    ext_hint = hint2;
                }
            }

            if (run_buffer.empty()) {
                run_off = slot_offset(ring.tail_slot);
            }

            size_t entry_bytes = static_cast<size_t>(needed) * SECTOR_SIZE;
            size_t off = run_buffer.size();
            run_buffer.resize(off + entry_bytes);
            uint8_t *region = run_buffer.data() + off;

            /* Entry 헤더 레이아웃 (raft.go 원본 그대로):
             *   [0:8)   Term
             *   [8:16)  cmdLen
             *   [16:24) needed (numSlots)
             *   [24:32) logIdx
             *   [32:..) Command 바이트 (ENTRY_META_SIZE = 32부터 시작) */
            put_u64_le(region + entry_hdr::kOffTerm, e.term);
            put_u64_le(region + entry_hdr::kOffCmdLen, e.command.size());
            put_u64_le(region + entry_hdr::kOffNumSlots, needed);
            put_u64_le(region + entry_hdr::kOffLogIndex, log_idx);
            if (!e.command.empty()) {
                /* no-op 엔트리는 command가 비어 있고 vector::data()가
                 * nullptr이다. memcpy(dst, nullptr, 0)은 형식상 UB라
                 * UBSan이 잡는다 (동작은 무해했음). */
                std::memcpy(region + ENTRY_META_SIZE, e.command.data(), e.command.size());
            }

            /* 원본 주석: "Zero only the trailing pad between
             * (header+command) and the slot boundary. Matches
             * goraft/raft.go:599-601 exactly... s.runBuf is reused...
             * without this clear stale tail bytes from prior calls
             * would leak onto the device." */
            size_t pad_start = ENTRY_META_SIZE + e.command.size();
            for (size_t j = pad_start; j < entry_bytes; j++) {
                region[j] = 0;
            }

            uint64_t start_slot = ring.tail_slot;
            ring.tail_slot += needed;

            if (raft.state == ServerState::Leader) {
                ring.log_slot_map[log_idx] = SlotRecord{start_slot, needed, LogEntryState::Using};
            }
        }

        /* flushRun -> pwrite + fdatasync (3. persistI/O) */
        flush_run();
        /* state는 엔트리별 logSlotMap insert 시점에 이미 설정되므로
         * 배치 후 markSlots 불필요 (원본 주석 그대로) */
    }

    /* conditional header write (원본 주석 그대로: "no calculate in
     * normal operation") */
    uint64_t cur_term = raft.current_term;
    uint64_t cur_vote = get_voted_for();
    bool write_header = !ring.persisted_init ||
                         cur_term != ring.persisted_term ||
                         cur_vote != ring.persisted_voted_for;

    if (write_header) {
        /* Aligned allocation: WriteAtFile은 O_DIRECT pwrite라 버퍼
         * 포인터가 섹터 정렬이어야 함 (원본 주석 그대로).
         * AlignedBuffer는 posix_memalign 메모리를 복사 없이 소유하므로
         * std::vector와 달리 정렬이 실제로 보장됨 */
        AlignedBuffer header(HEADER_SIZE, SECTOR_SIZE);

        /* File header layout (512B), 원본 주석 그대로:
         *   [ 0: 7] currentTerm
         *   [ 8:15] votedFor
         *   [16:23] tailLogIndex
         *   [24:31] tailSlot
         *   [32:39] commitIndex
         *   [40:47] lastApplied */
        put_u64_le(header.data() + file_hdr::kOffCurrentTerm, cur_term);
        put_u64_le(header.data() + file_hdr::kOffVotedFor, cur_vote);
        put_u64_le(header.data() + file_hdr::kOffTailLogIndex, ring.tail_log_index);
        put_u64_le(header.data() + file_hdr::kOffTailSlot, ring.tail_slot);
        put_u64_le(header.data() + file_hdr::kOffCommitIndex, raft.commit_index);
        put_u64_le(header.data() + file_hdr::kOffLastApplied, raft.last_applied);

        /* Header lives at logical offset 0 (RING_OFFSET == HEADER_SIZE
         * -- ring slot 0은 헤더 다음부터 시작, 원본 주석 그대로) */
        io.cached_fd->write_at_file(header.data(), header.size(), 0);

        ring.persisted_init = true;
        ring.persisted_term = cur_term;
        ring.persisted_voted_for = cur_vote;
    }

    if ((write_log && n_new_entries > 0) || write_header) {
        /* 3-2. fdatasync */
        io.cached_fd->fdatasync();
    }
}

} /* namespace nvmeof_raft */