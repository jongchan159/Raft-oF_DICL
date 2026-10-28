#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"
#include "cached_fd.h"   /* CachedFD 역참조 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdio>
#include <string>

namespace nvmeof_raft {

/* ============================================================
 * persistCircular (raft.go 원본, 로직 그대로 포팅)
 *
 * "writes Raft metadata and new log entries directly to the NVMe-oF
 *  storage device, bypassing the filesystem layer entirely. Log entries
 *  are written via DirectWrite at the FIEMAP-resolved device PBA...
 *  Returns nvmeNs: total wall time spent inside O_DIRECT WriteAtFile +
 *  Fdatasync syscalls (the actual NVMe-oF traffic)."
 * ============================================================ */
int64_t Server::persist_circular(bool write_log, int n_new_entries,
                                 std::shared_ptr<ReplSink> sink) {
    if (n_new_entries == 0 && write_log) {
        n_new_entries = static_cast<int>(raft.log.size()) - 1;
    }

    int64_t nvme_ns = 0;
    int flush_count = 0;

    /* ---- push 모드 (DECISIONS.md D20) ----
     * 이번 호출이 쓰는 엔트리를 push 대상 팔로워의 장치에도 같은 논리
     * 오프셋으로 쓴다. 쓰기는 대상별 push 워커에 맡기고 **기다리지 않는다**.
     * 그 대상의 쓰기와 flush가 끝나면 job이 끝난 구간을 늘리고 그 대상의
     * AE 워커만 깨운다(finish_push_job). 대상은 엔트리를 쓰는 호출에서만,
     * 엔트리 루프 앞에서 한 번 정한다. 헤더만 쓰는 호출과 리더가 아닐 때는
     * 대상이 없다 (push_write_targets가 리더만 본다).
     *
     * run: 링에 pwrite 한 번으로 쓰는 연속 구간. 보통 호출당 하나이고, 링
     * 끝에서 감아 돌거나(wrap) 엔트리가 extent 경계에 걸려 건너뛸 때만 여러
     * 개다. push 대상이 있으면 flush_run은 쓰지 않고 runs에 적기만 한다 --
     * 루프가 끝나야 모든 run을 알고, 그것을 job에 먼저 맡긴 뒤 자기 쓰기를
     * 해야 둘이 겹친다. job은 이 호출이 끝난 뒤에도 버퍼를 읽으므로 버퍼는
     * 공유 소유(push_buf)로 넘기고, run 사이에 비우지 않고 다음 run을 뒤에
     * 이어 붙인다. */
    struct Run {
        size_t buf_off;        /* 버퍼 안 시작 */
        size_t len;            /* 바이트 */
        int64_t logical_off;   /* 링 파일의 논리 오프셋 */
    };
    std::vector<int> push_fis;
    std::vector<Run> runs;
    std::shared_ptr<AlignedBuffer> push_buf;
    uint64_t push_first = 0;
    uint64_t push_last = 0;

    if (write_log && n_new_entries > 0) {
        int start = static_cast<int>(raft.log.size()) - n_new_entries;
        if (start < 1) {
            start = 1;
        }

        push_fis = push_write_targets(clock_type::now());
        /* 이번 호출이 쓰는 엔트리의 로그 인덱스 구간 (아래 루프의 log_idx와 같은 식) */
        push_first = ring.tail_log_index -
                     static_cast<uint64_t>(raft.log.size() - static_cast<size_t>(start));
        push_last = ring.tail_log_index - 1;

        size_t est_size = 0;
        for (int k = start; k < static_cast<int>(raft.log.size()); k++) {
            est_size += static_cast<size_t>(
                slots_for_entry(raft.log[static_cast<size_t>(k)].command.size())) * SECTOR_SIZE;
        }

        /* push 대상이 있으면 run을 모아 두었다가 루프 뒤에 쓴다 */
        const bool deferred = !push_fis.empty();
        if (deferred) {
            push_buf = acquire_push_buffer(est_size);
        } else if (ring.run_buf.capacity() < est_size) {
            /* 원본: makeAlignedBlock(estSize)[:0]. AlignedBuffer는
             * posix_memalign 메모리를 복사 없이 그대로 소유하므로
             * O_DIRECT 정렬이 항상 유지됨 (std::vector 버그 수정) */
            ring.run_buf.resize(est_size, SECTOR_SIZE);
            ring.run_buf.clear();   /* capacity는 유지, size만 0 (Go의 [:0] 슬라이싱과 동일 의도) */
        }

        /* 원본 runBuf 별칭, s.runBuf[:0] 대응 */
        AlignedBuffer &run_buffer = deferred ? *push_buf : ring.run_buf;
        run_buffer.clear();
        size_t run_start = 0;   /* 지금 run이 버퍼의 어디서 시작하나 (push가 없으면 늘 0) */
        int64_t run_off = 0;

        /* flushRun (원본 클로저 -> 람다)
         * "write the buffered runBuf to the metadata file at the current
         *  runOff via O_DIRECT WriteAtFile... One pwrite per flushRun
         *  call regardless of extent count"
         * push 대상이 있으면 쓰지 않고 run을 적기만 한다 (위 주석). */
        auto flush_run = [&]() {
            if (run_buffer.size() == run_start) {
                return;
            }
            size_t len = run_buffer.size() - run_start;
            if (deferred) {
                runs.push_back(Run{run_start, len, run_off});
                run_start = run_buffer.size();
                return;
            }
            auto t_io = clock_type::now();
            flush_count++;
            io.cached_fd->write_at_file(run_buffer.data(), len, run_off);
            nvme_ns += elapsed_ns(t_io);
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

            if (run_buffer.size() == run_start) {   /* 새 run의 첫 엔트리 */
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
                trace_slot_map('I', log_idx);
            }
        }

        /* flushRun -> pwrite + fdatasync (3. persistI/O) */
        flush_run();
        /* state는 엔트리별 logSlotMap insert 시점에 이미 설정되므로
         * 배치 후 markSlots 불필요 (원본 주석 그대로) */

        if (deferred) {
            /* 1) 대상마다 job 하나를 맡긴다 -- 자기 쓰기보다 먼저 시작시켜야
             *    둘이 겹친다. job은 모든 run을 쓰고 flush한 뒤 결과를 적고
             *    그 대상의 AE 워커를 깨운다. 기다리지 않는다. */
            for (int fi : push_fis) {
                size_t i = static_cast<size_t>(fi);
                IoWorker *w = push_worker(fi);
                std::shared_ptr<CachedFD> pfd = (i < io.push_fds.size()) ? io.push_fds[i] : nullptr;
                /* 맡길 때의 세대 번호를 들고 간다 -- 그 사이 구간이 비워지면
                 * (롤백, 리더 교체, 링 교체, 실패) 끝났을 때 결과를 버린다 */
                uint64_t epoch = raft.cluster[i].push_epoch;
                bool queued = false;
                if (w != nullptr && pfd != nullptr) {
                    queued = w->submit([this, fi, pfd, push_buf, runs, epoch,
                                        push_first, push_last, sink]() {
                        /* 워커 스레드, mu 없이. F의 장치와 맵은 F의 워커만
                         * 만진다 (맵 설치도 이 줄에 선다 -- note_push_response).
                         * 리더에서 물러난 뒤 큐에 남은 job의 쓰기는 펜싱(NVMe
                         * Reservation)이 타깃에서 거절한다고 가정한다 -- D20 한계. */
                        std::string err;
                        try {
                            for (const Run &r : runs) {
                                pfd->write_at_logical(push_buf->data() + r.buf_off, r.len,
                                                      r.logical_off);
                            }
                            /* 블록 장치 fdatasync = 디바이스 캐시 flush. 이것까지
                             * 끝나야 "팔로워 볼륨에 내구성 있게 있다" */
                            pfd->fdatasync_device();
                        } catch (const std::exception &e) {
                            err = e.what();
                        }
                        finish_push_job(fi, epoch, push_first, push_last, err, sink);
                    });
                }
                if (queued) {
                    note_push_submitted(fi, push_first, push_last);
                } else {
                    /* 워커가 없거나 큐가 꽉 찼다(그 대상이 너무 뒤처졌거나
                     * 멈췄다) -- 1초 쉬고 그동안 pull로 간다 */
                    note_push_failure(fi, (w == nullptr || pfd == nullptr)
                                              ? "no push worker" : "push queue full");
                }
            }
            /* 2) 자기 run 쓰기 (같은 버퍼, 복사 없음) */
            for (const Run &r : runs) {
                auto t_io = clock_type::now();
                flush_count++;
                io.cached_fd->write_at_file(run_buffer.data() + r.buf_off, r.len, r.logical_off);
                nvme_ns += elapsed_ns(t_io);
            }
        }
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
        /* 헤더는 push하지 않는다 (push job에 넣지 않는다). term/votedFor
         * 등 **이 노드 자신의** 영속 상태라, 팔로워 링에 쓰면 그 팔로워의
         * 투표 기록을 덮어 재시작 후 같은 term에 두 번 투표하게 만든다
         * (D18, DECISIONS.md D20). */
        auto t_hdr_io = clock_type::now();
        io.cached_fd->write_at_file(header.data(), header.size(), 0);
        nvme_ns += elapsed_ns(t_hdr_io);   /* header 작성 시 writeI/O에 추가 */

        ring.persisted_init = true;
        ring.persisted_term = cur_term;
        ring.persisted_voted_for = cur_vote;
    }

    if ((write_log && n_new_entries > 0) || write_header) {
        /* 3-2. fdatasync (자기 링만. push 대상 장치의 flush는 그 job이 한다) */
        auto t_sync_io = clock_type::now();
        io.cached_fd->fdatasync();
        nvme_ns += elapsed_ns(t_sync_io);
    }

    return nvme_ns;
}

std::shared_ptr<AlignedBuffer> Server::acquire_push_buffer(size_t capacity) {
    for (auto &b : ring.push_bufs) {
        /* 풀만 들고 있다 = 이 버퍼를 읽는 push job이 없다 */
        if (b.use_count() == 1) {
            /* use_count()는 relaxed로 읽힌다. job이 참조를 놓은 것(release)과
             * 짝을 맞춰, job의 읽기가 아래 덮어쓰기보다 먼저 끝났음을 보장한다 */
            std::atomic_thread_fence(std::memory_order_acquire);
            if (b->capacity() < capacity) {
                b->resize(capacity, SECTOR_SIZE);
            }
            b->clear();
            return b;
        }
    }
    auto b = std::make_shared<AlignedBuffer>(capacity, SECTOR_SIZE);
    b->clear();
    ring.push_bufs.push_back(b);
    return b;
}

} /* namespace nvmeof_raft */