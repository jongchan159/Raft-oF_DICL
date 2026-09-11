#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"
#include "cached_fd.h"   /* CachedFD 역참조 */

#include <chrono>
#include <stdexcept>

namespace nvmeof_raft {

/* PBA 복사의 실제 전송은 Server::blockcopy(core/include/raft_transport.h)가 한다.
 * TCP 구현은 net/src/raft_blkcopy_rpc_client.cpp. */

/* ============================================================
 * doPBACopy (raft.go 원본, 최신 배치+extent-splitting 버전 로직 포팅)
 *
 * "performs a storage-level block copy on behalf of the follower...
 *  If the follower's ring file is fragmented, the copy is split into
 *  multiple RPCs at extent boundaries so writes never spill past an
 *  extent... Must be called with s.mu held."
 *
 * 원본은 Pass 1(경계 계산, RPC 없음)과 Pass 2(실제 배치 RPC 발사)로
 * 나뉜다. Pass 1은 GetPBA → seg.Len/ring-wrap(maxBeforeWrap)/
 * MaxPBACopyChunkBytes 순으로 clamp (원본 raft.go:3122-3155 확인 완료).
 * Pass 2는 전체 청크 목록을 단일 배치 RPC(WritePBABatch)로 전달한다. */
Server::DoPbaCopyResult Server::do_pba_copy(uint64_t leader_pba_src,
                                             uint64_t log_block_length,
                                             uint64_t dst_slot,
                                             int src_dev, int dst_dev) {
    if (leader_pba_src == 0 || log_block_length == 0) {
        return {0, 0};
    }

    /* 스토리지 노드로의 연결과 그 재시도(lazy connect)는 BlockCopyClient
     * 구현체의 책임이다. do_pba_copy는 **s.mu를 놓은 상태에서** 불리고
     * (handle_append_entries_request의 mu.unlock() 직후, Leader-Side에서는
     *  append_entries_worker의 팔로워별 스레드마다) 여러 스레드가 동시에
     * 들어오므로, 필요한 직렬화도 구현체가 한다. */
    if (blockcopy == nullptr) {
        throw std::runtime_error("do_pba_copy: no block copy client configured");
    }

    uint64_t nbytes = align_up(log_block_length * SECTOR_SIZE, PAGE_SIZE);

    /* Pass 1: destination 범위를 훑으며 (src, dst, chunk) 튜플들을
     * extent 경계 + ring wrap + MaxPBACopyChunkBytes 경계에서 분할.
     * "the source range is contiguous (EXTENT CLAMP guarantee), so
     *  only the destination side fragments." */
    std::vector<uint64_t> pba_srcs, pba_dsts, chunk_nbytes;

    uint64_t remaining = nbytes;
    uint64_t cur_slot = dst_slot;
    uint64_t cur_src = leader_pba_src;

    while (remaining > 0) {
        if (cur_slot >= ring.ring_slots) {
            cur_slot = 0;   /* ring wrap */
        }

        int64_t logical_off = slot_offset(cur_slot);

        /* 원본(raft.go doPBACopy) 순서 그대로: GetPBA를 먼저 호출해
         * 그 extent의 seg.Len을 청크 상한으로 쓰고, 그 다음 ring-wrap
         * 경계(maxBeforeWrap)와 MaxPBACopyChunkBytes로 clamp한다. */
        PBASegment seg = io.cached_fd->get_pba(logical_off, remaining);
        if (seg.pba == 0) {
            throw std::runtime_error(
                "do_pba_copy: PBA=0 at offset " + std::to_string(logical_off) +
                " (hole in ring file)");
        }

        uint64_t max_before_wrap = (ring.ring_slots - cur_slot) * SECTOR_SIZE;

        uint64_t chunk = seg.len;
        if (chunk > remaining) {
            chunk = remaining;
        }
        if (chunk > max_before_wrap) {
            chunk = max_before_wrap;
        }
        if (chunk > MaxPBACopyChunkBytes) {
            chunk = MaxPBACopyChunkBytes;
        }
        if (chunk == 0) {
            throw std::runtime_error(
                "do_pba_copy: zero-size chunk at slot " + std::to_string(cur_slot) +
                " (extent lookup failed?)");
        }

        pba_srcs.push_back(cur_src);
        pba_dsts.push_back(seg.pba);
        chunk_nbytes.push_back(chunk);

        remaining -= chunk;
        cur_src += chunk;
        cur_slot += chunk / SECTOR_SIZE;
    }

    /* Pass 2: 배치 RPC 발사 (단일 배치 호출로 전체 청크 목록 전달) */
    auto t_rpc = clock_type::now();
    int64_t storage_copy_ns = 0;
    std::string bc_error;
    if (!blockcopy->write_pba_batch(pba_srcs, pba_dsts, chunk_nbytes,
                                     src_dev, dst_dev, &storage_copy_ns, &bc_error)) {
        /* 예전에는 구현체가 예외를 던졌다. 인터페이스는 bool을 쓰되 core
         * 내부의 제어 흐름은 그대로 둔다 -- 호출부
         * (handle_append_entries_request / append_entries_worker)가 예외를
         * 잡아 fail-soft로 이 배치를 건너뛰고 다음 라운드에 재시도한다. */
        throw std::runtime_error("do_pba_copy: " + bc_error);
    }
    int64_t write_pba_rt_ns = elapsed_ns(t_rpc);

    return {write_pba_rt_ns, storage_copy_ns};
}

/* ============================================================
 * HandleAppendEntriesRequest (raft.go 원본, 로직 그대로 포팅)
 *
 * "R2 timer: span the whole handler body, including lock-wait and the
 *  synchronous doPBACopy() WritePBA round-trip to the storage node."
 * ============================================================ */
void Server::handle_append_entries_request(const AppendEntriesRequest &req,
                                            AppendEntriesResponse &rsp) {
    auto t0 = clock_type::now();
    mu.lock();
    auto t_after_lock = clock_type::now();
    int64_t handle_ae_lock_wait = elapsed_ns(t0);
    int64_t handle_ae_pre = 0, handle_ae_lock_wait2 = 0, handle_ae_persist = 0, handle_ae_post = 0;
    clock_type::time_point t_post{};
    bool t_post_set = false;

    /* Go의 defer: 함수 반환 직전 unlock + 타이밍 필드 채우기.
     * C++에선 명시적으로 각 return 경로 앞에서 호출 */
    auto finalize = [&]() {
        int64_t handler_duration = elapsed_ns(t0);   /* Unlock 전에 측정 (스케줄링 지연 제외) */
        mu.unlock();
        rsp.handler_duration_ns = handler_duration;
        rsp.handle_ae_lock_wait_ns = handle_ae_lock_wait;
        rsp.handle_ae_pre_ns = handle_ae_pre;
        rsp.handle_ae_lock_wait2_ns = handle_ae_lock_wait2;
        rsp.handle_ae_post_ns = handle_ae_post;
        rsp.handle_ae_persist_ns = handle_ae_persist;
    };

    update_term(req.rpc.term);

    if (req.rpc.term == raft.current_term && raft.state == ServerState::Candidate) {
        raft.state = ServerState::Follower;
    }

    rsp.rpc.term = raft.current_term;
    rsp.success = false;

    if (raft.state != ServerState::Follower) {
        finalize();
        return;
    }
    if (req.rpc.term < raft.current_term) {
        finalize();
        return;
    }

    reset_election_timeout();

    /* PrevLog consistency check */
    if (req.prev_log_index > 0) {
        if (req.prev_log_index >= ring.tail_log_index) {
            rsp.conflict_term = 0;
            rsp.conflict_index = ring.tail_log_index;
            finalize();
            return;
        }
        uint64_t oldest = oldest_log_index();
        if (req.prev_log_index >= oldest) {
            uint64_t local_term = raft.log[log_slice(req.prev_log_index)].term;
            if (local_term != req.prev_log_term) {
                rsp.conflict_term = local_term;
                /* "Scan back to find the first index with that term." */
                uint64_t ci = req.prev_log_index;
                while (ci > oldest && raft.log[log_slice(ci - 1)].term == local_term) {
                    ci--;
                }
                rsp.conflict_index = ci;
                finalize();
                return;
            }
        }
    }

    /* "Compute how many leading entries in this batch the follower
     *  already has." */
    uint64_t new_tail = req.prev_log_index + 1;
    uint64_t skip_entries = 0;
    if (new_tail < ring.tail_log_index) {
        skip_entries = ring.tail_log_index - new_tail;
        if (skip_entries >= req.num_entries) {
            /* "Entire batch already in follower's log -- skip PBA copy." */
            if (req.leader_commit > raft.commit_index) {
                raft.commit_index = std::min(req.leader_commit, ring.tail_log_index - 1);
            }
            auto t_ps = clock_type::now();
            persist_circular(false, 0);   /* follower: persist header only */
            handle_ae_persist = elapsed_ns(t_ps);
            rsp.storage_copy_ns += handle_ae_persist;   /* persistCircular -> storageio */
            rsp.success = true;
            finalize();
            return;
        }
        /* "Don't truncate -- keep existing entries, skip them during
         *  readback." */
    }

    /* Storage-level block copy + in-memory log build */
    if (req.num_entries > 0 && req.leader_pba_src != 0) {
        /* "Sync follower tailSlot to leader's StartSlot." */
        if (ring.tail_slot != req.start_slot) {
            ring.tail_slot = req.start_slot;
        }
        uint64_t old_tail_slot = ring.tail_slot;

        handle_ae_pre = elapsed_ns(t_after_lock);

        /* Leader-Side(Server::ReplicationMode::LeaderSide)에서는
         * req.data_already_copied가 true -- leader의 storage node가
         * AppendEntries를 보내기 전에 이미 이 데이터를 우리 볼륨에
         * 직접 써놨으므로 do_pba_copy를 다시 돌릴 필요가 없다
         * (Destination-Side의 기본 동작만 아래 do_pba_copy 실행). */
        DoPbaCopyResult copy_result{};
        bool copy_failed = false;
        if (!req.data_already_copied) {
            mu.unlock();

            /* "PBA copy for durability -- data lands on device for crash
             *  recovery." */
            try {
                copy_result = do_pba_copy(req.leader_pba_src, req.log_block_length,
                                           old_tail_slot, req.leader_dev_index, raft.cluster_index);
            } catch (const std::exception &) {
                copy_failed = true;
            }

            auto t_lock2 = clock_type::now();
            mu.lock();
            handle_ae_lock_wait2 = elapsed_ns(t_lock2);
        }
        t_post = clock_type::now();
        t_post_set = true;

        rsp.write_pba_rt_ns = copy_result.write_pba_rt_ns;
        rsp.storage_copy_ns = copy_result.storage_copy_ns;
        if (copy_failed) {
            finalize();
            return;
        }

        if (req.entry_metas.size() == req.num_entries) {
            /* "Fast path: build in-memory log from metadata -- no
             *  readback. Command is nil; loaded lazily from device at
             *  apply time." */
            for (uint64_t i = 0; i < req.num_entries; i++) {
                const EntryMeta &meta = req.entry_metas[i];
                uint64_t needed = slots_for_entry(static_cast<size_t>(meta.cmd_len));

                /* "device에 이미 존재하는 엔트리 스킵" */
                if (i >= skip_entries) {
                    Entry e;
                    e.term = meta.term;
                    /* command는 비워둠 (nil 대응): 나중에 apply 시점에
                     * device에서 lazy load */
                    e.ring_slot = ring.tail_slot;
                    e.cmd_len = meta.cmd_len;
                    raft.log.push_back(std::move(e));

                    /* 팔로워는 log_slot_map을 **채우지 않는다.** 슬롯 수만
                     * 계산해 tail_slot을 전진시킨다.
                     *
                     * 원본 주석은 "Record slot mapping so a future
                     * becomeLeader on this node can PBA-replicate these
                     * entries..."라고 되어 있으나, 채우면 do_slot_gc가 리더
                     * 전용이라 팔로워의 맵이 무한히 자란다. 그래서 승격 직후
                     * 새 리더는 자기 no-op만 PBA 복제할 수 있다.
                     * 미해결 항목이며 슬롯맵 소유권/GC 책임을 함께 다시
                     * 봐야 한다 -- DECISIONS.md U2. */
                }

                /* [수정-11] tail_slot 전진은 엔트리 스킵 여부와 무관하게 항상 한다
                 * (건너뛴 엔트리도 링에서 슬롯을 차지한다) -- DECISIONS.md D11 */
                ring.tail_slot += needed;
                if (ring.tail_slot >= ring.ring_slots) {
                    ring.tail_slot = 0;
                }
            }
            ring.tail_log_index = new_tail + req.num_entries;
        }

    }

    /* commit_index 갱신. 원본 raft.go의 "// Update commit index"가 함수
     * 레벨에 있는 것과 같다.
     * [수정-5] 이 블록을 위 "엔트리가 실려 온" if 안으로 옮기지 말 것 --
     * 순수 하트비트가 팔로워의 커밋을 못 올린다 -- DECISIONS.md D5 */
    if (req.leader_commit > raft.commit_index) {
        raft.commit_index = std::min(req.leader_commit, ring.tail_log_index - 1);
    }

    if (t_post_set) {
        handle_ae_post = elapsed_ns(t_post);
    }

    /* "X -> writeHeader 조건 확인... 아무것도 안 씀" 은 term/vote가
     * 안 바뀌면 persist_circular가 헤더도 안 쓰고 조기 반환한다는
     * 원본 동작을 그대로 따름 (persist_circular 내부에서 이미 처리됨) */
    auto t_pn = clock_type::now();
    persist_circular(false, 0);   /* follower: persist header only
                                      (entries already durable via doPBACopy) */
    handle_ae_persist = elapsed_ns(t_pn);
    rsp.storage_copy_ns += handle_ae_persist;
    rsp.success = true;
    finalize();
}

} /* namespace nvmeof_raft */