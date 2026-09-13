/* ============================================================
 * raft_handle_append_entries.cpp -- 복제 수신 (팔로워 측)
 *
 * 팔로워가 AppendEntries를 받아서 하는 일은 두 가지다:
 *   1. do_pba_copy  -- 리더 링의 물리 주소에서 자기 볼륨으로 블록을 복사
 *   2. 인메모리 로그 빌드 -- entry_metas(term + cmd_len)만으로 엔트리를
 *      만든다. command는 비워 둔다 (device에 이미 올라와 있고, apply
 *      시점에 읽는다).
 *
 * 원본 대비 빠진 것: 구간 계측 전부, conflict_term/conflict_index 되감기
 * 힌트 계산, Leader-Side(data_already_copied) 분기.
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"
#include "cached_fd.h"   /* CachedFD 역참조 */

#include <algorithm>
#include <stdexcept>

namespace nvmeof_raft {

/* ============================================================
 * doPBACopy (raft.go 원본)
 *
 * "performs a storage-level block copy on behalf of the follower...
 *  If the follower's ring file is fragmented, the copy is split into
 *  multiple RPCs at extent boundaries so writes never spill past an
 *  extent."
 *
 * Pass 1은 (src, dst, chunk) 튜플을 extent 경계 / 링 wrap /
 * MaxPBACopyChunkBytes 에서 쪼개고, Pass 2가 그 목록을 한 번의 배치
 * RPC로 보낸다. **s.mu를 놓은 상태에서** 불리고 여러 스레드가 동시에
 * 들어오므로, 필요한 직렬화는 BlockCopyClient 구현체가 한다.
 * ============================================================ */
void Server::do_pba_copy(uint64_t leader_pba_src, uint64_t log_block_length,
                          uint64_t dst_slot, int src_dev, int dst_dev) {
    if (leader_pba_src == 0 || log_block_length == 0) {
        return;
    }

    if (blockcopy == nullptr) {
        throw std::runtime_error("do_pba_copy: no block copy client configured");
    }

    uint64_t nbytes = align_up(log_block_length * SECTOR_SIZE, PAGE_SIZE);

    /* Pass 1: destination 범위를 훑으며 경계에서 분할.
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

        /* 원본 순서 그대로: GetPBA를 먼저 호출해 그 extent의 seg.len을
         * 청크 상한으로 쓰고, 그 다음 ring-wrap 경계와
         * MaxPBACopyChunkBytes로 clamp한다. */
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

    /* Pass 2: 배치 RPC 발사 */
    std::string bc_error;
    if (!blockcopy->write_pba_batch(pba_srcs, pba_dsts, chunk_nbytes,
                                     src_dev, dst_dev, &bc_error)) {
        /* 호출부가 예외를 잡아 fail-soft로 이 배치를 건너뛰고 다음
         * 라운드에 재시도한다. */
        throw std::runtime_error("do_pba_copy: " + bc_error);
    }
}

/* ============================================================
 * HandleAppendEntriesRequest (raft.go 원본, 로직 그대로 포팅)
 * ============================================================ */
void Server::handle_append_entries_request(const AppendEntriesRequest &req,
                                            AppendEntriesResponse &rsp) {
    mu.lock();

    /* Go의 defer: 함수 반환 직전 unlock.
     * C++에선 명시적으로 각 return 경로 앞에서 호출 */
    auto finalize = [&]() { mu.unlock(); };

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

    /* PrevLog consistency check.
     * 원본은 거절할 때 어디서부터 다시 보내야 하는지(conflict_term /
     * conflict_index)를 계산해 실어 보낸다. 여기서는 거절만 하고 리더가
     * next_index를 1 되감는다 -- 로그가 갈라지는 상황은 이 버전의
     * 범위 밖이다. */
    if (req.prev_log_index > 0) {
        if (req.prev_log_index >= ring.tail_log_index) {
            finalize();
            return;
        }
        uint64_t oldest = oldest_log_index();
        if (req.prev_log_index >= oldest) {
            uint64_t local_term = raft.log[log_slice(req.prev_log_index)].term;
            if (local_term != req.prev_log_term) {
                finalize();
                return;
            }
        }
    }

    /* "Compute how many leading entries in this batch the follower
     *  already has."
     * 응답이 유실돼 리더가 같은 배치를 다시 보내는 것은 **정상 경로에서
     * 일어나는 일이다.** 이 계산이 없으면 재전송이 엔트리를 두 번 쌓는다. */
    uint64_t new_tail = req.prev_log_index + 1;
    uint64_t skip_entries = 0;
    if (new_tail < ring.tail_log_index) {
        skip_entries = ring.tail_log_index - new_tail;
        if (skip_entries >= req.num_entries) {
            /* "Entire batch already in follower's log -- skip PBA copy." */
            if (req.leader_commit > raft.commit_index) {
                raft.commit_index = std::min(req.leader_commit, ring.tail_log_index - 1);
            }
            persist_circular(false, 0);   /* follower: persist header only */
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

        mu.unlock();

        /* "PBA copy for durability -- data lands on device for crash
         *  recovery." */
        bool copy_failed = false;
        try {
            do_pba_copy(req.leader_pba_src, req.log_block_length,
                        old_tail_slot, req.leader_dev_index, raft.cluster_index);
        } catch (const std::exception &) {
            copy_failed = true;
        }

        mu.lock();

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
                     * 계산해 tail_slot을 전진시킨다 (do_slot_gc가 리더
                     * 전용이라 채우면 팔로워의 맵이 무한히 자란다). */
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

    /* commit_index 갱신.
     * [수정-5] 이 블록을 위 "엔트리가 실려 온" if 안으로 옮기지 말 것 --
     * 순수 하트비트가 팔로워의 커밋을 못 올린다 -- DECISIONS.md D5 */
    if (req.leader_commit > raft.commit_index) {
        raft.commit_index = std::min(req.leader_commit, ring.tail_log_index - 1);
    }

    /* term/vote가 안 바뀌면 persist_circular가 헤더도 안 쓰고 조기
     * 반환한다 (내부에서 처리). */
    persist_circular(false, 0);   /* follower: persist header only
                                      (entries already durable via doPBACopy) */
    rsp.success = true;
    finalize();
}

} /* namespace nvmeof_raft */
