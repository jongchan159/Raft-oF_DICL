#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"

#include <chrono>
#include <algorithm>
#include <iostream>
#include <sstream>

namespace nvmeof_raft {

/* 전송은 Server::transport(core/include/raft_transport.h)로만 한다. TCP 구현은
 * net/src/raft_rpc_client.cpp, 테스트용 fake는 tests/fake_transport.h.
 * 이 파일의 로직(Lock B/C, EntryMeta 빌드, backoff 계산)은 전송 구현과
 * 무관하다. */

/* ============================================================
 * appendEntries (raft.go 원본, 로직 그대로 포팅)
 *
 * 병렬화 완료: 팔로워마다 std::thread를 띄워 append_entries_worker를
 * 실행. 원본 goroutine의 fire-and-forget과 동일하게, append_entries
 * 자체는 스레드를 다 띄운 뒤 즉시 리턴한다 (완료를 기다리지 않음).
 * 스레드 생명주기는 Server::spawn_replication_thread /
 * join_all_replication_threads로 관리 (raft_server.h 참고,
 * use-after-free 방지를 위해 join도 detach도 아닌 절충안).
 * ============================================================ */
void Server::append_entries(std::shared_ptr<ReplSink> sink) {
    for (size_t i = 0; i < raft.cluster.size(); i++) {
        int fi = static_cast<int>(i);
        if (fi == raft.cluster_index) {
            continue;   /* "Don't need to send message to self" */
        }
        /* sink를 값으로 캡처해 워커가 살아 있는 동안 ReplSink도 살아
         * 있게 한다 (raft_server.h의 append_entries 주석 참고) */
        spawn_replication_thread([this, fi, sink]() {
            append_entries_worker(fi, sink.get());
        });
    }
}

/* append_entries_worker: 팔로워 한 명에 대한 처리 로직.
 * 원본 append_entries의 go func(fi) {...} 본문 그대로 (로직 변경 없음,
 * 감싸는 함수만 분리) */
void Server::append_entries_worker(int fi, ReplSink *sink) {
    {
        /* ---- 4-1. waitLockB ---- */
        auto t_b = clock_type::now();
        mu.lock();
        auto t_b_held_start = clock_type::now();
        int64_t mutex_b = elapsed_ns(t_b);

        /* ---- 4-2. logValid ---- */
        uint64_t next = raft.cluster[static_cast<size_t>(fi)].next_index;
        uint64_t last = ring.tail_log_index - 1;
        uint64_t oldest = oldest_log_index();

        /* "follower가 leader보다 앞서있을 때 -> last+1로 변경 (예외처리)" */
        if (next > last + 1) {
            next = last + 1;
            raft.cluster[static_cast<size_t>(fi)].next_index = next;
        }
        /* "Clamp: if next fell below what we have in memory, reset to
         * oldest -> follower에게 보내야 할 entry가 ring에서 해제됐을 때
         * oldest로 옮겨서 살아있는 가장 오래된 entry부터 전송" */
        if (next < oldest) {
            next = oldest;
            raft.cluster[static_cast<size_t>(fi)].next_index = next;
        }

        /* Raft Consistency check */
        uint64_t prev_log_index = 0;
        uint64_t prev_log_term = 0;
        if (next > 0) {
            prev_log_index = next - 1;
            uint64_t oldest2 = oldest_log_index();
            if (prev_log_index >= oldest2 && prev_log_index < ring.tail_log_index) {
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
            if (len_entries > max_ae_batch) {
                len_entries = max_ae_batch;
            }
        }

        uint64_t total_slots = 0;
        uint64_t slots_per_entry = 0;
        uint64_t start_slot = 0;
        int64_t ae_sub_slot_map_ns = 0;
        int64_t ae_sub_mark_slots_ns = 0;

        /* ---- 4-3. slotMapLookup (원본 raft.go:2475-2554 그대로 포팅) ----
         * "Get start slot from first entry's slot map. If missing (or
         *  already GC'd), the slot was freed -- PBA copy would read
         *  stale data. Send heartbeat only." */
        auto t_slot_map_lookup = clock_type::now();
        if (len_entries > 0) {
            /* gcUpTo fast path: 이미 GC된 인덱스면 조회 스킵.
             * gc_has_run이 false면(GC를 아직 한 번도 안 돌린 상태)
             * 이 fast path 자체를 건너뛴다 -- 안 그러면 next=0,
             * gc_up_to=0(원본 초기값)일 때 0<=0이 참이 되어 GC된 적
             * 없는 index 0을 "이미 GC됨"으로 오판한다 (실제 재현 확인). */
            auto it = ring.log_slot_map.find(next);
            if (ring.gc_has_run && next <= ring.gc_up_to) {
                log_skip_pba_diag(fi, next, last, prev_log_index);
                len_entries = 0;
            } else if (it != ring.log_slot_map.end()) {
                start_slot = it->second.start;
            } else {
                log_skip_pba_diag(fi, next, last, prev_log_index);
                len_entries = 0;
            }
        }

        if (len_entries > 0) {
            /* "Accumulate slots across entries, clamping at the per-round
             *  byte cap. entry를 하나씩 추가하면서 누적 바이트가
             *  MaxAppendEntriesBatchBytes를 넘으면 그 직전에서 자름" */
            uint64_t accepted = 0;
            for (uint64_t k = 0; k < len_entries; k++) {
                uint64_t log_idx = next + k;
                uint64_t needed = slots_for_log_index(log_idx);
                /* "Byte cap: break once cumulative bytes would exceed the
                 *  round ceiling. Always accept at least one entry so a
                 *  single oversized entry still makes progress." */
                if (accepted > 0 && (total_slots + needed) * SECTOR_SIZE > max_ae_batch_bytes) {
                    break;
                }
                total_slots += needed;
                accepted++;
            }
            len_entries = accepted;
            slots_per_entry = (total_slots + len_entries - 1) / len_entries;

            /* "Limit batch at ring wrap-around for contiguous FIEMAP" */
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
                    /* "Byte cap applies equally on the wrap-clamped path." */
                    if (len_entries > 0 &&
                        (total_slots + needed) * SECTOR_SIZE > max_ae_batch_bytes) {
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
        ae_sub_slot_map_ns = elapsed_ns(t_slot_map_lookup);

        /* PBALookup (원본 raft.go:2564-2613 그대로 포팅) */
        uint64_t leader_pba_src = 0;
        uint64_t log_block_length = 0;
        auto t_pba_lookup = clock_type::now();
        if (len_entries > 0) {
            PbaRangeResult r = leader_pba_for_range(start_slot, total_slots);
            leader_pba_src = r.pba_src;
            /* "actualBytes가 핵심, extent cache가 반환하는 실제로 연속된
             *  물리 블록의 바이트 수. 요청한 범위 전체가 하나의 extent
             *  안에 있으면 actualBytes == totalSlots*SECTOR_SIZE, extent
             *  경계에 걸리면 actualBytes < totalSlots*SECTOR_SIZE" */
            uint64_t actual_bytes = r.nbytes;

            /* "Clamp batch if FIEMAP extent doesn't cover the full range." */
            uint64_t actual_slots = actual_bytes / SECTOR_SIZE;
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
                total_slots = counted;
            }
            log_block_length = total_slots;
        }
        int64_t ae_sub_pba_lookup_ns = elapsed_ns(t_pba_lookup);

        uint64_t sent_next = next;
        bool has_entries = len_entries > 0;
        if (has_entries) {
            raft.cluster[static_cast<size_t>(fi)].inflight = true;
        }

        /* ---- 4-6. MetaBuild ----
         * "Send lightweight entry metadata so follower can build its
         *  in-memory log without readback. Command payload is on the
         *  device via PBA copy; follower loads it lazily at apply time." */
        std::vector<EntryMeta> metas;
        int64_t ae_sub_meta_build_ns = 0;
        if (len_entries > 0) {
            auto t_mb = clock_type::now();
            metas.resize(len_entries);
            for (uint64_t k = 0; k < len_entries; k++) {
                Entry &e = raft.log[log_slice(next + k)];
                uint64_t cl = e.command.size();
                if (e.command.empty() && e.ring_slot != 0) {
                    cl = e.cmd_len;   /* deferred entry: use stored cmdLen */
                }
                metas[k] = EntryMeta{e.term, cl};
            }
            ae_sub_meta_build_ns = elapsed_ns(t_mb);
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

        int64_t ae_lock_b_held = elapsed_ns(t_b_held_start);
        mu.unlock();   /* "lockB Finish" */

        /* ---- Leader-Side replication (Server::ReplicationMode::LeaderSide,
         *  hpdc15dare 3.1.2와 동일 정책) ----
         * Destination-Side(기본)에서는 follower의 handle_append_entries_
         * request 안에서 do_pba_copy가 실행된다. Leader-Side에서는 그
         * 대신 여기서 -- RPC를 보내기 *전에* -- leader 자신의 storage
         * node(Server::blockcopy)에게 "내 로그를 follower(dst_dev=fi) 볼륨에
         * 직접 써라"라고 지시한다. 복사가 실패하면 이 배치는 스킵하고
         * (다음 heartbeat에서 재시도), 성공하면 data_already_copied=true로
         * 표시해 follower가 do_pba_copy를 다시 하지 않도록 한다. */
        DoPbaCopyResult leader_side_copy{};
        bool leader_side_copy_failed = false;
        if (has_entries && replication_mode == ReplicationMode::LeaderSide) {
            try {
                leader_side_copy = do_pba_copy(leader_pba_src, log_block_length,
                                                start_slot, raft.cluster_index, fi);
                req.data_already_copied = true;
            } catch (const std::exception &) {
                leader_side_copy_failed = true;
            }
        }
        if (leader_side_copy_failed) {
            /* "복사 실패 -- 이번 배치는 건너뛰고 다음 AppendEntries
             *  라운드(heartbeat)에서 재시도" (destination-side가
             *  do_pba_copy 실패 시 finalize()로 조용히 리턴하는 것과
             *  동일한 fail-soft 정책). inflight를 반드시 되돌려야
             *  다음 append_entries_worker 호출이 이 팔로워를 계속
             *  건너뛰지 않는다 (Lock C 블록의 정상 실패 경로와 동일). */
            if (has_entries) {
                mu.lock();
                raft.cluster[static_cast<size_t>(fi)].inflight = false;
                mu.unlock();
            }
            return;
        }

        /* REPLICATION LATENCY */
        if (has_entries) {
            prof.ae_count.fetch_add(1);
            prof.ae_entries.fetch_add(len_entries);
        }

        AppendEntriesResponse rsp;
        auto t_rep = clock_type::now();
        /* transport가 없으면(주입 안 된 하네스) 전송 실패와 동일하게 다룬다 */
        bool ok = (transport != nullptr) && transport->append_entries(fi, req, rsp);
        int64_t rt_ns = elapsed_ns(t_rep);

        /* ---- Lock C: post-RPC bookkeeping ---- */
        auto t_c = clock_type::now();
        mu.lock();
        int64_t mutex_c = elapsed_ns(t_c);

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
         * handle_append_entries_request가 update_term으로 강등시킨다.
         * 다만 그만큼 강등이 늦는다. 원본과 맞추려면 update_term(rsp.rpc.term)
         * 을 부르면 되지만, 그건 동작 변경이므로 이번 리팩토링 범위 밖이다. */
        if (rsp.rpc.term > raft.current_term) {
            mu.unlock();
            return;
        }
        if (rsp.rpc.term != req.rpc.term && raft.state == ServerState::Leader) {
            mu.unlock();
            return;
        }

        /* 샘플 수집은 sink 유무와 무관하게 한다.
         *
         * ProfilingSink의 sample_* 는 apply_timed가 "이 Apply 안에서
         * data-bearing AE를 못 잡았을 때" 쓰는 폴백이다. 그 상황이
         * 발생하는 이유가 **바로 직전 하트비트가 이미 복제를 끝냈기
         * 때문**이므로(하트비트는 sink == nullptr), 폴백 값을 남기는
         * 코드가 sink 가드 안에 있으면 영원히 채워지지 않는다.
         * 실제로 그랬다 -- store가 0건이어서 폴백이 항상 0을 읽었다
         * (DECISIONS.md U4). 그래서 가드는 push에만 건다. */
        if (rsp.success && has_entries) {
            ReplSample sample;
            sample.r2_ns = rsp.handler_duration_ns;
            /* Leader-Side: 팔로워는 do_pba_copy를 안 돌리므로
             * rsp.write_pba_rt_ns/storage_copy_ns는 0 -- 대신 leader가
             * RPC 전에 직접 측정한 leader_side_copy 값을 기록해야
             * Destination-Side와 동일한 지표로 비교 가능하다. */
            if (replication_mode == ReplicationMode::LeaderSide) {
                sample.write_pba_rt_ns = leader_side_copy.write_pba_rt_ns;
                sample.storage_copy_ns = leader_side_copy.storage_copy_ns;
            } else {
                sample.write_pba_rt_ns = rsp.write_pba_rt_ns;
                sample.storage_copy_ns = rsp.storage_copy_ns;
            }
            /* sink가 없으면 Apply가 아니므로 Lock A 대기도 없다 */
            sample.mutex_ns = (sink != nullptr ? sink->mutex_a_ns() : 0) + mutex_b + mutex_c;
            sample.mutex_c_ns = mutex_c;
            sample.ae_rt_ns = rt_ns;
            sample.lock_b_held_ns = ae_lock_b_held;
            sample.slot_map_ns = ae_sub_slot_map_ns;
            sample.mark_slots_ns = ae_sub_mark_slots_ns;
            sample.pba_lookup_ns = ae_sub_pba_lookup_ns;
            sample.meta_build_ns = ae_sub_meta_build_ns;
            sample.handle_ae_lock_wait_ns = rsp.handle_ae_lock_wait_ns;
            sample.handle_ae_pre_ns = rsp.handle_ae_pre_ns;
            sample.handle_ae_lock_wait2_ns = rsp.handle_ae_lock_wait2_ns;
            sample.handle_ae_persist_ns = rsp.handle_ae_persist_ns;
            sample.handle_ae_post_ns = rsp.handle_ae_post_ns;

            /* 폴백용 "최근 값". prof.enabled로 게이팅하지 않는다 --
             * apply_timed의 항등식 7항은 -profile 없이도 동작해야 한다. */
            prof.sample_ae_rt_ns.store(sample.ae_rt_ns);
            prof.sample_r2_ns.store(sample.r2_ns);
            prof.sample_write_pba_rt_ns.store(sample.write_pba_rt_ns);
            prof.sample_storage_copy_ns.store(sample.storage_copy_ns);
            prof.sample_mutex_ns.store(sample.mutex_ns);
            prof.sample_mutex_c_ns.store(sample.mutex_c_ns);

            if (sink != nullptr) {
                sink->push(sample);
            }
        }

        /* 성공/실패 처리는 각각 별도 메서드로 뽑아 뒀다 -- 둘 다 순수하게
         * cluster[fi]와 log만 만지는 산술이라 단위 테스트가 붙는다
         * (tests/test_ae_bookkeeping.cpp). 호출 시 mu를 보유해야 한다. */
        if (rsp.success) {
            apply_ae_success(fi, next, len_entries, has_entries);
        } else {
            if (!apply_ae_failure_backoff(fi, req.prev_log_index, rsp.conflict_term,
                                          rsp.conflict_index, sent_next)) {
                /* stale failure -- 다른 워커가 이미 next_index를 전진시켰다 */
                mu.unlock();
                return;
            }
        }

        /* Lock C 보유 시간을 재놓고 버린다. ReplSample::lock_c_held_ns와
         * ProfilingSink::… 둘 다 기록하는 코드가 없어서, ApplyTimings의
         * post_rpc_ns가 항상 0으로 남고 wg_scheduling_ns 역산이 부정확하다
         * -- DECISIONS.md U4 (계측 배선 미완, 리팩토링 범위 밖). */
        mu.unlock();
    }
}

/* ============================================================
 * apply_ae_success / apply_ae_failure_backoff
 *
 * append_entries_worker가 RPC 응답을 받은 뒤 Lock C 안에서 하는
 * 기록 갱신. 원본 raft.go에서는 appendEntries 고루틴 본문에 인라인으로
 * 들어 있으나, 여기서는 (1) 워커 함수가 417줄이었고 (2) 이 두 블록이
 * 파일 I/O도 네트워크도 만지지 않는 순수 산술이라 단위 테스트를 붙일
 * 수 있는 유일한 지점이어서 분리했다. 로직은 한 줄도 바꾸지 않았다.
 *
 * 둘 다 **호출자가 mu를 보유한 상태**로 불러야 한다.
 * ============================================================ */

/* AppendEntries가 성공했을 때 next_index/match_index를 전진시키고,
 * 팔로워에게 아직 보낼 엔트리가 남았으면 다음 하트비트를 즉시 만료시킨다.
 *   fi           : cluster 인덱스
 *   next         : 이 배치의 첫 엔트리 인덱스 (전송 시점의 next_index)
 *   len_entries  : 이 배치에 실제로 실어 보낸 엔트리 수
 *   has_entries  : 데이터가 실린 AE였는가 (순수 하트비트면 false) */
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

/* AppendEntries가 거절됐을 때 next_index를 되감는다 (fast log backoff).
 *   prev_log_index : 방금 보낸 요청의 prev_log_index
 *   conflict_term  : 팔로워가 준 힌트 (0 = 힌트 없음)
 *   conflict_index : 팔로워가 준 힌트
 *   sent_next      : 요청을 보낼 때의 next_index (stale 응답 판별용)
 *
 * 반환: false면 **stale failure**다 -- 이 요청을 보낸 뒤 다른 워커가 이미
 * next_index를 전진시켰으므로 되감기를 적용하면 그 진전을 되돌린다.
 * 호출자는 아무것도 하지 않고 빠져나가야 한다. */
bool Server::apply_ae_failure_backoff(int fi, uint64_t prev_log_index,
                                       uint64_t conflict_term, uint64_t conflict_index,
                                       uint64_t sent_next) {
        /* "실패 시 -> follower 로그가 안 맞으니 되감기 (거의 호출되지 않음)" */

        /* "Stale failure guard: if nextIndex has moved since we sent
         *  this request, a concurrent goroutine has already updated
         *  it... Applying this stale failure would undo that progress." */
        if (raft.cluster[static_cast<size_t>(fi)].next_index != sent_next) {
            return false;
        }

        uint64_t prev = raft.cluster[static_cast<size_t>(fi)].next_index;
        uint64_t new_next = prev;

        if (conflict_term == 0 && conflict_index == 0) {
            /* "No hint (rejection from non-follower or stale term)." */
            new_next = raft.cluster[static_cast<size_t>(fi)].next_index - 1;
        } else if (conflict_term == 0) {
            /* "Follower's log is shorter than PrevLogIndex." */
            new_next = conflict_index;
        } else {
            /* "Search leader's log for ConflictTerm. If found, set
             *  nextIndex just past the last entry of that term.
             *  Otherwise jump to follower's conflictIndex." */
            new_next = conflict_index;
            uint64_t oldest3 = oldest_log_index();
            for (uint64_t j = prev_log_index; j >= 1 && j >= oldest3; j--) {
                if (raft.log[log_slice(j)].term == conflict_term) {
                    new_next = j + 1;
                    break;
                }
                if (raft.log[log_slice(j)].term < conflict_term) {
                    break;
                }
                if (j == oldest3) {
                    break;   /* uint64 underflow 방지: j-- 전에 탈출 */
                }
            }
        }

        /* "Ensure we always make progress."
         * [수정-12] prev == 0에서 prev - 1의 uint64 언더플로를 막는다
         * -- DECISIONS.md D12 */
        if (new_next >= prev) {
            new_next = (prev > 0) ? prev - 1 : 0;
        }
        /* "Never back off past matchIndex+1: the follower already
         *  confirmed entries up to matchIndex, and those PBA slots
         *  may have been freed by tier 1." */
        uint64_t floor = raft.cluster[static_cast<size_t>(fi)].match_index + 1;
        raft.cluster[static_cast<size_t>(fi)].next_index =
            std::max(std::max(new_next, uint64_t{1}), floor);

    return true;
}

} /* namespace nvmeof_raft */
