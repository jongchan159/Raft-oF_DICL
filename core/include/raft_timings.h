#ifndef RAFT_TIMINGS_HPP
#define RAFT_TIMINGS_HPP

#include <cstdint>
#include <atomic>
#include <mutex>
#include <vector>
#include <utility>


namespace nvmeof_raft {

/* ============================================================
 * ProfilingSink -- Raft 로직과 무관한 계측 상태를 한 곳에 모은다.
 *
 * 리팩토링 전에는 이 필드들이 Server의 87개 public 멤버 안에 흩어져
 * 있었고, 그중 프로파일링 atomic만 26개로 전체의 30%였다. 전수 조사
 * 결과 실제로 살아 있는 것은 advance_commit_index 서브스테이지 7개뿐이고
 * 12개는 **선언만 있고 참조가 0건**이어서 삭제했다.
 *
 * 그 뒤 2차 정리(2026-09-02)에서 반쪽이던 것들을 처리했다:
 *   - sample_* 6개는 load만 있고 store가 0건이었다 -> **배선 완료.**
 *     append_entries_worker가 data-bearing AE마다 채운다. store를
 *     sink 가드 **밖**에 두는 것이 핵심이다 -- 폴백이 노리는 상황이
 *     바로 하트비트(sink == nullptr)이기 때문이다.
 *   - last_persist_sub(PersistSubTimings 4개)는 write-only였다 ->
 *     **삭제.** persist_circular에서 타이머 6쌍이 함께 없어졌고,
 *     nvme_ns(= LPersist)를 만드는 3쌍만 남았다.
 *   - ae_lock_a_held_ns는 여전히 store만 있고 load가 없다 (U4에 남음).
 *
 * 이 헤더에 남은 것을 남긴 기준: **3개 프로세스에 걸친 지연을 국소화할
 * 수 있는 최소 집합.** 항등식 7항 + total + mutex + AE 카운터 2개.
 * ============================================================ */
struct ProfilingSink {
    /* 서브스테이지 계측 on/off. raft_node의 -profile 플래그가 세운다.
     * atomic인 이유는 메인 루프/AE 워커/apply 워커가 락 밖에서 읽기 때문.
     * (예전 이름: Server::sub_stage_profiling) */
    std::atomic<int32_t> enabled{0};

    bool on() const { return enabled.load() != 0; }

    /* --- AE 배치 통계 (리더 측). raft_client의 -op ae-stats가 읽는다 --- */
    std::atomic<uint64_t> ae_count{0};    /* 데이터가 실린 AE를 보낸 횟수 */
    std::atomic<uint64_t> ae_entries{0};  /* 그 AE들에 실린 엔트리 수의 합 */

    /* --- advance_commit_index 서브스테이지 (여기가 실제로 살아 있는 부분) ---
     * apply_timed가 CommitWait를 분해할 때 읽는다. */
    std::atomic<int64_t> aci_lock_wait_ns{0};   /* 메인 루프 -> mu 획득 */
    std::atomic<int64_t> aci_quorum_ns{0};      /* 쿼럼 정렬 + commit_index 전진 (slot GC 제외) */
    std::atomic<int64_t> aci_slot_gc_ns{0};     /* logSlotMap 스캔 + markSlots */
    std::atomic<int64_t> aci_apply_loop_ns{0};  /* 상태머신 apply 루프 */
    std::atomic<int64_t> aci_sort_ns{0};        /* matches 할당 + sort + majorityIdx */
    std::atomic<int64_t> aci_backpres_ns{0};    /* ring_not_full 브로드캐스트 */
    std::atomic<int64_t> aci_signal_ns{0};      /* committed 신호 루프 */

    /* --- ReplSink가 샘플을 못 잡았을 때의 폴백 (현재 store가 없다: U4) --- */
    std::atomic<int64_t> sample_ae_rt_ns{0};
    std::atomic<int64_t> sample_r2_ns{0};
    std::atomic<int64_t> sample_write_pba_rt_ns{0};
    std::atomic<int64_t> sample_storage_copy_ns{0};
    std::atomic<int64_t> sample_mutex_ns{0};
    std::atomic<int64_t> sample_mutex_c_ns{0};

    /* --- apply_timed의 LHandler 구간 (현재 load가 없다: U4) --- */
    std::atomic<int64_t> ae_lock_a_held_ns{0};
};

/* ============================================================
 * ApplyTimings (raft.go 원본, 필드/주석 그대로 포팅)
 *
 * Identity (rawft):
 *   Total ~= LHandler + LPersist + AENet + FHandler + ReplNet + StorageIO + QuorumWait
 * Identity (goraft baseline):
 *   Total ~= LHandler + LPersist + AENet + FHandler + StorageIO + QuorumWait   (ReplNet = 0)
 *
 * 이 구조체가 destination-side와 leader-side 비교 실험의 핵심 계측
 * 인프라이므로 원본 필드를 하나도 빠뜨리지 않고 그대로 가져온다.
 * time.Duration(int64 나노초 래퍼) -> int64_t 나노초로 대응.
 * ============================================================ */
struct ApplyTimings {
    /* Total: ApplyTimed 호출 전체의 wall time. 위 항등식의 좌변("Total")에
     * 해당하는데 원본 필드 목록을 옮길 때 빠져 있었다 (proto의
     * ClientApplyTimedResponse에는 total_nanos가 있어서 채울 값이 필요하다). */
    int64_t total_ns = 0;

    /* Leader-local handler vs persist split. */
    int64_t l_handler_ns = 0;   /* leader in-memory ring/log bookkeeping (incl. encode) */
    int64_t l_persist_ns = 0;   /* leader O_DIRECT WriteAtFile + Fdatasync */

    /* AENet = AE_RT - R2, clamp >= 0. Strict L<->F encode+transit+decode. */
    int64_t ae_net_ns = 0;

    /* FHandler = R2 - F_storage_wall, clamp >= 0. Follower handler
     * bookkeeping outside the storage block I/O (lock+checks+log
     * truncate/append+header persist).
     *   rawft: F_storage_wall = WritePBA_RT
     *   goraft: F_storage_wall = follower s.persist(...) wall */
    int64_t f_handler_ns = 0;

    /* ReplNet = WritePBA_RT - StorageIO, clamp >= 0. F<->S RPC
     * codec+transit+decode. Always 0 for goraft (no F->S RPC; follower
     * writes locally inside its own handler). */
    int64_t repl_net_ns = 0;

    /* StorageIO = the actual storage-block I/O wall.
     *   rawft: storage-server pread + pwrite
     *   goraft: follower's s.persist NVMe write+fdatasync wall */
    int64_t storage_io_ns = 0;

    /* QuorumWait = replicate - AE_RT, clamp >= 0. Leader post-AE
     * bookkeeping + apply loop + wg.Wait + commit advance. Equals
     * PostRPC + CommitWait + WgScheduling (within clock granularity). */
    int64_t quorum_wait_ns = 0;

    /* QuorumWait sub-stages:
     * PostRPC      = AE goroutine post-rpcCall critical section
     *                (Lock C wait + matchIndex/commit update + heartbeat reschedule).
     * CommitWait   = AE goroutine end -> last entry's result chan
     *                receive in ApplyTimed (next heartbeat tick latency
     *                + advanceCommitIndex apply loop processing).
     * WgScheduling = last receive -> wg.Wait return (goroutine wakeup). */
    int64_t post_rpc_ns = 0;
    int64_t commit_wait_ns = 0;
    int64_t wg_scheduling_ns = 0;

    /* Mutex (orthogonal): sum of s.mu.Lock waits on the critical path
     * (parent appendEntries lock A + quorum-completing goroutine's
     * pre-RPC build lock B + post-RPC bookkeeping lock C). */
    int64_t mutex_ns = 0;
    int64_t mutex_c_ns = 0;   /* Lock C wait alone (sub-component of PostRPC) */

    /* advanceCommitIndex sub-stages (CommitWait breakdown):
     * AciLockWait = main loop -> advanceCommitIndex mu.Lock() acquired
     * AciQuorum   = quorum sort + commitIndex advance (excl. slot GC)
     * AciSlotGc   = logSlotMap full scan + markSlots (nvmeof_raft only)
     * AciApplyLoop = apply loop (sm.Apply x N entries + chan sends) */
    int64_t aci_lock_wait_ns = 0;
    int64_t aci_quorum_ns = 0;
    int64_t aci_slot_gc_ns = 0;
    int64_t aci_apply_loop_ns = 0;

    /* aci_quorum sub-stage breakdown */
    int64_t aci_sort_ns = 0;       /* matches alloc + sort + majorityIdx */
    int64_t aci_backpres_ns = 0;   /* ringNotFull.Broadcast (separated from slotGC) */
    int64_t aci_signal_ns = 0;     /* committed channel signals loop */

    /* PureCommitWait = tLastCommitted - tAEDone, clamp >= 0. */
    int64_t pure_commit_wait_ns = 0;

    int64_t wait_lock_b_ns = 0;              /* Lock B wait alone (= Mutex - MutexC) */
    int64_t replicate_ns = 0;                /* t2-t1: total leader->quorum wall (base for corrected values) */
    int64_t replicate_corrected_ns = 0;      /* Replicate - Mutex (Lock B+C wait removed) */
    int64_t quorum_wait_corrected_ns = 0;    /* QuorumWait with Lock B+C wait removed (= ReplicateCorrected - aeRT) */
};

/* ============================================================
 * replSample (raft.go 원본)
 * "carries per-RPC measurement walls for one in-flight AE on this
 *  Apply's critical path. AENet/FHandler/ReplNet/Replication are
 *  derived from these on the leader side."
 * ============================================================ */
struct ReplSample {
    int64_t ae_rt_ns = 0;         /* leader rpcCall wall */
    int64_t r2_ns = 0;            /* follower handler wall (HandlerDuration) */
    int64_t write_pba_rt_ns = 0;  /* follower WritePBA wall (= R2 - FHandler) */
    int64_t storage_copy_ns = 0;  /* S internal pread+pwrite (Replication) */
    int64_t mutex_ns = 0;         /* sum of A+B+C lock waits */
    int64_t mutex_c_ns = 0;       /* Lock C wait alone (AE goroutine post-rpcCall) */
    int64_t post_rpc_wall_ns = 0; /* AE goroutine: rpcCall return -> Lock C critical section end */

    /* appendEntries Lock B/C sub-stages */
    int64_t lock_b_held_ns = 0;
    int64_t slot_map_ns = 0;      /* logSlotMap loops cumulative */
    int64_t mark_slots_ns = 0;
    int64_t pba_lookup_ns = 0;
    int64_t meta_build_ns = 0;
    int64_t lock_c_held_ns = 0;

    /* HandleAppendEntriesRequest follower sub-stages (from RPC response) */
    int64_t handle_ae_lock_wait_ns = 0;
    int64_t handle_ae_pre_ns = 0;
    int64_t handle_ae_lock_wait2_ns = 0;
    int64_t handle_ae_post_ns = 0;
    int64_t handle_ae_persist_ns = 0;
};

/* ============================================================
 * replSink (raft.go 원본)
 * "collects per-RPC samples from in-flight appendEntries goroutines for
 *  a single Apply call. Allocated per-Apply by ApplyTimed; non-timed
 *  Apply passes nil and goroutines skip the push. mutexA is set by the
 *  parent appendEntries(sink) before goroutines are spawned and is
 *  shared across all goroutines that push from this sink."
 * ============================================================ */
class ReplSink {
public:
    void set_mutex_a(int64_t d) { mutex_a_ns_ = d; }

    void push(const ReplSample &s);

    /* firstSample: "returns the first sample (the quorum-completing
     * follower's timings in a 3-node cluster with self-ACK) and
     * whether one was recorded." */
    std::pair<ReplSample, bool> first_sample();

    int64_t mutex_a_ns() const { return mutex_a_ns_; }

private:
    std::mutex mu_;
    std::vector<ReplSample> samples_;
    int64_t mutex_a_ns_ = 0;
};

/* clamp0: 음수를 0으로. 파생값이 클럭 해상도 때문에 음수가 나올 수 있어
 * 항등식 항들은 전부 이걸 통과한다 (원본 raft.go도 동일). */
inline int64_t timings_clamp0(int64_t v) { return v < 0 ? 0 : v; }

/* ============================================================
 * ApplyTimed가 측정한 wall 시간들. derive_apply_timings의 입력.
 * ============================================================ */
struct ApplyWalls {
    int64_t nvme_ns = 0;         /* persist_circular가 O_DIRECT write+fdatasync에 쓴 시간 */
    int64_t a_held_ns = 0;       /* Lock A 보유 시간 (인메모리 부기 + persist 포함) */
    int64_t replicate_ns = 0;    /* 리더 -> 쿼럼 전체 wall */
    int64_t mutex_a_ns = 0;      /* Lock A 대기 시간 */
    int64_t commit_wait_ns = 0;  /* AE 완료 -> 마지막 엔트리 커밋 신호 */
};

/* ============================================================
 * derive_apply_timings: 측정한 wall 시간과 ReplSample로부터
 * ApplyTimings의 모든 파생값을 계산한다.
 *
 * 항등식은 raft_timings.h 주석 그대로:
 *   Total ~= LHandler + LPersist + AENet + FHandler + ReplNet
 *            + StorageIO + QuorumWait
 *
 * 원본 raft.go에서는 ApplyTimed 본문에 인라인으로 들어 있었고 이 포팅도
 * 그랬다. apply_internal이 242줄이었던 이유의 일부가 이 40줄이며, 파일
 * I/O도 락도 만지지 않는 순수 산술이라 로직에서 떼어냈다.
 * (total_ns는 호출자가 apply_timed에서 채운다.)
 * ============================================================ */
void derive_apply_timings(const ApplyWalls &w, ReplSink *sink, const ProfilingSink &prof, ApplyTimings &t);

} /* namespace nvmeof_raft */

#endif /* RAFT_TIMINGS_HPP */