#ifndef RAFT_SERVER_HPP
#define RAFT_SERVER_HPP

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <functional>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <array>
#include <cstring>
#include <cstdlib>
#include <stdexcept>

#include "raft_constants.h"   /* SECTOR_SIZE (AlignedBuffer 기본 정렬) */
#include "raft_entry.h"
#include "raft_state.h"
#include "raft_timings.h"
#include "raft_transport.h"

namespace nvmeof_raft {

/* ---- ring buffer 자유 함수 (raft_ring_helpers.cpp에서 정의) ---- */
uint64_t slots_for_entry(size_t cmd_len);
uint64_t entry_slots(const Entry &e);
int64_t slot_offset(uint64_t slot);

/* ============================================================
 * StateMachine (raft.go 원본 인터페이스)
 * ============================================================ */
class StateMachine {
public:
    virtual ~StateMachine() = default;
    virtual ApplyResult apply(const std::vector<uint8_t> &cmd) = 0;
};

/* 전송 계층은 core/raft_transport.h의 두 인터페이스로만 접근한다.
 * 구현체(TCP / 추후 RDMA / 테스트용 fake)는 net/ 과 tests/ 에 있다. */

/* 캐시된 fd + extent 맵은 blockio/cached_fd.h의 CachedFD가 구현한다 */

/* ============================================================
 * Server -- Raft 노드 하나.
 *
 * 상태는 다섯 그룹으로 묶여 있다 (전부 위에 정의):
 *   raft     RaftState     합의 상태 (term, log, cluster, commit_index, state...)
 *   ring     RingLog       링버퍼 부기 + 링 크기 설정
 *   io       StorageIo     링 파일 / 디바이스 I/O 상태
 *   prof     ProfilingSink 계측 (Raft 로직과 무관)
 *   workers  WorkerPool    상시 스레드 3개 + 복제 스레드 수명 관리
 * 그 밖의 최상위 필드는 락(mu), 주입받는 것(statemachine, transport,
 * blockcopy), 그리고 런타임 튜닝 플래그다.
 *
 * **락 규약**: raft / ring 그룹과 statemachine 호출은 전부 mu 하나로
 * 보호된다 (원본 raft.go의 s.mu 그대로 -- 락을 쪼개지 않았다).
 * mu 안에서 잡아도 되는 것: workers.apply_notify_mu,
 * workers.slot_gc_notify_mu, Entry::committed->mu, statemachine의 내부 락,
 * ApplyCollector::mu.
 * mu **밖에서만** 잡아야 하는 것: workers.inflight_mu, 그리고 전송
 * 구현체 내부의 락 (do_pba_copy / rpc는 mu를 놓은 상태에서 불린다).
 *
 * Go 대응: sync.Mutex -> std::mutex, sync.Cond -> std::condition_variable,
 * sync.WaitGroup -> WorkerPool::inflight (완료 플래그 + join).
 * ============================================================ */
class Server {
public:
    /* 소멸자: join 안 된 std::thread가 남은 채로 Server가 파괴되면
     * std::thread의 소멸자가 std::terminate()를 호출한다 (실제로
     * 테스트 중 이 문제가 재현됨 -- join_all_replication_threads()를
     * 명시적으로 안 부르고 종료하면 abort가 남). 소멸자에서 자동으로
     * join하도록 만들어, 이 실수를 원천적으로 방지한다. */
    ~Server();

    /* --- 계측 상태는 전부 prof에 모여 있다 (core/include/raft_timings.h).
     * 예전에는 프로파일링 atomic 26개가 이 클래스의 다른 필드들과 섞여
     * 있었고 그중 12개는 참조가 0건이었다. --- */
    ProfilingSink prof;

    /* 워커 스레드와 그 신호 (core/include/raft_server.h 위 WorkerPool 참고) */
    WorkerPool workers;

    /* 링 파일 / 디바이스 I/O 상태 (위 StorageIo 참고) */
    StorageIo io;

    /* 링버퍼 부기 상태 (위 RingLog 참고). 전부 mu로 보호된다. */
    RingLog ring;

    /* done: 원본은 plain bool이었으나, 메인 루프/apply 워커/slot GC 워커가
     * s.mu 밖에서 이 값을 읽으므로 atomic으로 바꿨다 (값 의미는 동일) */
    std::atomic<bool> done{false};
    bool debug_enabled = false;

    /* main_loop 한 바퀴마다 쉬는 시간. 원본(goraft ulti loop)은 sleep 없이
     * 스핀하는데, 그러면 advance_commit_index가 매 바퀴 s.mu를 잡아
     * AE 스레드를 굶긴다. 0으로 두면 원본과 동일한 스핀 동작. */
    int loop_sleep_us = 200;

    /* appendEntries 한 라운드의 상한. 원본 raft.go의 package-level var
     * 대응이며, 벤치마크가 재컴파일 없이 조절해야 해서 상수가 아니다.
     * 기본값(사실상 무제한)을 그대로 두면 팔로워 하나가 죽었을 때 리더가
     * 매 라운드 밀린 백로그 전체를 재전송한다 -- 레이턴시를 측정할 때는
     * -ae-batch로 상한을 주는 편이 안전하다 (HANDOFF §6.2). */
    uint64_t max_ae_batch = DEFAULT_MAX_AE_BATCH;
    uint64_t max_ae_batch_bytes = DEFAULT_MAX_AE_BATCH_BYTES;


    /* --- 락 --- 원본은 s.mu 하나로 아래 raft/ring 상태 전부를 보호한다.
     * 이 포팅도 같다 (락을 쪼개지 않았다). 락 순서는 파일 상단 참고. */
    std::mutex mu;

    /* --- Raft 합의 상태 (위 RaftState 참고). 전부 mu로 보호된다 --- */
    RaftState raft;

    /* 상태머신은 주입받는다 (net/의 HashStateMachine 등). */
    std::shared_ptr<StateMachine> statemachine;

    /* --- Replication policy (Raft-oF 논문 3.1.2, DARE 비교용) ---
     * DestinationSide (기본, 논문 채택안): follower의 storage node가
     *   leader 로그를 읽어와 자기 볼륨에 쓴다 -- handle_append_entries_
     *   request 안에서 do_pba_copy 호출 (팔로워의 blockcopy 클라이언트 사용).
     * LeaderSide (DARE와 동일한 정책, 비교용): leader의 storage node가
     *   자기 로그를 읽어 follower 볼륨에 직접 쓴다 -- AppendEntries RPC를
     *   보내기 전에 append_entries_worker 안에서 do_pba_copy 호출
     *   (leader의 blockcopy 클라이언트 사용, dst_dev=fi). 이 경우 각 storage node는
     *   자기뿐 아니라 클러스터의 모든 멤버 볼륨을 NVMe-oF로 열어둔
     *   상태를 가정한다 (DARE의 모든 서버 쌍이 RC log QP로 연결되어
     *   서로의 로그를 MR로 노출해두는 것과 동일한 토폴로지 가정,
     *   hpdc15dare §3.1.2 "any pair of servers is connected by ... a log
     *   QP that grants remote access to the local log"). */
    enum class ReplicationMode { DestinationSide, LeaderSide };
    ReplicationMode replication_mode = ReplicationMode::DestinationSide;

    /* --- NVMe-oF Setting --- */
    /* --- 전송 (core/include/raft_transport.h) ---
     * 둘 다 **주입받는다.** null이면 그 경로가 실패로 취급된다:
     *   transport == null  -> append_entries / request_vote가 false를 반환한 것과 같다
     *   blockcopy == null  -> do_pba_copy가 예외를 던진다 (예전에 storage_host가
     *                         비었을 때와 동일)
     * selftest처럼 네트워크를 전혀 쓰지 않는 하네스는 둘 다 null로 둔다.
     *
     * 예전에는 이 자리에 net/ 에서 정의되는 blockcopy 핸들의 shared_ptr과
     * 그 전용 뮤텍스, 그리고 그 두 접근자가
     * 있었다. lazy connect와 그 직렬화는 이제 구현체 내부의 책임이다. */
    std::shared_ptr<RaftTransport> transport;
    std::shared_ptr<BlockCopyClient> blockcopy;




    /* --- Condition variable: Apply()가 ring 꽉 찼을 때 여기서 block ---
     * Go의 *sync.Cond -> condition_variable + 별도 mutex 필요 없이
     * s.mu를 그대로 재사용 (std::condition_variable은 unique_lock과 씀) */
    std::condition_variable ring_not_full;

    /* apply 워커와 slot GC 워커의 깨우기 신호는 workers(WorkerPool)에 있다.
     * advance_commit_index가 commit_index를 전진시킨 뒤 non-blocking으로
     * 신호하고, 각 워커가 별도 스레드에서 pending을 drain한다. */

    /* ---- ring buffer helpers (raft_ring_helpers.cpp에서 정의) ---- */
    uint64_t oldest_log_index() const;
    uint64_t log_slice(uint64_t log_idx) const;

    /* slots_for_log_index: 로그 인덱스 하나가 링에서 차지하는 슬롯 수.
     * log_slot_map에 기록이 있으면 그 값(리더가 실제로 배치한 슬롯 수)을
     * 쓰고, 없으면 in-memory 엔트리에서 계산한다. append_entries_worker의
     * 배치 clamp 루프 세 개가 이 조회를 각자 인라인으로 네 번 반복하고
     * 있었다. **호출자가 mu를 보유해야 한다.** */
    uint64_t slots_for_log_index(uint64_t log_idx) const;
    bool can_write_all(const std::vector<std::vector<uint8_t>> &commands) const;
    void recompute_head_slot();
    void init_slot_states();
    void rebuild_slot_states_from_map();
    void trace_slot_map(char op, uint64_t idx);
    char find_slot_map_trace(uint64_t idx) const;

    /* logSkipPBADiag (raft.go 원본) -- log_slot_map[next]가 없어서
     * PBA 복제를 못 하고 하트비트만 보내게 될 때의 구조화된 스냅샷.
     * "Caller must hold s.mu."  (raft_append_entries.cpp에서 정의)
     * 지금까지 find_slot_map_trace와 함께 호출부가 없어서, 팔로워가
     * 영구히 못 따라잡는 상태가 로그에 전혀 안 남았다. */
    void log_skip_pba_diag(int fi, uint64_t next, uint64_t last,
                            uint64_t prev_log_index);


    /* persistCircular (raft_persist.cpp에서 정의)
     * 반환값: nvmeNs -- O_DIRECT WriteAtFile + Fdatasync에 실제로 쓴
     * wall time (실제 NVMe-oF 트래픽). 호출자는 이걸로 in-memory
     * bookkeeping(LeaderMem)과 write+sync(LeaderPersist)를 분리 가능 */
    int64_t persist_circular(bool write_log, int n_new_entries);

    /* advanceCommitIndex, applyPending, doSlotGC (raft_commit.cpp에서 정의) */
    void advance_commit_index();
    void apply_pending();
    void do_slot_gc();

    /* trim_log_locked: log 벡터 앞부분에서 이미 GC된(=모든 팔로워가 받고
     * 로컬에서 apply까지 끝난) 엔트리를 잘라낸다. s.mu를 잡고 호출해야
     * 하며, do_slot_gc가 슬롯을 해제한 직후에 같은 기준으로 호출된다.
     * log_trim_threshold == 0이면 no-op (원본 동작). */
    void trim_log_locked(uint64_t min_match);

    /* 이 노드가 리더인가. **스스로 mu를 잡는다** -- 락을 이미 들고 있는
     * 곳에서 부르면 데드락이다. 호출부는 net/의 클라이언트 RPC 핸들러들.
     * (raft_ring_helpers.cpp) */
    bool is_leader();
    /* election timeout을 heartbeat_ms * [20, 30) 구간의 난수로 다시 잡는다.
     * 호출자가 mu를 보유해야 한다. (raft_election.cpp) */
    void reset_election_timeout();

    /* ============================================================
     * 클라이언트 진입점 (raft.go의 Apply/ApplyTimed 대응,
     * raft_apply.cpp에서 정의)
     *
     * Apply: commands를 리더 로그에 append -> persist_circular(write_log)
     * -> append_entries로 복제 -> 마지막 엔트리의 committed 신호를 대기
     * -> 상태머신 apply 결과 반환. 리더가 아니거나 링이 꽉 차면 error/busy.
     * ApplyTimed: 같은 일을 하면서 raft_timings.h의 ApplyTimings 항등식대로
     * 구간을 분해해 채운다 (destination-side vs leader-side 비교 실험용).
     * ============================================================ */
    ApplyResult apply(const std::vector<std::vector<uint8_t>> &commands,
                       bool *out_busy = nullptr);
    ApplyResult apply_timed(const std::vector<std::vector<uint8_t>> &commands,
                             ApplyTimings *out_timings, bool *out_busy = nullptr);

    /* ============================================================
     * 생명주기 (raft_lifecycle.cpp에서 정의)
     *
     * init_storage: 메타데이터 파일 생성 + fallocate + extent 캐시 빌드 +
     *   CachedFD open + 헤더 복구(있으면) + sentinel 로그 초기화.
     *   start() 전에 정확히 한 번 호출.
     * start: 메인 루프 / apply 워커 / slot GC 워커 스레드를 띄운다.
     * stop: done을 세우고 모든 CV를 깨워 스레드를 정리 (idempotent).
     * ============================================================ */
    void init_storage();
    void start();
    void stop();

    void main_loop();          /* timeout -> become_leader -> heartbeat -> advance_commit_index */
    void apply_worker_loop();  /* workers.apply_notify_cv 대기 -> apply_pending */
    void slot_gc_worker_loop();/* workers.slot_gc_notify_cv 대기 -> do_slot_gc */

    /* append_entries: 팔로워마다 스레드를 하나 띄워 append_entries_worker를
     * 실행하고 **결과를 기다리지 않고 즉시 리턴한다** (원본 Go의
     * fire-and-forget goroutine과 동일).
     * sink: 계측용 ReplSink. nullptr이면 측정을 건너뛴다 (non-timed Apply와
     *   하트비트가 그 경우다). (raft_append_entries.cpp) */
    /* [수정-7] sink는 shared_ptr로 받는다. AE 워커가 호출자보다 오래 살기
     * 때문에 스택 sink는 use-after-return이 된다 -- DECISIONS.md D7 */
    void append_entries(std::shared_ptr<ReplSink> sink);

    /* append_entries의 팔로워 1명분 처리 로직. 병렬화를 위해 별도
     * 함수로 분리 -- append_entries가 팔로워마다 스레드를 띄워 이걸 호출 */
    void append_entries_worker(int fi, ReplSink *sink);

    /* append_entries_worker가 RPC 응답을 받은 뒤 Lock C 안에서 수행하는
     * 기록 갱신. 파일 I/O도 네트워크도 만지지 않는 순수 산술이라 단위
     * 테스트가 붙는다 (tests/test_ae_bookkeeping.cpp).
     * 둘 다 **호출자가 mu를 보유**해야 한다. (raft_append_entries.cpp) */
    void apply_ae_success(int fi, uint64_t next, uint64_t len_entries, bool has_entries);
    /* 반환 false = stale failure (다른 워커가 이미 next_index를 전진시켰다).
     * 호출자는 되감기를 적용하지 않고 빠져나가야 한다. */
    bool apply_ae_failure_backoff(int fi, uint64_t prev_log_index,
                                  uint64_t conflict_term, uint64_t conflict_index,
                                  uint64_t sent_next);

    /* leaderPBAForRange, doPBACopy (raft_pba.cpp에서 정의) */
    struct PbaRangeResult { uint64_t pba_src; uint64_t nbytes; };
    PbaRangeResult leader_pba_for_range(uint64_t start_slot, uint64_t total_slots);

    /* HandleAppendEntriesRequest, doPBACopy (raft_handle_append_entries.cpp) */
    struct DoPbaCopyResult { int64_t write_pba_rt_ns; int64_t storage_copy_ns; };
    DoPbaCopyResult do_pba_copy(uint64_t leader_pba_src, uint64_t log_block_length,
                                uint64_t dst_slot, int src_dev, int dst_dev);
    void handle_append_entries_request(const AppendEntriesRequest &req,
                                        AppendEntriesResponse &rsp);

    /* update_term (raft.go의 updateTerm): msg_term이 우리 term보다 크면
     * 팔로워로 강등하고 term/투표를 갱신한 뒤 헤더를 persist한다.
     * 반환: 강등이 일어났으면 true.
     * 호출자가 mu를 보유해야 한다. (raft_election.cpp) */
    bool update_term(uint64_t msg_term);

    /* 이 서버 자신의 투표 상태를 읽고 쓴다. 저장 위치는
     * raft.cluster[raft.cluster_index].voted_for 이며 **그 한 자리뿐이다**.
     * [수정-2] 별도의 Server::voted_for 필드를 두지 말 것 -- 투표 상태가
     * 두 곳으로 갈려 같은 term에 리더가 둘 생긴다 -- DECISIONS.md D2 */
    uint64_t get_voted_for() const {
        return raft.cluster.at(static_cast<size_t>(raft.cluster_index)).voted_for;
    }
    void set_voted_for(uint64_t v) {
        raft.cluster.at(static_cast<size_t>(raft.cluster_index)).voted_for = v;
    }

    /* Election / Leadership (raft_election.cpp에서 정의) */
    void timeout();
    void become_leader();
    void request_vote();
    void request_vote_worker(int peer_index);
    void handle_request_vote_request(const RequestVoteRequest &req, RequestVoteResponse &rsp);
    void heartbeat();

    /* readEntryDirect (raft_pba.cpp에서 정의, becomeLeader가 deferred
     * entry 로드에 사용) */
    struct ReadEntryResult { Entry entry; uint64_t next_slot; };
    ReadEntryResult read_entry_direct(uint64_t header_slot);

    /* ============================================================
     * 병렬화 지원 (append_entries, request_vote가 팔로워마다 std::thread를
     * 띄움). 원본 Go는 goroutine을 fire-and-forget으로 띄우고 결과를
     * 안 기다리는데, std::thread는 detach() 시 this를 계속 참조하는
     * 스레드가 서버 파괴 후에도 남을 수 있어 use-after-free 위험이
     * 있다. 그래서 join도 detach도 아닌 절충안을 쓴다:
     *   - 각 스레드에 "완료 여부" 플래그를 붙이고, 워커 실행이 끝나면
     *     스스로 그 플래그를 세팅한다 (std::thread 자체는 완료 여부를
     *     물어볼 방법이 없으므로 -- joinable()은 "아직 join 안 했나"만
     *     알려주지 "실행이 끝났나"는 알려주지 않는다. 첫 구현에서
     *     이 둘을 혼동해서 reap가 사실상 아무것도 안 지우는 버그가 있었음)
     *   - 새 스레드를 띄우기 전, 완료 플래그가 선 것들만 join 후 제거
     *   - shutdown()에서 마지막으로 남은 전부를 강제 join
     * ============================================================ */

    void reap_finished_threads();

    void spawn_replication_thread(std::function<void()> work);

    /* shutdown 시 반드시 호출: 모든 진행 중인 replication 스레드가
     * 서버 파괴 전에 끝나도록 보장 (use-after-free 방지) */
    void join_all_replication_threads();

    /* ============================================================
     * 상시 워커 스레드 (start()가 띄우고 stop()이 정리)
     *   workers.main_thread    : timeout/become_leader/heartbeat/advance_commit_index
     *   workers.apply_thread   : apply_notify_cv를 받아 apply_pending
     *   workers.slot_gc_thread : slot_gc_notify_cv를 받아 do_slot_gc
     * 원본 Go의 ulti 루프 + applyWorker + slotGC 고루틴에 각각 대응.
     * ============================================================ */
};

} /* namespace nvmeof_raft */

#endif /* RAFT_SERVER_HPP */