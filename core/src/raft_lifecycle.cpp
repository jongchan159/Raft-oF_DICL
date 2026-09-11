/* ============================================================
 * raft_lifecycle.cpp -- 서버 부트스트랩과 상시 스레드
 *
 * 지금까지 timeout()/become_leader()/heartbeat()/advance_commit_index()/
 * apply_pending()/do_slot_gc()는 모두 구현돼 있었지만 이들을 호출하는
 * 주체가 없었고, workers.apply_notify_cv / workers.slot_gc_notify_cv / ring_not_full은
 * notify만 되고 아무도 wait하지 않았다. 이 파일이 그 빈자리를 채운다.
 *
 * 원본(goraft 계열) 대응:
 *   main_loop()           <- Start()의 상시 goroutine (상태별 switch:
 *                            leader=heartbeat, follower=timeout,
 *                            candidate=timeout+becomeLeader)
 *   apply_worker_loop()   <- applyWorker 고루틴
 *   slot_gc_worker_loop() <- slotGC 고루틴
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_basics.h"
#include "cached_fd.h"   /* CachedFD 역참조 */

#include <chrono>
#include <cstring>
#include <cerrno>
#include <sys/stat.h>
#include <sys/types.h>

namespace nvmeof_raft {

namespace {

/* 상시 워커 스레드가 CV에서 깨어나 done 플래그를 확인하는 주기.
 * 신호를 놓쳐도 이 주기로 종료를 감지한다. */
constexpr int kApplyWorkerPollMs = 50;
constexpr int kSlotGcWorkerPollMs = 100;

/* 메타데이터 디렉터리 생성 모드 (링 파일 자체는 0644 -- raft_cached_fd.cpp) */
constexpr int kMetadataDirMode = 0755;
void ensure_dir(const std::string &path) {
    if (path.empty()) {
        return;
    }
    if (::mkdir(path.c_str(), kMetadataDirMode) != 0 && errno != EEXIST) {
        throw std::runtime_error("ensure_dir: mkdir " + path + ": " + strerror(errno));
    }
}
} /* anonymous namespace */

/* ============================================================
 * init_storage
 *
 * 1) 메타데이터(=링) 파일을 만들고 fallocate로 블록을 실제 할당
 * 2) CachedFD open (meta O_RDONLY / meta O_RDWR|O_DIRECT / device O_DIRECT x2)
 * 3) extent 캐시 빌드 (FIEMAP, 또는 테스트 모드의 identity 맵)
 * 4) 헤더(offset 0, 512B)를 읽어 term/votedFor 복구
 * 5) sentinel 로그와 슬롯 상태 초기화
 *
 * 로그 인덱스 기준: log[0]은 sentinel이고 실제 엔트리는 인덱스 1부터
 * 시작한다. 즉 fresh 상태에서 tail_log_index = 1이다. 이렇게 두는
 * 이유는 AppendEntries의 prev_log_index == 0이 "이전 엔트리 없음"을
 * 뜻해야 하기 때문이다 -- handle_append_entries_request가
 * `if (req.prev_log_index > 0)`으로 consistency check를 건너뛰고
 * `new_tail = req.prev_log_index + 1`로 tail을 계산하는 것이 그 전제다.
 * (tail_log_index를 0에서 시작하면 첫 엔트리가 인덱스 0이 되어
 *  "index 0 자체"와 "이전 엔트리 없음"이 구분되지 않고, 팔로워의
 *  tail_log_index가 1 커지는 off-by-one이 생긴다.)
 *
 * 원본 raft.go restoreCircular과 대조 확인 완료: 원본도
 * tailLogIndex=1 / tailSlot=0 / commitIndex=0 / lastApplied=1로
 * 초기화한다. 기존 코드 주석의 "0-based로 결정했다"는 서술은 이
 * 초기값과 맞지 않는 오기였다.
 * ============================================================ */
void Server::init_storage() {
    ensure_dir(io.metadata_dir);

    std::string meta_path = io.metadata_dir + "/raft-" + std::to_string(raft.id) + ".ring";

    uint64_t want = ring.file_size_bytes();
    /* ZERO_RANGE는 FIEMAP 경로에서만 필수 (identity_pba 테스트 모드는
     * FIEMAP을 쓰지 않으므로 미지원 파일시스템에서도 돌아가게 둔다) */
    int64_t actual = create_ring_file(meta_path, want, !io.identity_pba);
    if (static_cast<uint64_t>(actual) < want) {
        throw std::runtime_error("init_storage: ring file " + meta_path + " is " +
            std::to_string(actual) + " bytes, need " + std::to_string(want));
    }

    /* identity_pba 테스트 모드에서는 device_path가 링 파일 자신이다
     * (블록 디바이스 없이 PBA 복사 경로를 그대로 돌리기 위함). */
    std::string dev = io.device_path.empty() ? meta_path : io.device_path;

    io.cached_fd = std::make_shared<CachedFD>(CachedFD::open(meta_path, dev));

    if (io.identity_pba) {
        io.cached_fd->cache_identity_extents(static_cast<int64_t>(want));
    } else {
        io.cached_fd->cache_extents(static_cast<int64_t>(want));
    }

    /* 헤더는 논리 오프셋 0에 있다 (write_at_file이 논리 오프셋을 쓴다).
     * PBA는 replication 경로가 아니라 진단용으로만 캐시해둔다. */
    /* 헤더 PBA를 기동 시점에 한 번 조회한다. 값은 쓰지 않는다 -- 헤더 쓰기는
     * 핫패스에서 논리 오프셋 0으로 write_at_file을 부른다. 이 호출의 목적은
     * **검증**이다: 오프셋 0이 extent 맵에 없거나 FIEMAP이 실패하면
     * get_pba가 예외를 던지므로, 링 파일이 sparse한 채로 기동하는 것을
     * 여기서 막는다. (예전에는 결과를 Server::header_pba에 넣어 뒀는데
     * 그 필드를 읽는 코드가 하나도 없었다.) */
    (void)io.cached_fd->get_pba(0, HEADER_SIZE);

    /* ---- 로그/슬롯 초기화 ---- */
    raft.log.clear();
    raft.log.push_back(Entry{});   /* sentinel: term 0, command 비어있음 */
    init_slot_states();

    /* ---- 헤더 복구 ----
     * File header layout (512B), persist_circular와 동일:
     *   [ 0: 7] currentTerm   [ 8:15] votedFor
     *   [16:23] tailLogIndex  [24:31] tailSlot
     *   [32:39] commitIndex   [40:47] lastApplied */
    /* 헤더에서 복구하는 것은 term/votedFor **둘뿐이다.**
     * [수정-8] tail_log_index / tail_slot / commit_index / last_applied는
     * 복구하지 않는다 (원본 restoreCircular과 동일). 복구하면 빈 로그와
     * 어긋나 재시작 노드가 첫 AppendEntries에서 깨진다. 이 결정이 팔로워
     * 재시작 catch-up 불가의 원인 (a)다 -- DECISIONS.md D8, U5 */
    try {
        std::vector<uint8_t> hdr = io.cached_fd->read_at_file(HEADER_SIZE, 0);
        uint64_t h_term = get_u64_le(hdr.data() + file_hdr::kOffCurrentTerm);
        uint64_t h_vote = get_u64_le(hdr.data() + file_hdr::kOffVotedFor);

        raft.current_term = h_term;
        set_voted_for(h_vote);
        if (h_term != 0 || h_vote != 0) {
            ring.persisted_init = true;
            ring.persisted_term = h_term;
            ring.persisted_voted_for = h_vote;
        }
    } catch (const std::exception &) {
        /* 헤더 읽기 실패(새 파일 등) -> term/vote는 0에서 시작 */
        raft.current_term = 0;
        set_voted_for(0);
        ring.persisted_init = false;
    }

    /* 인덱스/슬롯은 항상 초기 상태 (원본과 동일) */
    ring.tail_log_index = 1;   /* 실제 엔트리는 인덱스 1부터 (위 주석 참고) */
    ring.tail_slot = 0;
    raft.commit_index = 0;
    raft.last_applied = 1;     /* "아직 아무것도 apply 안 함" = 다음에 apply할 인덱스 */

    ring.head_slot = ring.tail_slot;
    raft.state = ServerState::Follower;
    reset_election_timeout();
}

/* ============================================================
 * start / stop
 * ============================================================ */
void Server::start() {
    if (workers.started) {
        return;
    }
    workers.started = true;
    done.store(false);

    workers.main_thread = std::thread([this]() { main_loop(); });
    workers.apply_thread = std::thread([this]() { apply_worker_loop(); });
    workers.slot_gc_thread = std::thread([this]() { slot_gc_worker_loop(); });
}

void Server::stop() {
    done.store(true);

    /* 대기 중인 워커/클라이언트를 모두 깨운다 */
    {
        std::lock_guard<std::mutex> lk(workers.apply_notify_mu);
        workers.apply_notify_pending = true;
    }
    workers.apply_notify_cv.notify_all();
    {
        std::lock_guard<std::mutex> lk(workers.slot_gc_notify_mu);
        workers.slot_gc_notify_pending = true;
    }
    workers.slot_gc_notify_cv.notify_all();
    {
        std::lock_guard<std::mutex> lk(mu);
        ring_not_full.notify_all();
    }

    if (workers.main_thread.joinable()) {
        workers.main_thread.join();
    }
    if (workers.apply_thread.joinable()) {
        workers.apply_thread.join();
    }
    if (workers.slot_gc_thread.joinable()) {
        workers.slot_gc_thread.join();
    }

    /* replication 스레드는 this를 계속 참조하므로 반드시 마지막에 */
    join_all_replication_threads();
    workers.started = false;
}

/* ============================================================
 * main_loop (원본 raft.go Start()의 상시 goroutine 루프)
 *
 * 상태별로 하는 일이 다르다 (원본 raft.go Start()의 switch와 동일):
 *   leader    : heartbeat();  advance_commit_index()
 *   follower  : timeout();    advance_commit_index()
 *   candidate : timeout();    become_leader()
 * 즉 heartbeat는 리더만, become_leader는 candidate만 호출한다.
 *
 * [수정-1] 이 분기를 없애고 매 바퀴 전부 호출하면 팔로워도 하트비트를
 * 쏘게 되어 모든 노드의 election timer가 영구히 갱신되고 선거가 한 번도
 * 일어나지 않는다 -- DECISIONS.md D1
 *
 * 원본은 sleep 없이 스핀한다. 그대로 두면 advance_commit_index가 매
 * 바퀴 s.mu를 잡아 AE 스레드/Apply를 굶으므로 loop_sleep_us(기본
 * 200us)만큼 쉰다. 0으로 두면 원본과 동일한 스핀 동작.
 * ============================================================ */
void Server::main_loop() {
    while (!done.load()) {
        ServerState st;
        {
            std::lock_guard<std::mutex> lk(mu);
            st = raft.state;
        }

        switch (st) {
        case ServerState::Leader:
            heartbeat();
            advance_commit_index();
            break;
        case ServerState::Follower:
            timeout();
            advance_commit_index();
            break;
        case ServerState::Candidate:
            timeout();
            become_leader();
            break;
        }

        if (loop_sleep_us > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(loop_sleep_us));
        }
    }
}

/* ============================================================
 * apply_worker_loop (원본 applyWorker 고루틴)
 *
 * advance_commit_index가 commitIndex를 전진시킨 뒤 apply_notify_pending을
 * 세우고 notify_one한다. 여기서 그걸 받아 apply_pending()을 돌린다.
 * (원본 주석: "advanceCommitIndex가 commitIndex 전진 후 non-blocking
 *  신호를 여기 보냄; applyWorker가 별도 고루틴에서 pending entries를 drain")
 * ============================================================ */
void Server::apply_worker_loop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lk(workers.apply_notify_mu);
            workers.apply_notify_cv.wait_for(lk, std::chrono::milliseconds(kApplyWorkerPollMs),
                [this]() { return workers.apply_notify_pending || done.load(); });
            workers.apply_notify_pending = false;
        }
        if (done.load()) {
            return;
        }
        apply_pending();
    }
}

/* ============================================================
 * slot_gc_worker_loop (원본 slotGC 고루틴)
 *
 * do_slot_gc는 "Must be called with s.mu held"라서 여기서 락을 잡는다.
 * ============================================================ */
void Server::slot_gc_worker_loop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lk(workers.slot_gc_notify_mu);
            workers.slot_gc_notify_cv.wait_for(lk, std::chrono::milliseconds(kSlotGcWorkerPollMs),
                [this]() { return workers.slot_gc_notify_pending || done.load(); });
            workers.slot_gc_notify_pending = false;
        }
        if (done.load()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lk(mu);
            do_slot_gc();
        }
    }
}


/* ============================================================
 * 워커 스레드 수명 관리 (예전에는 raft_server.h 에 inline 으로 있었다).
 * stop() 이 이 파일에 있어 같이 둔다 -- 셋 다 inflight_mu 를 잡거나
 * std::thread 를 만들고 join 하므로 헤더에 있을 이유가 없다.
 * ============================================================ */
Server::~Server(){
        stop();   /* 워커 스레드까지 정리 (내부에서 join_all_replication_threads 호출) */
    }

void Server::reap_finished_threads(){
        std::lock_guard<std::mutex> lk(workers.inflight_mu);
        auto it = std::remove_if(workers.inflight.begin(), workers.inflight.end(),
            [](ReplicationThreadSlot &slot) {
                if (slot.done->load(std::memory_order_acquire)) {
                    if (slot.th.joinable()) {
                        slot.th.join();   /* 실행은 끝났으니 join은 즉시 반환 */
                    }
                    return true;   /* 리스트에서 제거 */
                }
                return false;   /* 아직 실행 중, 유지 */
            });
        workers.inflight.erase(it, workers.inflight.end());
    }

void Server::spawn_replication_thread(std::function<void()> work) {
        reap_finished_threads();

        auto done = std::make_shared<std::atomic<bool>>(false);
        std::thread th([work = std::move(work), done]() mutable {
            work();
            done->store(true, std::memory_order_release);   /* 실행 완료 표시 */
        });

        std::lock_guard<std::mutex> lk(workers.inflight_mu);
        workers.inflight.push_back(ReplicationThreadSlot{std::move(th), done});
    }

void Server::join_all_replication_threads(){
        std::lock_guard<std::mutex> lk(workers.inflight_mu);
        for (auto &slot : workers.inflight) {
            if (slot.th.joinable()) {
                slot.th.join();
            }
        }
        workers.inflight.clear();
    }

} /* namespace nvmeof_raft */
