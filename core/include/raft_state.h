#ifndef RAFT_STATE_H
#define RAFT_STATE_H

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "raft_constants.h"
#include "raft_entry.h"

/* ============================================================
 * Server 가 들고 있는 상태 그룹들.
 *
 * Server(core/include/raft_server.h)의 필드는 이 다섯 struct로 묶여 있다:
 *   RaftState      합의 상태 (term, log, cluster, commit_index, state, 타이머)
 *   RingLog        링버퍼 부기 + 링 크기 설정
 *   StorageIo      링 파일 / 디바이스 I/O 상태
 *   ProfilingSink  계측 (core/include/raft_timings.h)
 *   WorkerPool     상시 스레드 + 복제 스레드 수명 관리
 *
 * 메서드는 전부 Server:: 에 있고 락(Server::mu)도 하나다 -- 이 파일은
 * **자료 정의만** 담는다. 예전에는 이 전부가 raft_server.h 한 파일에
 * 있어서 681줄이었다.
 *
 * **RaftState 와 RingLog 의 모든 필드는 Server::mu 로 보호된다**
 * (원본 raft.go 의 s.mu 하나로 전부 보호하는 규약 그대로).
 * ============================================================ */

namespace nvmeof_raft {

/* ============================================================
 * LogEntryState (raft.go 원본)
 * "leader-only, in-memory ring buffer protection"
 * ============================================================ */
enum class LogEntryState : uint8_t {
    Free = 0,   /* not in logSlotMap (unallocated or already freed) */
    Using = 1,  /* currently held in logSlotMap */
};

std::string to_string(LogEntryState st);

/* ============================================================
 * slotRecord (raft.go 원본)
 * "tracks which ring slots a single log entry occupies and its state."
 * ============================================================ */
struct SlotRecord {
    uint64_t start = 0;
    uint64_t num_slots = 0;
    LogEntryState state = LogEntryState::Free;
};

/* ============================================================
 * ServerState (raft.go 원본, Go의 typed string const를 enum class로) */
enum class ServerState {
    Leader,
    Follower,
    Candidate,
};

std::string to_string(ServerState s);

/* ============================================================
 * ClusterMember (raft.go 원본 필드 그대로)
 *
 * 원본은 rpcClient *rpc.Client 를 직접 들고 있었고(Go net/rpc), 이 포팅도
 * 한동안 그랬다 -- net/ 에서 정의되는 커넥션 핸들의 shared_ptr과 dial
 * backoff 시각이 여기 있었다. 그러면 (1) 그 타입의 정의가 net/ 에 있으므로
 * 한 바이너리에 전송이 하나만 존재할 수 있고 (2) 전송 구현체가 s.mu를 잡고
 * 이 필드들을 갱신해야 했다.
 *
 * 이제 커넥션 상태는 전송 구현체가 자기 안에 갖는다
 * (core/include/raft_transport.h 참고). 여기 남는 것은 **Raft가 아는 멤버 정보**뿐이다.
 * ============================================================ */
struct ClusterMember {
    uint64_t id = 0;
    std::string address;
    std::string device_path;
    std::string storage_host;   /* blockcopy RPC server hostname (storage node) */

    uint64_t next_index = 0;
    uint64_t match_index = 0;
    uint64_t voted_for = 0;

    /* True while an appendEntries goroutine with log entries is in-flight.
     * Prevents the leader from piling up duplicate large RPCs to a slow
     * follower. (원본 주석 그대로) */
    bool inflight = false;

    /* log_skip_pba_diag의 출력 스로틀용 (원본에는 없음 -- 원본은 매
     * 하트비트마다 무조건 찍어서 초당 10줄이 나온다). 같은 next에 대한
     * 경고는 1초에 한 번만 낸다. */
    uint64_t skip_pba_last_next = UINT64_MAX;
    std::chrono::steady_clock::time_point skip_pba_last_log{};
};

/* CachedFD는 blockio/cached_fd.h가 정의한다. 여기서는 shared_ptr로만 다루므로
 * **전방 선언으로 충분하다** -- std::shared_ptr는 불완전 타입에 대해서도
 * 선언·복사·소멸이 합법이다(삭제자가 make_shared 시점에 타입 소거된다).
 *
 * 전방 선언으로 두는 이유: 이 헤더는 core TU 16개가 전부 읽는데,
 * cached_fd.h(205줄)를 여기서 include하면 그 16개가 O_DIRECT/FIEMAP 헤더를
 * 매번 파싱한다. 실제로 CachedFD를 역참조하는 곳은 .cpp 4개뿐이고
 * (raft_persist / raft_pba / raft_lifecycle / raft_handle_append_entries),
 * 그 넷만 cached_fd.h를 직접 include한다. */
class CachedFD;

/* ============================================================
 * slotMapTraceEvt (raft.go 원본)
 * "records a single mutation of logSlotMap for diagnostics"
 * ============================================================ */
struct SlotMapTraceEvt {
    char op = 0;        /* 'I' = insert, 'F' = free */
    uint64_t idx = 0;   /* log index that was mutated */
};

/* ============================================================
 * AlignedBuffer -- O_DIRECT pwrite에 안전하게 넘길 수 있는, 실제로
 * 정렬이 보장되는 버퍼. std::vector는 생성자에서 데이터를 복사할 때
 * std::allocator(malloc)로 새로 할당하므로 posix_memalign으로 잡은
 * 정렬을 못 지킨다 -- 이게 오늘 발견된 EINVAL 버그의 원인.
 * 이 타입은 posix_memalign한 메모리를 복사 없이 그대로 소유한다.
 * ============================================================ */
class AlignedBuffer {
public:
    AlignedBuffer() = default;

    explicit AlignedBuffer(size_t size, size_t align = SECTOR_SIZE);

    ~AlignedBuffer();

    AlignedBuffer(const AlignedBuffer &) = delete;
    AlignedBuffer &operator=(const AlignedBuffer &) = delete;

    AlignedBuffer(AlignedBuffer &&other) noexcept;
    AlignedBuffer &operator=(AlignedBuffer &&other) noexcept;

    /* 기존 run_buf(std::vector) 코드가 clear()/size()/data()/resize()를
     * 쓰던 것과 최대한 같은 인터페이스를 제공해 교체 비용을 낮춘다 */
    void clear() { size_ = 0; }
    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
    uint8_t *data() { return static_cast<uint8_t *>(data_); }
    const uint8_t *data() const { return static_cast<const uint8_t *>(data_); }

    /* size만 늘리고 capacity가 부족하면 재할당. 정렬은 항상 유지.
     * std::vector::resize와 동일하게, 재할당 시 기존 데이터(최대
     * min(old_size, new_size)만큼)를 보존한다 -- run_buffer가 여러
     * 엔트리를 순차로 append하는 용도라 이 보존이 필수 */
    void resize(size_t new_size, size_t align = SECTOR_SIZE);

    /* std::vector::resize(off + entry_bytes) 패턴 지원용 */
    void grow_to(size_t new_size, size_t align = SECTOR_SIZE) { resize(new_size, align); }

private:
    void reset();

    void *data_ = nullptr;
    size_t size_ = 0;
    size_t capacity_ = 0;
};


/* ============================================================
 * RaftState -- Raft 합의 알고리즘 자체의 상태.
 *
 * 원본 raft.go의 Server 필드 중 "논문에 나오는 것들"이다. 전부
 * **Server::mu로 보호된다** (원본이 s.mu 하나로 전부 보호하는 규약 그대로).
 *
 * votedFor를 담는 별도 필드는 **없다.** 원본 정의가 다음과 같기 때문이다:
 *   func (s *Server) setVotedFor(id uint64) { s.cluster[s.clusterIndex].votedFor = id }
 *   func (s *Server) getVotedFor() uint64  { return s.cluster[s.clusterIndex].votedFor }
 * 즉 자기 투표 상태도 cluster[cluster_index].voted_for 한 자리에만 있다.
 * 별도 필드를 두면 같은 term에 리더가 둘 생긴다 (DECISIONS.md D2).
 * ============================================================ */
struct RaftState {
    /* ---- Raft persistent state (헤더에 기록되고 재시작 시 복구된다) ---- */
    uint64_t current_term = 0;

    /* log[0]은 sentinel (term=0, command 없음). 실제 엔트리는 절대 인덱스
     * 1부터다 (DECISIONS.md Q1). 절대 인덱스 <-> 벡터 인덱스 변환은
     * Server::log_slice() / oldest_log_index()가 한다 -- 앞쪽이 트리밍되면
     * 시작점이 움직이므로 직접 인덱싱하지 말 것. */
    std::vector<Entry> log;

    /* ---- 이 노드의 신원 ---- */
    uint64_t id = 0;
    std::string address;

    /* ---- 타이머 ---- */
    std::chrono::steady_clock::time_point election_timeout{};
    std::chrono::steady_clock::time_point heartbeat_timeout{};
    int heartbeat_ms = 0;   /* election timeout은 이 값의 20~30배 (raft_election.cpp) */

    /* ---- Raft volatile state ---- */
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;
    ServerState state = ServerState::Follower;

    /* ---- 클러스터 멤버십 ----
     * cluster[cluster_index]가 이 노드 자신이다. 멤버별 next_index /
     * match_index / voted_for / 전송 핸들은 ClusterMember에 있다. */
    std::vector<ClusterMember> cluster;
    int cluster_index = 0;
};

/* ============================================================
 * RingLog -- 단일 포인터 링버퍼의 상태.
 *
 * 온-디스크 링(헤더 512B + 슬롯들)에 대한 인메모리 부기다. 실제 인코딩과
 * I/O는 Server::persist_circular / read_entry_direct가 하고, 이 struct는
 * "어디까지 썼고, 어디부터 살아 있고, 어느 인덱스가 어느 슬롯에 있는가"만
 * 들고 있다. 레이아웃 상수는 core/raft_basics.h에 있다.
 *
 * 필드만 묶은 것이며 모두 **Server::mu로 보호된다** (원본 s.mu 하나로
 * 전부 보호하는 규약 그대로).
 * ============================================================ */
struct RingLog {
    /* ---- 쓰기 포인터 ---- */
    uint64_t tail_log_index = 0;   /* 다음에 쓸 엔트리의 절대 논리 인덱스 */
    uint64_t tail_slot = 0;        /* 다음에 쓸 링 슬롯 */

    /* head_slot: 살아 있는 log_slot_map 엔트리 중 가장 오래된 것의 시작 슬롯.
     * recompute_head_slot()이 유지한다. **리더 전용** -- 팔로워는 무시한다. */
    uint64_t head_slot = 0;

    /* gc_up_to: slot GC가 이미 해제한 최대 로그 인덱스.
     * log_slot_map 전체 스캔 대신 O(delta) 순회를 가능하게 한다. */
    uint64_t gc_up_to = 0;

    /* gc_has_run: 원본(raft.go)에는 없다. gc_up_to = 0이 "GC 미실행"과
     * "인덱스 0까지 GC 완료" 두 경우를 구분하지 못해서, GC가 최초 1회
     * 실행되기 전까지 gc_up_to fast-path를 건너뛰기 위한 가드다. */
    bool gc_has_run = false;

    /* 리더 전용: 로그 인덱스 -> 슬롯 기록 (범위 + 상태) */
    std::unordered_map<uint64_t, SlotRecord> log_slot_map;

    /* ---- persist_circular의 재사용 인코딩 버퍼 ----
     * "Grown on demand; never shrunk." persist_circular의 모든 호출부가
     * mu를 보유하므로 평범한 필드로 둬도 안전하다. */
    AlignedBuffer run_buf;

    /* ---- 헤더 쓰기 스킵 캐시 ----
     * "persistCircular skips the header write on the append-only hot path
     *  when currentTerm and votedFor match these." */
    bool persisted_init = false;
    uint64_t persisted_term = 0;
    uint64_t persisted_voted_for = 0;

    /* ---- 진단 링: 최근 log_slot_map 변경 이력 ----
     * [SKIP PBA] 경고를 분류할 때 쓴다 (core/src/raft_diagnostics.cpp).
     * mu 안에서만 접근하므로 추가 동기화가 필요 없다. */
    static constexpr size_t kSlotMapTraceRingSize = 256;
    std::array<SlotMapTraceEvt, kSlotMapTraceRingSize> slot_map_trace_ring{};
    int slot_map_trace_cursor = 0;
    bool slot_map_trace_inserts = false;   /* insert 기록 게이트 (delete는 항상 기록) */

    /* ---- 링 크기 설정 ----
     * 예전에는 core/raft_constants.h의 프로세스 전역 가변 변수였다
     * (자세한 경위는 그 파일의 DEFAULT_NUM_PAGES 주석 참고).
     * configure()는 init_storage() 이전에, 이 Server에 대해 한 번만
     * 불러야 한다 -- 링 파일을 fallocate한 뒤에 바꾸면 슬롯 인덱스 해석이
     * 깨진다. */
    uint64_t num_pages = DEFAULT_NUM_PAGES;
    uint64_t total_slots = DEFAULT_NUM_PAGES * SLOTS_PER_PAGE;
    uint64_t ring_slots = DEFAULT_NUM_PAGES * SLOTS_PER_PAGE - 1;

    void configure(uint64_t pages);

    /* 링 메타데이터 파일의 전체 바이트 크기 (헤더 + 링 슬롯 전체) */
    uint64_t file_size_bytes() const { return num_pages * PAGE_SIZE; }

    /* ---- in-memory log 벡터 트리밍 임계치 (0 = 비활성, 원본 동작) ----
     * Server::log는 push_back만 하고 축소되지 않아서, 링버퍼가 순환하는
     * 동안 인메모리 벡터는 무한히 자란다. do_slot_gc가 슬롯을 해제하는
     * 것과 같은 기준(min matchIndex, last_applied)으로 앞쪽을 잘라낸다.
     * oldest_log_index()/log_slice()가 이미 동적 시작점을 전제로 짜여
     * 있으므로 인덱스 시맨틱은 그대로다. */
    uint64_t log_trim_threshold = 8192;
};

/* ============================================================
 * StorageIo -- 링 메타데이터 파일과 대상 블록 디바이스에 대한 I/O 상태.
 *
 * 필드만 묶은 것이고, 실제 I/O는 CachedFD(blockio/cached_fd.h)와
 * Server의 persist_circular / read_entry_direct / leader_pba_for_range가
 * 한다.
 * ============================================================ */
struct StorageIo {
    /* 링 파일이 놓이는 디렉터리. 실서버(FIEMAP 모드)에서는 **대상 블록
     * 디바이스 위 파일시스템**이어야 한다 -- FIEMAP이 그 디바이스 기준
     * PBA를 돌려주기 때문이다. */
    std::string metadata_dir;

    /* NVMe-oF 블록 디바이스 경로. 비우면 링 파일 자체를 볼륨으로 쓴다. */
    std::string device_path;

    /* 캐시된 fd + extent 맵. init_storage가 열고, 이후 모든 O_DIRECT I/O가
     * 이걸 통한다. */
    std::shared_ptr<CachedFD> cached_fd;

};

/* ============================================================
 * WorkerPool -- 상시 워커 스레드와 그 깨우기 신호, 그리고 fire-and-forget
 * 복제 스레드의 수명 관리.
 *
 * 예전에는 이 11개 필드가 Server의 다른 필드들과 섞여 있었다.
 * 필드만 묶은 것이고 메서드는 그대로 Server::에 있다.
 *
 * inflight: 원본 Go는 복제 goroutine을 fire-and-forget으로 띄우고 결과를
 * 안 기다린다. std::thread는 detach()하면 this를 참조하는 스레드가 Server
 * 파괴 후에도 남아 use-after-free가 되므로, join도 detach도 아닌 절충안을
 * 쓴다 -- 워커가 스스로 완료 플래그를 세우고, 다음 spawn 때 완료된 것만
 * join·제거하며, stop()에서 남은 전부를 강제 join한다.
 * (`joinable()`은 "아직 join 안 했나"만 알려주지 "실행이 끝났나"는
 *  알려주지 않는다 -- 첫 구현에서 이 둘을 혼동해 reap가 아무것도 지우지
 *  않는 버그가 있었다.)
 * ============================================================ */
struct ReplicationThreadSlot {
    std::thread th;
    std::shared_ptr<std::atomic<bool>> done;
};

struct WorkerPool {
    /* 상시 스레드 3개. start()가 띄우고 stop()이 join한다. */
    std::thread main_thread;      /* timeout / become_leader / heartbeat / advance_commit_index */
    std::thread apply_thread;     /* apply_notify_cv 대기 -> apply_pending */
    std::thread slot_gc_thread;   /* slot_gc_notify_cv 대기 -> do_slot_gc */
    bool started = false;

    /* apply 워커 깨우기. advance_commit_index가 commit_index를 전진시킨 뒤
     * non-blocking으로 신호한다. */
    std::condition_variable apply_notify_cv;
    std::mutex apply_notify_mu;
    bool apply_notify_pending = false;

    /* slot GC 워커 깨우기. */
    std::condition_variable slot_gc_notify_cv;
    std::mutex slot_gc_notify_mu;
    bool slot_gc_notify_pending = false;
    uint64_t slot_gc_tick = 0;   /* advance_commit_index 호출마다 증가 (floor 트리거용) */

    /* 진행 중인 복제 스레드 (팔로워별 AE 워커 / RequestVote 워커). */
    std::mutex inflight_mu;
    std::vector<ReplicationThreadSlot> inflight;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_STATE_H */
