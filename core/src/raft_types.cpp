#include "raft_server.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <chrono>

using clock_type = std::chrono::steady_clock;
/* core/ 의 자료형 구현.
 *
 * raft_state.h 와 raft_timings.h 는 **자료 정의**를 담고 core TU 16~17개가
 * 전부 읽는다. 그래서 그 헤더에는 한두 줄짜리 접근자만 남기고, 길거나
 * 락/할당을 만지는 본문은 여기로 내렸다:
 *   AlignedBuffer      posix_memalign / memcpy / free
 *   RingLog::configure Server 당 한 번
 *   ReplSink           mutex 획득
 *   derive_apply_timings  ApplyTimings 항등식 계산 (apply_timed 당 한 번)
 *   to_string          진단 출력용
 *
 * Server 자신의 워커 스레드 관련 메서드는 stop() 옆이 자연스러워
 * core/src/raft_lifecycle.cpp 에 있다. */

namespace nvmeof_raft {

AlignedBuffer::AlignedBuffer(size_t size, size_t align){
        resize(size, align);
    }

AlignedBuffer::~AlignedBuffer(){ reset(); }

AlignedBuffer::AlignedBuffer(AlignedBuffer &&other) noexcept
    : data_(other.data_), size_(other.size_), capacity_(other.capacity_) {
        other.data_ = nullptr;
        other.size_ = other.capacity_ = 0;
    }

AlignedBuffer &AlignedBuffer::operator=(AlignedBuffer &&other) noexcept {
        if (this != &other) {
            reset();
            data_ = other.data_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            other.data_ = nullptr;
            other.size_ = other.capacity_ = 0;
        }
        return *this;
    }

void AlignedBuffer::resize(size_t new_size, size_t align){
        if (new_size > capacity_) {
            size_t aligned_cap = (new_size + align - 1) / align * align;
            void *new_raw = nullptr;
            if (posix_memalign(&new_raw, align, aligned_cap) != 0) {
                throw std::runtime_error("AlignedBuffer::resize: posix_memalign failed");
            }
            if (data_ != nullptr && size_ > 0) {
                std::memcpy(new_raw, data_, size_);
            }
            std::memset(static_cast<uint8_t *>(new_raw) + size_, 0, aligned_cap - size_);
            if (data_ != nullptr) {
                free(data_);
            }
            data_ = new_raw;
            capacity_ = aligned_cap;
        } else if (new_size > size_) {
            std::memset(static_cast<uint8_t *>(data_) + size_, 0, new_size - size_);
        }
        size_ = new_size;
    }

void AlignedBuffer::reset(){
        if (data_ != nullptr) {
            free(data_);
            data_ = nullptr;
        }
        size_ = capacity_ = 0;
    }

void RingLog::configure(uint64_t pages){
        if (pages < 2) {
            pages = 2;   /* 헤더 1섹터 + 최소한의 링 */
        }
        num_pages = pages;
        total_slots = pages * SLOTS_PER_PAGE;
        ring_slots = total_slots - 1;
    }

std::string to_string(LogEntryState st) {
    switch (st) {
    case LogEntryState::Free: return "FREE";
    case LogEntryState::Using: return "USING";
    default: return "UNKNOWN";
    }
}

std::string to_string(ServerState s) {
    switch (s) {
    case ServerState::Leader: return "leader";
    case ServerState::Follower: return "follower";
    case ServerState::Candidate: return "candidate";
    default: return "unknown";
    }
}

void ReplSink::push(const ReplSample &s) {
    std::lock_guard<std::mutex> lk(mu_);

    samples_.push_back(s);

    // [Diag]
    if (samples_.size() == 2) {
        const auto &s0 = samples_[0];
        const auto &s1 = samples_[1];

        const int64_t aenet_ns =
            (s0.ae_rt_ns - s0.r2_ns) +
            (s1.ae_rt_ns - s1.r2_ns);

        const int64_t fhandler_ns =
            (s0.r2_ns - s0.write_pba_rt_ns) +
            (s1.r2_ns - s1.write_pba_rt_ns);

        const int64_t replnet_ns =
            (s0.write_pba_rt_ns - s0.storage_copy_ns) +
            (s1.write_pba_rt_ns - s1.storage_copy_ns);

        const int64_t storage_ns =
            s0.storage_copy_ns +
            s1.storage_copy_ns;

        /*
         * ReplSink마다 mu_가 다르므로,
         * static 누적값은 별도 mutex로 보호.
         */
        static std::mutex diag_mu;

        static uint64_t total_requests = 0;
        static int64_t total_aenet_ns = 0;
        static int64_t total_fhandler_ns = 0;
        static int64_t total_replnet_ns = 0;
        static int64_t total_storage_ns = 0;

        static uint64_t window_requests = 0;
        static int64_t window_aenet_ns = 0;
        static int64_t window_fhandler_ns = 0;
        static int64_t window_replnet_ns = 0;
        static int64_t window_storage_ns = 0;

        std::lock_guard<std::mutex> diag_lk(diag_mu);

        total_requests++;
        total_aenet_ns += aenet_ns;
        total_fhandler_ns += fhandler_ns;
        total_replnet_ns += replnet_ns;
        total_storage_ns += storage_ns;

        window_requests++;
        window_aenet_ns += aenet_ns;
        window_fhandler_ns += fhandler_ns;
        window_replnet_ns += replnet_ns;
        window_storage_ns += storage_ns;

        if (window_requests == 1000) {
            /*
             * 각 request마다 follower sample이 2개이므로
             * denominator = requests * 2
             */
            const double window_samples =
                static_cast<double>(window_requests) * 2.0;

            const double total_samples =
                static_cast<double>(total_requests) * 2.0;

            std::fprintf(
                stderr,
                "[AE-STAGE-WINDOW] requests=%llu "
                "AENet=%.1fus FHandler=%.1fus "
                "ReplNet=%.1fus StorageIO=%.1fus\n",
                static_cast<unsigned long long>(window_requests),
                window_aenet_ns / 1000.0 / window_samples,
                window_fhandler_ns / 1000.0 / window_samples,
                window_replnet_ns / 1000.0 / window_samples,
                window_storage_ns / 1000.0 / window_samples);

            std::fprintf(
                stderr,
                "[AE-STAGE-TOTAL] requests=%llu "
                "AENet=%.1fus FHandler=%.1fus "
                "ReplNet=%.1fus StorageIO=%.1fus\n",
                static_cast<unsigned long long>(total_requests),
                total_aenet_ns / 1000.0 / total_samples,
                total_fhandler_ns / 1000.0 / total_samples,
                total_replnet_ns / 1000.0 / total_samples,
                total_storage_ns / 1000.0 / total_samples);

            window_requests = 0;
            window_aenet_ns = 0;
            window_fhandler_ns = 0;
            window_replnet_ns = 0;
            window_storage_ns = 0;
        }
    }
    // [Diag]
}

void note_worker_start(
    int peer_index,
    clock_type::time_point start,
    int64_t wake_ns)
{
    std::lock_guard<std::mutex> lk(mu_);

    diag_wake_sum_ns_ += wake_ns;
    diag_wake_count_++;

    if (!diag_first_worker_seen_) {
        diag_first_worker_seen_ = true;
        diag_first_peer_ = peer_index;
        diag_first_worker_start_ = start;
        return;
    }

    if (peer_index != diag_first_peer_) {
        diag_worker_start_gap_ns_ =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                start - diag_first_worker_start_).count();

        if (diag_worker_start_gap_ns_ < 0) {
            diag_worker_start_gap_ns_ =
                -diag_worker_start_gap_ns_;
        }
    }
}


std::pair<ReplSample, bool> ReplSink::first_sample(){
        std::lock_guard<std::mutex> lk(mu_);
        if (samples_.empty()) {
            return {ReplSample{}, false};
        }
        return {samples_.front(), true};
    }

void derive_apply_timings(const ApplyWalls &w, ReplSink *sink,
                          const ProfilingSink &prof, ApplyTimings &t) {
    t.l_persist_ns = w.nvme_ns;
    t.l_handler_ns =
        timings_clamp0(w.a_held_ns - w.nvme_ns);
    t.replicate_ns = w.replicate_ns;
    t.mutex_ns = w.mutex_a_ns;
    t.commit_wait_ns = w.commit_wait_ns;

    auto [sample, have] = sink->first_sample();

    if (!have) {
        /*
         * TODO(profiling):
         * global prof.sample_* fallback is not request-correlated.
         * Under concurrent ApplyTimed calls, another Apply/heartbeat may
         * overwrite these values.
         */
        sample.ae_rt_ns = prof.sample_ae_rt_ns.load();
        sample.r2_ns = prof.sample_r2_ns.load();
        sample.write_pba_rt_ns =
            prof.sample_write_pba_rt_ns.load();
        sample.storage_copy_ns =
            prof.sample_storage_copy_ns.load();
        sample.mutex_ns = prof.sample_mutex_ns.load();
        sample.mutex_c_ns = prof.sample_mutex_c_ns.load();

        /*
         * fallback을 유지하려면 이것도 같이 있어야 함.
         * ProfilingSink에 sample_leader_side가 추가되어 있다는 전제.
         */
        sample.leader_side =
            prof.sample_leader_side.load();
    }

    /*
     * AE network 자체는 두 mode 모두 동일:
     *
     * AE_RT = AENet + follower handler(R2)
     */
    t.ae_net_ns =
        timings_clamp0(
            sample.ae_rt_ns -
            sample.r2_ns);

    /*
     * WritePBABatch 자체도 두 mode 모두:
     *
     * WritePBA_RT = ReplNet + StorageIO
     */
    t.repl_net_ns =
        timings_clamp0(
            sample.write_pba_rt_ns -
            sample.storage_copy_ns);

    t.storage_io_ns =
        sample.storage_copy_ns;

    if (sample.leader_side) {
        /*
         * Leader-side:
         *
         * WritePBABatch가 AppendEntries RPC보다 먼저 일어남.
         *
         * Replicate:
         *
         *   WritePBA_RT
         *     ├─ ReplNet
         *     └─ StorageIO
         *
         *   AE_RT
         *     ├─ AENet
         *     └─ FHandler (= R2)
         *
         *   QuorumWait
         */

        /*
         * follower에서는 block copy를 하지 않으므로
         * follower handler 전체가 FHandler.
         */
        t.f_handler_ns =
            sample.r2_ns;

        /*
         * leader-side copy와 AE RPC가 둘 다 replicate_ns에 포함되므로
         * 둘 다 빼야 실제 이후 quorum wait가 남음.
         */
        t.quorum_wait_ns =
            timings_clamp0(
                w.replicate_ns
                - sample.write_pba_rt_ns
                - sample.ae_rt_ns);
    } else {
        /*
         * Destination-side:
         *
         * AE_RT
         *   ├─ AENet
         *   └─ R2
         *       ├─ FHandler
         *       └─ WritePBA_RT
         *           ├─ ReplNet
         *           └─ StorageIO
         *
         * 이후 QuorumWait
         */

        t.f_handler_ns =
            timings_clamp0(
                sample.r2_ns -
                sample.write_pba_rt_ns);

        t.quorum_wait_ns =
            timings_clamp0(
                w.replicate_ns -
                sample.ae_rt_ns);
    }

    /*
     * Mutex diagnostic
     */
    if (sample.mutex_ns != 0) {
        t.mutex_ns = sample.mutex_ns;
    }

    t.mutex_c_ns = sample.mutex_c_ns;

    t.wait_lock_b_ns =
        timings_clamp0(
            t.mutex_ns -
            t.mutex_c_ns);

    t.post_rpc_ns = sample.post_rpc_wall_ns;
    t.pure_commit_wait_ns = w.commit_wait_ns;

    t.replicate_corrected_ns =
        timings_clamp0(
            w.replicate_ns -
            t.mutex_ns);

    /*
     * corrected QuorumWait도 mode별로 동일하게 분기해야 함.
     */
    if (sample.leader_side) {
        t.quorum_wait_corrected_ns =
            timings_clamp0(
                t.replicate_corrected_ns
                - sample.write_pba_rt_ns
                - sample.ae_rt_ns);
    } else {
        t.quorum_wait_corrected_ns =
            timings_clamp0(
                t.replicate_corrected_ns
                - sample.ae_rt_ns);
    }

    /*
     * advanceCommitIndex 서브스테이지
     */
    t.aci_lock_wait_ns =
        prof.aci_lock_wait_ns.load();

    t.aci_quorum_ns =
        prof.aci_quorum_ns.load();

    t.aci_slot_gc_ns =
        prof.aci_slot_gc_ns.load();

    t.aci_apply_loop_ns =
        prof.aci_apply_loop_ns.load();

    t.aci_sort_ns =
        prof.aci_sort_ns.load();

    t.aci_backpres_ns =
        prof.aci_backpres_ns.load();

    t.aci_signal_ns =
        prof.aci_signal_ns.load();

    t.wg_scheduling_ns =
        timings_clamp0(
            t.quorum_wait_ns
            - t.post_rpc_ns
            - t.commit_wait_ns);
}

} /* namespace nvmeof_raft */
