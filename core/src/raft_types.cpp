#include "raft_server.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

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

void ReplSink::push(const ReplSample &s){
        std::lock_guard<std::mutex> lk(mu_);
        samples_.push_back(s);
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
    t.l_handler_ns = timings_clamp0(w.a_held_ns - w.nvme_ns);   /* Lock A 보유시간 - 실제 NVMe I/O */
    t.replicate_ns = w.replicate_ns;
    t.mutex_ns = w.mutex_a_ns;
    t.commit_wait_ns = w.commit_wait_ns;

    auto [sample, have] = sink->first_sample();
    if (!have) {
        /* "ApplyTimed가 replSink 비었을 때 쓰는 폴백": data-bearing AE가
         * 이 Apply 안에서 안 잡혔을 때 (예: 직전 heartbeat가 이미 복제를
         * 끝낸 경우) 최근 샘플 atomic을 쓴다 -- 원본과 동일한 폴백 */
        sample.ae_rt_ns = prof.sample_ae_rt_ns.load();
        sample.r2_ns = prof.sample_r2_ns.load();
        sample.write_pba_rt_ns = prof.sample_write_pba_rt_ns.load();
        sample.storage_copy_ns = prof.sample_storage_copy_ns.load();
        sample.mutex_ns = prof.sample_mutex_ns.load();
        sample.mutex_c_ns = prof.sample_mutex_c_ns.load();
    }

    t.ae_net_ns = timings_clamp0(sample.ae_rt_ns - sample.r2_ns);
    t.f_handler_ns = timings_clamp0(sample.r2_ns - sample.write_pba_rt_ns);
    t.repl_net_ns = timings_clamp0(sample.write_pba_rt_ns - sample.storage_copy_ns);
    t.storage_io_ns = sample.storage_copy_ns;
    t.quorum_wait_ns = timings_clamp0(w.replicate_ns - sample.ae_rt_ns);
    if (sample.mutex_ns != 0) {
        t.mutex_ns = sample.mutex_ns;
    }
    t.mutex_c_ns = sample.mutex_c_ns;
    t.wait_lock_b_ns = timings_clamp0(t.mutex_ns - t.mutex_c_ns);
    t.post_rpc_ns = sample.post_rpc_wall_ns;
    t.pure_commit_wait_ns = w.commit_wait_ns;
    t.replicate_corrected_ns = timings_clamp0(w.replicate_ns - t.mutex_ns);
    t.quorum_wait_corrected_ns = timings_clamp0(t.replicate_corrected_ns - sample.ae_rt_ns);

    /* advanceCommitIndex 서브스테이지 (프로파일링이 켜져 있을 때만 채워짐) */
    t.aci_lock_wait_ns = prof.aci_lock_wait_ns.load();
    t.aci_quorum_ns = prof.aci_quorum_ns.load();
    t.aci_slot_gc_ns = prof.aci_slot_gc_ns.load();
    t.aci_apply_loop_ns = prof.aci_apply_loop_ns.load();
    t.aci_sort_ns = prof.aci_sort_ns.load();
    t.aci_backpres_ns = prof.aci_backpres_ns.load();
    t.aci_signal_ns = prof.aci_signal_ns.load();

    t.wg_scheduling_ns = timings_clamp0(t.quorum_wait_ns - t.post_rpc_ns - t.commit_wait_ns);

}

} /* namespace nvmeof_raft */
