#include "raft_server.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

/* core/ 의 자료형 구현.
 *
 * raft_state.h 는 **자료 정의**를 담고 core TU 전부가 읽는다. 그래서 그 헤더에는 한두 줄짜리 접근자만 남기고, 길거나
 * 락/할당을 만지는 본문은 여기로 내렸다:
 *   AlignedBuffer      posix_memalign / memcpy / free
 *   RingLog::configure Server 당 한 번
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

} /* namespace nvmeof_raft */
