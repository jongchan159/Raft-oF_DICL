#ifndef RAFT_BASICS_H
#define RAFT_BASICS_H

#include <chrono>
#include <cstdint>
#include <cstddef>

#include "raft_constants.h"

/* ============================================================
 * core/ 안에서만 쓰는 기본 조각들 -- 온-디스크 레이아웃 오프셋과 작은 헬퍼.
 *
 * 이 파일은 **대응하는 .cpp 를 갖지 않는다.** 전부 constexpr 이거나 핫패스
 * inline 이고, 링크 의존을 만들면 안 되기 때문이다. 아래 두 묶음이 들어 있다:
 *
 *   1. 온-디스크 레이아웃  엔트리 헤더 32B / 파일 헤더 512B 의 필드 오프셋.
 *                          쓰기와 읽기가 서로 다른 .cpp 에 있어 짝을 맞춰야
 *                          하는 계약이므로 상수로 못 박아 둔다
 *   2. 작은 헬퍼           시간 측정, LE 직렬화, 정렬 올림
 *
 * 예전에는 raft_layout.h(오프셋)와 raft_util.h(헬퍼) 두 파일이었다. 둘 다
 * core 전용 리프였고 util 을 보는 TU 가 layout 을 보는 TU 를 포함해서,
 * 합쳐도 파싱 범위가 넓어지지 않는다. raft_constants.h 는 **합치지 않았다**
 * -- 그건 TU 19개 전부(core 를 안 쓰는 바이너리 포함)가 보는 헤더라
 * 여기 내용을 넣으면 그쪽까지 끌려간다.
 * ============================================================ */

namespace nvmeof_raft {

/* ---- 엔트리 헤더 (슬롯 0의 [0, ENTRY_META_SIZE) ) ---------------------- */
namespace entry_hdr {

constexpr uint64_t kOffTerm     = 0;    /* [ 0: 8) Entry::term */
constexpr uint64_t kOffCmdLen   = 8;    /* [ 8:16) 명령 바이트 수 */
constexpr uint64_t kOffNumSlots = 16;   /* [16:24) 이 엔트리가 차지하는 슬롯 수 */
constexpr uint64_t kOffLogIndex = 24;   /* [24:32) 절대 로그 인덱스 */
constexpr uint64_t kSize        = 32;   /* == ENTRY_META_SIZE. 명령은 여기서부터 */

static_assert(kSize == ENTRY_META_SIZE,
              "엔트리 헤더 크기가 ENTRY_META_SIZE와 어긋났다");
static_assert(kOffLogIndex + 8 == kSize,
              "엔트리 헤더 필드가 헤더 크기를 넘는다");

/* 슬롯 0에 명령 바이트가 몇 개까지 들어가는가.
 * 512 - 32 = 480. 이 값이 D13의 그 480이다. */
constexpr uint64_t kCmdBytesInFirstSlot = SECTOR_SIZE - kSize;
static_assert(kCmdBytesInFirstSlot == 480, "섹터/메타 크기가 바뀌었다");

} /* namespace entry_hdr */

/* ---- 파일 헤더 (논리 오프셋 0, HEADER_SIZE = 512B) --------------------- */
namespace file_hdr {

constexpr uint64_t kOffCurrentTerm = 0;    /* [ 0: 8) Raft persistent state */
constexpr uint64_t kOffVotedFor    = 8;    /* [ 8:16) Raft persistent state */

/* 아래 네 값은 **쓰기만 하고 재시작 시 읽지 않는다** (DECISIONS.md D8).
 * 원본 restoreCircular이 읽는 네 줄을 주석 처리해 둔 채 1/0/0/1로
 * 되돌리기 때문이다. 진단용으로는 유용하므로 계속 기록한다. */
constexpr uint64_t kOffTailLogIndex = 16;  /* [16:24) */
constexpr uint64_t kOffTailSlot     = 24;  /* [24:32) */
constexpr uint64_t kOffCommitIndex  = 32;  /* [32:40) */
constexpr uint64_t kOffLastApplied  = 40;  /* [40:48) */

constexpr uint64_t kUsedBytes = 48;

static_assert(kUsedBytes <= HEADER_SIZE, "파일 헤더 필드가 512B를 넘는다");
static_assert(HEADER_SIZE == SECTOR_SIZE,
              "헤더는 정확히 섹터 하나여야 한다 (RING_OFFSET 계산의 전제)");

} /* namespace file_hdr */

/* ---- 시간 ---------------------------------------------------------------
 * 프로파일링과 타임아웃 모두 steady_clock을 쓴다. system_clock을 쓰면
 * NTP 조정에 구간 측정이 흔들린다. */
using clock_type = std::chrono::steady_clock;

/* start 이후 지금까지 흐른 시간(나노초). ApplyTimings/ReplSample의 모든
 * 구간이 이 함수로 측정된다. */
inline int64_t elapsed_ns(clock_type::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        clock_type::now() - start).count();
}

/* ---- 리틀엔디언 64비트 직렬화 -------------------------------------------
 * 온-디스크 포맷(링 파일 헤더와 엔트리 메타)이 LE 고정이다. 와이어 포맷은
 * 별개로 빅엔디언을 쓴다(net/include/raft_wire_codec.h) -- 혼동하지 말 것. */
inline void put_u64_le(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) {
        p[i] = static_cast<uint8_t>((v >> (8 * i)) & 0xFF);
    }
}

inline uint64_t get_u64_le(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

/* ---- 정렬 --------------------------------------------------------------- */

/* n을 align의 배수로 올림. align은 2의 거듭제곱이어야 한다. */
inline uint64_t align_up(uint64_t n, uint64_t align) {
    return (n + align - 1) / align * align;
}

} /* namespace nvmeof_raft */

#endif /* RAFT_BASICS_H */
