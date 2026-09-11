#ifndef RAFT_CONSTANTS_HPP
#define RAFT_CONSTANTS_HPP

#include <cstdint>
#include <cstddef>

#include "block_geometry.h"

namespace nvmeof_raft {

/* ============================================================
 * raft.go 원본 상수 그대로 포팅 (Constants 섹션)
 * ============================================================ */

/* 섹터/페이지 크기는 blockio/ 가 소유한다 (디바이스 기하값이지 Raft 개념이
 * 아니다). 여기서는 core/ 코드가 쓰던 이름으로 재노출만 한다 -- 값의 정의는
 * blockio/block_geometry.h 한 곳뿐이다. */
constexpr uint64_t SECTOR_SIZE = blockio::kSectorSize;
constexpr uint64_t PAGE_SIZE = blockio::kPageSize;
constexpr uint64_t HEADER_SIZE = SECTOR_SIZE;
constexpr uint64_t RING_OFFSET = HEADER_SIZE;

/* ENTRY_META_SIZE: 각 엔트리 메타(Term, CmdLen, NumSlots, LogIndex)가
 * 슬롯 0의 앞부분에 packed되는 크기. Command 바이트는 이 뒤부터 시작 */
constexpr uint64_t ENTRY_META_SIZE = 32;

constexpr uint64_t SLOTS_PER_PAGE = PAGE_SIZE / SECTOR_SIZE;   /* 8 */

/* 링 크기의 기본값. 실제 값은 **Server::ring(RingLog)의 인스턴스 필드**다.
 *
 * 예전에는 num_pages / total_slots / ring_slots 가 이 헤더의 **비-const
 * 전역 변수**였고 configure_ring()이 그걸 갱신했다. 주석에 "프로세스당 한
 * 번만"이라고만 적혀 있고 강제 수단이 없었으며, 그 결과 한 프로세스에 링
 * 크기가 다른 Server를 두 개 만들 수 없었다 -- 유닛 테스트가 케이스마다
 * 다른 링 구성을 쓸 수 없던 근본 원인이다. RingLog로 옮겼다.
 *
 * 원본 raft.go는 constexpr NUM_PAGES = 8Mi 였고, 벤치마크에서 재컴파일
 * 없이 override할 수 있어야 해서 이 포팅이 변수로 바꾼 것이다. 그 요구는
 * 인스턴스 필드로도 그대로 충족된다 (-ring-pages 플래그).
 *
 * 링 파일 크기는 num_pages * PAGE_SIZE 바이트다. ring_slots가
 * total_slots-1인 이유가 이것: 슬롯 0이 RING_OFFSET(헤더 512B) 뒤에서
 * 시작하므로 헤더가 정확히 섹터 하나를 먹는다. */
constexpr uint64_t DEFAULT_NUM_PAGES = 8ull * 1024 * 1024;     /* 32GiB */

/* appendEntries 배치 상한의 기본값. 실제 값은 Server의 인스턴스 필드다
 * (max_ae_batch / max_ae_batch_bytes). 원본은 package-level var였다. */
constexpr uint64_t DEFAULT_MAX_AE_BATCH = 1000000;
constexpr uint64_t DEFAULT_MAX_AE_BATCH_BYTES = 5ull * 1024 * 1024 * 1024;  /* 5 GiB */

/* doPBACopy가 이 경계에서 연속 복사를 쪼갬. AE 배치 상한과 독립적으로,
 * 스토리지 노드의 posix_memalign O_DIRECT 버퍼가 임의로 커지는 걸 방지 */
constexpr uint64_t MaxPBACopyChunkBytes = 256ull * 1024 * 1024;

} /* namespace nvmeof_raft */

#endif /* RAFT_CONSTANTS_HPP */