#ifndef BLOCKIO_BLOCK_GEOMETRY_H
#define BLOCKIO_BLOCK_GEOMETRY_H

#include <cstdint>

/* ============================================================
 * 블록 디바이스 기하값의 단일 출처.
 *
 * 섹터 크기와 페이지 크기는 Raft의 개념이 아니라 저장 장치의 성질이므로
 * blockio/ 가 소유한다. core/include/raft_constants.h 가 이 값을 SECTOR_SIZE /
 * PAGE_SIZE 라는 기존 이름으로 재노출하므로 core/ 코드는 그대로다.
 *
 * 이렇게 둔 이유: 이 헤더가 없으면 blockio/ 가 core/include/raft_constants.h 를
 * include해야 하고, core/ 는 blockio/cached_fd.h 를 include하므로
 * 디렉터리 수준에서 순환이 생긴다. 값 자체를 blockio/ 로 내리면
 * 의존이 core -> blockio 한 방향이 된다.
 *
 * **두 값을 여기 말고 다른 곳에서 다시 정의하지 말 것.** 예전에
 * raft_cached_fd.cpp 의 `kAlign = 512` 와 blockcopy 서버의
 * `kAlign = 4096` 이 이름은 같고 값은 다른 상태로 공존한 적이 있다.
 * ============================================================ */

namespace nvmeof_raft {
namespace blockio {

/* O_DIRECT 와 링 슬롯의 기본 단위. */
constexpr uint64_t kSectorSize = 512;

/* FIEMAP extent 및 PBA 복사 버퍼의 정렬 단위. */
constexpr uint64_t kPageSize = 4096;

static_assert(kPageSize % kSectorSize == 0, "페이지는 섹터의 배수여야 한다");

} /* namespace blockio */
} /* namespace nvmeof_raft */

#endif /* BLOCKIO_BLOCK_GEOMETRY_H */
