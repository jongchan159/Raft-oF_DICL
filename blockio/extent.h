#ifndef BLOCKIO_EXTENT_H
#define BLOCKIO_EXTENT_H

#include <cstdint>

namespace nvmeof_raft {

/* ============================================================
 * extent (raft.go 원본, 소문자 비공개 타입)
 *
 * 링 파일의 논리 구간 하나가 디바이스의 어느 물리 구간에 있는가.
 *
 * cached_fd.h에서 이 구조체만 떼어 놓았다. push 모드에서 팔로워가 자기
 * 링의 extent 목록을 AE 응답(core/include/raft_entry.h)에 실어 보내는데,
 * raft_entry.h는 core TU 전부와 net이 include한다. 거기서 cached_fd.h
 * 전체를 끌어오면 "core는 CachedFD를 전방 선언으로만 안다"는 불변식
 * (core/README.md)이 깨지므로, POD 하나만 담은 헤더로 둔다
 * (DECISIONS.md D20).
 * ============================================================ */
struct Extent {
    int64_t logical = 0;     /* logical byte offset in the file */
    uint64_t physical = 0;   /* physical byte offset on the device */
    uint64_t length = 0;     /* bytes */
};

} /* namespace nvmeof_raft */

#endif /* BLOCKIO_EXTENT_H */
