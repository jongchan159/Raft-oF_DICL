# `blockio/` — O_DIRECT 블록 I/O와 PBA 조회

**Raft를 전혀 모른다.** 논리 파일 오프셋을 물리 블록 주소(PBA)로 바꿔주는
것이 이 층의 일이고, 그게 이 프로젝트의 복제 방식 전체를 성립시킨다.

파일이 3개뿐이라 `include/`·`src/` 로 나누지 않고 flat으로 둔다.

## 핵심

`CachedFD` 가 O_DIRECT fd 하나와 FIEMAP으로 얻은 extent 맵을 들고 있다.
`get_pba(offset, len)` 이 그 맵을 조회해 물리 주소를 돌려준다 — 리더는 이
값을 AppendEntries에 실어 보내고, 스토리지 노드가 그 주소로 직접
`pread`/`pwrite` 한다.

`block_geometry.h` 가 `kSectorSize`(512) / `kPageSize`(4096)의 **단일
출처**다. `core/include/raft_constants.h` 가 `SECTOR_SIZE` / `PAGE_SIZE` 로
재수출하므로, 값을 바꿀 일이 있으면 여기서 바꾼다.

## 불변식

- **`raft_*` 헤더를 하나도 include하지 않는다.** 의존 방향은
  `core/ → blockio/` 한 방향이다. 이걸 뒤집으면 `raft_unit_tests` 의
  링크 격리가 무너진다.
- 링 파일은 **ext4 / xfs 로컬 경로**여야 한다. O_DIRECT와 FIEMAP이 둘 다
  필요해서 NFS에서는 동작하지 않는다 (`findmnt -no FSTYPE <path>` 로 확인).
- `CachedFD::write` 의 unaligned slow path 는 여전히 `throw` 한다.
  정렬되지 않은 쓰기를 새로 넣지 말 것.

## 왜 정렬이 까다로운가

`std::vector` 는 생성자에서 데이터를 복사할 때 `std::allocator`(malloc)로
새로 할당하므로 `posix_memalign` 정렬을 못 지킨다 → O_DIRECT `pwrite` 가
EINVAL. 그래서 `core/` 에 `AlignedBuffer` 가 따로 있다. 버퍼를 다룰 때
`vector` 로 갈아타지 말 것 — DECISIONS.md §"원본에 없지만 추가한 것".

## CTest

`unit` 의 `test_extent_map.cpp` 가 extent 맵 로직을 덮는다.
