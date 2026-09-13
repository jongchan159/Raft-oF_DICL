# `core/` — Raft 합의 알고리즘

이 디렉터리가 프로토콜 본체다. 소켓도 protobuf도 모른다.

`include/` 는 선언, `src/` 는 정의. 헤더에 구현을 남기는 예외는 전부
DECISIONS.md §X "헤더에 구현을 남기는 유일한 이유들 (R0)" 에 이유와 함께
적혀 있다 — 새로 만들지 말고 그 목록에 먼저 추가할 것.

## 진입점

| 심볼 | 언제 도는가 |
|---|---|
| `Server::start` → `main_loop` | 상시 스레드 3개(main / apply / slot GC)를 띄운다. **읽기는 여기서 시작하는 것이 빠르다** |
| `Server::apply` | 리더가 클라이언트 명령을 받는 경로 |
| `Server::handle_append_entries_request` | 팔로워가 복제를 받는 경로 |
| `Server::advance_commit_index` | 커밋 판정 (리더의 `main_loop`가 매 바퀴 호출) |

## 불변식

- **`net/` 을 include하지 않는다.** 링크타임에도 `net/` 심볼을 요구하지 않는다.
  전송은 `raft_transport.h` 의 순수 가상 인터페이스(`RaftTransport` /
  `BlockCopyClient`)로만 닿고, 구현체는 `apps/` 가 주입한다.
- **protobuf를 링크하지 않는다.** 그래서 `raft_selftest` / `raft_unit_tests` 가
  protobuf 없이 링크된다. CTest `isolation_raft_selftest`,
  `isolation_raft_unit_tests` 가 이걸 강제한다.
- 의존 방향은 `core/ → blockio/` 한 방향이다. `raft_constants.h` 가
  `blockio/block_geometry.h` 의 `kSectorSize`/`kPageSize` 를 `SECTOR_SIZE`/
  `PAGE_SIZE` 로 재수출한다 — 값의 단일 출처는 `blockio/` 쪽이다.
- `raft_state.h` 는 `CachedFD` 를 **전방 선언으로만** 안다. 완전한 타입이
  필요한 `.cpp` 만 `blockio/cached_fd.h` 를 include한다. 되돌리면 core TU
  전부가 그 헤더를 파싱한다.
- `raft_basics.h` 는 **`.cpp` 를 가지면 안 된다.** `put_u64_le` 는 엔트리당
  4번 불리는 핫패스다.
- 락은 `Server::mu` 하나가 `raft`·`ring` 상태 전부를 보호한다. 전송 구현체
  내부 락과 `workers.inflight_mu` 는 **항상 `mu` 밖에서** 잡는다.

## 여기 두지 않는 것

소켓·protobuf 코드(`net/`), 블록 I/O(`blockio/`), 진입점(`apps/`),
스토리지 노드 서버(`storage/`).

## 왜 이렇게 됐는지

원본 Go(`raft.go`)와 다르게 만든 지점은 코드에 `[수정-N]` 으로 표시되어 있고
근거는 DECISIONS.md 의 D1~D14 에 있다. 미해결 항목은 §U.
