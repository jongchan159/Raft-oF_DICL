# `tests/` — 유닛 테스트와 셀프테스트 하네스

## 두 종류가 있다

| 대상 | 무엇 | 어떻게 돈다 |
|---|---|---|
| `raft_unit_tests` | doctest 기반 유닛 테스트 (`test_*.cpp`) | CTest `unit`. **CMake 전용** |
| `raft_selftest` | 단일 프로세스 안에서 3노드를 돌리는 하네스 (`raft_selftest_main.cpp`) | CTest `selftest` |

**`./build.sh` 에는 유닛 테스트 타깃이 없다.** `build.sh` 만 돌려서는 유닛
테스트 회귀를 잡을 수 없으므로, 코드를 고쳤으면 `ctest` 를 돌려야 한다.

## 불변식

- **두 바이너리 모두 protobuf를 링크하지 않는다.** `core/` + `blockio/` 만
  링크한다. CTest `isolation_raft_selftest` / `isolation_raft_unit_tests` 가
  강제한다. 이건 우연이 아니라 `raft_core_obj` 에서 `raft_protobuf` 를
  떼어내 만든 구조적 성질이다.
- 그게 가능한 이유는 `fake_transport.h` 다. `RaftTransport` /
  `BlockCopyClient` 의 Mock이라 소켓 없이 Raft 전체를 돌릴 수 있다.
  **새 테스트에서 실제 TCP를 쓰지 말 것** — 쓰는 순간 이 격리가 깨진다.
- `third_party/` 는 doctest 단일 헤더다. 손대지 않는다.

## 테스트를 쓸 때

기대값을 **코드에서 확인하고** 쓸 것. 예를 들어 `can_write_all` 은
`log_slot_map` 이 비어 있으면 크기와 무관하게 `true` 를 돌려준다(원본과
동일한 동작). "당연히 false겠지" 로 쓴 단정이 실제로 틀렸던 적이 있다.

## 전체 검증

```bash
cd build-cmake && ctest --output-on-failure    # 10개
```

`restart_follower` 는 **KNOWN GAP 2건이 GAP으로 남는 것이 정상**이고 그래도
종료코드 0이다 — 팔로워 재시작 catch-up 은 원본 `raft.go` 도 못 하는
설계 한계다 (DECISIONS.md §U5).
