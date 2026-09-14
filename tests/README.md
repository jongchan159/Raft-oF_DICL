# `tests/` — 유닛 테스트

`raft_unit_tests` (doctest 기반, `test_*.cpp`). CTest 이름은 `unit`이고
**CMake 전용**이다 —— `./build.sh` 에는 유닛 테스트 타깃이 없으므로,
코드를 고쳤으면 `ctest` 를 돌려야 회귀를 잡는다.

단일 프로세스 안에서 3노드를 돌리던 `raft_selftest` 하네스는 **제거되었다.**
`io.identity_pba = true` 위에서만 성립하던 것이라, 그 모드와 함께 사라졌다.

## 불변식

- **protobuf를 링크하지 않는다.** `core/` + `blockio/` 만 링크한다.
  CTest `isolation_raft_unit_tests` 가 강제한다. 이건 우연이 아니라
  `raft_core_obj` 에서 `raft_protobuf` 를 떼어내 만든 구조적 성질이다.
- 그게 가능한 이유는 `fake_transport.h` 다. `RaftTransport` /
  `BlockCopyClient` 의 Mock이라 소켓 없이 Raft 전체를 돌릴 수 있다.
  **새 테스트에서 실제 TCP를 쓰지 말 것** —— 쓰는 순간 이 격리가 깨진다.
- `third_party/` 는 doctest 단일 헤더다. 손대지 않는다.

## 테스트를 쓸 때

기대값을 **코드에서 확인하고** 쓸 것. 예를 들어 `can_write_all` 은
`log_slot_map` 이 비어 있으면 크기와 무관하게 `true` 를 돌려준다(원본과
동일한 동작). "당연히 false겠지" 로 쓴 단정이 실제로 틀렸던 적이 있다.

extent 경계 로직(`persist_circular` 의 extent-skip, `append_entries` 의
extent clamp)은 **단편화된 파일에서만** 도는 코드라, `ExtentCache::from_extents`
로 가짜 다중 extent 맵을 만들어 `test_extent_map.cpp` 에서 덮는다.

## 전체 검증

```bash
cd build-cmake && ctest --output-on-failure
```
