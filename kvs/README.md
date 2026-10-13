# `kvs/` — DARE KVS 상태머신

DARE(Poke & Hoefler, HPDC'15)의 KVS 상태머신 **semantics** 를 raft-of 상태머신으로
옮긴 것이다. `-statemachine kvs` 로 켠다. 벤치마크는 `raft_client -op kvs-lat` /
`-op kvs-loop` (apps/README.md).

| 파일 | 역할 |
|---|---|
| `dare_kvs_sm.{c,h}` | DARE 원본. **빌드에 넣지 않는다** — 참조용, 수정하지 않는다 |
| `raft_kvs_wire.h` | 명령 와이어 포맷 + `encode_put/get/rm` / `parse`. `raft_client` 도 쓴다 |
| `raft_statemachine_kvs.h` | `KvsStateMachine : StateMachine` |

## DARE 와 같은 것 / 다른 것

같은 것: PUT 은 덮어쓰기, GET 은 값(없으면 빈 결과), RM 은 삭제. 키는 64B 칸에
NUL 로 끝나는 문자열. `kvs_size` = Σ(엔트리 헤더 66B + 값 길이).

다른 것:

- **len 이 uint16 → uint32.** 커맨드 상한 128 KiB (`kMaxCommandBytes`), 값 상한
  128 KiB − 72B. 레이아웃: `type@0 key@1(64B) pad@65(3B) len@68(u32 LE) data@72`.
  C 구조체를 memcpy 하지 않고 오프셋으로 읽고 쓴다.
- 자료구조: 1024 버킷 체이닝 → `unordered_map`. 값은 불변 버퍼(`shared_ptr<const>`).
- **형식 검사를 한다** (DARE 는 안 한다). 헤더 미만·상한 초과·모르는 type·
  NUL 없는 key·len 불일치·PUT 의 len 0 은 전부 **에러 없이 무시**하고 `ignored`
  를 센다. 이유: 그 명령은 이미 로그에 커밋됐다. 에러를 내면 `apply_internal` 이
  클라이언트에 실패로 돌려주고, 노드마다 다르게 "실패" 할 여지가 생긴다. 리더 탐색
  probe(`"\x00"`)와 `-op apply` 의 `'x'` 페이로드가 여기로 떨어진다.
- GET 은 로그에 넣지 않는다 — 아래.

## GET 은 로그를 타지 않는다

PUT/RM 은 `ClientApply` → 로그 → 커밋 → `apply_pending`. GET 은 `ClientRead` →
`Server::read` (Raft ReadIndex, Ongaro §6.4, `core/src/raft_read.cpp`): 리더십을
**AE 와 분리된** heartbeat 1라운드로 확인하고, `last_applied` 가 그때의
`commit_index` 를 넘으면 리더 상태머신에서 읽어 바로 돌려준다. DARE 의 "RDMA 로
리더십을 확인한 뒤 로컬에서 읽기" 와 같은 위치다.

## 불변식

- **헤더 온리.** `.cpp` 를 만들지 말 것 (DECISIONS.md §X R0). `raft_client` 가
  `raft_kvs_wire.h` 를 쓰지만 kvs 를 링크하지 않는 것도 이 덕분이다.
- **스스로 thread-safe 해야 한다.** apply 경로는 `Server::mu` 아래에서, read 경로는
  `Server::mu` 밖에서 부른다 (core/include/raft_server.h 의 락 규약). 내부 `mu_` 는
  O(1) 구간만 잡는다 — PUT 의 버퍼 생성과 GET 의 바이트 복사는 락 밖이다. 그래야
  128 KiB GET 이 `apply_pending` 을 막지 않는다.
- read 경로로는 **GET 만** 보낸다. core 는 명령 내용을 모르므로 PUT 을 read 로
  보내면 리더 상태머신만 바뀌고 로그에는 남지 않는다.
- 팔로워의 상태머신은 리더가 될 때까지 비어 있다 (core 의 현행 동작: 리더만 apply,
  승격 시 replay). 벤치마크에 failover 를 넣지 말 것.
