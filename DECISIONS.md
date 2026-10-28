# 설계 결정 기록 (DECISIONS)

이 문서는 **"이 C++ 포팅이 원본 `~/RAFT/nvmeof_raft/raft.go`와 다르게 만들어진 지점과
그 이유"** 를 모아둔다. 이 디렉터리는 git 저장소가 아니므로 커밋 메시지가 없고, 그
역할을 이 문서가 대신한다.

- 코드에는 각 항목에 대응하는 **1줄 마커**만 남긴다: `/* [수정-3] ... — DECISIONS.md D3 */`
- 따라서 `grep -rn '\[수정\]' core net` 은 계속 동작한다 (HANDOFF.md §9가 지정한 검색 수단)
- 버그의 **발견 경위와 증상**은 HANDOFF.md §2에 서술형으로 남아 있다. 이 문서는
  **코드 위치별 계약**을 다룬다. 리팩토링 중 "이 줄을 왜 이렇게 뒀는가"를 물을 때 보는 곳

**리팩토링 규칙:** D 항목은 전부 실제로 재현된 버그를 근거로 한다. 함수를 쪼개거나
이름을 바꿀 때 **의미를 보존해야 하며, 되돌리면 안 된다.** R 항목은 그 반대다 —
한때 넣었다가 원인이 밝혀져 제거한 것이므로 **다시 넣으면 안 된다.**

---

## D1. `main_loop`은 상태별로 분기해야 한다

`core/src/raft_lifecycle.cpp` · `main_loop`

```
leader    : heartbeat()  ; advance_commit_index()
follower  : timeout()    ; advance_commit_index()
candidate : timeout()    ; become_leader()
```

`heartbeat()`는 상태를 보지 않고 `append_entries(nullptr)`를 쏜다. 매 바퀴 무조건
호출하면 팔로워도 하트비트를 보내고, 받은 쪽은 `reset_election_timeout()`을 한다 →
**모든 노드의 election timer가 영구히 갱신되어 선거가 한 번도 일어나지 않는다**
(term 0 고정, 리더 없음, `commit_index` 0 고정). 원본 `raft.go Start()`의 switch와 동일.

`scripts/smoke_test.sh`의 test 6(no runaway no-op growth)이 이 회귀를 감시한다.

## D2. `voted_for`는 `cluster[cluster_index].voted_for` 한 자리뿐이다

`core/include/raft_server.h` · `get_voted_for` / `set_voted_for`

원본 정의:
```go
func (s *Server) setVotedFor(id uint64) { s.cluster[s.clusterIndex].votedFor = id }
func (s *Server) getVotedFor() uint64  { return s.cluster[s.clusterIndex].votedFor }
```

별도의 `Server::voted_for` 필드를 두면 자기 투표 상태가 두 곳으로 갈린다:
`timeout()`은 `cluster[self].voted_for`에 쓰고(`become_leader`의 표 집계가 읽는 자리),
`set_voted_for()`는 별도 필드에만 쓴다(`handle_request_vote_request`의 grant 판정이 읽는
자리). 결과적으로 **후보가 된 노드도 `get_voted_for()`가 0이라 같은 term에 다른 후보에게
표를 또 준다** — 3노드에서 node1·node2가 둘 다 term 1의 리더가 되는 split brain 재현.

**별도 필드를 부활시키지 말 것.** 필드를 그룹화할 때도 이 값의 저장 위치는 하나여야 한다.

## D3. `accept()`로 얻은 fd에서 `SO_RCVTIMEO`를 반드시 지운다

`net/include/raft_tcp_server.h` · `net/include/raft_blockcopy_tcp_server.h` (두 곳에 **동일 계약**)

**Linux는 listen 소켓의 `SO_RCVTIMEO`/`SO_SNDTIMEO`를 accept된 소켓에 상속시킨다**
(이 환경에서 직접 확인). accept 루프가 `stop_flag`를 폴링하려고 listen fd에 200ms
타임아웃을 걸어두는데, 그게 커넥션에 붙으면 200ms 동안 요청 없는 커넥션마다 `read()`가
EAGAIN을 돌려주고 `serve_connection`이 이를 "연결 끊김"으로 보고 fd를 닫는다.

증상: 노드 간 롱리브드 RPC 커넥션이 200ms마다 죽어 **RequestVote(2~3초 간격)가 거의
항상 실패**했고 세 노드가 candidate에 머물렀다. 하트비트(100ms)는 타임아웃보다 촘촘해
살아남는 바람에 원인이 더 헷갈렸다.

> 현재 이 11줄 주석이 두 파일에 **verbatim 중복**이다. 두 accept 루프를 하나로
> 합치면(리팩토링 Phase 2-4) 이 계약도 한 자리로 모인다. 그때까지는 **한쪽만 고치면
> 즉시 갈라진다는 점**을 유의할 것.

## D4. election timeout 난수는 프로세스마다 다른 시드를 써야 한다

`core/src/raft_election.cpp` · `reset_election_timeout`

`std::rand()`는 `srand()`를 부르지 않으면 `srand(1)`과 같으므로 **모든 노드 프로세스가
완전히 동일한 난수 수열**을 뽑는다. 세 노드의 election timeout이 매 라운드 정확히
같아져 셋이 동시에 candidate가 되고, D2를 고친 뒤에는 서로에게 표를 주지 않으므로
**선거가 영원히 끝나지 않는다**(term만 계속 올라간다).

→ `std::mt19937_64` + `std::random_device` 시드. Go의 `math/rand`는 1.20부터 자동으로
랜덤 시드를 쓰기 때문에 원본에는 이 문제가 없다 — **C++ 포팅에서만 생기는 버그**.

배수: `interval = uniform(0, heartbeat_ms*10 - 1) + heartbeat_ms*20`, 즉 20x~30x.
(코드 위 주석이 "5x~10x"라고 서술하는 것은 원본 문서의 수치이며 실제 코드와 다르다 —
Phase 6에서 서술을 정정할 대상. **값 자체는 원본 그대로이므로 바꾸지 않는다.**)

## D5. 팔로워의 `commit_index` 갱신은 "엔트리가 실려 온" 블록 **밖**이다

`core/src/raft_handle_append_entries.cpp` · `handle_append_entries_request`

원본 `raft.go`의 `// Update commit index` 블록은 함수 레벨에 있다(들여쓰기로 확인).
`if (req.num_entries > 0 && ...)` 블록 **안**에 두면 `num_entries == 0`인 순수 하트비트가
팔로워의 `commit_index`를 못 올려서, **마지막 배치 이후의 커밋이 팔로워에 영원히
전달되지 않는다** — 리더 `commit_index=202`, 팔로워 `192`(마지막 배치 크기만큼 뒤처짐)로
고정되는 것으로 재현됐다.

`scripts/smoke_test.sh`의 test 3(commit_index 수렴)이 이 회귀를 감시한다.

## D6. Apply의 커밋 후 대기는 조건변수로 한다 (뮤텍스 쥔 채 sleep 금지)

`core/src/raft_apply.cpp` · `apply_internal`

원래는 100µs sleep을 2000번 도는 스핀 루프였고 `std::lock_guard`의 스코프가 루프 본문
전체라 **sleep 중에도 `collector->mu`를 쥐고 있었다.** `result_sink`는 `apply_pending`이
`s.mu`를 잡은 채로 호출하므로, sink가 `collector->mu`에서 막히면 **`s.mu`까지 같이 묶인다**
— 리더의 메인 루프 / AE / `advance_commit_index`가 전부 정지한다. 게다가 스핀 스레드가
매 바퀴 락을 즉시 재획득해(barging) sink가 계속 밀려나 상한(2000 × ~170µs)인 340ms까지
대기가 늘어났다. `apply-timed`에서 `total=340ms`인데 구간 합이 4ms였던 것이 이 현상이다.

**효과: 4064B 단건 Apply 224ms → 2.9ms (약 78배).** 성능 회귀 판정의 기준선이다.

## D7. `ReplSink`는 힙에 두고 `shared_ptr`로 넘긴다 (스택 금지)

`core/include/raft_server.h` · `append_entries(std::shared_ptr<ReplSink>)`
`core/src/raft_apply.cpp` · `apply_internal`

`append_entries`는 팔로워마다 fire-and-forget 스레드를 띄우고 즉시 리턴한다. 그 워커들은
호출자(`apply_internal`)보다 오래 산다. sink를 호출자 **스택에** 두면 `apply_internal`이
리턴한 뒤 워커가 죽은 프레임의 `sink->push()` / `sink->mutex_a_ns()`를 계속 호출한다.

실제 증상: 노드가 `double free or corruption (out)`으로 죽는다. `-op apply-timed`
(= 레이턴시 측정 경로)에서 5/5 재현. ASan이 `stack-use-after-return`으로 정확히 짚었다.
원본 Go는 GC가 sink를 살려주므로 이 문제가 없다 — **C++ 포팅에서만 생기는 클래스의 버그.**

각 워커 람다가 `shared_ptr`을 **값으로** 캡처해 마지막 워커가 끝날 때까지 살아 있게 한다.
`append_entries_worker(int, ReplSink*)`가 raw 포인터를 받는 것은 괜찮다 — 부모가 잡고
있는 동안만 유효하기 때문. **워커에 넘기는 소유권 모델을 바꾸지 말 것.**

## D8. `init_storage`는 tail/commit/applied를 헤더에서 복구하지 **않는다**

`core/src/raft_lifecycle.cpp` · `init_storage`

원본 `restoreCircular`은 헤더에서 `currentTerm`/`votedFor`만 복구하고
`tailLogIndex`/`tailSlot`/`commitIndex`/`lastApplied`를 읽는 네 줄을 **주석 처리해둔 채**
1/0/0/1로 되돌린다. 원본 주석:

> On restart, start with empty in-memory log. Leader election will resync all entries
> via appendEntries. The ring buffer on disk preserves data for PBA copy correctness.

네 값을 모두 복구하면 in-memory `log`는 sentinel 하나(size 1)인데 `tail_log_index`가
1보다 커져 `log_slice()`/`oldest_log_index()` 계산이 로그 벡터 범위를 벗어난다 —
재시작 노드가 첫 AppendEntries에서 바로 깨진다.

파일 헤더 레이아웃: `[0:7] currentTerm  [8:15] votedFor  [16:23] tailLogIndex
[24:31] tailSlot  [32:39] commitIndex  [40:47] lastApplied` (쓰기는 `raft_persist.cpp`,
읽기는 `raft_lifecycle.cpp` — **두 곳이 짝을 맞춰야 하는 암묵 계약이다**).

> 이 결정이 U5(팔로워 재시작 catch-up 불가)의 원인 (a)다. 고치려면 U5를 함께 봐야 한다.

> D18 이전에는 기동 시 ZERO_RANGE가 헤더까지 지워서, 위의 term/votedFor 복구가 실제로는
> 항상 0을 읽었다.

## D9. `create_ring_file`은 파일 전체에 `FALLOC_FL_ZERO_RANGE`를 건다

`blockio/cached_fd.cpp` · `create_ring_file`

원본은 매 기동마다 링 파일 전체에 ZERO_RANGE를 걸어 extent를 전부 "written"으로
뒤집는다. unwritten extent가 남으면 FIEMAP이 `FIEMAP_EXTENT_UNWRITTEN`을 보고하고
이후 첫 쓰기가 **파일시스템 저널을 통한 extent 상태 변환으로 직렬화된다** — 실서버
(FIEMAP 모드) 지연 측정에 직접 영향을 준다.

`identity_pba` 테스트 모드는 FIEMAP을 쓰지 않으므로 미지원 파일시스템에서도 넘어가게
인자로 분리했다: `create_ring_file(path, size, require_zero_range)`.

> 2026-10-24: 매 기동이 아니라 **새로 할당한 구간에만** 건다 — D18. 그 구간도 먼저
> `FALLOC_FL_WRITE_ZEROES` 로 할당하고, 지원되지 않을 때만 이 경로로 폴백한다 — D19.

## D10. 스토리지 클라이언트는 전용 락으로 보호한다 (`s.mu`로는 안 된다)

`net/src/raft_blkcopy_rpc_client.cpp` · `TcpBlockCopyClient`
(리팩토링 전: `core/src/raft_handle_append_entries.cpp`의 `blkcopy_client` / `blkcopy_reset`)

`do_pba_copy`는 **`s.mu`를 놓은 상태에서** 호출된다(`handle_append_entries_request`의
`mu.unlock()` 직후, Leader-Side에서는 `append_entries_worker`의 팔로워별 스레드마다).
게다가 여러 스레드가 동시에 들어온다. 그래서 커넥션 핸들을 `s.mu`로는 보호할 수 없다.

지금은 그 락과 lazy connect가 `TcpBlockCopyClient` 안에 있다. 예전에는
`Server::blkcopy_rpc` + `Server::blkcopy_mu` + `Server::blkcopy_client()` 였다.

**락 순서: 전송 구현체 내부의 락은 항상 `s.mu` 밖에서 잡힌다.**

> 관련 결함: RPC 실패 시 커넥션을 무효화하지 않는다 (U6).


## D11. `tail_slot` 전진은 엔트리 스킵 여부와 무관하게 항상 한다

`core/src/raft_handle_append_entries.cpp` · `handle_append_entries_request`

건너뛴 엔트리도 링에서 슬롯을 차지한다. 전진을 `if` 안에 두면 실제로 append되는 첫
엔트리가 배치 시작 슬롯(`req.start_slot`)을 자기 ring_slot으로 받아버려 **슬롯 매핑
전체가 밀리고**, lazy load가 다른 엔트리의 바이트를 읽는다.

## D12. `prev == 0`일 때 `prev - 1` 언더플로를 막는다

`core/src/raft_append_entries.cpp` · `append_entries_worker` (fast log backoff)

`prev == 0`이면 `prev - 1`이 uint64 언더플로로 `UINT64_MAX`가 되고, 뒤의
`max_u64(.., 1)` / `max(floor)`가 그걸 그대로 통과시켜 `next_index`가 `UINT64_MAX`로
튄다. 다음 라운드의 `next > last + 1` 클램프가 복구해주긴 하지만 한 라운드를 낭비한다.

같은 블록의 backoff floor guard는 **원본 그대로이며 안전 불변식이다**:
```
floor = cluster[fi].match_index + 1
next_index = max(max(new_next, 1), floor)
```
> "Never back off past matchIndex+1: the follower already confirmed entries up to
> matchIndex, and those PBA slots may have been freed by tier 1."

**이 guard를 풀지 말 것** — GC된 슬롯을 PBA로 읽어 stale 데이터를 복제하지 않는다는
보장이다. 푸는 것은 U5의 교착을 `[SKIP PBA]`로 옮기는 것에 불과하다.

## D13. `read_entry_direct`는 슬롯 0의 명령 바이트 480B를 먼저 복사한다

`core/src/raft_pba.cpp` · `read_entry_direct`

`persist_circular`의 인코딩은 슬롯 0의 `[0:ENTRY_META_SIZE)`에 메타를,
`[ENTRY_META_SIZE:SECTOR_SIZE)`에 명령의 첫 `512-32 = 480`바이트를 넣는다. 이 480바이트를
건너뛰고 슬롯 1부터 `cmd[0]`에 채우면 (1) 1슬롯 엔트리(`cmd_len <= 480`)는 명령이 전부
0이 되고 (2) 여러 슬롯 엔트리는 480바이트씩 밀린 채 뒤가 잘린다. `become_leader()`가
deferred 엔트리를 로드할 때 이 경로를 쓰므로 **승격 시 로그가 손상됐다.**

엔트리 헤더 레이아웃: `[0:7] Term  [8:15] CmdLen  [16:23] NumSlots  [24:31] LogIndex`
(쓰기는 `raft_persist.cpp`, 읽기는 여기 — **짝을 맞춰야 하는 암묵 계약**).

**selftest T1이 경계값 `{1,32,479,480,481,512,1000,4064,4065,9000}`으로 이 경로를 직접
검증한다.** 온-디스크 레이아웃을 만지는 리팩토링은 T1이 안전망이다.

## D14. `memcpy(dst, nullptr, 0)`을 피한다 (UBSan)

`core/src/raft_persist.cpp` (no-op 엔트리의 빈 command),
`net/include/raft_wire_codec.h` (반환값 없는 RPC의 빈 body)

동작은 무해했지만 형식상 UB라 UBSan이 잡는다. 빈 경우를 건너뛰게 했다 — 앞으로 진짜
문제가 이 노이즈에 묻히지 않도록.

---

## 원본에 없지만 C++ 포팅에 필요해서 추가한 것

| 항목 | 위치 | 이유 |
|---|---|---|
| `inflight_threads` + `spawn_replication_thread` / `join_all_replication_threads` | `core/include/raft_server.h` | 원본은 goroutine을 fire-and-forget으로 띄우고 결과를 안 기다린다. `std::thread`는 `detach()`하면 `this`를 참조하는 스레드가 서버 파괴 후에도 남아 use-after-free가 된다. join도 detach도 아닌 절충: 워커가 스스로 완료 플래그를 세우고, 다음 spawn 때 완료된 것만 join·제거하며, `stop()`에서 남은 전부를 강제 join. **`joinable()`은 "아직 join 안 했나"만 알려주지 "실행이 끝났나"는 알려주지 않는다** — 첫 구현에서 이 둘을 혼동해 reap가 아무것도 지우지 않는 버그가 있었다 |
| `~Server()`가 `stop()` 호출 | `core/include/raft_server.h` | join 안 된 `std::thread`가 남은 채로 파괴되면 소멸자가 `std::terminate()`를 부른다 (테스트 중 실제 재현) |
| `class AlignedBuffer` | `core/include/raft_server.h` | `std::vector`는 생성자에서 데이터를 복사할 때 `std::allocator`(malloc)로 새로 할당하므로 `posix_memalign` 정렬을 못 지킨다 → O_DIRECT `pwrite`가 EINVAL. 이 타입은 정렬된 메모리를 복사 없이 소유한다 |
| `done`을 `std::atomic<bool>`로 | `core/include/raft_server.h` | 원본은 plain bool. 메인 루프 / apply 워커 / slot GC 워커가 `s.mu` 밖에서 읽는다 (값 의미는 동일) |
| `log_trim_threshold` (기본 8192) | `core/include/raft_server.h` | `Server::log`는 `push_back`만 하고 축소되지 않아 링이 순환하는 동안 in-memory 벡터가 무한히 자란다. `do_slot_gc`와 같은 기준(min matchIndex, last_applied)으로 앞쪽을 잘라낸다. **`0`이면 원본 동작** |
| `loop_sleep_us` (기본 200) | `core/include/raft_server.h` | 원본은 sleep 없이 스핀하는데, 그러면 `advance_commit_index`가 매 바퀴 `s.mu`를 잡아 AE 스레드를 굶는다. **`0`이면 원본과 동일한 스핀**. D21 이후 커밋은 AE 응답 경로에서도 판정하므로 이 값은 커밋 지연에 들어가지 않는다 |
| `gc_has_run` | `core/include/raft_server.h` | `gc_up_to = 0`이 "GC 미실행"과 "index 0까지 GC 완료"를 구분 못 하므로, `gcUpTo` fast-path를 최초 1회 전까지 스킵하는 가드 |
| `skip_pba_last_next` / `skip_pba_last_log` 스로틀 | `core/include/raft_cluster.h` | 원본 `logSkipPBADiag`는 매 하트비트마다 무조건 찍어 팔로워당 초당 10줄이 나온다. 같은 `(follower, next)`는 1초에 한 번만 |
| `NUM_PAGES` / `MaxAppendEntriesBatch(Bytes)`를 런타임 변수로 | `core/raft_constants.{h,cpp}` | 원본은 package-level var 또는 constexpr. 벤치마크/테스트가 재컴파일 없이 override해야 한다. **기본값을 건드리지 않으면 동작은 원본과 동일** |
| `-debug` 상태 덤프, `[rpc-error]` 로그, `log_skip_pba_diag` 호출부, 스모크의 non-zero 검사 | HANDOFF §10 | 디버깅에 실제로 필요했던 관측 수단. 전부 유지할 가치가 있다 |

---

## R. 넣었다가 되돌린 것 — **다시 넣지 말 것**

둘 다 D1(`main_loop`에 상태 분기가 없던 시절)의 증상을 보고 넣은 보상 코드였다.
원인이 밝혀졌으므로 원본대로 되돌렸다.

### R1. `become_leader`에서 `cluster[i].voted_for = 0` 리셋
`core/src/raft_election.cpp` · `become_leader`

원본에 없다. `become_leader`는 candidate 상태에서만 호출되고 승격 즉시 state가 Leader가
되므로 재진입 자체가 없다. 다음 선거에서 `timeout()`이 전부 0으로 되돌린다.

### R2. `heartbeat`에서 리더의 `reset_election_timeout()`
`core/src/raft_election.cpp` · `heartbeat`

원본에 없다. "트래픽 없는 리더가 스스로 강등된다"고 보고 넣었지만 그것도 D1의 증상이었다.
원본처럼 리더는 `timeout()`을 아예 호출하지 않으므로 `election_timeout`이 만료돼도
강등되지 않는다.

---

## Q. 해소된 질문

### Q1. 로그 인덱스 규약 — 엔트리는 **인덱스 1부터**, `log[0]`은 sentinel

원본 `restoreCircular`과 대조해 확정했다:
```go
s.tailLogIndex = 1
s.tailSlot     = 0
s.commitIndex  = 0
s.lastApplied  = 1
```
즉 `init_storage`의 초기값 선택(`tail_log_index = 1`)이 원본과 정확히 같다.

**"인덱스를 0-based로 쓰기로 결정했다"는 기존 주석 서술은 오기다.** `core/include/raft_server.h`의
`gc_has_run` 주석에 그 오기가 아직 남아 있다 — Phase 6 정정 대상(S9).

---

## D15. blockcopy 측정 도구를 지연용에서 처리량용으로 **교체**했다

`apps/raft_blkcopy_scale_main.cpp` · `scripts/blkcopy_scaling.sh`
(제거: `apps/raft_blkcopy_bench_main.cpp` · `scripts/blkcopy_latency.sh`)

기존 도구는 지연 전용 설계였다. `BATCH=1` 과 `COPY_WORKERS=1` 이 계약이었고
벤치 usage 가 그 조건을 명시했다 — "otherwise copy_nanos is a sum across
workers, not a wall time". 새로 재려는 것은 **처리량과 포화점**(W × B × chunk)
이라 그 계약을 정면으로 깬다. 한 파일에서 두 계약이 충돌하므로 확장이 아니라
교체를 택했다.

**바이너리 이름을 `raft_blkcopy_bench` → `raft_blkcopy_scale` 로 바꾼 것도
의도적**이다. `/home` 이 세 호스트 공유라, 같은 이름을 재사용하면 예전 빌드
산출물과 새 것을 구분할 수 없다.

같이 정리한 것: 기존 arm 표(`local`/`fabric-tcp`/`fabric-ipoib`/`fabric-rdma`/
`host`)가 스크립트에 **하드코딩**되어 있었고 클러스터 구성이 바뀌자 문서와 함께
낡았다(`10.0.0.90`/`bcopy-a`/`port 3` 이 실제와 달랐다). 새 스크립트는 호스트·
경로·스윕 범위를 전부 환경변수로 받아 같은 일이 반복되지 않게 했다.

지연 측정 능력은 잃지 않는다 — W=1, B=1 이 지연 코너이고 새 도구도 wall 의
p50/p99 를 보고한다.

설계상 유지한 것 두 가지:
- **처리량은 언제나 wall 기준**이다. `copy_nanos` 는 워커별 시간의 합이라
  (`storage/raft_blockcopy_server.cpp:284`) 분모로 쓰면 안 된다
- `-workers` 는 **기록 전용**이다. 벤치는 서버의 실제 `-copy-workers` 를 알 수
  없으므로, 드라이버가 서버 기동 로그의 `copy-workers : N` 과 대조한다
  (불일치면 그 지점을 실패시킨다)

## D16. DARE KVS 는 C 코드가 아니라 semantics 를 옮겼다 (2026-10-13)

`kvs/raft_statemachine_kvs.h` · `kvs/raft_kvs_wire.h` (원본: `kvs/dare_kvs_sm.{c,h}`, 빌드 제외)

DARE 의 C 파일을 그대로 링크하지 않았다. `dare.h` 가 `#define PAGE_SIZE 4096` 을
해서 `core/include/raft_constants.h` 의 `constexpr PAGE_SIZE` 와 충돌하고,
`kvs_cmd_t` 의 `data[0]` 과 `##__VA_ARGS__` 매크로를 C++ 에 들여야 하며, 빌드에 C
언어 지원과 `$CC` 분기가 필요해진다. PUT/GET/RM 의미와 와이어 레이아웃만 C++
헤더 온리로 옮겼다 (`[수정-16]`).

DARE 와 일부러 다르게 한 것:
- **len uint16 → uint32**, 커맨드 상한 128 KiB. 레이아웃은 `len@68`, `data@72`.
- **형식이 틀린 명령은 에러 없이 무시한다.** 그 명령은 이미 로그에 커밋됐으므로,
  에러를 내면 `apply_internal` 이 클라이언트에 실패로 돌려준다. 리더 탐색 probe
  (`"\x00"`)와 `-op apply` 페이로드가 `-statemachine kvs` 에서 여기로 떨어진다.
- 값은 불변 버퍼(`shared_ptr<const>`). read 경로가 `Server::mu` 밖에서 상태머신을
  부르므로(D17) GET 이 내부 락을 O(1) 만 잡게 하려는 것이다.
- DARE 의 snapshot 함수(`create/apply_kvs_snapshot`)는 옮기지 않았다. raft-of 에
  스냅샷 경로가 없고, 원본은 포인터 산술이 틀렸다 (`kvs_snapshot += sizeof(...)`).

## D17. GET 은 ReadIndex 로, 리더십 확인은 AE 와 분리된 heartbeat 로 (2026-10-13)

`core/src/raft_read.cpp` · `RaftTransport::read_index_heartbeat` · `Server.ClientRead`

GET 은 로그를 타지 않는다 — Ongaro 학위논문 §6.4 ReadIndex 그대로다:
read_index = max(commit_index, term_start_index) → heartbeat 1라운드 과반 ack →
last_applied > read_index 대기 → 상태머신에서 읽기. `term_start_index` 는
`become_leader` 의 no-op 인덱스다 (§6.4 1단계, `[수정-17]`).

**heartbeat 를 AppendEntries 에 싣지 않은 이유:** 팔로워당 AE 워커가 하나이고
(persistent worker) 피어 커넥션도 하나를 `call_mu` 로 직렬화한다. 빈 AE 로 확인하면
진행 중인 데이터 AE — 팔로워의 blockcopy 왕복을 포함한다 — 가 끝날 때까지 줄을
서므로, mixed workload 에서 GET 이 PUT 의 로그 비용을 간접적으로 지불한다. 그래서
etcd/raft 의 `MsgHeartbeat`(≠ `MsgApp`) 처럼 별도 메시지(`ReadIndexHeartbeat`)를
**별도 워커(`read_hb_workers`)와 별도 커넥션(`RpcRaftTransport` 의
`kReadHbChannel`)**으로 보낸다. 팔로워 처리는 AE 핸들러의 term 처리와 같고
election timer 도 리셋한다 — 로그 일관성 검사가 없는 진짜 heartbeat 다.

배치: 라운드가 진행 중일 때 들어온 읽기는 워커의 pending 으로 합쳐지고, 다음
heartbeat 가 그 시점의 최신 `read_round` 를 실어 한꺼번에 확인한다 (DARE 의 "배치당
한 번의 리더십 확인"). `read_round` 는 term 이 바뀌어도 리셋하지 않는다 — 그래야
멤버별 `read_ack_round` 에 term 을 붙이지 않아도 이전 term 의 ack 가 새 term 읽기를
확인해 주지 않는다.

**상태머신을 `mu` 밖에서 부른다.** read 경로의 마지막 단계(상태머신 호출과 결과
복사)를 `mu` 아래 두면 128 KiB GET 이 PUT 의 commit/apply 경로를 막는다. 그래서
`Server::read` 가 `mu` 를 잡는 구간은 전부 O(1) 이고, 락 규약(`raft_server.h`)을
"read 경로의 상태머신 호출은 mu 밖 → 상태머신은 스스로 thread-safe" 로 바꿨다.
읽은 상태가 read_index 보다 새로울 수 있지만 그것도 커밋된 엔트리라 linearizable 하다.
read 경로로 들어오는 명령은 상태를 바꾸지 않아야 한다 (core 는 검사할 수 없다).

함께 고친 것: `Server::start()` 가 `main_thread` 를 팔로워별 워커 생성 **전에**
띄우고 있었다 (리더로 시작하는 하네스에서 빈 `ae_workers` 를 인덱싱한다). 순서를
바꾸고 `append_entries()` 에 가드를 넣었다. `stop()` 은 워커 `mu` 를 잡은 뒤
notify 한다 (predicate 확인과 wait 사이로 notify 가 지나가는 lost wakeup 방지).

## D18. ZERO_RANGE 는 **새로 할당한 구간에만** 건다 (2026-10-24)

`blockio/cached_fd.cpp` · `create_ring_file` · `core/src/raft_lifecycle.cpp` · `init_storage`

원본(D9)은 매 기동마다 링 파일 전체에 ZERO_RANGE 를 건다. 이 포팅은 fallocate 로
**방금 늘린 구간**에만 건다. 새 파일이면 그게 파일 전체이고, 크기가 충분한 기존 파일이면
아무것도 하지 않는다. 이유는 넷이다.

1. **헤더가 지워졌다.** `init_storage` 는 `create_ring_file` 을 부른 **뒤에** 헤더(오프셋 0,
   512B)에서 term/votedFor 를 읽는다 (D8). 오프셋 0부터 걸면 읽기 직전에 지워져 복구가
   항상 term=0, votedFor=0 을 읽었다. Raft 는 votedFor 영속을 요구한다 — 잃으면 재시작한
   노드가 같은 term 에 두 번 투표할 수 있다. 실험에서 드러나지 않은 이유는 E2E §8 이 매
   실행 전에 링 파일을 지우기 때문이다.
2. **원래 목적과 반대로 작동한다.** 목적은 extent 를 written 으로 만드는 것인데, ext4 에서
   ZERO_RANGE 는 extent 를 unwritten 으로 만든다 (U11). 매 기동마다 걸면 이전 실행이 써서
   written 이 된 extent 를 되돌리므로, 링을 미리 써 두거나 워밍업한 효과가 다음 기동에서
   사라진다.
3. **정확성에 기여하는 것이 없다.** 재시작한 노드는 빈 로그로 시작하고 (D8), 링을 읽는
   `read_entry_direct` 는 디바이스를 PBA 로 직접 읽는다. 파일시스템 메타데이터만 바꾸는
   ZERO_RANGE 는 디바이스의 이전 바이트를 지우지도 못한다.
4. **extent 위치가 재시작 전후로 같다는 것을 코드가 보장한다.** 파일시스템이 zero range 를
   "해제 후 재할당"으로 구현하더라도 이미 있던 구간에는 걸지 않으므로 상관없다. push 모델의
   "팔로워가 재시작해도 extent map 이 같다" 는 전제가 여기에 기댄다.

측정에 미치는 영향: E2E §8 절차(매 실행 전에 링 파일 삭제)에서는 매번 새 파일이므로
동작이 같다. 링 파일을 남겨 두고 재시작하면 이전 실행에서 written 이 된 extent 가 그대로
남아 첫 랩 `LPersist` 가 달라진다 — 그런 실행은 그 조건을 함께 기록할 것.

D8 의 결정(tail/commit/applied 는 복구하지 않음)은 그대로이므로 U5 는 풀리지 않는다.

**실클러스터 검증 전이다.** 링 파일을 지우지 않고 노드를 재시작해 (1) 기동 배너의
`term/tail : term=` 이 이전 값으로 나오는지, (2) `filefrag -v` 출력이 재시작 전후로
같은지 확인할 것.

## D19. 새로 할당하는 링 구간은 `FALLOC_FL_WRITE_ZEROES` 로 (2026-10-24)

`blockio/cached_fd.cpp` · `create_ring_file` · `core/src/raft_lifecycle.cpp` · `apps/raft_node_main.cpp`

D9 의 목적은 새 링 파일의 extent 를 written 으로 만들어 첫 쓰기의 unwritten→written
변환을 없애는 것인데, ZERO_RANGE 로는 이뤄지지 않는다 (U11). Linux 6.17 에 들어간
`FALLOC_FL_WRITE_ZEROES` 가 바로 그 일을 하는 모드다. 범위를 0 이 쓰인 **written** extent 로
할당하고, 0 쓰기는 디바이스의 Write Zeroes(+deallocate) 로 처리하므로 데이터를 실제로
보내지 않는다.

- `create_ring_file` 은 늘린 구간(새 파일이면 전체)을 먼저 WRITE_ZEROES 로 할당한다.
  KEEP_SIZE 와 함께 쓸 수 없으므로 파일 크기는 이 호출이 늘린다.
- 파일시스템이나 디바이스가 지원하지 않으면 `EOPNOTSUPP`(또는 `EINVAL`)이 온다. ext4 는
  디바이스가 빠른 zeroing(unmap write zeroes)을 지원할 때만 이 모드를 받아 준다. 그때는
  예전 경로(fallocate + ZERO_RANGE, D18)로 폴백한다. 그 밖의 오류(ENOSPC, EIO)는 그대로
  던진다 — 폴백해도 같은 이유로 실패하거나 문제를 가린다.
- 어느 쪽으로 할당됐는지 기동 배너의 `ring alloc` 줄에 남는다. **측정 결과와 함께
  기록할 것** — 폴백이면 U11 워밍업이 여전히 필요하다.

효과: 새 링부터 extent 가 written 이라 첫 랩 `LPersist` 가 부풀지 않고 워밍업이 필요 없다.
또 unwritten 블록에 4 KiB 미만으로 O_DIRECT 쓰기를 할 때 커널이 블록의 나머지를 0 으로
채우는 동작이 일어나지 않는다. 그래서 팔로워 시절 raw 로 받은 엔트리가 리더 전환 뒤의
파일 경유 쓰기에 덮일 수 있는 경로도 막힌다 (커널 동작에 근거한 추론, 미검증).

측정 비교 주의: D19 이전 수치는 unwritten 링에서 첫 랩을 워밍업으로 버린 뒤 잰 것이다.
워밍업 뒤의 정상 상태 수치는 같아야 하지만, 첫 랩을 포함한 비교는 조건이 다르다.

기존 링 파일은 D18 대로 건드리지 않으므로 written 으로 바뀌지 않는다. 한 번 지우고 다시
만들 것 (E2E §8 은 어차피 매 실행 전에 지운다).

**실클러스터 검증 전이다.** 기동 배너의 `ring alloc` 이 `FALLOC_FL_WRITE_ZEROES` 로 나오는지,
`filefrag -v <링 파일>` 에 `unwritten` 플래그가 없는지 확인할 것.

## D20. push 복제 모드 (진행 중, 2026-10-25 ~ 2026-10-27)

`core/src/raft_push.cpp` · `-mode push` · `-push-devices` · `AppendEntriesResponse.push_map_hash` ·
`AppendEntriesRequest.need_extent_map` · `CachedFD::open_device` / `write_at_logical` · `persist_circular` ·
`push_gate` / `push_covers` → `AppendEntriesRequest.data_already_copied` · `IoWorker`(`core/include/raft_io_worker.h`)

원본에 없는 세 번째 복제 정책이다. 리더가 자기 볼륨에 로그를 쓸 때 push 팔로워의
볼륨에도 직접 쓰고, 그 팔로워의 쓰기와 flush 가 끝나면 그 팔로워에게 AE 를 보낸다. AE 배치가
이미 push 한 구간이면 `data_already_copied` 를 실어 팔로워가 pull 을 건너뛴다. AE 가 쓰기 뒤에
나가므로 AE 자체가 "네 볼륨에 썼다"는 알림이다 — 처음 설계에 있던 OK 채널은 두지 않았다(아래
"OK 는 AE 가 대신한다"). 여러 단계로 나눠 들어오고 있다. **지금은 대상 인식, 맵 받기,
리더의 push 쓰기, push 된 배치의 pull 건너뛰기, 대상별 비동기 push(8단계 1부)까지 있다.**
push 는 대상별 워커가 하고 Lock A 는 기다리지 않는다. 그래서 먼저 끝난 대상은 다른 대상을
기다리지 않고 바로 AE 를 받는다. 남은 것(2부)은 리더 자기 쓰기도 `mu` 밖으로 빼서, push 가 자기
쓰기보다 먼저 끝난 대상에게는 자기 쓰기를 기다리지 않고 AE 를 보내는 것이다.

**누가 push 대상인가.** 각 노드의 `-mode push` 가 후보 자격이다. 정적 목록은 없다.
- 팔로워는 모든 AE 응답(거절 포함)에 `push_map_hash` 를 싣는다. 0 이면 push 모드가 아니고,
  그 밖의 값이면 push 모드이면서 그 값이 자기 링의 extent 맵 해시다
  (`ExtentCache::mapping_hash`, 인접 extent 를 합친 뒤 FNV-1a).
- 리더는 이 값을 `cluster[fi].push_map_hash` 에 기록한다(`note_push_response`).
  `become_leader` 에서 0 으로 리셋해 이번 term 의 응답으로 다시 알게 한다. 이 리셋은
  **no-op 을 쓰기 전에** 해야 한다 — 이전 리더십 때의 낡은 정보로 no-op 이 push 되지 않게
  하기 위해서다(no-op 은 pull 로 간다).
- push 모드 리더는 조건을 만족하는 팔로워 **전원**에게 push 한다(`is_push_target`).
  조건은 팔로워가 push 모드라고 알렸고, 이 호스트에 그 볼륨 장치가 매핑돼 있고, 받아서
  설치한 맵의 해시가 지금 알린 해시와 같은 것이다. 매핑이 없으면 한 번 경고하고 pull 로
  남긴다.

**왜 해시인가.** D18 이후로 링 파일은 재시작해도 그대로라 맵이 바뀌지 않지만, 링 파일을
지우고 다시 만들면 바뀐다. 해시 하나로 "push 후보인가" 와 "받아 둔 맵이 아직 맞는가" 를
함께 알린다. 정규화하므로 extent 레코드가 쪼개지기만 하면 해시는 그대로다.
- **한계: 해시는 바뀐 뒤에야 안다.** 팔로워가 링 파일을 지우고 다시 만드는 동안(그 팔로워가
  내려가 있는 동안) 리더는 새 해시를 받을 수 없으므로 받아 둔 옛 맵으로 계속 push 한다 — 즉
  옛 물리 블록에 쓴다. 그 블록이 새 링 파일이나 다른 파일에 다시 할당됐으면 그 내용을 덮는다.
  대상별 비동기 push(8 단계 1 부) 이후에는 큐에 남은 job 까지 옛 맵으로 쓴다. 운영 규칙으로
  막는다: **리더가 떠 있는 동안 팔로워의 링 파일을 지우지 않는다**(E2E §8 정리는 클러스터를 모두
  내린 뒤에 한다).

**맵은 한 번만 받는다 (`need_extent_map`).** 리더는 팔로워가 알린 해시가 설치한 맵과 다를 때만
AE 요청에 `need_extent_map` 을 싣는다(heartbeat 포함). 팔로워는 자기 링의 extent 목록과 볼륨
wwid 를 응답에 싣는다. 리더는 설치 전에 세 가지를 대조한다(`push_map_reject_reason`).
- 받은 맵으로 계산한 해시가 알린 해시와 같은가
- 맵이 덮는 끝이 자기 링 파일 크기와 같은가(`-ring-pages` 가 노드마다 다르면 깨진다)
- **자기 호스트에서 매핑한 장치의 wwid 가 팔로워 볼륨의 wwid 와 같은가.** 해시 검사로는
  잡지 못한다 — 맵은 팔로워가 보낸 진짜 맵이라 해시가 맞는다. `-push-devices` 의 경로가
  엇갈렸을 때 다른 멤버의 볼륨에 쓰는 것을 이 대조가 막는다. 기동 배너의 수동 대조를
  자동으로 하는 것이다.

통과하면 그 팔로워의 `open_device` 핸들에 `set_extents` 로 설치한다. 설치는 그 팔로워의 push
워커 **줄에 세워** 한다 — push job 은 `mu` 없이 워커 스레드에서 맵을 읽으므로, Lock C(`mu`
아래)에서 바로 바꾸면 쓰는 중인 job 과 겹친다. 줄에 세우면 앞서 맡긴 job 은 옛 맵으로 끝까지
쓰고(결과는 구간 비우기로 버려진다) 뒤에 맡긴 job 은 새 맵을 쓴다. 큐가 꽉 차 있으면 이번엔
설치하지 않고 다음 AE 가 다시 요청한다. 캐시 해시
(`push_map_cached_hash`)를 **선거가 바뀌어도 유지**한다 — 같은 해시를 다시 알리면 받지
않는다. 실패하면 `push_map_rejected_hash` 를 기록해, 같은 맵을 매 AE 마다 다시 요청하지
않게 한다. 메시지에는 blockio `Extent`(`blockio/cached_fd.h`)를 그대로 싣고, 그래서 `raft_entry.h`
가 `cached_fd.h` 를 include한다. 처음에는 core TU 가 그 헤더 전체를 파싱하지 않도록 구조체만
`blockio/extent.h` 로 떼어 두었으나, 2026-10-27 에 `cached_fd.h` 로 합쳤다(사용자 결정).

**push 쓰기 (`persist_circular`, 8단계 1부 2026-10-27).** 엔트리를 쓰는 호출에서만, 엔트리 루프
앞에서 대상을 한 번 정한다(`push_write_targets`). 쓰기는 대상마다 상시 push 워커(`IoWorker`,
`workers.push_io`)에 job 하나로 맡기고 **기다리지 않는다**. job 은 같은 바이트를 같은 논리
오프셋으로 대상 장치에 쓰고(`write_at_logical` — 팔로워 맵으로 PBA 를 구하고 팔로워 extent
경계에서 쪼갠다), flush(`fdatasync_device`)한 뒤 `finish_push_job` 으로 끝난 구간을 늘리고 그
대상의 AE 워커만 깨운다.
- **순서**: 루프에서 `flush_run` 은 run(링에 pwrite 한 번으로 쓰는 연속 구간 — 보통 호출당 하나,
  wrap·extent 건너뛰기 때만 여럿)을 쓰지 않고 목록에 적기만 한다. 루프 뒤에 job 을 먼저 맡기고
  그다음 자기 run 을 쓴다 — 그래야 둘이 겹친다. 대상이 없는 호출은 예전 그대로다.
- **버퍼**: job 은 호출이 끝난 뒤에도 버퍼를 읽으므로 공유 소유로 넘긴다(`ring.push_bufs` 풀,
  `acquire_push_buffer`). 어떤 job 도 들고 있지 않은 것을 재사용하고 run 사이에 비우지 않는다.
- **워커**: 이 호스트에 장치가 매핑된 멤버마다 `start()` 가 하나 띄운다(멤버와 매핑은 기동 때
  고정). 넣은 순서대로 하나씩 돌리므로 대상마다 완료가 인덱스 순서로 온다. 큐가 필요한 이유는 Lock
  A 가 push 를 기다리지 않아 같은 대상의 push 가 겹칠 수 있어서다(동시 클라이언트, 느린 팔로워).
  큐 상한(16)을 넘으면 그 대상은 너무 뒤처졌거나 멈춘 것이라 1 초 쉬고 pull 로 간다. 일을 마친 뒤
  1 ms 동안은 spin 하며 다음 일을 기다린다 — cv 깨우기 지연이 NVMe-oF 쓰기 한 번과 같은 규모라서다.
  워커는 job 을 큐 락을 놓은 채로 돌린다(job 이 끝에서 `mu` 를 잡고, persist 는 `mu` 를 쥔 채
  submit 한다 — 반대 순서면 교착).
- **헤더는 push 하지 않는다.** term/votedFor 등 그 노드 자신의 영속 상태라, 팔로워 링에
  쓰면 그 팔로워의 투표 기록을 덮어 재시작 후 같은 term 에 두 번 투표하게 만든다.
- **계측**: `nvme_ns`(= `LPersist`)는 자기 링 쓰기만이다. push 가 Lock A 뒤로 삐져나온 시간(그
  팔로워의 push 끝 − Lock A 종료, 0 이상)은 Leader-Side 의 "AE 전 복사" 자리에 넣어
  `derive_apply_timings` 가 같은 분기로 계산한다: StorageIO = push 대기, QuorumWait = replicate −
  push 대기 − AE_RT. 측정 상자(`ReplSink`)는 persist 를 거쳐 job 이 들고 간다.
- 대상 하나가 실패해도 리더 자신의 persist 는 실패시키지 않는다. 그 대상은 1 초 동안 push 를
  쉬고 pull 로 간다(`note_push_failure`). 이제 멈춘 대상은 Lock A 도 다른 대상도 막지 않는다.
- 이전(5 단계까지)에는 Lock A 안에서 자기 쓰기 → 대상 1 → 대상 2 를 차례로 쓰고 flush 해서
  `LPersist` 가 그 합이었고, 먼저 끝난 대상도 가장 느린 대상을 기다렸으며, 멈춘 대상이 I/O 실패까지
  Lock A 전체를 멈췄다.

**push 기록 (팔로워마다 한 벌, `ClusterMember`, `mu` 로 보호).**
- **끝난 구간** `[push_from, push_upto]`: 이 리더가 이 팔로워 장치에 쓰고 flush 까지 마친 **연속**
  로그 인덱스 구간(0 = 없음). push job 이 끝날 때 늘어난다(`record_push_completion` → `note_pushed`).
  AE 배치가 이 안에 통째로 들 때만 `data_already_copied` 를 세운다(아래 pull 건너뛰기). 시작값이
  필요한 이유는 push 가 로그 중간부터 시작하기 때문이다(맵 전의 no-op 등은 push 되지 않았다).
- **맡긴 구간** `[push_sub_from, push_sub_upto]`: 워커에 맡긴(쓰는 중이거나 끝난) 구간. persist 가
  job 을 맡길 때 늘린다(`note_push_submitted`). AE 워커가 "쓰는 중이니 기다린다"를 판정하는 데 쓴다.
- **세대 번호** `push_epoch`: 두 구간을 비울 때마다 1 씩 오른다(`reset_push_range`). job 은 맡길
  때의 번호를 들고 가고, 끝났을 때 번호가 다르면 결과를 버린다. 비우는 순간은 팔로워 볼륨의 i 번
  자리가 무엇을 뜻하는지 바뀐 순간이다 — 새 term(`become_leader`), 알린 해시 변화, 맵 (재)설치,
  push 실패, 자기 쓰기 실패 롤백(같은 인덱스에 곧 다른 명령이 들어온다). 그 전에 맡긴 job 이 늦게
  끝나 결과를 새 구간에 적으면, 팔로워 볼륨에 없는 엔트리를 "있다"고 표시해 보내게 된다. 5 단계까지는
  push 가 Lock A 안에서 동기로 끝나 이런 일이 없었다. 비울 때 job 을 기다리는 방법은 멈춘 대상 하나가
  다른 처리까지 막아서 쓰지 않는다.

**pull 건너뛰기 (`push_gate`, `push_covers`).** `append_entries_worker` 의 Lock B 는 배치를 자르는
다른 단계보다 먼저 관문(`push_gate`)을 지난다. 규칙은 하나다: **`next` 가 그 팔로워에게 맡긴
구간(쓰는 중이거나 끝난 엔트리) 안이면 `last` 를 쓰기가 끝난 데(`push_upto`)까지 자른다.**
- 자르고 남은 게 있으면 그만큼 보낸다. 최종 배치(바이트 상한, wrap, extent clamp 를 다 거친 값)도
  끝난 구간 안이면(`push_covers`) `data_already_copied` 를 세운다.
- 남은 게 없으면(`last < next`) 끝났는데 팔로워가 아직 안 받은 엔트리가 없다 — 나머지는 쓰는
  중이므로 보내지 않는다. 그 job 이 끝나면 이 워커를 다시 깨운다. 이번 회차를 하트비트 타이머가
  깨웠으면(`heartbeat_due`) 빈 AE 만 보내 팔로워가 선거를 시작하지 않게 하고, 아니면 RPC 없이
  돌아간다 — 매번 보내면 그 RPC 가 오가는 동안 push 가 끝났을 때 진짜 AE 가 한 왕복 늦어진다.
  완료는 `mu` 아래에서 기록한 뒤 깨우므로 깨우기를 잃지 않는다.
- 맡긴 구간 밖이면 지금 동작 그대로 통째로 pull 한다 — push 대상이 아닌 팔로워, 빈 배치, 맡긴 적
  없는 엔트리(기록이 비었음, 구간 시작 전에 쓴 것). 기다려도 써 줄 job 이 없기 때문이다. 일부만
  걸치면 배치를 쪼개지 않는다. `-debug` 의 `pull=` 은 push 대상일 때만 센다.
- 한 세대 안에서 job 은 이어서 맡겨지고 넣은 순서대로 끝나므로 끝난 구간은 늘 맡긴 구간의
  앞부분이다. 그래서 이 한 규칙이 "끝났으면 보내고, 쓰는 중이면 기다리고, 맡긴 적 없으면 pull"과
  같다. 처음에는 판정을 네 종류(대상 아님 / 끝남 / 쓰는 중 / pull)로 나눴으나, 동작이 같은 것을
  합쳐 2026-10-28 에 이 규칙 하나로 줄였다.
- 앞쪽이 걸치는 배치는 실제로 생긴다. 팔로워는 리더가 그 AE 응답을 처리할 때(선거 뒤 해시를 다시
  알 때, 맵 설치) 비로소 대상이 되므로 그 전에 쓴 엔트리는 push 되지 않았다. 그런데 AE 워커는
  `next_index` 부터 tail 까지를 한 배치로 보내고 엔트리를 실은 AE 가 날아가는 동안은 다음 배치를
  보내지 않으므로(`inflight`), 대상이 된 직후의 첫 배치는 push 안 된 앞부분과 push 된 뒷부분에
  걸치기 쉽다. push 실패 뒤 1 초 쉬는 구간과 catch-up 도 같다. 이 배치에 플래그를 세우면 팔로워는
  자기 볼륨에 없는 엔트리를 성공으로 응답하고, 리더가 된 뒤 그 슬롯(0 이나 이전 바퀴의 엔트리)을
  명령으로 읽는다. 통째로 pull 하면 이미 push 된 뒷부분은 같은 바이트를 한 번 더 쓸 뿐이다.
  경계에서 잘라 앞부분만 pull 하면 뒷부분이 다음 라운드로 밀려 왕복이 하나 는다.
- 뒤쪽(`last > push_upto`, 아직 쓰는 중인 엔트리)은 Covered 에서 `last` 를 `push_upto` 로 잘라
  보내지 않는다. 그 엔트리는 job 이 끝나 다시 깨워진 회차에 보낸다.

팔로워 코드는 바꾸지 않았다.
`data_already_copied` 면 `do_pba_copy` 를 건너뛰고 메타데이터로만 로그를 잇는 경로가 Leader-Side 용으로
이미 있었다.
- 슬롯이 맞는 이유: 배치는 리더 extent 하나 안에 있고 wrap 하지 않으므로 슬롯이 연속이다.
  팔로워가 `start_slot` 부터 차례로 계산하는 슬롯이 리더가 같은 논리 오프셋으로 push 한 위치와 같다.
- 계측: push 된 배치는 팔로워가 pull 하지 않으므로 응답의 `write_pba_rt_ns`/`storage_copy_ns` 가 0
  이다. 리더는 그 자리에 push 대기(위 "push 쓰기"의 계측)를 넣는다(`ReplSample.pushed`). 팔로워의
  `FHandler` 에서는 pull 이 빠진다.
- 관측: 리더 `-debug` 의 `skip=`(push 덕에 pull 을 건너뛴 배치)과 `pull=`(push 대상인데 구간 밖이라
  pull 한 배치). 보낸 배치 기준의 누적값이다(`push_ae_skipped` / `push_ae_pulled`).

**OK 는 AE 가 대신한다 (A안, 2026-10-26 결정).** 처음 설계는 AE 를 push 쓰기와 병렬로 보내고,
쓰기가 끝나면 RDMA Write 로 OK 를 따로 보내, 팔로워가 OK 를 받은 뒤 응답하는 것이었다. 팔로워는
자기 볼륨에 내구성 있게 있다는 것을 알기 전에는 성공으로 응답할 수 없으므로 어떤 형태로든 "썼다"는
신호가 필요하다. 지금은 AE 를 push 쓰기와 flush 가 끝난 뒤에 보내 AE 자체를 그 신호로 쓴다. 끝난
구간은 그 팔로워의 push 쓰기와 flush 가 끝난 뒤에만 늘어나고(`finish_push_job`), AE 워커는 끝난
구간 안의 엔트리에만 `data_already_copied` 를 세운다. 그래서 그 플래그가 선 배치는 팔로워 볼륨에
이미 내구성 있게 있다.
이렇게 정한 이유:
- 다른 Raft 구현(etcd, braft, LogCabin)으로 옮길 때 고칠 곳이 팔로워별 송신기의 관문 하나다
  ("push 대상이면 그 push 가 끝난 뒤 보낸다"). 그 구현들의 "리더 자기 쓰기와 AE 의 병렬"
  (Raft 논문 §10.2.1)은 그대로 남는다 — 그 구현에서도 팔로워의 쓰기는 응답 앞에 있고, 여기서는
  그 쓰기를 리더가 대신 하면서 AE 앞으로 옮겼을 뿐이다. OK 채널안은 팔로워 저장 경로(쓰기 대신
  OK 대기, 타임아웃, fallback)와 RDMA 부채널까지 고쳐야 한다.
- 전송과 무관하다. push 쓰기는 커널 NVMe-oF 장치를 거치고 AE 는 기존 전송을 쓰므로 TCP 에서도
  동작한다. OK 채널은 RDMA 전용이었다.
- 포기하는 것은 라운드마다 팔로워의 AE 처리 시간(`FHandler`)과 AE RPC 의 수신·파싱 정도다. OK
  채널이라면 이 시간이 push 쓰기와 겹쳐 가려진다. push 쓰기(NVMe-oF 왕복 + flush)보다 작을 것으로
  보지만 측정으로 확인한다(E2E_EXPERIMENT.md §6-4). 크면 그때 OK 채널을 얹는다.
- NVMe-oF 에는 팔로워가 쓰기 완료를 알 표준 수단이 없다. 타깃의 RDMA READ 는 쓰기를 낸 리더의
  메모리로 가고(keyed SGL 은 명령이 들어온 QP 에서만 해석된다), one-sided 라 받는 쪽에 완료가
  생기지 않으며, SSD 쓰기 **전에** 일어난다. 쓰기 완료는 명령을 낸 호스트만 완료 캡슐로 받는다.
  데이터 쓰기를 알리는 AEN 종류도 없다.

병렬화도 이 결정 위에서 한다. 1 부(2026-10-27)는 대상별 관문까지다 — push 는 대상별 워커가 하고,
push 대상의 AE 는 그 대상의 push 가 끝나는 대로 나간다(`push_gate`). 다만 자기 쓰기는 아직 Lock A
안이라 F 의 AE 는 max(자기 쓰기, F 의 push) 뒤에 나간다. 2 부는 자기 쓰기도 `mu` 밖으로 뺀다 —
그러면 pull 팔로워는 리더 자기 쓰기가 끝난 인덱스까지만 보내야 하고(pull 은 리더 볼륨에서 읽어
가므로), 리더 자신의 match 는 `tail−1` 이 아니라 내구 인덱스로 세야 하며, 자기 쓰기 실패는 이미
다른 팔로워에게 나간 엔트리를 되돌릴 수 없어 치명 오류로 다뤄야 한다.

**왜 파일이 아니라 장치에 쓰나.** 팔로워 링 파일은 팔로워 호스트의 ext4 위에 있다. 리더
호스트가 그 ext4 를 같이 마운트하면 두 커널의 메타데이터 캐시가 어긋나 파일시스템이 깨진다.
그래서 리더는 같은 바이트를 같은 논리 오프셋에 쓰되, 팔로워 맵으로 구한 PBA 로 팔로워
**장치**에 직접 쓴다. 스토리지 노드 blockcopy 가 하는 것과 같은 방식이다. 핸들은 리더가 자기
로그에 쓰는 `CachedFD` 를 재사용한다. `CachedFD::open_device` 는 `dev_wr_fd_` 하나만 열고,
블록 장치(`S_ISBLK`)가 아니면 거절한다. 팔로워의 맵은 팔로워만 만들 수 있다 — FIEMAP 은 그
파일이 마운트된 호스트에서만 걸린다.

**장치 매핑.** `-push-devices ID=PATH,...` 로 받는다. 위치 기반 목록이 아닌 이유는, 순서를
잘못 주면 다른 멤버의 볼륨(실사용 ext4)을 조용히 덮어쓰기 때문이다. 같은 볼륨이라도
호스트마다 장치 이름이 달라서, 기동 배너에 각 장치의 wwid 와 자기 볼륨의 wwid 를 찍는다.
호스트 A 의 `5 -> ...` wwid 와 호스트 5 의 `own volume` wwid 가 같아야 한다.

**데이터 경로.** push 쓰기는 보통의 NVMe-oF Write 다. 리더 메모리 → (타깃의 RDMA READ) →
스토리지 노드 DRAM → 팔로워 SSD 순서로 간다. 리더가 자기 로그를 쓸 때와 같다. 그래서 push 는
대상마다 "리더→스토리지 전송 1번 + 스토리지 DRAM 경유 1번" 이 늘어난다. 이 경유를 없애려면
nvmet `p2pmem`(SSD CMB + PCIe P2P)이나 RNIC 의 NVMe-oF 타깃 오프로드가 필요하다.

**blockcopy RPC 는 남는다.** push 된 배치에서만 팔로워가 `do_pba_copy` 를 건너뛴다. 맵을
받기 전의 no-op, 리더 교체 뒤 catch-up, 해시 불일치·장치 미매핑으로 push 를 멈춘 범위는
fallback 으로 pull 한다.

**한계 — 펜싱 없음 (2026-10-26 결정).** 리더가 바뀔 때 옛 리더의 push 쓰기가 이니시에이터 큐에
묶였다가 늦게 도착하면, 새 리더가 쓴 슬롯을 덮을 수 있다. NVMe Reservation(새 리더가 첫 쓰기
전에 옛 리더의 등록을 preempt)으로 막을 수 있지만, nvmet 지원이 확인되지 않아 넣지 않았다.
SETUP.md 의 `--fast_io_fail_tmo` 로 지연 창을 줄이는 것이 완화책이다. 리더가 바뀌지 않는
측정에는 영향이 없다. 인프라 준비 절차는 SETUP.md "PUSH MODE".
- 8 단계 1 부(비동기 push)로 이 창이 하나 더 생긴다. 물러난 뒤에도 워커 큐에 남은 job(대상마다 최대
  16 개)이 돌 수 있기 때문이다. 이것은 **펜싱이 막는다고 가정한다**(2026-10-27 결정). 새 리더가 첫
  push 전에 각 push 대상 네임스페이스에서 옛 리더의 등록을 Preempt and Abort 하면, 그 뒤에 도착하는
  옛 리더의 쓰기는 큐에 있던 job 이든 아니든 타깃이 Reservation Conflict 로 거절한다. 리더 쪽에서 따로
  막지 않는 이유는, 펜싱이 이 경우뿐 아니라 물러난 줄 모르는 옛 리더(Raft 피어와는 끊겼지만
  스토리지와는 이어진 경우)까지 막으므로 리더 쪽 장치가 중복이기 때문이다. 리더 쪽에서 막는 리더십
  순번 검사를 넣었다가 같은 날 뺐다.
- 그 가정이 서려면 세 가지가 필요하다. (1) 새 리더가 push 하지 않는 팔로워의 볼륨도 선점해야 한다 —
  그 팔로워의 pull 쓰기는 스토리지 노드가 로컬로 해서 Reservation 을 거치지 않는데, 옛 리더의 늦은
  push 가 그 슬롯을 덮을 수 있다. 그래서 새 리더의 호스트도 그 볼륨을 attach·등록해야 한다.
  (2) 그냥 Preempt 가 아니라 Preempt and Abort 여야 한다 — 선점 시점에 처리 중이던 옛 쓰기가 새 쓰기
  뒤에 끝나지 않게. (3) nvmet 이 이것을 강제해야 한다(클러스터 커널에서 미확인). **펜싱을 구현하기
  전까지는 이 창이 열려 있다.**

## D21. 커밋 판정을 AE 응답 경로에서도 한다 (2026-10-28)

`Server::advance_commit_locked` · `wake_apply_worker` (`core/src/raft_commit.cpp`) ·
`append_entries_worker` Lock C (`core/src/raft_append_entries.cpp`)

원본은 커밋을 `main_loop` 폴링(`advanceCommitIndex`)에서만 판정한다. 원본은 루프가 스핀하므로 문제가
없지만, 이 포팅은 `mu` 굶김을 피하려고 바퀴마다 `loop_sleep_us`(기본 200µs, timer slack 포함 실제
~250µs)를 쉰다. 그래서 과반 응답이 도착한 뒤에도 커밋이 다음 바퀴까지 늦었다. 클라이언트가 한 번에
하나씩 보내면 응답 시각이 루프 주기와 맞물려 지연이 무작위로 퍼지지 않고 몰린다 — 2026-10-28 실측에서
push 16K 샘플의 82% 가 QuorumWait ≈253µs 였다(E2E_EXPERIMENT.md §7-6). `-loop-sleep-us 20` 으로 줄이면
사라졌으므로 원인은 폴링이다.

- 커밋 판정(원본 `advanceCommitIndex` 의 (1) 단계)을 `advance_commit_locked` 로 떼어 냈다. 호출자가
  `mu` 를 보유하고, 전진했으면 true 를 돌려준다.
- `append_entries_worker` 의 Lock C 가 `apply_ae_success` 로 match_index 를 올렸으면 바로
  `advance_commit_locked` 를 부르고, 전진했으면 apply 워커를 깨운다. Lock C 는 이미 `mu` 를 쥐고
  있으므로 락을 더 잡지 않는다. 추가 비용은 멤버 수만큼의 정렬(3개) 한 번이다.
- `main_loop` 의 `advance_commit_index` 는 그대로 둔다. 응답이 없는 동안의 안전망이고, slot GC 를
  깨우는 일(`slot_gc_tick` 은 바퀴 수로 센다)도 여기서만 한다. 응답 경로는 GC 를 깨우지 않는다.
- 판정 로직(정렬, §5.4.2 current-term 검사, 커밋 신호)은 한 줄도 바꾸지 않았다. 판정하는 **시점**만
  늘었다. 판정은 `mu` 아래에서만 하므로 두 경로가 겹쳐도 commit_index 는 단조 증가한다.
- 결과: `loop_sleep_us` 는 하트비트·선거 타이머의 정밀도만 정하고 커밋 지연에는 들어가지 않는다.
  기본값 200 으로도 지연 측정이 왜곡되지 않는다.
- 실측(2026-10-28, push, 리더 eternity4, `-loop-sleep-us` 기본 200, REPS=3, `csv/lat_push_d21_1028`).
  QuorumWait ≥150µs 샘플은 세 크기 모두 0.01%다(수정 전 16K 는 82%).

  | p50 (µs) | 수정 전, sleep 200 | 수정 전, sleep 20 | **수정 후, sleep 200** |
  |---|---|---|---|
  | Total 1K / 4K / 16K | 233 / 226 / 465 | 175 / 200 / 258 | **140 / 159 / 220** |
  | QuorumWait 1K / 4K / 16K | 114 / 83 / 251 | 71 / 72 / 71 | **27 / 23 / 21** |
  | CommitWait 1K / 4K / 16K | 142 / 112 / 286 | 87 / 87 / 87 | **49 / 47 / 48** |

  sleep 20 보다도 ~40µs 빠르다 — 20µs 폴링도 timer slack 을 합치면 평균 수십 µs 를 더하고 있었다.
  남은 CommitWait − QuorumWait(~25µs)는 커밋 신호 → Apply 스레드 → apply 워커로 가는 조건변수 깨우기다.
- destination 도 같다(`csv/lat_dest_d21_1028`): QuorumWait p50 1K/4K/16K = 19/19/21µs (수정 전 sleep 200:
  178/156/107, sleep 20: 52/52/47). 다만 같은 실행에서 `StorageIO`(스토리지 노드의 pread+pwrite, D21 이
  건드리지 않는 항)가 4K/16K 는 ~170 → 81/91µs 로 줄고 1K 는 166 → 213µs 로 늘어 Total 이 그 영향을
  크게 받았다. 크기 순서를 뒤집어 다시 돌려도(`csv/lat_dest_d21b_1028`) ±3µs 안으로 같아 크기에 따른 효과다.
  원인은 미확인이다(스토리지 C6 는 이미 꺼져 있다). 후보: 1K 엔트리는 1536B(3 슬롯) 쓰기라 연속 pull 이 같은
  4K 페이지를 반복해 부분적으로 덮는다.

## U. 미해결 — 리팩토링 중 마주치면 여기를 볼 것

| # | 항목 | 위치 | 상태 |
|---|---|---|---|
| U1 | `find_slot_map_trace`의 로직이 **원본 대조 미완**이다. 주석이 스스로 "원본에 언급만 있고 본문은 못 봤음 — 로직 추정"이라고 밝힌다 | `core/src/raft_ring_helpers.cpp` | 진단용이라 정확성이 안전성에 영향을 주지는 않는다. 원본 확인 필요 |
| U2 | 팔로워가 `log_slot_map`을 채우지 않는다 → 승격 직후 새 리더는 자기 no-op만 PBA 복제 가능. 채우면 `do_slot_gc`가 리더 전용이라 팔로워 맵이 무한히 자란다 | `core/src/raft_handle_append_entries.cpp` (TODO) | HANDOFF §7-3. 슬롯 맵 소유권/GC 책임 재설계 문제 |
| U3 | `apply_pending`의 배치 락 양보가 **미구현**이다. `lock_guard`라 명시적 unlock이 불가해 `kApplyBatchLimit=64` 도달 시 `reset_election_timeout()`만 부르고 락을 계속 쥔다 = 의도한 양보가 전혀 동작하지 않는다 | `core/src/raft_commit.cpp` (TODO) | |
| U4 | **`ae_lock_a_held_ns` 가 write-only다.** `apply_internal` 이 store하지만 load가 0건이다. (같은 항목에 있던 나머지 둘은 2026-09-02 계측 정리에서 해소됐다: `sample_*` 6개는 `append_entries_worker` 에 store를 넣어 **배선 완료** — 가드를 `sink != nullptr` 밖에 두는 것이 핵심이었다. 폴백이 노리는 상황이 바로 하트비트이기 때문이다. `last_persist_sub`(`PersistSubTimings`)는 **삭제** — `persist_circular` 에서 타이머 6쌍이 함께 없어졌고 `nvme_ns`(= LPersist)를 만드는 3쌍만 남았다) | `core/src/raft_apply.cpp` | HANDOFF §7-6, §7-7 |
| U5 | **팔로워 재시작 catch-up 불가 (설계 한계).** D8(빈 로그 재시작)과 D12(floor guard)가 서로 모순이라 리더는 영원히 같은 `prev_log_index`를 보내고 팔로워는 영원히 거절한다. 원본 `raft.go`도 동일하므로 포팅 버그가 아니다. 안전성은 유지된다(빈 로그 노드는 `last_log_term=0`이라 리더가 될 수 없다) | `core/src/raft_lifecycle.cpp` + `core/src/raft_append_entries.cpp` | HANDOFF §5. `scripts/restart_test.sh`가 `[KNOWN GAP]`으로 보고하고 종료코드 0. 정해는 `init_storage`가 `read_entry_direct`로 링을 스캔해 in-memory 로그를 복원하는 것 |
| U6 | **스토리지 커넥션이 한 번 죽으면 이후 모든 PBA 복사가 영구히 실패한다.** RPC 실패 시 커넥션 핸들을 버리지 않기 때문이다. 리팩토링 전에는 `Server::blkcopy_reset()`이 존재하기만 하고 호출부가 0건이었고, 지금은 `TcpBlockCopyClient::write_pba_batch`의 catch 블록에 같은 누락이 그대로 남아 있다(주석으로 표시해 뒀다). raft RPC 경로(`TcpRaftTransport::call_peer`)에는 이 무효화가 제대로 있다 | `net/src/raft_blkcopy_rpc_client.cpp` | 리팩토링 범위 밖(버그). **고치는 방법은 그 catch 안에 `std::lock_guard lk(mu_); if (handle_ == handle) handle_.reset();` 한 줄**이다 |
| U7 | **성능 기준선을 이 호스트에서 재확인해야 한다.** 리팩토링 후 `eternitymaster`에서 측정하면 4064B 단건 Apply가 ~9.0ms, 512B×2000 batch=10이 ~900µs/command로, HANDOFF §6.2의 2.9ms / 334µs보다 3배 가까이 느리다. **격차 전부가 `StorageIO` 한 항에 있다** (186µs → ~6.6ms). 나머지 항은 기준선과 거의 같다(LPersist 1.65ms → 1.4~1.5ms, AENet 126 → 150~260, FHandler 4.6 → 6~13, QuorumWait 362 → 140~450, Mutex 1.2 → 2). `StorageIO`를 재고 수행하는 코드(`storage/raft_blockcopy_server.h`)는 리팩토링이 바꾸지 않았고(diff로 확인: 같은 값의 상수 이름 하나 + include 한 줄), 같은 링 파일에 같은 pread+pwrite를 직접 걸어 보면 아이들 108~152µs / 부하 중 690µs다. `-loop-sleep-us 0`, 링 파일 사전 기록(extent 전부 written), `-copy-workers` 변경 모두 영향이 없었다. HANDOFF의 기준선은 **다른 머신**(Ubuntu 24.04, /home NFS)에서 측정된 것이다 | 측정: `apply-timed -n 20 -size 4064 -batch 1`, `apply -n 2000 -size 512 -batch 10` | **리팩토링 전 리비전에서 같은 측정을 한 번 돌려 비교해야 한다.** 이 디렉터리에는 리팩토링 전 트리가 없어(버전관리가 외부) 이 호스트에서의 before/after를 만들 수 없었다. 기준선이 머신 차이라면 HANDOFF §6.2에 측정 환경을 명시할 것 |
| U8 | **`AlignedBufPool::acquire` 의 use-after-free (2026-09-07 수정).** `if (cap < n) free(p); return {p, cap};` — 풀에서 꺼낸 버퍼가 요청보다 작으면 해제한 뒤 그 **해제된** 포인터를 옛 capacity 와 함께 돌려줬다. 워커는 해제된 메모리에 `n` 바이트를 pread/pwrite 한다. `do_pba_copy` 는 청크 크기가 사실상 균일해서 `cap < n` 분기 자체가 안 잡혔고, 그래서 3노드 e2e·ASan 이 전부 통과하는 동안에도 드러나지 않았다. `raft_blkcopy_bench` 로 청크를 4Ki→64Ki→1Mi 로 키워가며 부르면 **첫 확대에서 바로 터진다** (ASan: `attempting double-free`, 서버 프로세스 사망). U1~U7 에 없던 미기록 버그다 | `storage/raft_blockcopy_server.cpp:96-99` | **수정 완료** — `cap >= n` 이면 재사용, 아니면 free 후 아래에서 새로 `posix_memalign`. 검증: 같은 스윕을 수정 전/후 ASan 빌드로 돌려 전자만 리포트가 나오는 것을 확인했다. storage/ 가 U7 대조군이었으므로 U7 비교 시 이 변경을 밝힐 것. **그 재현 도구 `raft_blkcopy_bench` 는 2026-09-10 에 `raft_blkcopy_scale` 로 교체됐다(D15)** — 같은 스윕은 `scripts/blkcopy_scaling.sh` 로 돌린다 |
| U9 | **`WritePBABatchResponse` 에 `read_nanos`/`write_nanos` 추가 (2026-09-07, additive).** `copy_nanos` 합계만으로는 로컬 PCIe 복사와 NVMe-oF attach 볼륨 복사의 지연 차이가 pread 쪽인지 pwrite 쪽인지 알 수 없다. 서버는 이미 `total_read_ns`/`total_write_ns` 를 따로 들고 있어 합치기 전 값을 실어 보내는 것뿐이고, 측정 비용은 0이다. proto3 additive 필드(3, 4번)라 이 필드를 모르는 피어는 0으로 읽는다 — `AppendEntriesResponse` 8~12번과 같은 선례 | `proto/rpcproto.proto` · `storage/raft_blockcopy_server.{h,cpp}` · `net/include/raft_proto_conv.h` | 재생성은 `protoc 3.21.12` (커밋된 `.pb.cc` 와 같은 버전, eternity5 의 `/usr/bin/protoc`). CTest 11개 전부 통과 확인 |
| U10 | **blockcopy 에 노드↔노드 데이터 경로가 없다** (`storage/raft_blockcopy_server.cpp:16-18` 의 `TODO: inter-node storage copy`). `WritePBABatchRequest.src_dev`/`dst_dev` 는 **한 프로세스**가 `-devices` 로 연 fd 배열의 인덱스이고, 복사는 그 두 fd 사이의 pread+pwrite 다. 설계가 말하는 "원격 노드" 는 NVMe-oF 로 attach 되어 로컬 경로처럼 보이는 볼륨이다 (`apps/raft_blockcopy_server_main.cpp` 머리 주석) | `storage/raft_blockcopy_server.cpp` | 미구현 상태 그대로 둔다. 측정 실험(`scripts/blkcopy_scaling.sh`, 2026-09-10 이전에는 `blkcopy_latency.sh`)은 이 정의를 따라 "로컬 PCIe 두 네임스페이스" vs "같은 SSD 의 NVMe-oF attach 뷰 두 개" 로 arm 을 나눈다. **주의: 이 클러스터의 NVMe-oF/TCP 는 1 GbE(eno, MTU 1500) 위에서 돈다** — 상한 ~118 MB/s 라, 이 경로로만 재면 blockcopy 가 아니라 링크 대역폭을 재게 된다. 전송별 arm(`fabric-tcp`/`fabric-ipoib`/`fabric-rdma`) 설정과 RDMA 준비 상태는 `scripts/README.md` 참고. 실측 RTT 는 IPoIB 0.197ms vs 1GbE 0.214ms 로 **사실상 같다**(IPoIB 도 커널 IP 스택을 탄다) — 지연을 줄이려면 IB 로 옮기는 것으로는 부족하고 `trtype=rdma` 여야 한다 |
| U11 | **`FALLOC_FL_ZERO_RANGE` 가 ext4 에서 extent 를 "written" 으로 만들지 않는다 — D9 의 전제가 성립하지 않는다.** `create_ring_file` 은 매 기동마다 링 파일 전체에 ZERO_RANGE 를 걸고, D9 와 `blockio/cached_fd.h:31-36` 는 그것이 extent 를 전부 written 으로 뒤집어 "이후 첫 쓰기가 파일시스템 저널을 통한 extent 상태 변환으로 직렬화" 되는 것을 막는다고 서술한다. **실측 결과 ZERO_RANGE 후에도 extent 는 unwritten 으로 남는다** — ext4 에서는 범위를 unwritten 으로 두는 것이 가장 싼 zeroing 구현이기 때문으로 보인다. 커널 4.15(eternitymaster)와 6.8(eternitystorage) 양쪽에서 동일했다: `fallocate -l` → unwritten, `fallocate -l` + `fallocate -z` → **여전히 unwritten**, `dd oflag=direct`(실제 쓰기) → written. 라이브 링 파일 `/mnt/raftvol/node5/raft_meta.bin`(32GiB, 3시간 가동)도 **257 extent 중 257 개가 unwritten** 이고 블록 0(헤더 쓰기가 닿은 곳)만 written 이었다 | `blockio/cached_fd.cpp` `create_ring_file` · `DECISIONS.md D9` | **미해결. 이번에는 고치지 않았다** (blockcopy 벤치 교체 작업 범위 밖이고, `blockio`/`storage` 변경은 U7 대조군에 영향을 준다). 확인된 것은 **전제가 틀렸다는 사실**까지다 — 그 결과로 실제 지연이 얼마나 늘어나는지는 측정하지 않았다. U7(StorageIO 격차)과 관련이 있을 수 있으므로 그 조사에서 같이 볼 것. 고치려면 extent 를 실제로 written 으로 만들어야 하는데, 링 파일 전체를 한 번 쓰는 비용(기본 32GiB/노드)이 기동 시간에 그대로 붙는다 — 비용/이득을 따져 결정할 일이다. 새 벤치(`scripts/blkcopy_scaling.sh`)는 이 문제를 피하려고 백킹 파일을 `dd oflag=direct` 로 준비하고 preflight 에서 unwritten extent 를 검사해 거부한다. **2026-10-24 (D18):** 이제 기존 링 파일에는 ZERO_RANGE 를 다시 걸지 않으므로, 링 파일을 파일시스템을 통해 한 번 써 두면 그 written 상태가 재시작 후에도 남는다. **2026-10-24 (D19):** `FALLOC_FL_WRITE_ZEROES` 를 지원하는 환경에서는 새 링 파일이 처음부터 written 으로 할당된다 (기동 배너 `ring alloc` 으로 확인) |

| U12 | **`-statemachine kvs` 에서 failover 직후 새 리더의 KVS 가 불완전할 수 있다.** 팔로워는 apply 하지 않고(리더만 `apply_pending`), 승격 시 `become_leader` 가 링에서 명령을 읽어 replay 한다. `read_entry_direct` 실패는 조용히 건너뛰고(`raft_election.cpp`), 링이 한 바퀴 돌았으면 덮어쓴 데이터를 읽을 수도 있다. 그 상태에서 GET(ReadIndex)은 빠진 PUT 을 못 본다 | `core/src/raft_election.cpp` `become_leader` · `core/src/raft_commit.cpp` `apply_pending` | 2026-10-13 결정으로 팔로워 apply 는 현행 유지. **벤치마크에 failover 를 넣지 말 것.** 고치려면 팔로워도 커밋 시 링에서 읽어 apply 해야 한다 |
| U13 | **`tests/test_apply_timings.cpp` 가 HEAD 에서 컴파일되지 않는다.** `ApplyWalls::replicate_commit_ns` / `ApplyTimings::replicate_commit_corrected_ns` 를 참조하는데 `core/include/raft_timings.h` 에 그 필드가 없다 (persistent worker 계측 변경 이후로 보인다). `raft_unit_tests` 전체가 빌드되지 않는다 | `tests/test_apply_timings.cpp` | 2026-10-13 발견. **2026-10-27 `db0a794` 에서 해소** — 필드 이름 변경(`replicate_commit_ns` → `replicate_ns`, `replicate_commit_corrected_ns` → `replicate_corrected_ns`)을 따랐다. push 8 단계 1 부에서 push 분기 테스트를 더했다 |
| U14 | **리더 자기 쓰기 실패 처리 (다음 작업 항목, 2026-10-28).** 지금 `apply_internal` 은 persist 가 실패하면 엔트리를 되돌리고 클라이언트에 실패를 돌려준 뒤 **리더로 남는다.** 문제는 셋이다. (1) 하트비트는 계속 나가 팔로워가 선거를 시작하지 않는데 모든 Apply 는 실패한다 — 고장 난 리더가 자리를 비키지 않는다. (2) 같은 term 에서 같은 인덱스를 다른 명령으로 다시 쓴다 — Raft 가 기대는 "(인덱스, term) 이 같으면 같은 엔트리"가 깨지고, push(이미 맡긴 job 의 바이트가 팔로워 볼륨에 있을 수 있다)와 재시작 replay 에서 정합성 문제가 된다. (3) `ring.tail_slot`/`log_slot_map` 은 되돌리지 않는다. **계획:** 되돌리지 않고 같은 엔트리를 같은 위치에 다시 쓴다. 재시도 시간은 선거 타임아웃보다 짧게 잡는다. 오류 종류를 나눠 EIO·타임아웃(경로 문제)만 재시도하고 EINVAL 등 설정·버그 오류는 바로 끝낸다. 재시도가 다 실패하면 프로세스를 끝낸다(fail-stop — etcd 도 WAL 저장 실패 시 Fatal). 리더를 내려놓기만 하면 투표 기록(votedFor)도 남길 수 없어 안전하게 참여할 수 없다. 다시 띄워 따라잡으려면 U5(재시작 replay)가 필요하다. disaggregated 구성에서는 대부분의 경로 흔들림을 NVMe 드라이버(재시도·재연결·멀티패스)가 흡수하고, Raft 까지 오는 것은 느려짐과 `fast_io_fail_tmo` 뒤의 EIO 다 | `core/src/raft_apply.cpp` `apply_internal` · `core/src/raft_persist.cpp` | 쓰기가 오래 멈추면 Lock A 가 `mu` 를 쥐고 있어 하트비트도 멈추므로, 선거 타임아웃 뒤 팔로워가 이미 새 리더를 뽑는다(지금도 그렇다). push 대상 쪽 실패는 리더를 바꾸지 않는다(1 초 쉬고 pull, D20). 이걸 하면 push 세대 번호의 한 용도(롤백)는 사라지지만, 링 교체·재선출 경우가 남아 세대 번호는 유지한다 |

그 외 남은 작업 목록은 HANDOFF.md §7에 있다.

---

## S. Phase 6에서 정리할 stale 주석

코드가 이미 그 단계를 지났는데 주석만 옛 상태로 남은 것들. **삭제/정정 대상이며 계약이
아니다.**

| # | 위치 | 주석이 주장하는 것 | 실제 |
|---|---|---|---|
| S1 | `core/include/raft_server.h` (`class Server` 머리) | "오늘은 필드 선언만 정확히 옮기고 동시성 로직은 다음 단계에서 채운다" | 동시성 로직 전부 구현됨 |
| S2 | `core/include/raft_server.h` (`apply_notify_*` 근처) | "Go의 sync.WaitGroup 대응: 아직 미해결" | `inflight_threads`로 해결됨 |
| S3 | `core/include/raft_server.h` (`reset_election_timeout` 선언) | "선언만, election 로직 포팅 시 구현" | `raft_election.cpp`에 구현됨 |
| S4 | `core/include/raft_server.h` (`update_term` 선언) | "election 로직 미포팅이라 오늘은 …까지만, resetElectionTimeout은 이미 stub" | 둘 다 구현됨 |
| S5 | `core/src/raft_handle_append_entries.cpp` (`do_pba_copy` 머리) | `blkcopy_write_pba_batch`가 "RDMA 미결선 상태라 스텁" | `net/src/raft_blkcopy_rpc_client.cpp`에 TCP 구현 존재 |
| S6 | `core/src/raft_commit.cpp` (머리) | "profiling atomic은 항상 활성화된 것으로 단순화… 오버헤드 최적화는 다음 단계" | 실제로 `sub_stage_profiling`을 확인함 |
| S7 | `core/src/raft_append_entries.cpp` (term 비교 근처) | "election 로직 미포팅이라 term이 더 높으면 그냥 중단만" | election은 포팅됨. **단, 코드가 실제로 `update_term`을 부르지 않고 return하는 것은 사실** — 서술만 고치고 동작은 건드리지 않는다 |
| S8 | `blockio/cached_fd.h` | "C 헬퍼들을 별도 파일(`raft_pba_native.c` 등, 다음 단계)로 분리", `CachedFD::write`/`read` "오늘은 시그니처만" | 분리하지 않았고 구현되어 있다 (`write`의 unaligned slow path는 여전히 throw) |
| S9 | `core/include/raft_server.h` (`gc_has_run` 주석) | "이 C++ 포팅은 로그 인덱스를 0-based로 쓰기로 결정했고…" | **Q1에서 오기로 확정.** 실제 엔트리는 인덱스 1부터 |
| S10 | `core/include/raft_server.h` (`cached_fd` 주석) | `raft_cached_fd.**hpp**` | 파일명은 `.h` |
| S11 | `core/include/raft_server.h` | `persist_count_placeholder` "원본 persistCount uint64, 용도 확인 후 정정 예정" | 참조 0건. 필드 자체가 삭제 대상 (HANDOFF §7-8) |
| S12 | `core/src/raft_lifecycle.cpp` | `<iostream>` include | 사용하지 않는다 |

---

## X. 리팩토링(2026-08-27) 이후의 구조

이 문서의 D/R/Q/U 항목은 리팩토링 전후로 **의미가 그대로**다. 다만 코드 위치가
바뀐 것이 있어 어디를 봐야 하는지 정리한다.

### 디렉터리

| 경로 | 내용 |
|---|---|
| `core/` | **Raft 합의 알고리즘만.** `include/` 는 선언, `src/` 는 정의. net/ 을 include하지도, 링크타임에 net/ 심볼을 요구하지도 않는다 |
| `blockio/` | O_DIRECT + FIEMAP 블록 I/O (`cached_fd.*`, `block_geometry.h`). **Raft 타입을 하나도 모른다.** 예전에는 `core/raft_cached_fd.*` 였다 |
| `net/` | TCP 전송 + protobuf 코덱 + `raft_statemachine_hash.h`. `include/` 는 선언, `src/` 는 정의. RDMA로 교체될 부분 |
| `apps/` | 실행 파일 진입점 세 개(`raft_node_main` / `raft_client_main` / `raft_blockcopy_server_main`)**만**. 알고리즘은 없다 — 조립부(composition root)다. 예전에는 `net/src/` 에 있었다 |
| `storage/` | 스토리지(blockcopy) 노드 서버 구현. 예전에는 `core/` 에 있었다 |
| `proto/` | 와이어 메시지 정의와 생성된 코드 |
| `tests/` | doctest 유닛 테스트 + 전송 Mock + `raft_selftest_main.cpp` (예전에는 `core/` 에 있었다) |
| `scripts/` | 3노드 e2e |

**의존 방향**은 단방향이다:

```
apps/ ──> net/ ─┬─> core/ ──> blockio/
tests/ ─────────┴─> core/ ──> blockio/
net/  ────> storage/
```

`core/include/raft_constants.h` 가 `blockio/block_geometry.h` 의 `kSectorSize`/`kPageSize` 를
`SECTOR_SIZE`/`PAGE_SIZE` 로 재노출한다. 섹터·페이지 크기는 디바이스 기하값이라
blockio/ 가 소유하며, 그렇게 두지 않으면 core ↔ blockio 순환이 생긴다.

### core/ 의 새 파일

| 파일 | 역할 |
|---|---|
| `raft_transport.h` | `RaftTransport` / `BlockCopyClient` 추상 인터페이스. core가 아는 전송의 전부 |
| `raft_state.h` | `RaftState` / `RingLog` / `StorageIo` / `WorkerPool` + `AlignedBuffer` 자료 정의. `raft_server.h` 를 681 → 386줄로 줄이려고 분리했다 |
| `raft_basics.h` | 온-디스크 레이아웃 오프셋(엔트리 헤더 32B / 파일 헤더 512B)의 단일 출처 + 작은 헬퍼(`elapsed_ns`, LE 직렬화, `align_up`). **대응 .cpp 를 갖지 않는다** — 전부 constexpr 이거나 핫패스 inline이다 |
| `raft_timings.h` | `ProfilingSink` + `PersistSubTimings`. 계측 상태 전부 |
| `raft_diagnostics.cpp` | `log_skip_pba_diag`, 슬롯맵 추적 링 |

### `Server` 의 상태 그룹

메서드는 전부 `Server::`에 그대로 있고, 락(`mu`)도 하나다. 필드만 묶었다.

```
raft     RaftState      term, log, id, cluster, commit_index, last_applied, state, 타이머
ring     RingLog        tail/head 슬롯, log_slot_map, run_buf, 진단 링, 링 크기 설정
io       StorageIo      metadata_dir, device_path, cached_fd, identity_pba
prof     ProfilingSink  계측 (Raft 로직과 무관)
workers  WorkerPool     상시 스레드 3개 + CV + 복제 스레드 수명 관리
```

최상위에 남은 것: `mu`, `statemachine`, `transport`, `blockcopy`,
`replication_mode`, `done`, `debug_enabled`, 그리고 튜닝 플래그
(`loop_sleep_us`, `max_ae_batch`, `max_ae_batch_bytes`).

**링 크기는 이제 인스턴스 필드다** (`ring.num_pages` / `total_slots` / `ring_slots`).
예전에는 `core/include/raft_constants.h`의 프로세스 전역 가변 변수 + `configure_ring()`이었고,
그 때문에 한 프로세스에 링 크기가 다른 Server를 둘 만들 수 없었다.

### 헤더에 구현을 남기는 유일한 이유들 (R0)

선언/정의 분리를 하면서 **일부 헤더는 의도적으로 구현을 그대로 뒀다.**
예외는 이 일곱 가지뿐이고, 새 예외를 만들려면 여기에 이유를 적어야 한다:

| 대상 | 이유 |
|---|---|
| `core/include/raft_basics.h` 전부 | **핫패스.** `elapsed_ns` 는 Apply 한 번에 수십 번, `put_u64_le` 는 엔트리당 4번 불린다. (예전에 여기 적혀 있던 두 번째 근거 — "`storage/` 가 include 하는데 그 바이너리는 core 를 링크하지 않는다" — 는 **더 이상 성립하지 않는다.** Phase A-2 가 `storage/` → `core/` 의존을 없앴고, 지금 이 헤더를 include 하는 것은 `core/src/*.cpp` 뿐이다. 핫패스 근거만 남는다) |
| `core/include/raft_entry.h` 의 `Entry::signal_committed` | 이 헤더는 core 를 링크하지 않는 TU 3개도 본다. `.cpp` 를 만들면 그 바이너리가 core 를 요구하게 된다 |
| `net/include/raft_proto_conv.h` 의 template 2개 | 템플릿은 헤더에 있어야 한다. **나머지 34개 변환 함수는 일부러 헤더에 남겼다** — 헤더가 여전히 `rpcproto.pb.h` 를 include해야 해서 컴파일 이득이 없고, 시그니처 68줄이 중복되며 proto 필드 추가 시 고칠 곳이 2배가 된다 |
| `net/include/raft_statemachine_hash.h` 전부 | `.cpp` 를 만들면 이 헤더를 쓰는 TU 가 `net/` 링크를 요구하게 되고, "유닛 테스트는 core 만 링크한다(protobuf 0)" 가 깨진다. (예전 근거였던 "`tests/` 가 이 헤더를 include 한다" 는 **더 이상 사실이 아니다** — 그러던 `raft_selftest` 하네스가 2026-09-14 에 제거됐다. 결론만 유효하다) |
| `core/include/raft_statemachine_noop.h` 전부 (2026-09-17 추가) | **기본 상태머신.** 의존성이 `raft_server.h` + 표준 헤더 셋뿐이고 본문이 10줄이다. `.cpp` 를 만들면 core TU 전부가 링크 의존을 얻는다. `net/` 이 아니라 `core/` 에 둔 이유는 유닛 테스트가 이걸로 `apply_pending` 을 protobuf 없이 덮을 수 있게 하기 위함이다 |
| `kvs/raft_kvs_wire.h` · `kvs/raft_statemachine_kvs.h` 전부 (2026-10-13 추가, D16) | `raft_client` 가 와이어 인코더를 쓰지만 kvs 를 링크하지 않는다 — `.cpp` 를 만들면 그 바이너리에 링크 의존이 생긴다. 상태머신 쪽은 `raft_statemachine_hash.h` 와 같은 이유 |
| 한두 줄짜리 접근자 (`Server::get_voted_for`, `AlignedBuffer::data()`, `ProfilingSink::on()`, `timings_clamp0` 등) | 내려서 얻는 것이 없다 |

이 규약이 지켜지는지는 `ctest -R isolation` 4개가 감시한다.

### 검증

```bash
export PROTOBUF_SYSROOT=/tmp/pb-sysroot
cmake -S . -B build-cmake && cmake --build build-cmake -j
cd build-cmake && ctest --output-on-failure     # unit / selftest / smoke x3 / restart
```

`build.sh`도 그대로 동작한다(같은 소스, 증분 빌드 없음).
`restart_follower`는 **KNOWN GAP 2건이 그대로 GAP으로 남아야** 정상이다 -- 갑자기
통과하면 U5의 설계 한계가 바뀐 것이므로 원인을 확인해야 한다.
