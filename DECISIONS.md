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

## D9. `create_ring_file`은 파일 전체에 `FALLOC_FL_ZERO_RANGE`를 건다

`blockio/cached_fd.cpp` · `create_ring_file`

원본은 매 기동마다 링 파일 전체에 ZERO_RANGE를 걸어 extent를 전부 "written"으로
뒤집는다. unwritten extent가 남으면 FIEMAP이 `FIEMAP_EXTENT_UNWRITTEN`을 보고하고
이후 첫 쓰기가 **파일시스템 저널을 통한 extent 상태 변환으로 직렬화된다** — 실서버
(FIEMAP 모드) 지연 측정에 직접 영향을 준다.

`identity_pba` 테스트 모드는 FIEMAP을 쓰지 않으므로 미지원 파일시스템에서도 넘어가게
인자로 분리했다: `create_ring_file(path, size, require_zero_range)`.

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
| `loop_sleep_us` (기본 200) | `core/include/raft_server.h` | 원본은 sleep 없이 스핀하는데, 그러면 `advance_commit_index`가 매 바퀴 `s.mu`를 잡아 AE 스레드를 굶는다. **`0`이면 원본과 동일한 스핀** — 레이턴시 측정 시 이 값의 영향을 반드시 확인할 것 |
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
| U11 | **`FALLOC_FL_ZERO_RANGE` 가 ext4 에서 extent 를 "written" 으로 만들지 않는다 — D9 의 전제가 성립하지 않는다.** `create_ring_file` 은 매 기동마다 링 파일 전체에 ZERO_RANGE 를 걸고, D9 와 `blockio/cached_fd.h:29-35` 는 그것이 extent 를 전부 written 으로 뒤집어 "이후 첫 쓰기가 파일시스템 저널을 통한 extent 상태 변환으로 직렬화" 되는 것을 막는다고 서술한다. **실측 결과 ZERO_RANGE 후에도 extent 는 unwritten 으로 남는다** — ext4 에서는 범위를 unwritten 으로 두는 것이 가장 싼 zeroing 구현이기 때문으로 보인다. 커널 4.15(eternitymaster)와 6.8(eternitystorage) 양쪽에서 동일했다: `fallocate -l` → unwritten, `fallocate -l` + `fallocate -z` → **여전히 unwritten**, `dd oflag=direct`(실제 쓰기) → written. 라이브 링 파일 `/mnt/raftvol/node5/raft_meta.bin`(32GiB, 3시간 가동)도 **257 extent 중 257 개가 unwritten** 이고 블록 0(헤더 쓰기가 닿은 곳)만 written 이었다 | `blockio/cached_fd.cpp` `create_ring_file` · `DECISIONS.md D9` | **미해결. 이번에는 고치지 않았다** (blockcopy 벤치 교체 작업 범위 밖이고, `blockio`/`storage` 변경은 U7 대조군에 영향을 준다). 확인된 것은 **전제가 틀렸다는 사실**까지다 — 그 결과로 실제 지연이 얼마나 늘어나는지는 측정하지 않았다. U7(StorageIO 격차)과 관련이 있을 수 있으므로 그 조사에서 같이 볼 것. 고치려면 extent 를 실제로 written 으로 만들어야 하는데, 링 파일 전체를 한 번 쓰는 비용(기본 32GiB/노드)이 기동 시간에 그대로 붙는다 — 비용/이득을 따져 결정할 일이다. 새 벤치(`scripts/blkcopy_scaling.sh`)는 이 문제를 피하려고 백킹 파일을 `dd oflag=direct` 로 준비하고 preflight 에서 unwritten extent 를 검사해 거부한다 |

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
예외는 이 다섯 가지뿐이고, 새 예외를 만들려면 여기에 이유를 적어야 한다:

| 대상 | 이유 |
|---|---|
| `core/include/raft_basics.h` 전부 | **핫패스.** `elapsed_ns` 는 Apply 한 번에 수십 번, `put_u64_le` 는 엔트리당 4번 불린다. (예전에 여기 적혀 있던 두 번째 근거 — "`storage/` 가 include 하는데 그 바이너리는 core 를 링크하지 않는다" — 는 **더 이상 성립하지 않는다.** Phase A-2 가 `storage/` → `core/` 의존을 없앴고, 지금 이 헤더를 include 하는 것은 `core/src/*.cpp` 뿐이다. 핫패스 근거만 남는다) |
| `core/include/raft_entry.h` 의 `Entry::signal_committed` | 이 헤더는 core 를 링크하지 않는 TU 3개도 본다. `.cpp` 를 만들면 그 바이너리가 core 를 요구하게 된다 |
| `net/include/raft_proto_conv.h` 의 template 2개 | 템플릿은 헤더에 있어야 한다. **나머지 34개 변환 함수는 일부러 헤더에 남겼다** — 헤더가 여전히 `rpcproto.pb.h` 를 include해야 해서 컴파일 이득이 없고, 시그니처 68줄이 중복되며 proto 필드 추가 시 고칠 곳이 2배가 된다 |
| `net/include/raft_statemachine_hash.h` 전부 | `tests/` 가 `net/` 을 링크하지 않고 이 헤더만 include 한다. `.cpp` 를 만들면 `raft_selftest` 가 net 을 링크해야 하고 protobuf-0 성질이 깨진다 |
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
