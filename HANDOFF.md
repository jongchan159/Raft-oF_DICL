# raftof+dare — 인수인계 문서

최종 갱신: 2026-08-27 (3차 세션: 리팩토링) · 대상: 이어서 진행할 사람

> **3차 세션에서 구조 리팩토링을 했다.** 기능·성능·와이어 포맷은 불변이고,
> 이 문서의 버그 이력(§2)과 실측값(§6)은 그대로 유효하다. 다만 **코드 위치가
> 많이 바뀌었다** — 어디를 봐야 하는지는 **[DECISIONS.md](DECISIONS.md) §X**에
> 정리해 뒀다. 요약:
>
> - `Server`의 필드가 다섯 그룹으로 묶였다: `raft` / `ring` / `io` / `prof` / `workers`.
>   예를 들어 `s.current_term` → `s.raft.current_term`, `s.tail_slot` → `s.ring.tail_slot`.
> - 전송이 추상 인터페이스가 됐다 (`core/include/raft_transport.h`). `rpc_call_append_entries`
>   같은 링크타임 자유 함수는 없어졌고, `Server::transport` / `Server::blockcopy`에
>   구현체를 주입한다. 그 덕분에 **selftest가 protobuf 없이 링크된다.**
> - 링 크기가 프로세스 전역에서 인스턴스 필드로 옮겨졌다 (`s.ring.configure(pages)`).
>   `configure_ring()`과 `core/src/raft_constants.cpp`는 없어졌다.
> - 디렉터리가 정리됐다: 스토리지 서버 구현 `core/` → `storage/`,
>   블록 I/O 계층(`raft_cached_fd.*`) → `blockio/cached_fd.*`,
>   selftest 하네스 → `tests/`, `raft_statemachine_hash.h` → `net/`.
>   `core/` 는 Raft 알고리즘만 남았고 `include/`(선언) · `src/`(정의) 로 나뉘었다.
>   `net/` 도 같다. **그 뒤 세 개의 main 이 `net/src/` → `apps/` 로 나갔다**
>   (`ls net/src` 에서 진입점이 사라지고, "진입점은 `apps/`, 프로토콜은 `core/`"
>   가 디렉터리로 드러난다). 자료 정의는
>   `core/include/raft_state.h` 로 갈라져 `raft_server.h` 가 681 → 386줄이 됐다.
> - CMake + doctest 유닛 테스트 + CTest가 추가됐다 (`build.sh`도 그대로 동작한다).
> - 원본과 다른 지점의 긴 서술은 DECISIONS.md로 옮기고 코드에는 `[수정-N]`
>   1줄 포인터만 남겼다. `grep -rn '\[수정' core net` 은 계속 동작한다.

1차 세션은 "컴파일·링크까지만 확인, 런타임 검증 0"에서 끝났다. 2차
세션에서 실제로 돌려봤고, **그 과정에서 클러스터가 아예 동작하지 못하게
만드는 버그 7건과 노드를 죽이는 메모리 버그 1건이 나왔다.** 전부 수정
완료이며 지금은 3노드 e2e가 통과한다.

원본 Go 소스를 이 서버에서 찾았다: **`~/RAFT/nvmeof_raft/raft.go`**
(3255줄). 1차 세션에서 "원본을 못 봐서 추정했다"고 남긴 지점들을 전부
대조해 정정했다. 앞으로도 판단이 갈리면 이 파일이 기준이다.

---

## 1. 현재 상태

| 항목 | 상태 |
|---|---|
| 빌드 (`./build.sh all`) | ✅ `-Wall -Wextra` 경고 0 |
| self-test (`raft_selftest`) | ✅ 통과 (T1~T4, 18개 항목) |
| 3노드 e2e destination-side | ✅ ALL PASSED |
| 3노드 e2e leader-side (`MODE=leader`) | ✅ ALL PASSED |
| 링 wrap-around 스트레스 (3000×4064B / 4MiB 링 = 링 3바퀴) | ✅ ALL PASSED |
| ASan + UBSan | ✅ 클린 (검출 0) |
| 팔로워 재시작 catch-up | ⚠️ **설계상 불가** (§5, 원본도 동일) |
| ApplyTimings 항등식 | ✅ 실측으로 성립 (§4) |

검증 명령은 §6에 모아뒀다.

---

## 2. 2차 세션에서 찾은 버그 (전부 수정 완료)

모두 `grep -rn '\[수정\]' core net`으로 찾을 수 있다. 발견 순서대로
적는다 — 앞의 것을 고쳐야 뒤의 것이 보이는 구조였다.

### (1) `main_loop`에 상태 분기가 없었다 — 선거가 한 번도 일어나지 않음

`core/src/raft_lifecycle.cpp`

1차 세션의 `main_loop`은 매 바퀴 `timeout()` → `become_leader()` →
`heartbeat()` → `advance_commit_index()`를 **무조건** 호출했다. 그러면
팔로워도 `heartbeat()`를 부르고, `heartbeat()`는 상태를 보지 않고
`append_entries(nullptr)`를 쏜다. 세 노드가 서로에게 100ms마다 term 0짜리
AppendEntries를 보내고, 받은 쪽은 `reset_election_timeout()`을 한다 →
**모든 노드의 election timer가 영구히 갱신되어 선거가 시작조차 안 된다.**
증상은 term 0 고정, 리더 없음, `commit_index=0` 고정.

원본 `raft.go Start()`은 상태별 switch다. 그대로 맞췄다:

```
leader    : heartbeat();  advance_commit_index()
follower  : timeout();    advance_commit_index()
candidate : timeout();    become_leader()
```

즉 `heartbeat`는 리더만, `become_leader`는 candidate만 호출한다.

### (2) `get/set_voted_for`가 잘못된 필드를 봤다 — 같은 term에 리더 2명

`core/include/raft_server.h`

원본 정의를 확인했다:

```go
func (s *Server) setVotedFor(id uint64) { s.cluster[s.clusterIndex].votedFor = id }
func (s *Server) getVotedFor() uint64  { return s.cluster[s.clusterIndex].votedFor }
```

1차 세션은 별도의 `Server::voted_for` 필드를 두고 "원본 확인 필수"라고
TODO를 달아뒀다. 그러면 자기 투표 상태가 두 곳으로 갈린다:

- `timeout()`은 후보가 되면서 `cluster[self].voted_for = id`로 쓴다
  (`become_leader`의 표 집계가 읽는 자리)
- `set_voted_for()`는 별도 필드에만 쓴다
  (`handle_request_vote_request`의 grant 판정이 읽는 자리)

결과: **후보가 된 노드도 `get_voted_for()`가 계속 0이라 같은 term에 다른
후보에게 표를 또 준다.** 3노드 스모크에서 node1과 node2가 둘 다 term 1의
리더가 되는 split brain이 재현됐다 (서로에게 표를 줬다). 별도 필드를
없애고 `cluster.at(cluster_index).voted_for` 한 자리로 합쳤다.

### (3) `accept()`된 소켓이 listen 소켓의 `SO_RCVTIMEO`를 상속 — RPC 커넥션이 200ms마다 끊김

`net/include/raft_tcp_server.h`, `net/include/raft_blockcopy_tcp_server.h`

`run_tcp_server`가 accept 루프에서 `stop_flag`를 폴링하려고 listen fd에
200ms 수신 타임아웃을 걸어둔다. **Linux는 이 옵션을 accept된 소켓에
상속시킨다** (이 서버에서 직접 확인). 그러면 200ms 동안 요청이 없는
커넥션마다 `read()`가 EAGAIN을 돌려주고, `serve_connection`이 이를
"연결 끊김"으로 보고 fd를 닫는다.

노드 사이 RPC 커넥션은 롱리브드다. 하트비트(100ms)는 타임아웃보다
촘촘해서 살아남지만 **RequestVote(2~3초 간격)는 거의 항상 죽은 커넥션에
떨어져 실패했다** — 선거가 끝나지 않고 세 노드가 candidate에 머물렀다.
하트비트만 멀쩡한 탓에 원인이 더 헷갈렸다.

`accept()` 직후 `conn_fd`의 `SO_RCVTIMEO`를 0으로 지운다. 스토리지 서버도
같은 패턴이었으므로 같이 고쳤다 (PBA 복사 커넥션도 롱리브드라 동일한
간헐 실패가 났을 것).

### (4) `std::rand()`를 시드하지 않아 세 노드의 election timeout이 동일

`core/src/raft_election.cpp`

`srand()`를 부르지 않으면 `srand(1)`과 같으므로 **모든 노드 프로세스가
완전히 동일한 난수 수열을 뽑는다.** 세 노드의 election timeout이 매
라운드 정확히 같아져 셋이 동시에 candidate가 되고, (2)를 고친 뒤에는
서로에게 표를 주지 않으므로 **선거가 영원히 안 끝난다** (term만 계속
올라간다). `std::mt19937_64` + `std::random_device` 시드로 교체했다.
Go의 `math/rand`는 1.20부터 자동으로 랜덤 시드를 쓰기 때문에 원본에는
이 문제가 없다.

### (5) 팔로워의 `commit_index` 갱신이 엔트리 블록 안에 있었다

`core/src/raft_handle_append_entries.cpp`

원본 `raft.go`의 `// Update commit index` 블록은 함수 레벨에 있는데
(들여쓰기로 확인), 포팅에서는 `if (req.num_entries > 0 && ...)` 블록
**안**에 들어가 있었다. 그러면 `num_entries == 0`인 순수 하트비트가
팔로워의 `commit_index`를 못 올려서, 마지막 배치 이후의 커밋이 팔로워에
영원히 전달되지 않는다. 스모크에서 리더 `commit_index=202`, 팔로워
`192`(= 마지막 배치 크기만큼 뒤처짐)로 고정되는 것으로 재현됐다.

### (6) Apply의 커밋 후 대기가 뮤텍스를 쥔 채 sleep — Apply 하나에 340ms

`core/src/raft_apply.cpp`

`result_sink` 도착을 기다리는 루프가 `std::lock_guard`의 스코프를 루프
본문 전체로 잡아서 **100µs sleep 중에도 `collector->mu`를 쥐고 있었다.**
`result_sink`는 `apply_pending`이 `s.mu`를 잡은 채로 호출하므로, sink가
`collector->mu`에서 막히면 `s.mu`까지 같이 묶인다 — 리더의 메인 루프 /
AE / `advance_commit_index`가 전부 정지한다. 게다가 스핀 스레드가 매
바퀴 락을 즉시 재획득해서(barging) sink가 계속 밀려나 상한(2000 ×
~170µs)인 340ms까지 대기가 늘어났다.

`apply-timed`에서 `total=340ms`인데 구간 합은 4ms였던 것이 이 현상이다.
조건변수로 바꿔 대기 중에는 락을 놓게 했다. **4064B 단건 Apply
224ms → 2.9ms (약 78배).**

### (7) `ReplSink`를 호출자 스택에 두어 stack-use-after-return — 노드 크래시

`core/src/raft_apply.cpp`, `core/src/raft_append_entries.cpp`, `core/include/raft_server.h`

`apply_internal`이 `ReplSink`를 **스택에** 만들고 `&sink_storage`를
`append_entries`에 넘겼다. `append_entries`는 팔로워마다 fire-and-forget
스레드를 띄우고 즉시 리턴하는데, 그 워커들은 `apply_internal`보다 오래
산다. `apply_internal`이 리턴하면 그 스택 프레임이 사라지고, 워커가
계속 `sink->push()` / `sink->mutex_a_ns()`를 호출한다.

실제 증상: 노드가 `double free or corruption (out)`으로 죽는다.
`-op apply-timed`(= `ClientApplyTimed`, 즉 **레이턴시 측정 경로**)를 쓰면
5회 시도 5회 재현됐다. ASan이 `stack-use-after-return`으로 정확히 짚었다:

```
READ of size 8 ... in ReplSink::mutex_a_ns() core/include/raft_timings.h:158
  in Server::append_entries_worker(int, ReplSink*) core/src/raft_append_entries.cpp:468
Address ... is located in stack of thread T121 at offset 5552 in frame
  #0 apply_internal core/src/raft_apply.cpp:71
```

원본 Go는 GC가 sink를 살려주므로 이 문제가 없다 — C++ 포팅에서만 생기는
클래스의 버그다. `append_entries(std::shared_ptr<ReplSink>)`로 바꾸고
각 워커 람다가 shared_ptr을 값으로 캡처하게 했다. 마지막 워커가 끝날
때까지 살아 있는다. **재검증: ASan 클린, 5/5 무크래시.**

### (8) `init_storage` 재시작 경로가 원본과 달랐다 (그리고 깨져 있었다)

`core/src/raft_lifecycle.cpp`

원본 `restoreCircular`은 헤더에서 `currentTerm`/`votedFor`만 복구하고
`tailLogIndex`/`tailSlot`/`commitIndex`/`lastApplied`를 읽는 네 줄을
**주석 처리해둔 채** 1/0/0/1로 되돌린다. 원본 주석:

> On restart, start with empty in-memory log. Leader election will resync
> all entries via appendEntries. The ring buffer on disk preserves data
> for PBA copy correctness.

1차 세션은 네 값을 모두 복구했다. 그러면 in-memory `log`는 sentinel
하나(size 1)인데 `tail_log_index`가 1보다 커져서
`log_slice()`/`oldest_log_index()` 계산이 로그 벡터 범위를 벗어난다 —
재시작 노드가 첫 AppendEntries에서 바로 깨진다. 원본과 동일하게
term/vote만 복구하도록 고쳤다.

### (9) `create_ring_file`에 `FALLOC_FL_ZERO_RANGE`가 빠져 있었다

`blockio/cached_fd.cpp`

원본은 매 기동마다 링 파일 전체에 ZERO_RANGE를 걸어 extent를 전부
"written"으로 뒤집는다. 원본 주석: unwritten extent가 남으면 FIEMAP이
`FIEMAP_EXTENT_UNWRITTEN`을 보고하고 이후 첫 쓰기가 파일시스템 저널을
통한 extent 상태 변환으로 직렬화된다. **실서버(FIEMAP 모드) 지연 측정에
직접 영향을 준다.** 추가했고, `identity_pba` 테스트 모드에서는
FIEMAP을 안 쓰므로 미지원 파일시스템에서도 넘어가게 인자로 분리했다
(`create_ring_file(path, size, require_zero_range)`).

### (10) UBSan: `memcpy(dst, nullptr, 0)` 2건

`core/src/raft_persist.cpp` (no-op 엔트리의 빈 command),
`net/include/raft_wire_codec.h` (반환값 없는 RPC의 빈 body). 동작은 무해했지만
형식상 UB라 UBSan이 잡는다. 빈 경우를 건너뛰게 했다 — 앞으로 진짜
문제가 묻히지 않도록.

---

## 3. 1차 세션 수정 중 되돌린 것 2건

둘 다 "(1) main_loop에 상태 분기가 없던" 시절의 증상을 보고 넣은
보상 코드였다. 원인이 밝혀졌으므로 원본대로 되돌렸다. 코드에 이유를
주석으로 남겨뒀다.

| 파일 | 되돌린 내용 | 이유 |
|---|---|---|
| `raft_election.cpp` `become_leader` | `cluster[i].voted_for = 0` 리셋 | 원본에 없다. `become_leader`는 candidate 상태에서만 호출되고 승격 즉시 state가 Leader가 되므로 재진입 자체가 없다. 다음 선거에서 `timeout()`이 전부 0으로 되돌린다 |
| `raft_election.cpp` `heartbeat` | 리더의 `reset_election_timeout()` | 원본에 없다. 리더는 `timeout()`을 아예 호출하지 않으므로 election_timeout이 만료돼도 강등되지 않는다 |

1차 세션 §2.A의 버그 수정 8건(a~g)은 **그대로 유효하다.** 특히 (a)
`read_entry_direct`의 슬롯 0 480바이트 누락은 self-test T1이 경계값
480/481/4064를 포함해 직접 검증한다.

---

## 4. 해소된 질문: 로그 인덱스 규약

1차 세션이 "기존 주석은 0-based라는데 `prev_log_index == 0` 처리와
모순이다"라고 남긴 지점. **원본 `restoreCircular`과 대조해 확정했다:**

```go
s.tailLogIndex = 1
s.tailSlot     = 0
s.commitIndex  = 0
s.lastApplied  = 1
```

즉 `init_storage`의 초기값 선택(`tail_log_index = 1`)이 원본과 정확히
같다. **"0-based로 결정했다"는 기존 주석 서술이 오기였다.** 실제 엔트리는
인덱스 1부터고 인덱스 0은 sentinel 자리다. 코드 주석도 이에 맞게
정정했다.

---

## 5. 남아 있는 설계 한계: 팔로워 재시작 catch-up 불가

`./scripts/restart_test.sh`가 이걸 재현한다. **원본 `raft.go`도 동일한
코드이므로 포팅 버그가 아니다.** 서로 모순인 두 결정 때문이다:

- **(a)** `restoreCircular` / `init_storage`: 재시작 시 `tailLogIndex`를
  1로 되돌리고 in-memory 로그를 비운다
  ("Leader election will resync all entries via appendEntries")
- **(b)** `appendEntries`의 backoff floor guard:
  ```
  floor = cluster[fi].matchIndex + 1
  nextIndex = max(max(newNext, 1), floor)
  ```
  ("Never back off past matchIndex+1 ... those PBA slots may have been
  freed by tier 1")

(a) 때문에 재시작 노드는 어떤 `prev_log_index`도 인정할 수 없어
`conflict_index = tail_log_index = 1`을 돌려주는데, (b) 때문에 리더는
`nextIndex`를 죽기 직전의 `matchIndex+1` 밑으로 못 내린다. 그래서 리더는
영원히 같은 `prev_log_index`를 보내고 팔로워는 영원히 거절한다.
실측: 리더 `commit_index=203`, 재시작 노드 `commit_index=0` 고정.

재시작 노드는 자기 링에 데이터를 그대로 갖고 있으므로, 제대로 고치려면
**`init_storage`가 `read_entry_direct`로 링을 스캔해 in-memory 로그를
복원**해야 한다 (§7의 1번). floor guard를 그냥 푸는 것은 권하지 않는다 —
그 guard가 지키는 건 "GC된 슬롯을 PBA로 읽어 stale 데이터를 복제하지
않는다"는 안전 불변식이고, 풀면 `[SKIP PBA]`로 막히는 자리로 교착이
옮겨갈 뿐이다.

안전성 자체는 유지된다: 빈 로그로 재시작한 노드는 `last_log_term=0`이라
로그가 더 긴 노드들이 표를 주지 않으므로 리더가 될 수 없다.

`restart_test.sh`는 기본적으로 이 두 항목을 `[KNOWN GAP]`으로 보고하고
종료코드 0을 낸다. 복원 경로를 구현한 뒤 `EXPECT_CATCHUP=1`로 돌리면
하드 실패로 바뀐다. 나머지 항목(헤더 term/vote 복구, 2/3 쿼럼으로 커밋
계속, 재시작 자체의 정상 기동)은 실제로 검증되며 통과한다.

---

## 6. 실측값

### 6.1 ApplyTimings 항등식

`Total ≈ LHandler + LPersist + AENet + FHandler + ReplNet + StorageIO + QuorumWait`

4064B 단건 Apply, 3노드 identity-pba, `-profile` (단위 µs):

```
total=2565.7  LHandler=8.6  LPersist=1650.3  AENet=125.6  FHandler=4.6
              ReplNet=194.0  StorageIO=186.6  QuorumWait=362.1
              Mutex=1.2  CommitWait=784.2
```

우변 합 = 2531.8 vs total 2565.7 → 오차 1.3%. **항등식 성립.**
`LPersist`가 64%를 차지한다 (O_DIRECT write + fdatasync).

`AENet/FHandler/ReplNet/StorageIO`가 전부 0이고 `QuorumWait`이 replicate
전체를 흡수한 행이 섞여 나오는 것은, 그 Apply 안에서 data-bearing AE
샘플을 못 잡아 `ProfilingSink::sample_*` 폴백으로 넘어간 경우다
(직전 하트비트가 이미 복제를 끝낸 상황).

**2026-09-02 이전에는 이 행이 진짜 0이었다.** 폴백이 읽는 `sample_*`에
store가 0건이어서 항상 0을 반환했기 때문이다 — 즉 "빨랐다"로 오독되는
0이었다. 지금은 `append_entries_worker`가 data-bearing AE마다 `sample_*`를
채우므로(가드가 `sink != nullptr` **밖**에 있어야 하트비트도 남긴다)
직전 라운드의 실제 값이 나온다. 3노드 60행 교차 측정에서 전부-0 행이
1건 → 0건이 됐다. 자세한 경위는 DECISIONS.md U4.

### 6.2 스루풋 (로컬 3노드, identity-pba, 링 16MiB)

| 워크로드 | 결과 |
|---|---|
| 512B × 2000, batch=10 | 668 ms, **334 µs/command**, busy_retries=0 |
| 4064B × 20, batch=1 | **2.9 ms/command** (수정 전 224 ms) |
| 4064B × 3000, batch=10, 링 4MiB (링 3바퀴) | 1047 ms, 349 µs/command, busy_retries=0 |

AE 증폭도 정상이다: 2000건에서 `ae_count=404 ae_entries=4004` =
정확히 2 팔로워 × 2002 엔트리, 즉 **엔트리당 1회 전송.**

> 주의: `MaxAppendEntriesBatch` 기본값이 `1000000`(사실상 무제한)이라,
> 팔로워 하나가 죽어 있으면 리더가 매 라운드 밀린 백로그 전체를
> 재전송한다 (죽은 노드 상대로 `ae_entries`가 엔트리 수의 100배까지
> 올라가는 것을 관측했다). 레이턴시 측정 시 `-ae-batch`로 상한을 주는
> 편이 안전하다.

### 6.3 복제 정확성의 직접 증거

세 노드 링 파일이 헤더(512B) 이후 **바이트 단위로 동일**하고, 비교 구간이
실제로 데이터를 담고 있음을 함께 확인한다 (200건에서 103,207 non-zero
바이트, 3000건에서 4,164,630 non-zero 바이트). 리더 선출이 안 되던
동안에는 세 파일이 모두 0이라 `cmp`가 **무의미하게 통과**했으므로,
스모크 테스트에 non-zero 검사를 추가했다.

---

## 7. 다음에 할 일 (우선순위 순)

> 3차 세션 이후: 아래 중 8번(미사용 필드 정리)과 10번(단위 테스트 프레임워크)은
> 처리됐다. 6·7번(계측 배선 미완)과 3번(팔로워 slot_map)은 DECISIONS.md의
> U4 / U2로 옮겨 코드 주석에서 바로 참조된다. 나머지는 그대로 남아 있다.

1. **재시작 노드의 로그 복원** (§5). `init_storage`가 `read_entry_direct`로
   링을 스캔해 in-memory 로그와 `tail_log_index`/`tail_slot`를 복원한다.
   이게 되면 `EXPECT_CATCHUP=1 ./scripts/restart_test.sh`가 통과해야
   한다. 크래시 복구 시나리오 전체의 전제 조건이다.
2. **실서버(FIEMAP + 실제 NVMe-oF) 경로.** `-identity-pba`를 빼고 돌린다.
   조건은 §8.3에 정리. ZERO_RANGE가 이제 들어갔으니 unwritten extent
   직렬화는 걱정하지 않아도 된다.
3. **팔로워의 `log_slot_map` 미기록.** 팔로워가 맵을 채우지 않아 승격
   직후 새 리더는 자기 no-op만 PBA 복제할 수 있다. 채우면 되지만
   `do_slot_gc`가 리더 전용이라 팔로워 맵이 무한히 자란다. 원본이 이
   지점을 어떻게 처리하는지 `~/RAFT/nvmeof_raft/raft.go`에서 확인 후
   같이 고쳐야 한다. 코드에 TODO로 남아 있다.
4. **`apply_pending`이 리더 전용.** 팔로워는 상태머신에 반영하지 않아
   `-op hash`로 팔로워를 비교할 수 없다(그래서 링 파일 바이트 비교로
   검증한다). 원본 주석이 "(1) Leader:"로 시작하니 의도된 것으로
   보이지만, 리더 페일오버 테스트에는 확인이 필요하다.
5. **`main_loop`의 `loop_sleep_us` 기본 200µs.** 원본은 sleep 없이
   스핀한다. **레이턴시 측정 전에 이 값의 영향을 반드시 확인할 것**
   (`-loop-sleep-us 0`이 원본 동작).
6. **`ReplSample.post_rpc_wall_ns` / `lock_c_held_ns` 미기록.**
   `append_entries_worker`가 Lock C 보유 시간을 `(void)t_c_held_start;`로
   버린다. `ApplyTimings.post_rpc_ns`가 0으로 남아 `wg_scheduling_ns`
   역산이 부정확하다.
7. **`handle_ae_*` 5필드의 실측 확인.** 1차 세션에서 proto 필드를 추가해
   와이어로 넘어가게 만들었지만(수정 e), `ClientApplyTimedResponse`가
   이 값들을 노출하지 않아 클라이언트에서 볼 방법이 없다. 같은 메시지의
   `handler_duration_ns`는 `FHandler`로 실측 확인됐으므로 배선 자체는
   동작한다. 팔로워 핸들러 내부 분해가 필요하면 proto에 필드를 더
   노출해야 한다.
8. **`persist_count` / `persist_count_placeholder`** 미사용 필드 정리.
9. `handle_write_pba`(단건)/`GetTime`/`ResetTime`은 proto 메시지도 없고
   디스패치도 없다. `do_pba_copy`가 배치만 쓰므로 당장 필요하지 않다.
10. **단위 테스트 프레임워크가 없다.** 지금은 self-test + 스크립트 3개다.
    최소한 fast log backoff 경로와 extent 경계 clamp에 테스트가 필요하다.

---

## 8. 빌드하고 돌리기

### 8.1 protobuf 준비

생성된 `proto/rpcproto.pb.h`가 **protobuf ≥ 3.21.0** 을 요구하고
`protoc 3.21.12`로 생성돼 있다 (Ubuntu 24.04 noble 기본 버전과 일치).

**root 있으면:** `sudo apt install -y libprotobuf-dev protobuf-compiler`

**root 없으면:** `.deb`를 로컬에 풀어서 쓴다. **단 배포판에 3.21 이상이 있어야
한다** (아래 패키지명은 Ubuntu 24.04 noble 기준).

```bash
SYSROOT=/tmp/pb-sysroot          # 반드시 로컬 디스크. /home이 NFS면 매우 느리다
mkdir -p /tmp/pbdeb "$SYSROOT" && cd /tmp/pbdeb
apt-get download libprotobuf-dev libprotobuf32t64 protobuf-compiler libprotoc32t64
for d in *.deb; do dpkg-deb -x "$d" "$SYSROOT"; done
```

**배포판이 오래된 경우 — 소스에서 짓는다.** 예를 들어 `eternitymaster`는
Ubuntu 18.04(bionic)이고 apt에는 protobuf 3.0.0뿐이다. noble .deb를 가져오는
것은 glibc 버전이 안 맞아 통하지 않는다. 40코어에서 ~2분 걸린다.

```bash
cd /tmp && curl -fsSLO https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-cpp-3.21.12.tar.gz
tar xzf protobuf-cpp-3.21.12.tar.gz && cd protobuf-3.21.12
cmake -S . -B _b -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/tmp/pb-sysroot/usr \
  -DCMAKE_INSTALL_LIBDIR=lib/x86_64-linux-gnu \
  -DBUILD_SHARED_LIBS=ON -Dprotobuf_BUILD_TESTS=OFF
cmake --build _b -j"$(nproc)" && cmake --install _b
```

> bionic의 protoc 3.0.0으로 `.proto`를 재생성하는 것도 문법상 가능하지만
> (이 `.proto`는 평범한 proto3 스칼라뿐이다), 그러면 커밋된
> `rpcproto.pb.{h,cc}`(protoc 3.21.12 생성물)를 덮어써서 실서버 쪽과
> 갈라진다. 최후 수단으로만 쓸 것.

> `$HOME/pb-sysroot`에 풀지 말 것. `/home`이 NFS(115.145.173.24:/home)라
> 헤더 수천 개 추출이 2분 넘게 걸려 타임아웃 났다. `/tmp/pb-sysroot`는
> 몇 초면 끝난다. 단, **재부팅하면 사라지므로 위 3줄을 다시 돌려야 한다.**

```bash
PROTOBUF_SYSROOT=/tmp/pb-sysroot ./build.sh all
# .proto를 수정했다면 (패키지 버전이 3.21.12가 아니면 필수)
LD_LIBRARY_PATH=/tmp/pb-sysroot/usr/lib/x86_64-linux-gnu \
  PROTOC=/tmp/pb-sysroot/usr/bin/protoc ./build.sh regen-proto
```

`build.sh`가 `-Wl,-rpath`를 넣어주므로 실행 시 `LD_LIBRARY_PATH`는
필요 없다 (protoc 직접 실행만 예외).

### 8.2 빌드 타깃

```bash
./build.sh              # 라이브러리 오브젝트만
./build.sh check        # 링크까지 해서 undefined 심볼 확인
./build.sh all          # raft_node, raft_client, raft_blockcopy_server, raft_selftest
./build.sh asan         # raft_node_asan (ASan+UBSan, -O1 -g)  <- 신규
./build.sh clean
```

### 8.3 파일시스템 주의

- **`/home`(NFS)에서는 못 돌린다.** O_DIRECT와 FIEMAP이 필요하다.
- 테스트 작업 디렉터리는 ext4/xfs 로컬 경로(`/tmp` 등)를 써야 한다.
  이 서버의 `/`는 ext4(455G)이고 `/tmp`는 거기 속한다.
- 확인: `findmnt -no FSTYPE <경로>`

실서버(FIEMAP) 경로의 추가 조건:

- 링 메타데이터 파일이 대상 블록 디바이스 위 파일시스템에 있어야 한다
- 스토리지 노드의 `-devices`는 **클러스터 인덱스 순서**로 각 멤버의
  볼륨 경로여야 한다
- `raft_blockcopy_server`가 디바이스를 `O_RDWR|O_DIRECT`로 열어야 하므로
  보통 root 또는 적절한 그룹 권한이 필요하다
- `-ring-pages` 기본값(8Mi = 32GiB/노드)은 fallocate + ZERO_RANGE에
  시간과 공간이 든다. 처음에는 작게 잡고 올릴 것
- **Leader-Side 모드(`-mode leader`)**: 리더의 스토리지 노드가 팔로워
  볼륨에 직접 쓰므로, 각 스토리지 노드가 **모든** 멤버 볼륨을 NVMe-oF로
  attach해 열어둔 상태여야 한다 (`core/include/raft_server.h`의
  `ReplicationMode` 주석 참고)

---

## 9. 검증 명령 모음

```bash
export PROTOBUF_SYSROOT=/tmp/pb-sysroot
./build.sh all

# 1) 네트워크 없는 로컬 검증 (T1~T4). 종료코드 0 = 전부 통과
mkdir -p /tmp/raftof_selftest && ./build/raft_selftest /tmp/raftof_selftest

# 2) 3노드 e2e (destination-side)
./scripts/smoke_test.sh /tmp/raftof_smoke 200

# 3) 3노드 e2e (leader-side / DARE 정책)
MODE=leader ./scripts/smoke_test.sh /tmp/raftof_smoke_ls 200

# 4) 링 wrap-around + slot GC 스트레스 (4MiB 링에 12MB = 링 3바퀴)
RING_PAGES=1024 CMD_SIZE=4064 BATCH=10 \
  ./scripts/smoke_test.sh /tmp/raftof_wrap 3000

# 5) 팔로워 재시작 (§5의 KNOWN GAP 포함)
./scripts/restart_test.sh /tmp/raftof_restart 100
EXPECT_CATCHUP=1 ./scripts/restart_test.sh   # 복원 구현 후 하드 검증용

# 6) ASan + UBSan (노드만)
./build.sh asan
#   raft_node 대신 build/raft_node_asan을 띄우고 위 워크로드를 돌린 뒤
#   노드 로그에서 'ERROR: AddressSanitizer' / 'runtime error'를 grep
```

### 조회/측정

```bash
ADDRS=127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003
./build/raft_client -addrs $ADDRS -op apply-timed -n 20 -size 4064 -batch 1
./build/raft_client -addrs $ADDRS -op commit-index
./build/raft_client -addrs $ADDRS -op hash -at-count 201
./build/raft_client -addrs $ADDRS -op ae-stats

# 노드 상태를 1초마다 한 줄로 (신규): -debug
./build/raft_node ... -debug
#   [state] leader term=1 tail=204 commit=203 applied=204 log_len=204
#           | p1 vf=3 next=103 match=102 | p2 vf=3 next=204 match=203 ...
#   선거/복제가 멈췄을 때 이걸로 어느 단계에서 막혔는지 바로 보인다.

# 원본과 다르게 만든 지점 찾기 (설명은 DECISIONS.md)
grep -rn '\[수정' core net
```

---

## 10. 2차 세션에 추가된 관측 수단

디버깅에 실제로 필요했던 것들이다. 전부 유지할 가치가 있다.

| 위치 | 내용 |
|---|---|
| `apps/raft_node_main.cpp` | `-debug` 상태 덤프 (1초 간격). `debug_enabled`는 필드만 있고 아무것도 출력하지 않았다 — 밖에서 상태를 볼 방법이 전혀 없었다 |
| `net/include/raft_tcp_server.h` | `[rpc-error]` 로그. 서버 쪽 RPC 에러가 완전히 안 보였다 (클라이언트는 프레임을 못 쓰면 그마저도 못 봤다) |
| `core/src/raft_append_entries.cpp` | **`log_skip_pba_diag` 포팅** (원본 `logSkipPBADiag`). `log_slot_map[next]`가 없어 PBA 복제를 포기하고 하트비트만 보내는 순간의 스냅샷. 기존에는 `find_slot_map_trace`와 함께 호출부가 없어서, 팔로워가 영구히 못 따라잡는 상태가 로그에 전혀 안 남았다. 원본은 매번 무조건 찍어 팔로워당 초당 10줄이 나오므로, **같은 (follower, next)는 1초에 한 번**만 찍도록 스로틀만 추가했다 |
| `scripts/smoke_test.sh` | 링 비교 구간의 non-zero 검사 (§6.3). 없으면 복제가 아예 없었을 때도 `cmp`가 통과한다 |
| `scripts/restart_test.sh` | 신규 (§5) |
| `build.sh` | `asan` 타깃 |

---

## 11. 참고

- 원본 Go: `~/RAFT/nvmeof_raft/raft.go` (3255줄).
  다른 변형: `~/RAFT/goraft/raft.go`, `~/RAFT/save/urp_raft/raft.go`,
  `~/RAFT/save/nvmeof_raft_backup/raft.go`
- 스토리지 서버 원본 대응: `~/RAFT/server_random`
- 이 디렉터리는 작업 중 `raftof` → `raftof+dare`로 이름이 바뀌었다.
  `~/raftof`는 빈 디렉터리로 남아 있다.
