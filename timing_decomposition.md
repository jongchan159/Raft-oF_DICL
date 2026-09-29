# Raft-oF v2 Timing Decomposition: Destination vs Leader Mode

## 목적

`raftofv2`의 `apply-timed` 계측에서 `destination` mode와 `leader` mode는 block-copy와 AppendEntries의 실행 순서가 다르다.

따라서 두 mode에 동일한 timing decomposition 식을 적용하면 `FHandler`, `QuorumWait`, `residual` 등이 잘못 계산될 수 있다.

이 문서는 두 mode의 실제 호출 순서와 이를 반영하기 위해 필요한 `ReplSample::leader_side` 플래그 및 timing 계산식을 정리한다.

---

## 1. 주요 timing 값

`ReplSample`에는 대략 다음 raw timing 값들이 들어간다.

```
struct ReplSample {
    int64_t ae_rt_ns{0};
    int64_t r2_ns{0};

    int64_t write_pba_rt_ns{0};
    int64_t storage_copy_ns{0};

    int64_t mutex_ns{0};
    int64_t mutex_c_ns{0};
    int64_t post_rpc_wall_ns{0};

    bool leader_side{false};
};
```

각 값의 의미:

* `ae_rt_ns`

  * leader가 본 AppendEntries RPC 전체 wall-clock
* `r2_ns`

  * follower의 `handle_append_entries_request()` 전체 wall-clock
* `write_pba_rt_ns`

  * `WritePBABatch` RPC 전체 wall-clock
* `storage_copy_ns`

  * blockcopy server 내부 StorageIO wall-clock
  * parallel `pread/pwrite + fdatasync`
* `leader_side`

  * 이 sample이 leader-side replication mode에서 측정된 것인지 표시

`leader_side`는 timing 값이 아니라 동일한 raw timing을 어떤 decomposition 식으로 해석해야 하는지 결정하는 플래그다.

---

## 2. Destination mode 호출 순서

Destination mode에서는 follower가 block-copy를 요청한다.

```
Leader
  |
  | AppendEntries RPC
  v
Follower
  |
  | handle_append_entries_request()
  |
  +-- follower local processing
  |
  +-- WritePBABatch RPC
  |      |
  |      +-- ReplNet
  |      |
  |      +-- StorageIO
  |
  +-- follower local processing
  |
  v
AppendEntries response
```

따라서 timing 포함 관계는:

```
AE_RT
├─ AENet
└─ R2
   ├─ FHandler
   └─ WritePBA_RT
      ├─ ReplNet
      └─ StorageIO
```

즉:

```
AE_RT
= AENet
+ FHandler
+ ReplNet
+ StorageIO
```

Destination mode의 계산식:

```
AENet =
    ae_rt_ns - r2_ns;

FHandler =
    r2_ns - write_pba_rt_ns;

ReplNet =
    write_pba_rt_ns - storage_copy_ns;

StorageIO =
    storage_copy_ns;

QuorumWait =
    replicate_ns - ae_rt_ns;
```

---

## 3. Leader mode 호출 순서

Leader mode에서는 leader가 block-copy를 먼저 수행한다.

```
Leader
  |
  +-- WritePBABatch RPC
  |      |
  |      +-- ReplNet
  |      |
  |      +-- StorageIO
  |
  +-- AppendEntries RPC
         |
         v
      Follower
         |
         +-- follower handler
         |
         v
      AE response
```

중요한 점은 `WritePBA_RT`가 `AE_RT` 안에 포함되지 않는다는 것이다.

실행 순서는:

```
WritePBA_RT
→ AE_RT
→ quorum/commit wait
```

따라서 timing 구조는:

```
Replicate
├─ WritePBA_RT
│  ├─ ReplNet
│  └─ StorageIO
│
├─ AE_RT
│  ├─ AENet
│  └─ FHandler
│
└─ QuorumWait
```

Leader mode의 계산식:

```
AENet =
    ae_rt_ns - r2_ns;

FHandler =
    r2_ns;

ReplNet =
    write_pba_rt_ns - storage_copy_ns;

StorageIO =
    storage_copy_ns;

QuorumWait =
    replicate_ns
    - write_pba_rt_ns
    - ae_rt_ns;
```

---

## 4. 기존 코드가 Leader mode에서 틀렸던 이유

기존 `derive_apply_timings()`는 destination mode 식을 모든 mode에 적용했다.

기존 코드:

```
t.f_handler_ns =
    timings_clamp0(
        sample.r2_ns -
        sample.write_pba_rt_ns);

t.quorum_wait_ns =
    timings_clamp0(
        w.replicate_ns -
        sample.ae_rt_ns);
```

이 식은 destination mode에서는 맞다.

하지만 leader mode에서는 `WritePBA_RT`가 `R2` 내부에 있지 않다.

따라서:

```
FHandler = R2 - WritePBA_RT
```

를 계산하면 보통:

```
R2 << WritePBA_RT
```

이므로 음수가 되고 `clamp0()`에 의해 0으로 뭉개진다.

실제 측정에서도:

```
FHandler mean ≈ 0
p50 = 0
p99 = 0
```

같은 결과가 나타날 수 있다.

---

## 5. 기존 QuorumWait가 틀렸던 이유

Leader mode에서 `replicate_ns`는 다음을 포함한다.

```
replicate_ns
=
leader-side WritePBA_RT
+ AE_RT
+ 실제 quorum/commit wait
+ 기타 scheduling/overhead
```

그런데 기존 코드는:

```
QuorumWait =
    replicate_ns - ae_rt_ns;
```

만 수행했다.

따라서 실제로는:

```
기존 QuorumWait
≈ WritePBA_RT
  + 실제 QuorumWait
```

가 된다.

그리고 동시에 `ReplNet`, `StorageIO`를 별도 identity term으로 더하므로 leader-side block-copy 시간이 중복 계상된다.

그 결과:

```
7개 identity term 합 > Total
```

이 되어 `residual`이 큰 음수가 될 수 있다.

예:

```
Total      = 779.5 us
stage sum  = 968.1 us
residual   = -188.6 us
```

이것은 실제 성능 특성이 아니라 timing decomposition의 중복 계상 문제다.

---

## 6. ReplSample에 leader_side 추가

`ReplSample` 정의에 다음 필드를 추가한다.

```
bool leader_side{false};
```

예:

```
struct ReplSample {
    int64_t ae_rt_ns{0};
    int64_t r2_ns{0};

    int64_t write_pba_rt_ns{0};
    int64_t storage_copy_ns{0};

    int64_t mutex_ns{0};
    int64_t mutex_c_ns{0};
    int64_t post_rpc_wall_ns{0};

    bool leader_side{false};
};
```

의미:

```
false
→ Destination mode timing formula 사용

true
→ Leader-side timing formula 사용
```

---

## 7. append_entries_worker()에서 mode 기록

`append_entries_worker()`는 현재 서버의 replication mode를 이미 알고 있다.

sample 생성 시:

```
sample.leader_side =
    (replication_mode == ReplicationMode::LeaderSide);
```

를 추가한다.

예:

```
ReplSample sample;

sample.ae_rt_ns = rt_ns;
sample.r2_ns = rsp.handler_duration_ns;
sample.write_pba_rt_ns = write_pba_rt_ns;
sample.storage_copy_ns = storage_copy_ns;

sample.leader_side =
    (replication_mode == ReplicationMode::LeaderSide);
```

이후:

```
sink->push(sample);
```

하면 해당 Apply가 받은 timing sample에 mode 정보도 같이 들어간다.

---

## 8. derive_apply_timings() 수정

공통 계산:

```
t.ae_net_ns =
    timings_clamp0(
        sample.ae_rt_ns -
        sample.r2_ns);

t.repl_net_ns =
    timings_clamp0(
        sample.write_pba_rt_ns -
        sample.storage_copy_ns);

t.storage_io_ns =
    sample.storage_copy_ns;
```

그 뒤 mode별로 나눈다.

```
if (sample.leader_side) {
    t.f_handler_ns =
        sample.r2_ns;

    t.quorum_wait_ns =
        timings_clamp0(
            w.replicate_ns
            - sample.write_pba_rt_ns
            - sample.ae_rt_ns);
} else {
    t.f_handler_ns =
        timings_clamp0(
            sample.r2_ns
            - sample.write_pba_rt_ns);

    t.quorum_wait_ns =
        timings_clamp0(
            w.replicate_ns
            - sample.ae_rt_ns);
}
```

---

## 9. corrected QuorumWait도 mode별 처리 필요

현재 corrected replication:

```
t.replicate_corrected_ns =
    timings_clamp0(
        w.replicate_ns -
        t.mutex_ns);
```

Destination mode에서는:

```
t.quorum_wait_corrected_ns =
    timings_clamp0(
        t.replicate_corrected_ns -
        sample.ae_rt_ns);
```

Leader mode에서는 `WritePBA_RT`도 따로 빼야 한다.

```
if (sample.leader_side) {
    t.quorum_wait_corrected_ns =
        timings_clamp0(
            t.replicate_corrected_ns
            - sample.write_pba_rt_ns
            - sample.ae_rt_ns);
} else {
    t.quorum_wait_corrected_ns =
        timings_clamp0(
            t.replicate_corrected_ns
            - sample.ae_rt_ns);
}
```

---

## 10. ProfilingSink의 sample_leader_side

현재 코드에는 `ReplSink`가 비었을 때 global `prof.sample_*`를 사용하는 fallback이 있다.

```
auto [sample, have] = sink->first_sample();

if (!have) {
    sample.ae_rt_ns =
        prof.sample_ae_rt_ns.load();

    ...
}
```

이 fallback까지 mode-aware하게 만들려면 global sample에도 mode를 저장해야 한다.

`ProfilingSink`:

```
std::atomic<bool> sample_leader_side{false};
```

global sample 갱신 시:

```
prof.sample_leader_side.store(
    replication_mode == ReplicationMode::LeaderSide);
```

fallback 시:

```
sample.leader_side =
    prof.sample_leader_side.load();
```

---

## 11. Global fallback 자체는 별도 TODO

`sample_leader_side`를 추가한다고 해서 global fallback의 correlation 문제가 해결되는 것은 아니다.

현재:

```
prof.sample_*
```

는 서버 전체의 가장 최근 sample 한 개만 보관한다.

따라서 concurrent Apply 상황에서는:

```
Apply #1
→ sink empty

Apply #2
→ global prof.sample_* 갱신

Apply #1
→ Apply #2의 sample을 fallback으로 읽음
```

이 가능하다.

이는 별도의 profiling correctness 이슈다.

현재 권장 상태:

```
mode-aware decomposition
→ 지금 수정

per-Apply sample correlation
→ TODO
```

---

## 12. 수정 대상 파일

### ReplSample 정의 파일

추가:

```
bool leader_side{false};
```

### core/src/raft_append_entries.cpp

추가:

```
sample.leader_side =
    (replication_mode == ReplicationMode::LeaderSide);
```

global fallback까지 유지한다면:

```
prof.sample_leader_side.store(sample.leader_side);
```

### ProfilingSink 정의 파일

global fallback을 유지한다면:

```
std::atomic<bool> sample_leader_side{false};
```

### core/src/raft_types.cpp

`derive_apply_timings()`에서 destination / leader decomposition 분리.

---

## 13. 검증 방법

Destination mode:

```
./bench_latency.sh destination rdma 4064 10000 1
```

기대:

```
FHandler > 0
residual이 기존처럼 작은 값
```

기존 destination 결과 예:

```
Total        ≈ 631 us
FHandler     ≈ 5 us
residual     ≈ +22 us
```

Leader mode:

```
./bench_latency.sh leader rdma 4064 10000 1
```

기존 잘못된 결과 예:

```
FHandler     ≈ 0
residual     ≈ -188 us
```

수정 후 기대:

```
FHandler
→ follower R2 수준의 작은 양수

QuorumWait
→ 기존보다 WritePBA_RT 만큼 감소

residual
→ 큰 음수에서 벗어남
```

정상적인 결과라면 평균 기준:

```
Total
≈ LHandler
 + LPersist
 + AENet
 + FHandler
 + ReplNet
 + StorageIO
 + QuorumWait
 + residual
```

이 성립해야 한다.

---

## 14. 핵심 요약

Destination mode:

```
AE_RT
└─ R2
   └─ WritePBA_RT
```

따라서:

```
FHandler = R2 - WritePBA_RT
QuorumWait = Replicate - AE_RT
```

Leader mode:

```
WritePBA_RT
→ AE_RT
```

따라서:

```
FHandler = R2

QuorumWait =
    Replicate
    - WritePBA_RT
    - AE_RT
```

`sample.leader_side`는 이 두 계산식 중 어느 것을 사용할지 결정하는 플래그다.

---

## 15. 남아 있는 한계

이 수정으로 해결되는 것:

* Leader mode의 `FHandler ≈ 0` 문제
* Leader-side `WritePBA` 중복 계상
* 큰 negative residual의 주요 원인
* destination / leader mode별 timing 구조 불일치

아직 남는 것:

* sink-empty global fallback correlation
* concurrent Apply 간 sample contamination
* `first_sample()`이 실제 quorum-critical follower인지 여부
* Total residual / ApplyWait decomposition
* global `aci_*`의 per-Apply correlation

따라서 이 수정은 leader-mode timing decomposition의 구조적 오류를 바로잡는 최소 수정이고, 전체 profiling을 완전히 per-request 정확하게 만드는 최종 수정은 아니다.
