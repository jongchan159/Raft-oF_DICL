# raftof+dare

NVMe-oF 스토리지 레벨 블록복사(PBA copy)로 로그를 복제하는 Raft 구현.
Go 원본(`~/RAFT/nvmeof_raft/raft.go`)의 C++17 포팅.

- `core/` — **Raft 합의 알고리즘만.** 네트워크 계층에 의존하지 않는다
  (net/ 을 include하지도, 링크타임에 net/ 심볼을 요구하지도 않는다).
  `core/include/` 는 선언, `core/src/` 는 정의
- `blockio/` — O_DIRECT + FIEMAP 블록 I/O. **Raft를 전혀 모른다**
  (의존 방향은 core → blockio 한 방향)
- `net/` — 전송 계층 (TCP + protobuf). **나중에 RDMA로 교체될 부분**.
  `net/include/` 는 선언, `net/src/` 는 정의
- `apps/` — 실행 파일 진입점 3개**만**. 알고리즘은 없고 조립만 한다
  (`raft_node_main.cpp` 가 전송 구현체를 주입하는 유일한 지점)
- `storage/` — 스토리지(blockcopy) 노드 서버 구현
- `proto/` — 와이어 메시지 정의와 생성된 코드
- `tests/` — doctest 유닛 테스트 + 전송 Mock + `raft_selftest` 하네스
- `scripts/` — 3노드 e2e 테스트

각 디렉터리에 `README.md` 가 있다. 그 계층의 **불변식**(무엇이 깨지면
회귀인지)과 진입점 심볼이 적혀 있으므로, 해당 디렉터리를 고치기 전에
먼저 읽을 것. 파일 목록은 일부러 넣지 않았다 — 금방 낡는다.

문서 셋:

- 이 문서 — "빌드해서 돌리는 방법"
- **[DECISIONS.md](DECISIONS.md)** — 원본 `raft.go`와 다르게 만든 지점과 그 이유,
  그리고 미해결 항목. 코드의 `[수정-N]` 마커가 여기를 가리킨다
- **[HANDOFF.md](HANDOFF.md)** — 설계 배경, 버그 발견 이력, 실측값, 남은 작업

---

## 1. 준비

### protobuf (≥ 3.21.0)

`proto/rpcproto.pb.h`가 protobuf 3.21.0 이상을 요구한다.
`.proto`를 수정하지 않는다면 `protoc`은 필요 없다.

**root 있으면:**

```bash
sudo apt install -y libprotobuf-dev protobuf-compiler
```

**root 없고 배포판에 3.21 이상이 있으면** — `.deb`를 로컬에 풀어 쓴다
(Ubuntu 24.04 noble 기준 패키지명):

```bash
SYSROOT=/tmp/pb-sysroot
mkdir -p /tmp/pbdeb "$SYSROOT" && cd /tmp/pbdeb
apt-get download libprotobuf-dev libprotobuf32t64 protobuf-compiler libprotoc32t64
for d in *.deb; do dpkg-deb -x "$d" "$SYSROOT"; done
```

**배포판이 오래된 경우 (예: Ubuntu 18.04는 protobuf 3.0.0뿐)** — 소스에서 짓는다.
다른 배포판의 .deb를 가져오는 것은 glibc 버전이 안 맞아 통하지 않는다.

```bash
cd /tmp && curl -fsSLO https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-cpp-3.21.12.tar.gz
tar xzf protobuf-cpp-3.21.12.tar.gz && cd protobuf-3.21.12
cmake -S . -B _b -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/tmp/pb-sysroot/usr \
  -DCMAKE_INSTALL_LIBDIR=lib/x86_64-linux-gnu \
  -DBUILD_SHARED_LIBS=ON -Dprotobuf_BUILD_TESTS=OFF
cmake --build _b -j"$(nproc)" && cmake --install _b
```

> `$HOME`에 풀지 말 것. `/home`이 NFS면 헤더 수천 개 추출에 몇 분이 걸린다.
> `/tmp`는 몇 초. 단 **재부팅하면 사라지므로 위 절차를 다시 돌려야 한다.**

### 작업 디렉터리 파일시스템

링 파일에 **O_DIRECT와 FIEMAP이 필요하다.** NFS에서는 동작하지 않는다.

```bash
findmnt -no FSTYPE /tmp    # ext4 / xfs 여야 한다
```

그래서 아래 예시가 모두 `/tmp` 기준이다.

---

## 2. 빌드

두 가지 빌드 시스템이 **병행 유지**된다. 내용은 같고, CMake만 증분 빌드와
유닛 테스트를 지원한다.

### CMake (권장)

```bash
export PROTOBUF_SYSROOT=/tmp/pb-sysroot   # 시스템에 설치했다면 생략
cmake -S . -B build-cmake
cmake --build build-cmake -j
cd build-cmake && ctest --output-on-failure
```

같은 소스를 두 번 컴파일하지 않아 `build.sh all`보다 7배쯤 빠르고,
`compile_commands.json`을 내보내므로 clangd/clang-tidy가 바로 동작한다.
추가 타깃: `raft_node_asan`, `raft_link_check`, `regen-proto`
(전부 `cmake --build build-cmake --target <이름>`).

### build.sh (기존)

```bash
export PROTOBUF_SYSROOT=/tmp/pb-sysroot
./build.sh all
```

| 타깃 | 결과 |
|---|---|
| `./build.sh all` | 아래 4개 바이너리 전부 |
| `./build.sh node` | `build/raft_node` — Raft 노드 |
| `./build.sh blockcopy-server` | `build/raft_blockcopy_server` — 스토리지 노드 |
| `./build.sh client` | `build/raft_client` — 클라이언트 / 벤치 하네스 |
| `./build.sh selftest` | `build/raft_selftest` — 네트워크 없는 로컬 검증 |
| `./build.sh check` | 컴파일 + 링크만 확인 (undefined 심볼 검출) |
| `./build.sh asan` | `build/raft_node_asan` — ASan+UBSan 빌드 |
| `./build.sh regen-proto` | `.proto` 수정 후 재생성 |
| `./build.sh clean` | `build/` 삭제 |

---

## 3. 돌리기 (로컬 3노드 — 블록 디바이스도 root도 필요 없음)

가장 빠른 길은 스크립트다. 3 Raft 노드 + 3 스토리지 노드를 띄우고
선출 → Apply → 커밋 수렴 → 링 파일 바이트 비교까지 검증한다.

```bash
./scripts/smoke_test.sh /tmp/raftof_smoke 200
```

`-identity-pba`(논리 오프셋 == 물리 오프셋)로 링 파일 자체를 볼륨처럼
취급하므로, 실서버에서 NVMe-oF 볼륨 사이에 일어나는 것과 **같은 코드
경로**를 로컬 파일 I/O로 돌린다.

환경변수로 조절한다:

```bash
MODE=leader        ./scripts/smoke_test.sh /tmp/s 200   # DARE 방식 정책 비교
CMD_SIZE=4064      ./scripts/smoke_test.sh /tmp/s 200   # 명령 크기(B)
BATCH=1            ./scripts/smoke_test.sh /tmp/s 200   # Apply RPC당 명령 수
RING_PAGES=1024    ./scripts/smoke_test.sh /tmp/s 3000  # 링 크기(4KiB 페이지)
HEARTBEAT_MS=300   ./scripts/smoke_test.sh /tmp/s 200
```

실패하면 `/tmp/raftof_smoke/{node,stor}*.log`를 먼저 볼 것.

### 손으로 띄우기

```bash
W=/tmp/raftof_manual
RING_PAGES=4096                       # 4096 * 4096B = 16MiB / 노드
DEV=""; CL=""
for id in 1 2 3; do
    mkdir -p $W/n$id
    fallocate -l $((RING_PAGES*4096)) $W/n$id/raft-$id.ring
    DEV+="${DEV:+,}$W/n$id/raft-$id.ring"
    CL+="${CL:+,}$id@127.0.0.1:$((6000+id))@@127.0.0.1:$((5050+id))"
done

# 스토리지 노드 3개. -devices는 반드시 "클러스터 인덱스 순서"
for id in 1 2 3; do
    ./build/raft_blockcopy_server -addr 0.0.0.0:$((5050+id)) \
        -devices "$DEV" -copy-workers 4 > $W/stor$id.log 2>&1 &
done

# Raft 노드 3개
for id in 1 2 3; do
    ./build/raft_node -id $id -cluster "$CL" -metadata-dir $W/n$id \
        -heartbeat-ms 100 -ring-pages $RING_PAGES \
        -identity-pba -profile > $W/node$id.log 2>&1 &
done
```

`-cluster` 형식은 **`id@raft_addr@device_path@storage_host`** (쉼표 구분)
이고, `device_path`를 비우면 노드가 자기 링 파일을 볼륨으로 쓴다.

---

## 4. 조회와 측정

```bash
ADDRS=127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003

# 명령 적용 (리더는 자동으로 찾는다)
./build/raft_client -addrs $ADDRS -op apply -n 2000 -size 512 -batch 10

# 레이턴시 분해 — Total ≈ LHandler+LPersist+AENet+FHandler+ReplNet+StorageIO+QuorumWait
./build/raft_client -addrs $ADDRS -op apply-timed -n 20 -size 4064 -batch 1

./build/raft_client -addrs $ADDRS -op commit-index      # 노드별 커밋 인덱스
./build/raft_client -addrs $ADDRS -op hash -at-count 201 # 상태머신 해시/카운트
./build/raft_client -addrs $ADDRS -op ae-stats          # AE 배치 카운터
./build/raft_client -addrs $ADDRS -op echo -n 100       # 코덱/네트워크 왕복만
```

노드가 멈춘 것 같으면 `-debug`로 띄운다. 1초마다 한 줄 나온다:

```
[state] leader term=1 tail=204 commit=203 applied=204 log_len=204
        | p1 vf=3 next=204 match=203 | p2 vf=3 next=204 match=203 | ...
```

---

## 5. 테스트

가장 간단한 방법은 CTest다 (유닛 + selftest + e2e 4개를 순서대로 돌린다):

```bash
cd build-cmake && ctest --output-on-failure
```

개별로 돌리려면:

```bash
./build-cmake/raft_unit_tests                           # 유닛 테스트 (doctest)
./build/raft_selftest /tmp/raftof_selftest              # 네트워크 없는 로컬 검증

./scripts/smoke_test.sh /tmp/raftof_smoke 200           # 3노드 e2e
MODE=leader ./scripts/smoke_test.sh /tmp/raftof_ls 200  # leader-side 정책

RING_PAGES=1024 CMD_SIZE=4064 BATCH=10 \
  ./scripts/smoke_test.sh /tmp/raftof_wrap 3000         # 링 wrap-around 스트레스

./scripts/restart_test.sh /tmp/raftof_restart 100       # 팔로워 재시작
```

`restart_test.sh`의 catch-up 항목은 **현재 설계에서 통과할 수 없다**
(`[KNOWN GAP]`으로 보고하고 종료코드 0). 이유는 HANDOFF.md §5에 있다.

메모리 버그 확인:

```bash
./build.sh asan
# raft_node 대신 build/raft_node_asan을 띄우고 워크로드를 돌린 뒤
grep -E 'ERROR: AddressSanitizer|runtime error' /tmp/.../node*.log
```

---

## 6. 실서버 (FIEMAP + 실제 NVMe-oF)

> 이 클러스터(eternity3/5/6 + eternitystorage)에서 3노드 E2E 를 실제로 돌리는
> 절차서는 **`E2E_EXPERIMENT.md`** 에 있다 — 선점검, 권한 준비, 클러스터 스펙,
> 정합성 검증, 측정(워밍업 포함), 정리까지. 아래는 일반적인 전제 조건이다.

`-identity-pba`를 **빼고** 돌린다. 전제 조건:

- 링 메타데이터 파일이 **대상 블록 디바이스 위 파일시스템**에 있어야 한다
  (FIEMAP이 그 디바이스 기준 PBA를 돌려준다)
- 스토리지 노드의 `-devices`는 **클러스터 인덱스 순서**로 각 멤버의 볼륨 경로
- `raft_blockcopy_server`가 디바이스를 `O_RDWR|O_DIRECT`로 열어야 하므로
  보통 root 또는 적절한 그룹 권한이 필요하다
- 파일시스템이 `FALLOC_FL_ZERO_RANGE`를 지원해야 한다 (ext4/xfs/btrfs)

```bash
# 노드 i
./build/raft_node -id 1 \
  -cluster "1@10.0.0.1:6001@/dev/nvme0n1@10.0.0.1:5050,2@10.0.0.2:6001@/dev/nvme1n1@10.0.0.2:5050,3@10.0.0.3:6001@/dev/nvme2n1@10.0.0.3:5050" \
  -metadata-dir /mnt/nvme0/raftof -heartbeat-ms 300 -ring-pages 262144 -profile

# 스토리지 노드
./build/raft_blockcopy_server -addr 0.0.0.0:5050 \
  -devices /dev/nvme0n1,/dev/nvme1n1,/dev/nvme2n1 -copy-workers 8
```

`-ring-pages` 기본값은 8Mi 페이지 = **32GiB/노드**다. fallocate와
ZERO_RANGE에 시간과 공간이 드므로 처음에는 작게 잡고 올릴 것.

**`-mode leader`** (DARE 방식) 를 쓸 때는 각 스토리지 노드가 **모든** 멤버
볼륨을 NVMe-oF로 attach해 열어둔 상태여야 한다 — 리더의 스토리지 노드가
팔로워 볼륨에 직접 쓴다.

---

## 7. 주요 플래그

**`raft_node`**

| 플래그 | 기본값 | 설명 |
|---|---|---|
| `-id N` | (필수) | 이 노드의 id. `-cluster`에 있어야 한다 |
| `-cluster SPEC` | (필수) | `id@raft_addr@device_path@storage_host` 쉼표 구분 |
| `-metadata-dir DIR` | `.` | 링 파일 위치 |
| `-heartbeat-ms N` | 300 | 하트비트 주기. election timeout은 이것의 20~30배 |
| `-mode destination\|leader` | destination | 복제 정책 |
| `-ring-pages N` | 8388608 (32GiB) | 링 크기 (4KiB 페이지) |
| `-identity-pba` | off | **테스트 전용.** FIEMAP 대신 논리==물리 |
| `-profile` | off | 서브스테이지 프로파일링 (apply-timed에 필요) |
| `-loop-sleep-us N` | 200 | 메인 루프 바퀴당 대기. `0` = 원본과 같은 스핀 |
| `-log-trim N` | 8192 | in-memory 로그 벡터 트리밍 임계값. `0` = 안 함 |
| `-ae-batch N` | 1000000 | `MaxAppendEntriesBatch` 상한 |
| `-ae-batch-bytes N` | 5GiB | 라운드당 바이트 상한 |
| `-debug` | off | 1초 간격 상태 덤프 |

**`raft_blockcopy_server`**: `-addr` (기본 `0.0.0.0:5050`),
`-devices` (쉼표 구분, 클러스터 인덱스 순서), `-copy-workers` (기본 nproc, 최대 16)

**`raft_client`**: `-addrs`, `-op apply|apply-timed|echo|commit-index|hash|ae-stats`,
`-n`, `-size`, `-batch`, `-at-count`, `-timeout-s`

---

## 8. 문제가 생기면

| 증상 | 확인할 것 |
|---|---|
| `no leader found within timeout` | 노드 로그에 `[warn]`/`[rpc-error]`가 있는지. `-debug`로 상태 확인 |
| `init_storage failed: ... fallocate` | 작업 디렉터리가 NFS인지 (`findmnt -no FSTYPE`) |
| `fallocate(ZERO_RANGE)` 실패 | 파일시스템이 지원하지 않음. 테스트라면 `-identity-pba` |
| `PBA=0 ... hole in ring file` | 링 파일이 sparse. `fallocate`로 미리 만들 것 |
| `libprotobuf.so.32 => not found` | `/tmp/pb-sysroot`가 사라짐 → §1 재실행 후 재빌드 |
| 팔로워가 커밋을 안 따라옴 | 리더 로그에 `[SKIP PBA]`가 있는지 (HANDOFF.md §5) |
| 노드가 조용히 죽음 | `./build.sh asan`으로 재현 |

이번 세션에 수정한 지점을 코드에서 찾으려면:

```bash
grep -rn '\[수정\]' core net
```
