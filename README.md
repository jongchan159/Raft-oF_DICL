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
- `tests/` — doctest 유닛 테스트 + 전송 Mock
- `scripts/` — blockcopy 스케일링 실험

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
| `./build.sh check` | 컴파일 + 링크만 확인 (undefined 심볼 검출) |
| `./build.sh asan` | `build/raft_node_asan` — ASan+UBSan 빌드 |
| `./build.sh regen-proto` | `.proto` 수정 후 재생성 |
| `./build.sh clean` | `build/` 삭제 |

---

## 3. 돌리기

**실제 NVMe-oF 볼륨이 필요하다.** PBA는 FIEMAP이 디바이스 기준으로
해석하므로, 링 메타데이터 파일이 **대상 블록 디바이스 위 파일시스템**에
있어야 하고 스토리지 노드가 그 디바이스를 `O_RDWR|O_DIRECT`로 열 수 있어야
한다. 로컬 파일만으로 돌리는 경로는 없다.

이 클러스터(eternity3/5/6 + eternitystorage)에서의 전체 절차 —— 선점검,
권한 준비, 기동, 정합성 검증, 정리 —— 는 **[E2E_EXPERIMENT.md](E2E_EXPERIMENT.md)**
에 있다. 아래는 기동 명령의 형태만 보인 것이다.

```bash
# 노드 i (세 호스트에서 -id 만 바꿔 실행)
./build/raft_node -id 1 \
  -cluster "1@10.0.0.1:6001@/dev/nvme0n1@10.0.0.1:5050,2@10.0.0.2:6001@/dev/nvme1n1@10.0.0.2:5050,3@10.0.0.3:6001@/dev/nvme2n1@10.0.0.3:5050" \
  -metadata-dir /mnt/raftvol/node1 -heartbeat-ms 300 -ring-pages 4096

# 스토리지 노드 (호스트마다 하나). -devices 는 반드시 "클러스터 인덱스 순서"
./build/raft_blockcopy_server -addr 0.0.0.0:5050 \
  -devices /dev/nvme0n1,/dev/nvme1n1,/dev/nvme2n1 -copy-workers 8
```

`-cluster` 형식은 **`id@raft_addr@device_path@storage_host`** (쉼표 구분)이고,
`device_path`는 그 멤버의 링 파일이 올라가 있는 볼륨이다 —— **비워 둘 수 없다.**

`-ring-pages` 기본값은 8Mi 페이지 = **32GiB/노드**다. fallocate와 ZERO_RANGE에
시간과 공간이 드므로 처음에는 작게 잡고 올릴 것.

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
#   ↑ 노드를 `-statemachine hash` 로 띄웠을 때만 값이 나온다. 기본값(noop)에서는
#     err=state machine does not expose a hash ... 가 돌아온다 (§7 참고)
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

```bash
cd build-cmake && ctest --output-on-failure
```

남아 있는 것은 **유닛 테스트와 컴포넌트 격리 검사**다:

| 테스트 | 무엇을 보는가 |
|---|---|
| `unit` | doctest 유닛 테스트 (링 기하, extent 맵, AE 부기, 선거) |
| `isolation_raft_unit_tests` | 유닛 테스트 바이너리에 protobuf 심볼이 없다 |
| `isolation_raft_client` | `raft_client`에 `nvmeof_raft::Server::` 심볼이 없다 |
| `isolation_raft_blockcopy_server` | 스토리지 노드도 마찬가지 |
| `isolation_raft_blkcopy_scale` | 측정 도구도 마찬가지 |

**복제 경로의 e2e 검증은 실클러스터에서만 가능하다** ——
[E2E_EXPERIMENT.md](E2E_EXPERIMENT.md) §6 정합성 검증을 쓸 것.
예전에는 `-identity-pba`(논리 오프셋 == 물리 오프셋)로 링 파일 자체를
볼륨처럼 취급해 로컬에서 같은 경로를 돌리는 스크립트가 있었으나,
그 모드는 제거되었다.

메모리 버그 확인:

```bash
./build.sh asan
# raft_node 대신 build/raft_node_asan 을 띄우고 워크로드를 돌린 뒤
grep -E 'ERROR: AddressSanitizer|runtime error' <노드 로그>
```

---

## 6. 실서버 (FIEMAP + 실제 NVMe-oF)

> 이 클러스터(eternity3/5/6 + eternitystorage)에서 3노드 E2E 를 실제로 돌리는
> 절차서는 **`E2E_EXPERIMENT.md`** 에 있다 — 선점검, 권한 준비, 클러스터 스펙,
> 정합성 검증, 측정(워밍업 포함), 정리까지. 아래는 일반적인 전제 조건이다.

전제 조건:

- 링 메타데이터 파일이 **대상 블록 디바이스 위 파일시스템**에 있어야 한다
  (FIEMAP이 그 디바이스 기준 PBA를 돌려준다)
- `-cluster`의 `device_path`는 **비워 둘 수 없다**
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

## 6.5 전송 (RDMA 기본)

Raft RPC(AppendEntries / RequestVote / Client\*)와 blockcopy RPC(WritePBABatch)는
**기본적으로 RDMA**(`rdma_cm` + RC QP, SEND/RECV)로 오간다. TCP 는 IB 링크가 없는
호스트를 위한 선택지로 남아 있다.

```bash
# 기본값 -- 아무것도 안 주면 rdma
./build/raft_node -id 5 -cluster "..." ...
# 명시적으로 TCP
./build/raft_node -id 5 -cluster "..." -transport tcp
```

**주소가 전송을 결정하지 않는다 — 플래그가 결정한다.** `rdma_cm` 은 IP 주소로
RDMA 장치를 찾으므로, `-transport rdma` 일 때 `-cluster` 의 `raft_addr` 과
`storage_host` 는 **IPoIB 주소**여야 한다 (이 클러스터에서는 `10.0.0.x`).
1GbE 주소를 주면 `rdma_resolve_addr` 이 실패한다.

세 바이너리(`raft_node` / `raft_blockcopy_server` / `raft_client`)가 같은 플래그를
갖고, **양쪽 끝이 같은 전송이어야 한다** (프레이밍이 다르다 — TCP 는 Go net/rpc
HTTP CONNECT + 길이 프레이밍, RDMA 는 SEND 메시지 하나가 프레임 하나).

와이어에 실리는 **protobuf 바디와 메서드 이름은 두 전송이 완전히 같다.**
`dispatch_raft_method` / `dispatch_blockcopy_method` 는 전송을 전혀 모른다.

실측(eternity5 → eternity6, 파일 백엔드 64MiB, chunk 64KiB, batch 4, 100회):

| 전송 | 처리량 | wall p50 | wall p99 |
|---|---|---|---|
| rdma | 299.9 MiB/s | 826 µs | 934 µs |
| tcp (IPoIB 경유) | 261.6 MiB/s | 940 µs | 1074 µs |

---

## 7. 주요 플래그

**`raft_node`**

| 플래그 | 기본값 | 설명 |
|---|---|---|
| `-id N` | (필수) | 이 노드의 id. `-cluster`에 있어야 한다 |
| `-cluster SPEC` | (필수) | `id@raft_addr@device_path@storage_host` 쉼표 구분 |
| `-metadata-dir DIR` | `.` | 링 파일 위치 |
| `-heartbeat-ms N` | 300 | 하트비트 주기. election timeout은 이것의 20~30배 |
| `-transport rdma\|tcp` | **rdma** | Raft/blockcopy RPC 전송. rdma는 `-cluster`의 주소를 **IPoIB 주소**로 해석한다 |
| `-statemachine noop\|hash` | **noop** | noop은 명령 바이트를 읽지 않는다 = 애플리케이션 비용 0. `-op hash`로 정합성을 확인하려면 `hash`로 띄워야 하고, 그 해시는 1 MiB 명령당 ~2ms를 먹는다 |
| `-mode destination\|leader` | destination | 복제 정책 |
| `-ring-pages N` | 8388608 (32GiB) | 링 크기 (4KiB 페이지) |
| `-profile` | off | 서브스테이지 프로파일링 (apply-timed에 필요) |
| `-loop-sleep-us N` | 200 | 메인 루프 바퀴당 대기. `0` = 원본과 같은 스핀 |
| `-log-trim N` | 8192 | in-memory 로그 벡터 트리밍 임계값. `0` = 안 함 |
| `-ae-batch N` | 1000000 | `MaxAppendEntriesBatch` 상한 |
| `-ae-batch-bytes N` | 5GiB | 라운드당 바이트 상한 |
| `-debug` | off | 1초 간격 상태 덤프 |

**`raft_blockcopy_server`**: `-addr` (기본 `0.0.0.0:5050`),
`-devices` (쉼표 구분, 클러스터 인덱스 순서), `-copy-workers` (기본 nproc, 최대 16),
`-transport rdma|tcp` (기본 **rdma**)

**`raft_client`**: `-addrs`, `-op apply|apply-timed|echo|commit-index|hash|ae-stats`,
`-n`, `-size`, `-batch`, `-at-count`, `-timeout-s`

---

## 8. 문제가 생기면

| 증상 | 확인할 것 |
|---|---|
| `no leader found within timeout` | 노드 로그에 `[warn]`/`[rpc-error]`가 있는지. `-debug`로 상태 확인 |
| `init_storage failed: ... fallocate` | 작업 디렉터리가 NFS인지 (`findmnt -no FSTYPE`) |
| `fallocate(ZERO_RANGE)` 실패 | 파일시스템이 ext4/xfs/btrfs 인지 확인 |
| `PBA=0 ... hole in ring file` | 링 파일이 sparse. `fallocate`로 미리 만들 것 |
| `libprotobuf.so.32 => not found` | `/tmp/pb-sysroot`가 사라짐 → §1 재실행 후 재빌드 |
| 팔로워가 커밋을 안 따라옴 | 리더 로그에 `[SKIP PBA]`가 있는지 (HANDOFF.md §5) |
| 노드가 조용히 죽음 | `./build.sh asan`으로 재현 |
| `init_storage: device_path is empty` | `-cluster`의 세 번째 필드에 볼륨 경로를 넣을 것 |

이번 세션에 수정한 지점을 코드에서 찾으려면:

```bash
grep -rn '\[수정\]' core net
```
