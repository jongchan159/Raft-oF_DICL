# `scripts/` — 3노드 e2e 테스트와 blockcopy 지연 실험

블록 디바이스도 root 권한도 없이 **PBA 블록복사 경로 전체**를 돌린다.

## 호출 규약

```bash
BIN=<바이너리 디렉터리> ./scripts/smoke_test.sh   <작업디렉터리> <명령 수>
BIN=<바이너리 디렉터리> ./scripts/restart_test.sh <작업디렉터리> <명령 수>
```

환경변수로 조절한다: `MODE`(destination / leader), `RING_PAGES`,
`CMD_SIZE`, `BATCH`. CTest가 이 방식으로 네 가지 조합을 돌린다 —
`smoke_destination` / `smoke_leader_side` / `smoke_ring_wrap` /
`restart_follower`.

**작업 디렉터리는 ext4 / xfs 로컬 경로여야 한다** (`/tmp` 등). 링 파일에
O_DIRECT와 FIEMAP이 필요해서 NFS에서는 동작하지 않는다.

## 왜 디바이스 없이도 실제 경로가 되는가

`raft_node` 를 `-identity-pba` 로 띄워 "논리 오프셋 == 물리 오프셋" 으로
두고, 스토리지 노드의 `-devices` 에 세 노드의 링 파일을 클러스터 인덱스
순서로 그대로 넘긴다. 그러면 팔로워의 스토리지 노드가
`pread(리더 링 파일, PBA)` → `pwrite(자기 링 파일, PBA)` 를 수행하므로,
실서버에서 NVMe-oF 볼륨 사이에 일어나는 일과 **같은 코드 경로**를 탄다
(전송만 로컬 파일 I/O로 바뀐다).

실서버에서는 `-identity-pba` 를 빼고 `-devices` 에 실제 블록 디바이스를
넣는다. 그때는 링 메타데이터 파일이 그 디바이스 위 파일시스템에 있어야
하고 FIEMAP으로 PBA가 해석된다.

## `restart_test.sh` 의 KNOWN GAP

팔로워 재시작 catch-up 은 **원본 `raft.go` 도 못 한다.** 그래서 이
스크립트는 GAP 2건을 `[KNOWN GAP]` 으로 보고하고 **종료코드 0으로
끝난다.** GAP이 GAP으로 남는 것이 정상이고, 갑자기 통과하기 시작하면
동작이 바뀐 것이므로 의심할 것 — DECISIONS.md §U5.

## `blkcopy_scaling.sh` — blockcopy 병렬도·배치 스케일링 실험

e2e 테스트가 아니라 **측정 도구**다. CTest 에 등록하지 않는다 (실제 장치가
필요하고 목적지를 덮어쓴다).

묻는 것: **스토리지 노드를 `-copy-workers` 몇으로 운영해야 하는가**, 그리고
실제 Raft 경로가 만드는 배치 크기에서 그 값이 의미가 있는가.

```bash
ARM_LABEL=A-local ./scripts/blkcopy_scaling.sh [출력디렉터리]
```

축 3개를 훑는다. W = 서버의 `-copy-workers`, B = 벤치의 `-batch`, 그리고 청크 크기.
호스트·경로·스윕 범위는 **전부 환경변수**다 (`STORAGE_HOST` / `SRC_FILE` /
`DST_FILE` / `WORKERS` / `BATCHES` / `CHUNKS` / `TARGET_BYTES` / `PORT` …).
스크립트 머리 주석에 전체 목록이 있다.

**하드코딩된 arm 표를 두지 않는 것이 의도**다. 예전 `blkcopy_latency.sh` 는 arm
정의를 코드에 박아두었고, 클러스터 구성이 바뀌자 문서와 함께 낡아버렸다
(`10.0.0.90`/`bcopy-a` 같은 값이 실제와 달랐다). 스토리지 노드를 옮겨도 이
파일은 수정 대상이 아니어야 한다.

### 구조적으로 알고 시작하는 것

`storage/raft_blockcopy_server.cpp` 를 읽으면 세 가지가 미리 보인다:

- **`w = min(copy_workers_, count)`** (`:208`) — 배치가 병렬도의 상한이다.
  따라서 **W > B 구간은 W = B 와 같아야 하고**, 안 그러면 계측이 틀린 것이다
  (W 가 서버에 안 먹었다는 뜻 — 지점별 `server-*.log` 의 `copy-workers` 를 볼 것)
- 워커 스레드를 **RPC 마다 새로 생성**한다 (`:244`). 작은 청크 + 큰 W 에서
  이게 지배하면 처리량이 되레 떨어질 수 있다 → 스레드 풀로 바꾸라는 근거가 된다
- **`copy_nanos` 는 워커별 시간의 합**이다 (`:284`). 병렬 구간에서 경과시간이
  아니므로 **처리량 분모로 쓰면 안 된다.** 벤치는 언제나 wall 로 낸다

클라이언트는 커넥션당 `call_mu` 로 완전 직렬화라 in-flight RPC 가 1개다. 실제
Raft 경로도 팔로워 핸들러들이 클라이언트 하나를 공유하므로 이 제약은 충실하다.

### ext4 파일 기반 — 이유와 대가

`-devices` 에 블록 디바이스가 아니라 **파일**을 준다. `open_device` 가
`O_RDWR|O_DIRECT` 로 열므로 파일에도 그대로 동작한다.

- **얻는 것**: 비파괴(기존 파일시스템을 지우지 않는다), `disk` 그룹 불필요
- **잃는 것**: production 의 `raft_blockcopy_server` 는 **블록 디바이스**를 열고
  FIEMAP 물리 오프셋에 pread/pwrite 한다. 파일 기반은 논리 오프셋을 쓰므로
  **완전히 같은 경로가 아니다.** 결과에 `file-backed on ext4` 를 반드시 병기할 것

### ⚠ 백킹 파일은 `dd` 로 준비해야 한다 (`fallocate -z` 로는 안 된다)

`fallocate -l` 만 하면 extent 가 **unwritten** 으로 남는다. 거기 첫 쓰기가 extent
상태 변환을 파일시스템 저널로 **직렬화**시켜, W 를 올려도 처리량이 안 오르는
가짜 결과가 나온다. 게다가 한 번 쓴 영역은 효과가 사라지므로 워밍업이 측정
구간을 덮었는지에 따라 결과가 흔들린다 — 에러 없이 재현이 안 되는 오측이다.

**`fallocate -z`(ZERO_RANGE)로는 해결되지 않는다.** ext4 에서 ZERO_RANGE 는 범위를
unwritten 으로 두는 것이 가장 싼 zeroing 구현이라, extent 를 written 으로
뒤집지 않는다. 커널 4.15 와 6.8 양쪽에서 실측 확인했다 (2026-09-10).
`blockio/cached_fd.h:29-35` 와 `DECISIONS.md D9` 는 ZERO_RANGE 가 written 으로
만든다고 서술하는데 **그 전제가 성립하지 않는다** — `DECISIONS.md U11` 참조.

**0 으로 채우는 것으로도 부족하다.** 미기록/전부-0 LBA 의 pread 는 SSD FTL 이
NAND 를 거치지 않고 즉답해서 비현실적으로 빠르다. 랜덤 시드로 채운다
(urandom 직접 쓰기는 느리므로 1GiB 시드를 반복 복사):

```bash
dd if=/dev/urandom of=/tmp/seed bs=1M count=1024 status=none
# 36GiB = SRC_OFF(4GiB) + REGION(32GiB)
for f in /mnt/bc-src/blkcopy/src /mnt/bc-dst/blkcopy/dst; do
    for i in $(seq 0 35); do
        dd if=/tmp/seed of="$f" bs=1M seek=$((i * 1024)) \
           conv=notrunc oflag=direct status=none
    done
    echo "prefilled $f"
done
# 검증: 0 이어야 한다
filefrag -v /mnt/bc-dst/blkcopy/dst | grep -c unwritten
```

preflight 가 **둘 다** 검사한다 — unwritten extent 가 남아 있으면 거부하고,
src 측정 구간의 표본이 전부 0 이면 거부한다.

### 두 arm

| | 어디서 | src | dst | 먼저 막히는 것 |
|---|---|---|---|---|
| **A** | eternitystorage | 로컬 PCIe 위 ext4 파일 | 로컬 PCIe 위 ext4 파일 | 디바이스 |
| **B** | eternity5 | 같은 SSD 를 RDMA 로 | 같은 SSD 를 RDMA 로 | 링크 (10 Gb/s) |

A 를 대조군으로 둬야 포화 원인이 갈린다 — 같은 W 에서 둘 다 평평하면
소프트웨어 탓, B 만 일찍 평평하면 링크 탓이다. **두 arm 이 같은 물리 SSD**
(node5/node6 의 백킹)를 쓰도록 해서 차이가 순수한 경로 차이가 되게 한다.

### 클러스터 실측 배치 (2026-09-09)

**eternitystorage** (타깃, Ubuntu 24.04 / 6.8.0, 32코어) — nvmet

| subsystem | 백킹 디바이스 | 모델 |
|---|---|---|
| `node3` | `/dev/nvme1n1` | MZQLB960HAJR 894.3G |
| `node5` | `/dev/nvme3n1` | MZQLB960HAJR 894.3G |
| `node6` | `/dev/nvme5n1` | MZQLB960HAJR 894.3G |

포트: `port 1` = **tcp** @ `115.145.173.243:4420` (node3/node5/node6) ·
`port 10` = **rdma** @ `10.0.0.91:4420` (node5/node6)
여분(파일시스템 없음, nvmet 미노출): `/dev/nvme12n1`, `/dev/nvme13n1` (PM9A3)

**eternity5** (이니시에이터, Ubuntu 24.04 / 6.8.0, 40코어)
`/dev/nvme0n1` 로컬 PCIe 970 EVO 232.9G (시스템은 `sda`, 무관) ·
`nvme1n1`=tcp/node3 · `nvme2n1`=rdma/node5 · `nvme3n1`=rdma/node6.
세 fabric 컨트롤러 `queue_count` 전부 41.

### 왜 tcp / IPoIB arm 을 뺐는가

`DECISIONS.md U10` 에 기록된 대로 **이 클러스터의 NVMe-oF/TCP 는 1 GbE(MTU 1500)
위에서 돈다 — 상한 ~118 MB/s.** 그 경로로 재면 blockcopy 가 아니라 링크
대역폭을 재게 된다. IPoIB 도 커널 IP 스택을 그대로 타서 실측 RTT 가 1GbE 와
사실상 같다(0.197ms vs 0.214ms). 그래서 이번 설계는 **RDMA 전용**이다.

IB 링크가 **4X SDR(10 Gb/s ≈ 1.25 GB/s)** 로 트레이닝돼 있다(eternity5 는
100 Gb/s 급인데 eternitystorage 쪽 포트가 SDR). arm B 에서 1.25 GB/s 근처
포화는 링크 한계지 소프트웨어 문제가 아니다. **수치를 보고할 때 반드시 병기할 것.**

### arm A 준비 순서 (라이브 클러스터 종료 후)

순서가 중요하다. 이니시에이터가 붙어 있는 채로 타깃에서 로컬 마운트·쓰기를
하면 두 컨트롤러가 같은 네임스페이스를 서로 모르게 건드려 양쪽 파일시스템이
깨진다.

```bash
# 1. 라이브 클러스터 정지 (양 호스트)
# 2. eternity5 (root) -- 이니시에이터를 **먼저** 떼어낸다
umount /mnt/raftvol
nvme disconnect -n node5 && nvme disconnect -n node6
# 3. eternitystorage (root) -- 실험 중 재attach 방지 인터락
echo 0 > /sys/kernel/config/nvmet/subsystems/node5/namespaces/10/enable
echo 0 > /sys/kernel/config/nvmet/subsystems/node6/namespaces/10/enable
# 4. eternitystorage (root) -- 마운트 + 쓰기 가능한 디렉터리
mkdir -p /mnt/bc-src /mnt/bc-dst
mount /dev/nvme3n1 /mnt/bc-src && mount /dev/nvme5n1 /mnt/bc-dst
install -d -o jongc -g jongc /mnt/bc-src/blkcopy /mnt/bc-dst/blkcopy
# 5. 백킹 파일 (위 dd 절 참조) + filefrag 검증
```

arm B 로 갈 때는 `umount` → 3의 `enable` 을 1 로 → eternity5 에서
`nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node5|node6 -i 41` → 마운트.

### `RAW_CONTROL=1` — 해결책이 아니라 진단

파일 기반에서 **W 스케일링이 안 나올 때만** 쓴다. ext4 가 병렬 O_DIRECT 쓰기를
제한하는 것인지, blockcopy 구조(RPC 마다 스레드 생성 / 커넥션 직렬화)가
제한하는 것인지를 가르는 대조군이다. 파일시스템 계층이 없는 raw 블록에
chunk=1Mi, W∈{1,8} 두 점을 찍는다.

| 파일 W=1→8 | raw W=1→8 | 결론 |
|---|---|---|
| 안 오름 | 오름 | **ext4 가 병목** |
| 안 오름 | 안 오름 | **blockcopy 구조가 병목** ← 진짜 발견 |

대조점은 실험용 dst 가 아니라 별도의 빈 블록 디바이스(`RAW_SRC`/`RAW_DST`,
기본 `/dev/nvme12n1`→`/dev/nvme13n1`)에 돌린다 — 마운트된 ext4 위에 raw 로 쓰면
그 파일시스템이 깨진다. **이 경로만 블록 디바이스 접근 권한을 요구한다.**

> ⚠ `usermod -aG disk` 는 쓰지 말 것. 이 호스트에는 Lustre MDT/OST 가 있어서
> `disk` 그룹은 그것까지 전부 연다. 해당 wwid 만 지정한 udev 규칙을 쓴다 —
> 절차는 `BLKCOPY_EXPERIMENT.md` §권한.

권한이 없으면 안내하고 건너뛴다.

### 향후: eternity2 를 스토리지 노드로

Ubuntu 24.04.4 / 6.8.0 / 40코어, IB `ibp59s0`, `nvmet-rdma`·`nvmet-tcp`·
`nvme-loop` 모듈 전부 있음, `/home` 공유. **제약: 로컬 NVMe 가 `nvme0n1`
(970 EVO Plus 232.9G) 하나뿐**이라 src/dst 두 개를 요구하는 blockcopy 를 돌리려면
(1) NVMe 추가, (2) 한 ext4 안에 src/dst 파일을 둘 다(읽기·쓰기가 같은 컨트롤러와
저널을 다투므로 포화 특성이 달라진다 — 비교표에 명시), (3) 다른 노드 볼륨을
NVMe-oF 로 attach 중 하나가 필요하다. 970 EVO 는 컨슈머 SSD 라 MZQLB/PM9A3 와
지속 쓰기 특성이 다른 것도 병기할 것. 스크립트가 호스트·경로를 환경변수로
받으므로 **코드 변경은 필요 없다.**

## 스크립트는 소스 경로를 참조하지 않는다

바이너리 이름만 쓴다. 디렉터리를 옮기는 리팩토링에서 이 파일들은
수정 대상이 아니다.
