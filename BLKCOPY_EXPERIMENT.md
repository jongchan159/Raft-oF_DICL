# blockcopy 병렬도·배치 스케일링 실험 — 실행 절차

**무엇을 재는가:** `raft_blockcopy_server` 의 **처리량이 워커 수와 배치 크기에
따라 어떻게 확장되고 어디서 포화하는가.** 축은 W(`-copy-workers`) ×
B(`-batch`) × 청크 크기다. 답하려는 실무 질문은 "스토리지 노드를
`-copy-workers` 몇으로 운영해야 하는가" 다.

설계 배경은 `scripts/README.md` §`blkcopy_scaling.sh`, 결정 기록은
`DECISIONS.md` D15·U8·U10·U11. 이 문서는 **순서대로 따라가는 절차서**다.

전제: 타깃 = `eternitystorage`(115.145.173.243), 이니시에이터 = `eternity5`(115.145.173.124).

> **2026-09-10 에 설계가 바뀌었다.** 이전 판은 *지연* 실험이었고
> `raft_blkcopy_bench` + `scripts/blkcopy_latency.sh` 를 썼다. 두 도구는
> 제거됐다 (D15). 지연이 필요하면 새 도구의 W=1, B=1 코너를 보면 된다 —
> wall 의 p50/p99 를 그대로 보고한다.
>
> 이전 판은 또 "`node3`/`node5`/`node6` 은 실험 대상이 될 수 없다 — raw PBA 를
> 쓰면 그 위 파일시스템이 깨진다" 고 적었다. **이번 판은 raw 가 아니라 그
> ext4 위의 파일을 쓰므로 그 제약이 사라진다.** 오히려 arm A 와 arm B 가 같은
> 물리 SSD 를 쓰게 되어 비교가 깨끗해진다.

---

## 두 arm

| | 어디서 | src | dst | 먼저 막히는 것 |
|---|---|---|---|---|
| **A** | eternitystorage | 로컬 PCIe 위 ext4 파일 | 로컬 PCIe 위 ext4 파일 | 디바이스 |
| **B** | eternity5 | 같은 SSD 를 RDMA 로 | 같은 SSD 를 RDMA 로 | 링크 (10 Gb/s) |

A 를 대조군으로 둬야 포화 원인이 갈린다 — 같은 W 에서 둘 다 평평하면
소프트웨어 탓, B 만 일찍 평평하면 링크 탓. **A 를 먼저 한다.**

---

## 진행 상황 체크리스트

| | 항목 | 상태 |
|---|---|---|
| ✅ | 빌드 (`raft_blkcopy_scale`, CTest 11개) | 완료 (2026-09-10) |
| ✅ | 하네스 리허설 (root 불필요) | 완료 — §0 |
| ✅ | `nvmet_rdma` / `nvme_rdma` 모듈 로드 | 양쪽 완료 |
| ✅ | nvmet port 1 = tcp 115.145.173.243:4420 (node3/5/6) | 기존 |
| ✅ | nvmet port 10 = **rdma 10.0.0.91:4420** (node5/6) | 완료 |
| ✅ | eternity5 가 node5/node6 을 **rdma 로 attach** | 완료 |
| ❌ | **라이브 3노드 클러스터 정지** | **미완 — 이게 시작 조건이다** |
| ❌ | arm A: 타깃에서 node5/node6 마운트 + 백킹 파일 | 미완 — §2 |
| ❌ | arm B: 재연결 + 마운트 | 미완 — §4 |
| ⬜ | udev 권한 규칙 | RAW_CONTROL 진단을 쓸 때만 — §5 |

> ⚠ **라이브 클러스터가 도는 동안에는 시작할 수 없다.** 그 클러스터가
> `/dev/nvme1n1`·`nvme3n1`·`nvme5n1` 을 쓰고 있고 우리도 같은 SSD 를 쓴다.
> 스크립트 preflight 가 `pgrep -x` 로 검사해서 **차단한다.**

---

## 0. 빌드와 무해한 리허설 (root 불필요)

```bash
cd ~/raftof_clean
export PROTOBUF_SYSROOT=/tmp/pb-sysroot     # 없으면 README §1 절차로 먼저 만든다
cmake -S . -B build-cmake && cmake --build build-cmake -j
cd build-cmake && ctest --output-on-failure && cd ..   # 11개 전부 통과해야 한다
./build.sh all        # 타깃을 하나씩 부를 것 (한 호출에 몰면 OOM-kill 된 적 있다)
```

권한도 원격도 없이 전 파이프라인(서버 → 벤치 → CSV → 행렬 요약)이 도는지
로컬 파일로 먼저 확인한다:

```bash
D=/tmp/bc_rehearse && mkdir -p $D
dd if=/dev/urandom of=$D/src bs=1M count=256 oflag=direct status=none
dd if=/dev/urandom of=$D/dst bs=1M count=256 oflag=direct status=none

STORAGE_HOST=localhost SRC_FILE=$D/src DST_FILE=$D/dst ARM_LABEL=rehearse \
WORKERS="1 2 4" BATCHES="1 4" CHUNKS="4096 65536" \
SRC_OFF=0 DST_OFF=0 REGION=$((192*1024*1024)) \
TARGET_BYTES=$((64*1024*1024)) PORT=5099 \
  ./scripts/blkcopy_scaling.sh /tmp/bc_rehearse_out
```

W×B 행렬이 나오고 `[ok] all points completed` 로 끝나면 하네스는 정상이다.
**B=1 열이 W 를 올려도 평평한 것**이 정상이다 (`w = min(W, B)`).

---

## 1. 라이브 클러스터 정지 + 이니시에이터 분리

**순서가 중요하다.** 이니시에이터가 붙어 있는 채로 타깃에서 로컬 마운트·쓰기를
하면 두 컨트롤러가 같은 네임스페이스를 서로 모르게 건드려 **양쪽 파일시스템이
깨진다.**

```bash
# (1) 라이브 raft 정지 — 양 호스트에서
ssh eternity5       'sudo pkill -x raft_node'
ssh eternitystorage 'sudo pkill -x raft_blockcopy_server'

# (2) eternity5 (root) — 이니시에이터를 먼저 떼어낸다
ssh eternity5 'sudo umount /mnt/raftvol
               sudo nvme disconnect -n node5
               sudo nvme disconnect -n node6'

# (3) 확인
ssh eternity5 'findmnt -no TARGET /dev/nvme2n1 || echo unmounted; nvme list | grep -c node'
```

---

## 2. arm A 준비 (eternitystorage, root)

### 2-1. 실험 중 재attach 방지 인터락

```bash
ssh eternitystorage 'cd /sys/kernel/config/nvmet
  echo 0 | sudo tee subsystems/node5/namespaces/10/enable
  echo 0 | sudo tee subsystems/node6/namespaces/10/enable'
```

subsystem 이나 port 는 **지우지 않는다** — `enable` 만 토글하면 §4 에서 되돌리기 쉽다.

### 2-2. 마운트와 쓰기 가능한 디렉터리

```bash
ssh eternitystorage '
  sudo mkdir -p /mnt/bc-src /mnt/bc-dst
  sudo mount /dev/nvme3n1 /mnt/bc-src      # node5 백킹
  sudo mount /dev/nvme5n1 /mnt/bc-dst      # node6 백킹
  ls -la /mnt/bc-src /mnt/bc-dst           # 내용 확인 — 예상 밖이면 멈출 것
  sudo install -d -o jongc -g jongc /mnt/bc-src/blkcopy /mnt/bc-dst/blkcopy
  df -h /mnt/bc-src /mnt/bc-dst            # 각각 40GiB 이상 여유'
```

기존 파일은 건드리지 않는다 (디렉터리만 추가). **비파괴다.**
재부팅하면 마운트가 사라지므로 반복할 거면 `/etc/fstab` 에 넣는다.

### 2-3. 백킹 파일 사전 기록 — **건너뛰면 안 된다**

이유가 둘이다. `fallocate` 만 하면 extent 가 **unwritten** 으로 남아 첫 쓰기가
extent 상태 변환으로 직렬화되고, 미기록/전부-0 LBA 의 pread 는 **FTL 이 즉답해서
비현실적으로 빠르다.** 그래서 **랜덤 데이터로 실제로 채운다.**

> `fallocate -z`(ZERO_RANGE)로는 첫 번째 문제도 안 풀린다 — ext4 에서는 범위를
> unwritten 으로 두는 것이 가장 싼 zeroing 이라 extent 가 written 이 되지 않는다.
> 커널 4.15/6.8 양쪽에서 실측 확인했다. `DECISIONS.md` **U11** 참조.

```bash
ssh eternitystorage '
  dd if=/dev/urandom of=/tmp/seed bs=1M count=1024 status=none
  for f in /mnt/bc-src/blkcopy/src /mnt/bc-dst/blkcopy/dst; do
    for i in $(seq 0 35); do        # 36GiB = SRC_OFF(4GiB) + REGION(32GiB)
      dd if=/tmp/seed of="$f" bs=1M seek=$((i*1024)) conv=notrunc oflag=direct status=none
    done
    echo "prefilled $f"
  done'

# 검증: 둘 다 0 이어야 한다
ssh eternitystorage 'for f in /mnt/bc-src/blkcopy/src /mnt/bc-dst/blkcopy/dst; do
  echo "$f unwritten=$(filefrag -v $f | grep -c unwritten)"; done'
```

스크립트 preflight 가 이 둘(unwritten extent, src 가 전부 0인지)을 다시 검사해서
걸리면 **거부한다.**

---

## 3. arm A 실행

```bash
cd ~/raftof_clean
ARM_LABEL=A-local \
STORAGE_HOST=eternitystorage \
SRC_FILE=/mnt/bc-src/blkcopy/src \
DST_FILE=/mnt/bc-dst/blkcopy/dst \
  ./scripts/blkcopy_scaling.sh ~/blkcopy_scale_$(date +%m%d-%H%M)
```

기본 스윕: W ∈ {1,2,4,8,16,32} × B ∈ {1,4,16,64} × chunk ∈ {4Ki,64Ki,1Mi} = 72 지점.
지점당 2GiB 를 옮기도록 반복 횟수가 자동 산출되고, 지점마다 서버를 새로 띄운다.
10~20분 걸린다. 빠르게 훑으려면
`WORKERS="1 4 16" BATCHES="1 16" CHUNKS="65536"`.

---

## 4. arm B 준비와 실행 (eternity5)

A 에서 만든 파일이 그대로 있으므로 재생성이 필요 없다.

```bash
# 타깃: 로컬 마운트 해제 + 노출 복구
ssh eternitystorage '
  sudo umount /mnt/bc-src /mnt/bc-dst
  cd /sys/kernel/config/nvmet
  echo 1 | sudo tee subsystems/node5/namespaces/10/enable
  echo 1 | sudo tee subsystems/node6/namespaces/10/enable'

# 이니시에이터: 재연결. -i 를 A/B 간 고정하지 않으면 큐 수가 달라져 비교가 오염된다
ssh eternity5 '
  sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node5 -i 41
  sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node6 -i 41
  for c in /sys/class/nvme/nvme*; do
    echo "$(basename $c) $(cat $c/transport) $(cat $c/subsysnqn) q=$(cat $c/queue_count)"
  done
  lsblk -o NAME,SIZE,WWN'

# 새로 생긴 경로를 마운트하고 같은 파일 경로를 쓴다 (X/Y 는 위에서 확인한 값)
ssh eternity5 'sudo mkdir -p /mnt/bc-src /mnt/bc-dst
               sudo mount /dev/nvmeXn1 /mnt/bc-src
               sudo mount /dev/nvmeYn1 /mnt/bc-dst'

cd ~/raftof_clean
ARM_LABEL=B-rdma STORAGE_HOST=eternity5 \
SRC_FILE=/mnt/bc-src/blkcopy/src DST_FILE=/mnt/bc-dst/blkcopy/dst \
  ./scripts/blkcopy_scaling.sh ~/blkcopy_scale_$(date +%m%d-%H%M)-B
```

---

## 5. 권한 — `RAW_CONTROL=1` 진단을 쓸 때만

파일 기반 본 경로는 블록 디바이스를 열지 않으므로 **권한이 필요 없다.**
raw 대조점(§6 해석에서 필요해질 때만)은 블록 디바이스를 열어야 한다.

> ⚠ **`usermod -aG disk jongc` 는 쓰지 말 것** — 이 호스트에는 Lustre MDT/OST 가
> 있고 `disk` 그룹은 그것까지 **전부** 연다. 해당 wwid 만 지정한 udev 규칙을 쓴다.

`setfacl` 이 없으므로 udev 로 한다. `/etc/udev/rules.d/99-blkcopy-target.rules`:

```
KERNEL=="nvme*n*", ATTR{wwid}=="eui.36344630525019940025384500000001", OWNER="jongc", MODE="0660"
KERNEL=="nvme*n*", ATTR{wwid}=="eui.36344630529068800025384500000001", OWNER="jongc", MODE="0660"
```
```bash
sudo udevadm control --reload && sudo udevadm trigger --subsystem-match=block
```

(위 wwid 는 `/dev/nvme12n1`·`/dev/nvme13n1` 의 것이다. 다른 디바이스를 쓸 거면
`cat /sys/block/nvmeXn1/wwid` 로 확인해 바꿀 것.)

---

## 6. 결과 확인

요약: `<OUTDIR>/summary.csv` + 표준출력의 **chunk 별 W×B 행렬**(값 = MiB/s,
괄호 = eff_workers). 원시 샘플은 `<OUTDIR>/raw-w<W>-b<B>-c<chunk>.csv`,
지점별 서버 로그는 `<OUTDIR>/server-*.log`.

**계측이 맞는지 먼저 본다:**

- **W > B 구간이 평평해야 한다** (`w = min(copy_workers, batch)`).
  아니면 W 가 서버에 안 먹은 것 — `server-*.log` 의 `copy-workers : N` 을 볼 것
  (스크립트가 자동 대조하지만 눈으로도 확인)
- `eff_workers`(= `copy_ns/wall_ns`)에서 `min(W,B)` 는 **상한**이지 목표치가
  아니다. wall 에 RPC 코덱·전송이 함께 들어 있어 W=1 에서 1 미만이 정상이다.
  **판단은 상대적으로** — B 를 키울 때 이 값이 `min(W,B)` 를 따라 올라가면
  병렬화가 먹는 것이다
- 처리량이 매체의 물리 상한을 넘으면 계측 오류다 (오프셋 겹침이나 캐시 경유)

**해석 규칙:**

- 수치는 **file-backed on ext4** 다. production 은 블록 디바이스 + FIEMAP 물리
  오프셋이라 같은 경로가 아니다. 보고할 때 반드시 병기할 것
- **IB 링크가 4X SDR(10 Gb/s ≈ 1.25 GB/s)로 트레이닝된다** (eternity5 는 EDR
  100 Gb/s 인데 타깃 포트가 SDR). arm B 에서 1.25 GB/s 근처 포화는 **링크 한계**지
  소프트웨어 문제가 아니다. 수치와 함께 적을 것
- arm A 와 B 는 **호스트가 다르다** (32코어 Xeon Silver vs 40코어 Xeon Gold).
  CPU 차이가 걱정되면 두 호스트에서 같은 로컬 파일 워크로드를 돌려 델타를 뺀다

**W 스케일링이 아예 안 나오면** — 그때만 `RAW_CONTROL=1` 로 원인을 가른다:

| 파일 W=1→8 | raw W=1→8 | 결론 |
|---|---|---|
| 안 오름 | 오름 | **ext4 가 병목** |
| 안 오름 | 안 오름 | **blockcopy 구조가 병목** ← 진짜 발견 |

복사 정확성을 한 번은 직접 확인한다 (타깃에서):

```bash
cmp -n $((32*1024*1024*1024)) -i 4294967296:4294967296 \
    /mnt/bc-src/blkcopy/src /mnt/bc-dst/blkcopy/dst && echo IDENTICAL
```

---

## 7. 정리 (원상복구)

```bash
# 이니시에이터
ssh eternity5 'sudo umount /mnt/bc-src /mnt/bc-dst 2>/dev/null
               sudo nvme disconnect -n node5; sudo nvme disconnect -n node6'

# 타깃 — enable 을 되돌리고 마운트를 푼다. subsystem/port 는 애초에 안 건드렸다
ssh eternitystorage 'sudo umount /mnt/bc-src /mnt/bc-dst 2>/dev/null
  cd /sys/kernel/config/nvmet
  echo 1 | sudo tee subsystems/node5/namespaces/10/enable
  echo 1 | sudo tee subsystems/node6/namespaces/10/enable'

# 실험 파일이 필요 없으면 (마운트한 상태에서)
#   rm -f /mnt/bc-src/blkcopy/src /mnt/bc-dst/blkcopy/dst
```

`node3` 과 port 1 / port 10 의 구성은 전 과정에서 건드리지 않는다.
라이브 클러스터를 다시 띄우기 전에 `/mnt/raftvol` 재마운트를 잊지 말 것.

---

## 막혔을 때

| 증상 | 원인 |
|---|---|
| `live raft processes … stop the cluster first` | §1 을 안 했다. 같은 SSD 를 쓰므로 차단이 정상 |
| `has N unwritten extents` | §2-3 사전 기록을 건너뛰었다. `fallocate -z` 로는 안 풀린다 (U11) |
| `src … reads as all zeros` | 0 으로 채웠다. 랜덤 시드로 다시 채울 것 (FTL 즉답) |
| `server did not start within 10s` | 서버 로그 확인. `stdbuf` 없이 띄우면 배너가 버퍼에 갇혀 오판된다 (스크립트는 이미 붙인다) |
| `server reports copy-workers=X but we asked for Y` | 이전 서버가 안 죽고 포트를 잡고 있다. `pkill -f '[r]aft_blockcopy_server -addr 0.0.0.0:5060'` |
| 모든 복사가 갑자기 실패 | `DECISIONS.md` **U6** — 커넥션이 한 번 죽으면 그 프로세스는 영구 실패. 지점마다 서버를 새로 띄우는 이유다 |
| B=1 인데 W 를 올려도 그대로 | **정상이다.** `w = min(W, B)` |
| arm 간 큐 수가 다름 | `nvme connect -i N` 을 고정하지 않았다 |
| `Permission denied` (RAW_CONTROL) | §5 udev 규칙 미적용. `usermod -aG disk` 로 우회하지 말 것 |
