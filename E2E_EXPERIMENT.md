# raftof 3노드 E2E 실험 — 실클러스터 런북 (disaggregated storage)

실제 NVMe-oF 볼륨 위에서 `-identity-pba` **없이** (FIEMAP 으로 PBA 해석) 3노드
Raft 를 띄우고, 복제 정합성을 확인한 뒤 지연·처리량을 측정한다.

로컬 단일 호스트 검증은 `scripts/smoke_test.sh` 가 따로 있다. 이 문서는
**실클러스터 전용**이며 거기서만 나타나는 함정을 다룬다.

---

## 1. 구조 — disaggregated

**컴퓨트와 스토리지가 분리돼 있다.**

| 역할 | 호스트 | 무엇을 돌리나 |
|---|---|---|
| 컴퓨트 노드 ×3 | eternity3 / eternity5 / eternity6 | `raft_node` (Raft 합의, 링 로그 I/O) |
| 스토리지 노드 ×1 | **eternitystorage** (115.145.173.243) | `raft_blockcopy_server` (PBA 블록 복사) |

세 컴퓨트 노드가 **모두 같은 스토리지 노드**를 본다 (`storage_host` =
`115.145.173.243:5050`). 스토리지 노드는 세 멤버 볼륨을 전부 **로컬 PCIe** 로
갖고 있으므로 **블록 복사가 패브릭을 타지 않는다.**

### 볼륨 — 같은 이름이 호스트마다 다른 것을 가리킨다

| 멤버 | subsystem | 컴퓨트 노드에서의 경로 | **스토리지 노드에서의 경로** |
|---|---|---|---|
| id 7 (eternity7) | `node3` | `/dev/nvme1n1` | `/dev/nvme1n1` |
| id 5 (eternity5) | `node5` | `/dev/nvme2n1` | **`/dev/nvme3n1`** |
| id 6 (eternity6) | `node6` | `/dev/nvme3n1` | **`/dev/nvme5n1`** |

> ⚠ **이 표가 이 문서에서 가장 중요하다.** 컴퓨트 노드의 `/dev/nvme3n1` 은
> node6 이지만 스토리지 노드의 `/dev/nvme3n1` 은 **node5** 다. 번호가 우연히
> 겹칠 뿐 의미가 다르다. `-devices` 순서를 잘못 주면 **에러 없이 조용히 엉뚱한
> 볼륨을 덮어쓴다** — 세 볼륨 전부 실사용 ext4 다.
>
> 매핑은 반드시 §2 로 확인한다. 외우지 말 것.

### 컴퓨트 노드는 **자기 볼륨 하나만** attach 하면 된다

`raft_node` 가 여는 디바이스는 `io.device_path`, 즉 **자기 항목 하나뿐**이다
(`apps/raft_node_main.cpp:222`). `raft.cluster[i].device_path` 는 저장만 되고
core 에서 읽는 곳이 **하나도 없다**(전수 확인). 블록 복사는 스토리지 노드가
로컬에서 하므로 컴퓨트 노드가 남의 볼륨을 볼 이유가 없다.

→ eternity3 은 `node3` 만, eternity5 는 `node5` 만, eternity6 은 `node6` 만
attach 돼 있으면 된다. (멤버별 스토리지 노드 구성이었다면 세 볼륨이 전부
필요했을 것이다.)

### 왜 이 구조가 성립하는가 (PBA 주소공간 동일성)

컴퓨트 노드가 FIEMAP 으로 구한 PBA 를 스토리지 노드가 **자기 로컬 디바이스에
그대로** 적용한다. 이게 맞는 이유는 nvmet 이 블록 디바이스를 **오프셋 없이 통째로**
export 하기 때문이다 (`device_path=/dev/nvme1n1`). 따라서 이니시에이터에서 본 LBA N
과 타깃 로컬 디바이스의 LBA N 이 같은 블록이다. **부분 export 나 파티션 단위
export 로 바꾸면 이 전제가 깨진다.**

### 전송 불균일 — 무엇에 영향을 주나

eternity3 은 `ibp59s0` 링크가 DOWN·IP 없음이라 10.0.0.91 에 못 닿는다. 그래서
nvmet port 10(rdma)은 node5/node6 만 export 하고, **eternity3 은 자기 볼륨을
1GbE TCP 로** 붙인다 (eternity5·6 은 RDMA).

이 구조에서 그 비대칭이 닿는 곳은 **`StorageIO` 가 아니라 컴퓨트 노드의 링 I/O**
다 — 블록 복사는 스토리지 노드 로컬 PCIe 에서 끝나기 때문이다:

- **영향 받음**: `LPersist` (리더가 자기 링에 O_DIRECT write + fdatasync),
  apply 시 `read_entry_direct` 의 디바이스 읽기. eternity3 이 리더면 1GbE.
- **영향 없음**: `StorageIO` (스토리지 노드의 pread+pwrite, 전부 로컬 PCIe).

→ **어느 멤버가 리더였는지와 함께 수치를 기록한다.** 리더가 eternity3 이냐
아니냐로 `LPersist` 가 달라진다.

---

## 2. 선점검 (읽기 전용 — 매 실험 전에)

### 2-1. 컴퓨트 노드 3개

```bash
for h in eternity3 eternity5 eternity6; do
  echo "===== $h ====="
  ssh $h '
    for s in /sys/class/nvme-subsystem/*; do
      nqn=$(cat $s/subsysnqn 2>/dev/null)
      case "$nqn" in node*) echo "  $nqn -> $(ls -d $s/nvme*n* | xargs -n1 basename | tr "\n" " ")";; esac
    done
    echo "  mount : $(mount | grep /mnt/raftvol || echo NONE)"
    echo "  port  : $(ss -ltn | grep ":6001 " || echo "6001 free")"
    echo "  bins  : $(ls ~/raftof_clean/build/raft_node >/dev/null 2>&1 && echo ok || echo MISSING)"
    own=$(mount | grep /mnt/raftvol | cut -d" " -f1)
    [ -w "$own" ] || echo "  NOT WRITABLE: $own (자기 볼륨)   -> §3-1"
    [ -w /mnt/raftvol/raftof-cpp ] || echo "  NOT WRITABLE: /mnt/raftvol/raftof-cpp   -> §3-2"
  '
done
```

기대: 세 호스트 모두 `node3->nvme1n1, node5->nvme2n1, node6->nvme3n1`,
`/mnt/raftvol` 마운트됨(각자 자기 볼륨), 6001 free, `NOT WRITABLE` 없음.

컴퓨트 노드는 **자기 볼륨만** rw 로 필요하다 — `CachedFD::open` 이 `device_path` 를
`O_RDWR|O_DIRECT` 로 연다 (`blockio/cached_fd.cpp:250`).

### 2-2. 스토리지 노드

```bash
ssh eternitystorage '
  echo "  nvmet 매핑 (이게 -devices 순서의 근거다):"
  for s in /sys/kernel/config/nvmet/subsystems/*; do
    echo "    $(basename $s) -> $(cat $s/namespaces/*/device_path 2>/dev/null)"
  done
  echo "  5050  : $(ss -ltn | grep ":5050 " || echo free)"
  pgrep -af raft_blockcopy_server | grep -v pgrep || echo "  (서버 미기동)"
'
```

기대: `node3 -> /dev/nvme1n1`, `node5 -> /dev/nvme3n1`, `node6 -> /dev/nvme5n1`.
**이 출력 순서 그대로가 `-devices` 다.**

---

## 3. 사전 준비 (root — 호스트마다 1회)

### 3-1. 컴퓨트 노드: 자기 볼륨 rw

`/etc/udev/rules.d/99-raftof-vols.rules` — `subsysnqn` 매칭이라 장치 번호가
바뀌어도 따라간다:

```
KERNEL=="nvme*n*", ATTRS{subsysnqn}=="node3", OWNER="jongc", MODE="0660"
KERNEL=="nvme*n*", ATTRS{subsysnqn}=="node5", OWNER="jongc", MODE="0660"
KERNEL=="nvme*n*", ATTRS{subsysnqn}=="node6", OWNER="jongc", MODE="0660"
```
```bash
sudo udevadm control --reload && sudo udevadm trigger --subsystem-match=block
```

엄밀히는 각 노드가 자기 볼륨 하나만 있으면 되지만, 세 줄을 다 두면 역할을
호스트 간에 옮겨도 그대로 동작한다.

> **`usermod -aG disk jongc` 는 쓰지 말 것** — Lustre MDT(eternity5)/OST(eternity6)
> 까지 열린다.

### 3-2. 컴퓨트 노드: 메타데이터 디렉터리

링 파일은 **대상 블록 디바이스 위 파일시스템**에 있어야 한다 (FIEMAP 이 그
디바이스 기준 PBA 를 돌려준다). 각 호스트의 `/mnt/raftvol` 이 자기 볼륨이다.

```bash
sudo install -d -o jongc -g jongc /mnt/raftvol/raftof-cpp
```

> `/mnt/raftvol/node{3,5,6}/raft_meta.bin`(정확히 32 GiB)은 **Go 원본** 산출물이다.
> 이 C++ 포팅은 `<metadata-dir>/raft-<id>.ring` 을 만든다
> (`core/src/raft_lifecycle.cpp:75`). 별도 디렉터리로 분리한다.

### 3-3. 재부팅 후 복구 (컴퓨트 노드)

**`/etc/fstab` 에 `/mnt/raftvol` 항목이 없고, `nvmf-autoconnect` 도 실제로는
복구해 주지 못한다.** 재부팅한 컴퓨트 노드는 NVMe-oF 연결과 마운트가 모두
사라진 상태로 올라온다 (2026-09-10 eternity3 에서 실제로 발생). §2-1 이
`mount : NONE` 과 빈 subsystem 목록으로 잡아낸다.

자기 볼륨만 되살리면 된다 (`<NQN>` 은 그 호스트 것 — node3/node5/node6):

```bash
sudo modprobe nvme-tcp                     # RDMA 로 붙일 거면 nvme-rdma
sudo nvme connect -t tcp -a 115.145.173.243 -s 4420 -n <NQN>
# 새로 생긴 경로를 subsysnqn 으로 찾는다 (장치 번호를 가정하지 말 것)
for sub in /sys/class/nvme-subsystem/*; do
  [ "$(cat $sub/subsysnqn)" = "<NQN>" ] && ls -d $sub/nvme*n* | xargs -n1 basename
done
sudo mount /dev/<위에서 나온 것> /mnt/raftvol
```

되살린 뒤 §2-1 을 다시 돌려 통과하는지 확인한다.

### 3-4. 스토리지 노드

`raft_blockcopy_server` 가 세 볼륨을 `O_RDWR|O_DIRECT` 로 연다. `sudo` 로 띄우면
된다 (종료도 `sudo kill`). 별도 준비 없음.

---

## 4. 클러스터 스펙

한 문자열을 **세 컴퓨트 노드에 그대로** 넘긴다.

```bash
CLUSTER="7@10.0.0.7:6001@/dev/nvme1n1@10.0.0.91:5050,\
5@10.0.0.5:6001@/dev/nvme2n1@10.0.0.91:5050,\
6@10.0.0.6:6001@/dev/nvme3n1@10.0.0.91:5050"
```

형식은 `id@raft_addr@device_path@storage_host`.

- **`storage_host` 는 세 항목 모두 `10.0.0.91:5050`** — disaggregated 구조라
  스토리지 노드가 하나다.
- **`device_path` 는 컴퓨트 노드에서 본 경로**다. 각 노드는 **자기 항목만** 쓴다
  (`apps/raft_node_main.cpp:222` → `io.device_path`, FIEMAP 기준 디바이스).
  eternity3 은 `/dev/nvme1n1`(node3), eternity5 는 `/dev/nvme2n1`(node5),
  eternity6 은 `/dev/nvme3n1`(node6) — §1 표의 왼쪽 열이다.
- `storage_host` 도 자기 항목만 쓰인다 (`:263`, `TcpBlockCopyClient`).
- **raft_addr 은 1GbE 주소여야 한다.** eternity3 에 쓸 수 있는 IB 가 없다.
- 항목 순서 = cluster 인덱스 = **스토리지 노드 `-devices` 순서**. id 를 3/5/6 으로
  둔 것은 subsystem 이름과 맞춰 자기설명적으로 하려는 것이다.

---

## 5. 기동

### 5-1. 스토리지 노드 먼저 (eternitystorage)

**`-devices` 는 §1 표의 오른쪽 열, 즉 타깃 로컬 경로다.**

```bash
cd ~/raftof_clean
sudo ./build/raft_blockcopy_server -addr 0.0.0.0:5050 \
    -devices /dev/nvme1n1,/dev/nvme3n1,/dev/nvme5n1 -copy-workers 8
```

기동 로그에서 `device[0..2]` 세 줄과 `copy-workers : 8` 을 확인한다.
`bind() failed on port 5050` 이 나오면 이미 떠 있는 것이다 — `pgrep -af
raft_blockcopy_server` 로 확인하고, **`-devices` 가 위와 같은지 반드시 본다.**

### 5-2. 컴퓨트 노드 3개

```bash
# eternity7
sudo ./build/raft_node -id 7 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/node7 \
    -ring-pages 4096 -heartbeat-ms 300 -profile > /tmp/node.log 2>&1 
# eternity5 는 -id 5, eternity6 은 -id 6 (나머지 동일)


# eternity5
sudo ./build/raft_node -id 5 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/node5 \
    -ring-pages 4096 -heartbeat-ms 300 -profile > /tmp/node.log 2>&1 

# eternity6
sudo ./build/raft_node -id 6 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/node6 \
    -ring-pages 4096 -heartbeat-ms 300 -profile > /tmp/node.log 2>&1 
```

**측정용 링은 `-ring-pages 4096`(16 MiB)으로 잡는다.** 기본값 8Mi 페이지 =
32 GiB/노드는 `fallocate` + ZERO_RANGE 비용이 크고(README §6), §7-1 워밍업도
비싸진다. 16 MiB 는 HANDOFF §6.2 기준선과 같은 조건이라 비교에도 유리하다.
**세 노드의 `-ring-pages` 는 반드시 같아야 한다.** `-profile` 은 처음부터 켠다.

```bash
ADDRS=10.0.0.7:6001,10.0.0.5:6001,10.0.0.6:6001
./build/raft_client -addrs $ADDRS -op commit-index    # 세 줄이 나와야 한다
```
막히면 `-debug` 로 다시 띄우면 1초마다 상태 한 줄이 나온다.

> 실행 중에는 **`/mnt/raftvol` 을 다른 도구로 건드리지 말 것.** 스토리지 노드가
> 같은 LBA 를 raw 로 쓰는 동안 파일 extent 가 바뀌면 PBA 가 어긋난다.

---

## 6. 정합성 검증

### 6-1. 복제가 수렴하는가

```bash
./build/raft_client -addrs $ADDRS -op apply -n 200 -size 512 -batch 10
./build/raft_client -addrs $ADDRS -op commit-index    # 세 값이 같아져야 한다
./build/raft_client -addrs $ADDRS -op ae-stats        # 엔트리당 1회 전송인지
```

`ae_entries ≈ 2 × 엔트리수` 여야 정상이다(팔로워 2개 × 1회). 훨씬 크면 팔로워가
못 따라오고 리더가 백로그를 재전송하는 중이다.

### 6-2. 블록 복사가 옳은 바이트를 옮겼는가

세 링 파일이 **512B 헤더 이후 바이트 단위로 같아야** 한다. 파일이 서로 다른
컴퓨트 노드에 있으므로 각자 해시를 떠서 비교한다.

```bash
for hi in eternity3:3 eternity5:5 eternity6:6; do
  h=${hi%%:*}; id=${hi##*:}
  ssh $h "dd if=/mnt/raftvol/raftof-cpp/raft-$id.ring bs=512 skip=1 2>/dev/null | md5sum"
done
```

**세 해시가 같아야 한다.** 그리고 반드시 함께:

```bash
ssh eternity3 'dd if=/mnt/raftvol/raftof-cpp/raft-3.ring bs=512 skip=1 2>/dev/null \
               | tr -d "\0" | wc -c'
```

> **non-zero 검사를 빼면 안 된다.** 복제가 아예 일어나지 않아 세 파일이 전부 0
> 이어도 해시는 사이좋게 일치한다. `scripts/smoke_test.sh:205` 에 이 검사가 들어간
> 이유가 정확히 그것이다(리더 선출이 안 되던 동안 `cmp` 가 무의미하게 통과했다).

---

## 7. 측정

### 7-1. 먼저 링을 한 바퀴 워밍업한다 — U11

**측정 전에 반드시 해야 한다.** `create_ring_file` 은 기동할 때마다 링 전체에
`FALLOC_FL_ZERO_RANGE` 를 건다 (`blockio/cached_fd.cpp:202`). D9 는 그것이 extent 를
전부 written 으로 만든다고 전제하지만, **DECISIONS.md U11 의 실측은 그 전제가
틀렸다는 것을 보여준다** — ext4 에서 ZERO_RANGE 후에도 extent 는 unwritten 으로
남는다 (라이브 링 파일에서 257 extent 중 257 개가 unwritten 이었다).

그래서 **각 extent 에 처음 닿는 쓰기가 ext4 저널을 통한 extent 상태 변환으로
직렬화된다.** 그냥 측정을 시작하면 첫 랩의 `LPersist` 가 그 비용까지 포함해
부풀어 오른다.

`dd` 로 미리 써 두는 방법은 **통하지 않는다** — 노드가 다음 기동 때 다시
ZERO_RANGE 를 걸어 되돌린다. 링이 원형이므로 **한 바퀴 돌려 워밍업**하는 것이
지금 쓸 수 있는 유일한 방법이다.

16 MiB 링 = 32,768 슬롯(512B)이고 4064B 명령은 8슬롯을 쓰므로 **한 바퀴 ≈ 4,100
명령**이다. 넉넉히 넘겨 돌린다:

```bash
./build/raft_client -addrs $ADDRS -op apply -n 5000 -size 4064 -batch 10   # 버린다
```

> U11 은 미해결이며 U7(StorageIO 격차)과 관련이 있을 수 있다. 워밍업 전/후를
> 둘 다 기록해 두면 그 조사에 그대로 쓸 수 있다.

### 7-2. ApplyTimings 7항 분해 (지연)

```bash
./build/raft_client -addrs $ADDRS -op apply-timed -n 10000 -size 4064 -batch 1
```

항등식이 성립해야 한다:
```
Total ≈ LHandler + LPersist + AENet + FHandler + ReplNet + StorageIO + QuorumWait
```
성립하지 않으면 노드에 `-profile` 이 빠졌거나 샘플 폴백 경로다 (HANDOFF §6.1).

이 구조에서는 **`StorageIO` 가 로컬 PCIe 복사**이고 **`LPersist` 가 패브릭**이다
(§1). 두 항을 그렇게 읽는다.

### 7-3. 처리량

```bash
./build/raft_client -addrs $ADDRS -op apply -n 2000 -size 512 -batch 10
```

### 7-4. 기준선과 나란히 놓기

HANDOFF §6.2 는 **다른 머신, 로컬 3노드, identity-pba, 링 16 MiB** 기준이라
직접 비교되지 않는다. 조건을 함께 적는다.

| 워크로드 | HANDOFF §6.2 | 이번 실측 | 리더 | 조건 차이 |
|---|---|---|---|---|
| 4064B ×20, batch=1 | 2.9 ms/command | | | disaggregated / FIEMAP / 실디바이스 |
| 512B ×2000, batch=10 | 334 µs/command | | | 〃 |

### 7-5. 측정 함정 — 반드시 지킬 것

1. **`-ae-batch` 로 상한을 줄 것.** `MaxAppendEntriesBatch` 기본값이
   1,000,000(사실상 무제한)이라, 팔로워 하나가 뒤처지면 리더가 매 라운드 밀린
   백로그를 통째로 재전송한다 (HANDOFF §6.2 주석 — `ae_entries` 가 엔트리 수의
   100배까지 올라가는 것을 관측했다).
2. **Apply 당 샘플은 1건뿐이다** (`ReplSink::first_sample`). 한 번의 출력으로
   분포를 논하지 말 것 — 여러 번 돌려 모은다.
3. **리더가 누구였는지 기록할 것.** eternity3 이 리더면 `LPersist` 가 1GbE 를
   탄다 (§1).

---

## 8. 정리

```bash
for h in eternity3 eternity5 eternity6; do ssh $h 'pkill -f "raft_node -id"'; done
ssh eternitystorage 'sudo pkill -f raft_blockcopy_server'
```

**재실행할 때는 링 파일을 지우고 깨끗하게 시작한다:**

```bash
for hi in eternity3:3 eternity5:5 eternity6:6; do
  h=${hi%%:*}; id=${hi##*:}
  ssh $h "rm -f /mnt/raftvol/raftof-cpp/raft-$id.ring"
done
```

> 남은 링으로 재시작하면 **DECISIONS.md U5** 에 걸린다 — 팔로워 재시작 catch-up 이
> 설계상 불가능해서(D8 과 D12 가 모순) 리더가 영원히 같은 `prev_log_index` 를
> 보내고 팔로워가 영원히 거절한다. 원본 `raft.go` 도 같다. 안전성은 유지되지만
> 그 노드는 영영 따라오지 못하므로, 측정에는 항상 깨끗한 상태로 들어간다.

---

## 9. 막혔을 때

| 증상 | 원인 / 대응 |
|---|---|
| `bind() failed on port 505` | 스토리지 노드가 이미 떠 있다. `-devices` 가 §1 오른쪽 열과 같은지 확인하고 같으면 그대로 쓴다 |
| 복제는 되는데 **엉뚱한 볼륨이 깨짐** | `-devices` 순서가 cluster 인덱스와 어긋났다. 컴퓨트/스토리지의 경로가 다르다(§1). 에러가 안 나므로 §2 로만 잡을 수 있다 |
| `do_pba_copy: PBA=0 ... hole in ring file` | 링 파일이 sparse 다. 파일시스템이 `FALLOC_FL_ZERO_RANGE` 를 지원해야 한다(ext4/xfs/btrfs) |
| 스토리지 노드가 즉시 종료 | 디바이스를 못 연다. `sudo` 로 띄웠는지, 경로가 타깃 로컬 경로인지 |
| `raft_node` 가 링 생성/열기 실패 | §3-1(자기 볼륨 rw) 또는 §3-2(메타 디렉터리) |
| 리더가 안 뽑힌다 | `-debug` 로 재기동. 세 노드가 서로의 6001 에 닿는지(1GbE 주소인지) 확인 |
| 복사가 갑자기 전부 실패 | **U6** — 스토리지 커넥션이 한 번 죽으면 그 컴퓨트 노드는 이후 PBA 복사가 영구히 실패한다. 해당 `raft_node` 재시작 |
| 링 해시는 같은데 전부 0 | 복제가 일어나지 않았다. §6-2 non-zero 검사 |
| 첫 측정만 유독 느림 | **U11** — 링 extent 가 unwritten. §7-1 워밍업 |
| 재부팅 후 `mount : NONE`, subsystem 목록 비어 있음 | fstab 에 항목이 없고 autoconnect 가 복구 못 한다. §3-3 |
