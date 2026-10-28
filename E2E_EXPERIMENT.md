# raftof 3노드 E2E 실험 — 실클러스터 런북 (disaggregated storage)

실제 NVMe-oF 볼륨 위에서 FIEMAP 으로 PBA 를 해석해 3노드 Raft 를 띄우고,
복제 정합성을 확인한 뒤 지연·처리량을 측정한다.

**로컬 단일 호스트 검증 수단은 더 이상 없다.** `-identity-pba` 테스트 모드와
`scripts/smoke_test.sh` / `scripts/restart_test.sh` / `raft_selftest` 는 2026-09-14 에
제거됐다. 복제 경로를 검증하는 곳은 이 문서의 §6 뿐이며, 거기서만 나타나는 함정을 다룬다.

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
| id 4 (eternity4) | `node4` | `/dev/nvme1n1` | **`/dev/nvme1n1`** |
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
CLUSTER="4@10.0.0.4:6001@/dev/nvme1n1@10.0.0.91:5050,\
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
# eternity4
sudo ./build/raft_node -id 4 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/node4 \
    -ring-pages 4096 -heartbeat-ms 300 -profile
# eternity5 는 -id 5, eternity6 은 -id 6 (나머지 동일)

# eternity5
sudo ./build/raft_node -id 5 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/node5 \
    -ring-pages 4096 -heartbeat-ms 100 -profile

# eternity6
sudo ./build/raft_node -id 6 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/node6 \
    -ring-pages 4096 -heartbeat-ms 300 -profile
```

**측정용 링은 `-ring-pages 4096`(16 MiB)으로 잡는다.** 기본값 8Mi 페이지 =
32 GiB/노드는 `fallocate` + ZERO_RANGE 비용이 크고(README §6), §7-1 워밍업도
비싸진다. 16 MiB 는 HANDOFF §6.2 기준선과 같은 조건이라 비교에도 유리하다.
**세 노드의 `-ring-pages` 는 반드시 같아야 한다.** `-profile` 은 처음부터 켠다.

```bash
ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001
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

### 6-2. 무엇으로는 검증할 수 없는가 — 6-3 전에 읽을 것

**두 가지가 안 된다. 둘 다 예전에 이 문서와 `HANDOFF.md` 가 지정했던 방법이다.**

**(a) `-op hash` 는 리더만 본다.** `apply_pending` 이 리더 전용이다 —
`core/src/raft_commit.cpp:141` 이 `state == Leader` 일 때만 `apply_notify_cv` 를
깨우므로 팔로워의 apply 워커는 영원히 안 깬다. 팔로워가
`count=0  hash=cbf29ce484222325`(FNV offset basis = 빈 상태머신)로 나오는 것이
**정상**이고, 이것을 "팔로워가 복제를 못 받았다"로 읽으면 안 된다 (`HANDOFF.md` §7.4).

**(b) 링 파일 md5 비교는 팔로워에 대해 성립하지 않는다.** Destination-side 복제에서
팔로워의 바이트는 스토리지 노드가 **raw 디바이스에 직접 pwrite** 한다 — 팔로워의 ext4 를
거치지 않는다. 그런데 U11 때문에 팔로워 링의 블록은 기동 직후 전부 `unwritten` extent 이고,
**ext4 는 unwritten extent 읽기를 장치에 내리지 않고 0 으로 채워 돌려준다**
(`iflag=direct` 를 줘도 같다). 그래서 팔로워 링 파일은 팔로워가 *자기 파일시스템을 통해* 쓴
블록 — 512B 헤더가 들어 있는 **블록 0 하나** — 말고는 전부 0 으로 보인다.

증상은 "리더 해시만 바뀌고 팔로워 둘은 그대로"다. extent 를 보면 바로 구분된다
(2026-09-15 실측, 65536B × 21 적용 직후):

```
eternity5 (리더)    0..322 written / 323..4095 unwritten   <- 자기가 쓴 만큼만 written
eternity4 (팔로워)  0..0   written / 1..4095   unwritten
eternity6 (팔로워)  0..0   written / 1..4095   unwritten
```

그리고 불일치 경계가 **엔트리 경계가 아니라 4096B 블록 경계**(파일 오프셋 4096)에 떨어진다 —
복제가 끊긴 게 아니라 파일시스템 레이어에서 잘렸다는 신호다. 엔트리 한가운데서 잘렸다면
언제나 이쪽을 의심할 것.

여기에 **페이지 캐시 staleness** 가 하나 더 겹친다. 컴퓨트 노드의 페이지 캐시는 타깃이
밑에서 raw 로 쓴 것을 모르므로, `dd` 를 그냥 쓰면 낡은 내용이 나온다. 즉 이 검사는
**독립적인 이유로 두 번** 깨져 있다.

> 이 절은 예전에 "세 링 파일 md5 가 같아야 한다"였다. 그 검사가 통했던 것은
> `scripts/smoke_test.sh` 의 `-identity-pba` 모드에서 **링 파일 자신이 디바이스**여서 복사가
> 파일을 통해 들어왔기 때문이다. 그 모드와 스크립트는 2026-09-14 에 제거됐고 실클러스터에서는
> 성립하지 않는다. 함께 지정돼 있던 non-zero 검사도 구해주지 못한다 — 복제가 완벽해도
> 팔로워는 0 으로 읽힌다.

### 6-3. 블록 복사가 옳은 바이트를 옮겼는가 — 타깃에서 raw 디바이스로 비교

**지금 유일하게 성립하는 검사다.** 세 멤버 볼륨이 전부 스토리지 노드의 로컬 디바이스이므로
(§1), 거기서 같은 PBA 구간을 읽어 비교하면 컴퓨트 노드의 ext4 와 페이지 캐시를 통째로
우회한다.

전제 두 가지: **클러스터는 떠 있되 apply 를 멈춘 상태**여야 하고, 리더가 비교 구간만큼은
이미 써 둔 뒤여야 한다(아래 예는 1 MiB 를 비교하므로 6-1 의 200×512B 로는 부족하다 —
`-size 4096 -n 300` 정도를 먼저 흘려보낼 것).

**PBA 는 매 기동마다 바뀐다.** 외우지 말고 매번 `filefrag` 로 다시 뽑는다:

```bash
# 1) 각 링 파일의 시작 PBA -> 512B 섹터 오프셋 (+1 = 512B 헤더 건너뜀)
for hi in eternity4:4 eternity5:5 eternity6:6; do
  h=${hi%%:*}; id=${hi##*:}
  blk=$(ssh $h "filefrag -v /mnt/raftvol/node$id/raft-$id.ring | awk 'NR==4{print \$4}'" | tr -d '.')
  echo "id=$id  skip=$((blk*8+1))"
done
```

`filefrag -v` 의 4번째 줄이 첫 extent 이고 4번째 필드가 physical_offset 시작값(4 KiB 블록
단위)이다. `× 8` 로 512B 섹터로 바꾸고 `+ 1` 로 헤더를 건너뛴다.

```bash
# 2) 타깃에서 세 볼륨의 같은 구간을 읽어 비교한다 (id -> 타깃 로컬 장치명은 §1 표)
#    아래 skip 값은 예시다. 반드시 1) 의 출력으로 갈아끼울 것.
ssh eternitystorage '
  sudo dd if=/dev/nvme3n1 bs=512 skip=267485185  count=2047 iflag=direct 2>/dev/null | md5sum  # id 5
  sudo dd if=/dev/nvme1n1 bs=512 skip=1421017089 count=2047 iflag=direct 2>/dev/null | md5sum  # id 7
  sudo dd if=/dev/nvme5n1 bs=512 skip=80019457   count=2047 iflag=direct 2>/dev/null | md5sum  # id 6
'
```

`iflag=direct` 를 빼지 말 것 — blockcopy 서버가 O_DIRECT 로 쓰므로 타깃의 버퍼 캐시가
낡아 있을 수 있다. `count=2047` 은 헤더를 뺀 약 1 MiB 다.

**세 해시가 같아야 한다.** 그리고 6-2 의 교훈대로 **전부 0 이 아닌지 함께 확인한다**:

```bash
ssh eternitystorage 'sudo dd if=/dev/nvme3n1 bs=512 skip=267485185 count=2047 iflag=direct \
                     2>/dev/null | tr -d "\0" | wc -c'      # 0 이면 리더가 아직 안 쓴 것
```

#### 결과를 어떻게 읽나

- **세 해시가 같다** → 복제 정상. 6-2 에서 팔로워 링 파일이 0 으로 보이던 것은 U11 의
  관측 부작용일 뿐이다.
- **다르다** → 진짜 복제 결함이다. `do_pba_copy` 의 목적지 계산부터 본다
  (`core/src/raft_handle_append_entries.cpp` — `dst_slot = old_tail_slot`,
  `dst_dev = raft.cluster_index`, 그리고 `-devices` 순서가 cluster 인덱스와 맞는지 §2).

> **미해결로 기록해 둘 것.** 이 절차는 스토리지 노드의 root 권한을 요구하고 수동이다.
> U11 을 고쳐 extent 가 실제로 written 이 되면 6-2 의 파일 단위 비교가 되살아난다 —
> U11 은 지연(첫 랩 `LPersist` 2배)뿐 아니라 **복제 관측 가능성**의 선결 조건이기도 하다.

### 6-4. push 모드 — push 된 바이트가 맞는가

`-mode push` 로 띄울 때만 한다(DECISIONS.md D20). 인프라 준비(다른 멤버 볼륨 attach,
`-push-devices`, 배너 wwid 대조)는 SETUP.md "PUSH MODE" 를 먼저 끝낸다. 세 노드 모두
`-mode push -statemachine hash -debug` 로 띄우고, 각 노드의 `-push-devices` 에 나머지 두 멤버를 준다.

push 된 배치는 팔로워가 pull 을 건너뛰므로, 그 구간의 팔로워 바이트는 **리더의 push 만** 썼다.
아래 (2)(3) 이 그 바이트를 확인한다.

**순서: (1) → (1-a) → (2) → (3) → (5) → (6).** (3) 은 링 한 바퀴 안에서만 성립하므로 링을 돌리는
(5) 보다 먼저 한다. (6) 은 리더 교체로 상태를 흐트러뜨리므로 맨 끝이다. (4) 는 측정이다(§7-6).
매 회 새 링 디렉터리(`-metadata-dir`)로 띄운다. 코드를 받은 뒤에는 `./build.sh all` 로 `build/` 를
다시 만든다 — 8 단계 1 부(비동기 push) 이전 바이너리로 돌리면 이 절의 (1-a)(6) 은 의미가 없다.

**(1) push 가 실제로 쓰이는가 — 리더 `-debug`**

```bash
./build/raft_client -addrs $ADDRS -op apply -n 300 -size 4096
```

리더의 `[state]` 줄에서 두 팔로워가 `push=target pushed=[A..B] skip=N pull=M` 으로 나와야 한다.
- `skip`(push 덕에 pull 을 건너뛴 배치)이 Apply 마다 늘고, `B` 가 리더 `tail` 을 따라간다.
- `pull`(대상인데 push 구간 밖이라 pull 한 배치)은 거의 늘지 않아야 한다. 세는 것은 두 가지다.
  하나는 대상이 된 직후의 경계 배치다 — 대상이 되기 전에 쓴(push 안 된) 엔트리와 그 뒤 push 된
  엔트리에 걸쳐 있어 통째로 pull 한다. 선거나 맵 설치마다 보통 하나다. 다른 하나는 push 실패 뒤
  1 초 동안의 배치다. 맵을 받기 전에 보낸 배치는 아직 대상이 아니므로 세지 않는다.
- `push=nomap` 이 계속되면 맵 검증이 실패한 것이다. 리더 stderr 의
  `[ERROR] [PUSH] ... extent map rejected -- <이유>` 를 본다. `push=nodev` 면 그 리더 호스트의
  `-push-devices` 에 그 멤버가 없다.

**(1-a) push job 이 겹쳐도 맞는가 — 동시 클라이언트**

비동기 push 에서는 Lock A 가 push 를 기다리지 않으므로 같은 대상의 job 이 워커 큐에 쌓인다(상한 16).
클라이언트를 여러 개 동시에 돌려 그 상황을 만든다. `-clients` 는 `kvs-loop` 전용이라 `apply` 는
프로세스를 여러 개 띄운다.

```bash
for i in $(seq 8); do ./build/raft_client -addrs $ADDRS -op apply -n 200 -size 4096 & done; wait
#   명령 수 = 8 × 200 = 1600. (3) 의 -at-count 에 (1) 의 300 과 합쳐 넣는다 (= 1900)
```

- 리더 stderr 에 아래가 없어야 한다. 있으면 그 대상은 1 초 동안 pull 로 갔다.
  - `[WARN] [PUSH] follower id=N: push write failed (push queue full) -- pulling for the next 1000 ms`
    — 큐 상한(16)을 넘었다. 팔로워 경로가 리더 apply 속도를 못 따라간다는 뜻이다
  - `push write failed (<errno 메시지>)` — 장치 쓰기·flush 실패. 경로(ARP, `fast_io_fail_tmo`)부터 본다
  - `[WARN] IoWorker: job threw (...)` — job 안의 예외
- `[state]` 의 `pull` 이 (1) 대비 거의 늘지 않고, `skip` 과 `pushed=[..B]` 가 tail 을 따라가야 한다.
  위 경고가 났으면 그 1 초 동안의 배치가 `pull` 로 잡힌다 — 정합성 문제는 아니다(그 구간은 pull 로 복제된다).

**(2) push 된 바이트가 리더와 같은가 — 스토리지 노드 raw 비교**

apply 를 멈춘 뒤 §6-3 처럼 스토리지 노드에서 세 볼륨의 같은 구간을 읽어 비교한다. 단, **비교 끝을
리더 데이터의 끝으로 자른다.** 팔로워 링은 `unwritten` extent 라(U11) 아직 Raft 가 쓰지 않은 슬롯에
예전 디스크 내용이 남아 있다 — 그 구간까지 비교하면 push 가 맞아도 해시가 다르다(2026-10-27 node6 에서
실제로 데이터 끝 바로 뒤 3 섹터만 달랐다). 헤더의 `tail_slot` 은 apply 중에 갱신되지 않으므로(진단용,
D8) 끝을 아는 데 못 쓴다.

```bash
# 1) 각 링의 시작 섹터 (+1 = 512B 헤더 건너뜀). 아무 호스트에서
for hi in eternity4:4 eternity5:5 eternity6:6; do h=${hi%%:*}; id=${hi##*:}
  blk=$(ssh $h "filefrag -v <metadata-dir>/raft-$id.ring | awk 'NR==4{print \$4}'" | tr -d '.')
  echo "id=$id skip=$((blk*8+1))"; done
#    extent 가 하나인지 본다 (filefrag 끝 줄 "1 extent found"). 여럿이면 extent 마다 따로 비교해야 한다

# 2) 스토리지 노드(root). 장치는 §2-2 매핑 (2026-10-27: node4=nvme1n1 node5=nvme3n1 node6=nvme5n1)
L=nvme1n1; LS=<리더 skip>      # 리더 볼륨
N=$(dd if=/dev/$L bs=512 skip=$LS count=32767 iflag=direct 2>/dev/null \
    | od -An -tx1 -v -w512 | grep -nv '^\( 00\)*$' | tail -1 | cut -d: -f1)   # = 마지막 0 아닌 슬롯 + 1
echo "compare $N slots"
for x in node4:nvme1n1:<skip4> node5:nvme3n1:<skip5> node6:nvme5n1:<skip6>; do IFS=: read n d s <<< "$x"
  echo "$n $(dd if=/dev/$d bs=512 skip=$s count=$N iflag=direct 2>/dev/null | md5sum | cut -c1-32)"; done
```

세 해시가 같아야 한다. 다르면 `cmp -l` 로 처음 다른 섹터를 찾는다 — 그 섹터가 `N` 이상이면 위 자르기가
틀린 것이고, 안쪽이면 push 경로 결함이다. `-push-devices` 매핑(배너 wwid)과 `write_at_logical` 이
팔로워 맵으로 구한 PBA 부터 본다.

**(3) push 받은 노드가 리더가 되어 같은 상태를 만드는가 — 리더 교체 해시**

(2) 는 바이트만 본다. 이 검사는 push 받은 노드가 리더가 된 뒤 **자기 링에서** 명령을 읽어 apply 한
결과가 원래 리더와 같은지 본다. (1) 에 이어서 한다. `-at-count` 는 지금까지 apply 한 명령 수와 정확히
같아야 한다 — (1) 전에 다른 apply 를 했다면 300 대신 그 합을 쓴다.

```bash
./build/raft_client -addrs $ADDRS -op hash -at-count 300
#   리더 줄의 hash=H count=300 을 기록한다 (팔로워 줄은 count=0 err=... 가 정상, §6-2 (a))
#   리더 프로세스를 끈다 (Ctrl-C). 남은 둘 중 하나가 리더가 된다.
./build/raft_client -addrs $ADDRS -op hash -at-count 300
#   새 리더 줄이 hash=H count=300 이어야 한다 (끈 노드는 unreachable)
```

- 새 리더는 `become_leader` 에서, 팔로워일 때 메타데이터만 받아 둔 엔트리의 명령을 자기 링에서
  읽어 apply 한다(`core/src/raft_election.cpp` 의 deferred entry 로드). 그 노드에서는 push 된 배치에
  pull 이 돌지 않았으므로, 해시가 같으면 push 된 바이트가 끝까지 맞다는 뜻이다.
- **링 한 바퀴 안에서 할 것.** 그 로드는 `ring_slot` 이 0 인 엔트리를 건너뛴다(`e.ring_slot != 0`).
  첫 바퀴의 슬롯 0 은 첫 no-op(명령 없음)이라 무해하지만, 랩 뒤에 슬롯 0 에 놓인 명령은 빠진다.
  300 × 4 KiB 는 16 MiB 링의 한 바퀴(§7-1)에 한참 못 미친다.

**(4) A안이 포기하는 몫 — apply-timed**

§7-2 를 push 모드로 돌린다. push 는 대상별 워커가 자기 쓰기와 겹쳐 하므로(DECISIONS.md D20,
8 단계 1 부) 항이 이렇게 읽힌다.
- `LPersist` 는 리더 자기 링 쓰기만이다.
- `StorageIO` 는 push 대기다 — 커밋을 만든 팔로워의 push 가 Lock A 뒤로 삐져나온 시간(push 가 자기
  쓰기보다 일찍 끝났으면 0). 팔로워의 pull 은 없다.
- `ReplNet` 은 0 이다.

`FHandler` 를 기록해 둔다 — OK 채널 대신 AE 를 쓰기 뒤에 보내는 결정(A안)이 라운드마다 포기하는
몫의 대부분이 이것이다. 같은 조건의 `-mode destination` 결과와 나란히 놓으면 push 의 이득이 보인다.
8 단계 1 부 이전(push 를 Lock A 안에서 차례로 쓰던 때)의 결과가 있으면 그것과도 비교한다 — 그때는
`LPersist` 가 자기 쓰기와 push 쓰기의 합이었고 `StorageIO ≈ 0` 이었다. 절차와 비교 도구는 §7-6.

**(5) 링을 돈 뒤에도 맞는가 — 전체 링 raw 비교**

(3) 을 마친 뒤 클러스터를 새로 띄워(새 링 디렉터리) 링을 1.2 바퀴 이상 돌린다. 한 바퀴를 넘기면
모든 슬롯이 복제 경로(push, 또는 push 전 엔트리의 pull)로 덮이므로 (2) 의 자르기 없이 **링 전체**를
비교할 수 있다. 랩과 extent 경계에서 push 가 쪼개지는 경로(`write_at_logical`)와 job 이 넣은 순서대로
끝나는지를 함께 본다.

```bash
./build/raft_client -addrs $ADDRS -op apply -n 5000 -size 4064 -batch 10     # 16 MiB 링 ≈ 1.2 바퀴
# 스토리지 노드: (2) 의 2) 에서 count=$N 대신 count=32767 (링 전체 = ring_slots)
```

세 해시가 같아야 한다. 여기서는 (3) 을 하지 않는다 — 랩 뒤 슬롯 0 의 명령을 건너뛰는 한계 때문이다.

**(6) (선택) 리더 교체 중 늦게 끝난 job — 펜싱 없음의 확인**

(1-a) 의 동시 apply 를 돌리는 도중 리더를 Ctrl-C 로 끈다. 새 리더 아래에서 apply 를 조금 더 한 뒤
(2) 를 한다. 옛 리더 워커 큐에 남은 job 이 새 리더가 쓴 슬롯을 덮으면 해시가 다르다 — DECISIONS.md D20
"한계 — 펜싱 없음" 이 재현된 것이므로 결함이 아니라 **알려진 한계의 관측**으로 기록한다(언제, 어느
슬롯). 같게 나와도 "창이 없다" 는 뜻은 아니다. 리더가 Ctrl-C 로 바로 죽으면 큐도 함께 사라지므로,
늦은 job 은 프로세스가 살아 있는 채 물러날 때(네트워크 분리) 주로 생긴다.

---

## 7. 측정

### 7-1. 먼저 링을 한 바퀴 워밍업한다 — U11

**먼저 각 노드 기동 배너의 `ring alloc` 줄을 본다 (D19).** `new, FALLOC_FL_WRITE_ZEROES`
이면 새 링부터 extent 가 written 이라 이 절의 워밍업이 필요 없다. `ZERO_RANGE fallback` 이거나
`existing` (D19 이전에 만든 파일) 이면 아래대로 **측정 전에 반드시 워밍업한다.**

`create_ring_file` 은 (WRITE_ZEROES 를 못 쓸 때) 링 파일을 새로 만들면서
`FALLOC_FL_ZERO_RANGE` 를 건다 (`blockio/cached_fd.cpp`, D18 이후로는 새로 할당한 구간에만). D9 는 그것이 extent 를
전부 written 으로 만든다고 전제하지만, **DECISIONS.md U11 의 실측은 그 전제가
틀렸다는 것을 보여준다** — ext4 에서 ZERO_RANGE 후에도 extent 는 unwritten 으로
남는다 (라이브 링 파일에서 257 extent 중 257 개가 unwritten 이었다).

그래서 **각 extent 에 처음 닿는 쓰기가 ext4 저널을 통한 extent 상태 변환으로
직렬화된다.** 그냥 측정을 시작하면 첫 랩의 `LPersist` 가 그 비용까지 포함해
부풀어 오른다.

§8 처럼 매 실행 전에 링 파일을 지우면 매번 새 파일이므로 **한 바퀴 돌려 워밍업**해야
한다 (링이 원형이므로 한 바퀴면 된다). D18 이후로는 기존 링 파일에 ZERO_RANGE 를 다시
걸지 않는다. 그래서 링 파일을 파일시스템을 통해 한 번 써 두고(`dd oflag=direct`)
지우지 않고 재사용하면 written 상태가 남아 이 워밍업이 필요 없다. 그렇게 할 때는
클러스터 전체를 함께 재시작할 것 — 리더가 살아 있는 동안 팔로워만 재시작하면 U5 에 걸린다.

16 MiB 링 = 32,768 슬롯(512B)이고 4064B 명령은 8슬롯을 쓰므로 **한 바퀴 ≈ 4,100
명령**이다. 넉넉히 넘겨 돌린다:

```bash
./build/raft_client -addrs $ADDRS -op apply -n 5000 -size 4064 -batch 10   # 버린다
```

> U11 은 미해결이며 U7(StorageIO 격차)과 관련이 있을 수 있다. 워밍업 전/후를
> 둘 다 기록해 두면 그 조사에 그대로 쓸 수 있다.

### 7-2. ApplyTimings 7항 분해 (지연)

ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001

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

### 7-6. pull(destination) vs push 지연 비교

같은 클러스터·같은 바이너리에서 **`-mode` 만 바꿔** `scripts/apply_latency_sweep.sh` 를 두 번 돌리고,
`scripts/compare_latency.sh` 로 크기별·항별로 나란히 놓는다.

**같게 둘 것 — 결과 디렉터리에 메모로 남긴다**
- 노드 플래그: `-ring-pages 4096 -heartbeat-ms 300 -profile -ae-batch 256 -loop-sleep-us 20`
  (+ `-statemachine noop`. `hash` 는 1 MiB 당 ~2ms 를 먹어 큰 크기에서 apply 비용이 섞인다).
  `-mode` 와 `-push-devices` 만 다르다
- **`-loop-sleep-us 20` — 빼면 비교가 뒤집힌다.** 리더는 커밋을 메인 루프(`main_loop`,
  `core/src/raft_lifecycle.cpp`)의 `advance_commit_index` 로만 진전시키고, 루프는 바퀴마다 이 값만큼
  잔다(기본 200µs, timer slack 포함 실제 ~250µs). 클라이언트가 한 번에 하나씩 보내므로 응답 시각이
  루프 주기와 맞물려 `QuorumWait`/`CommitWait` 에 위상에 따라 0~250µs 가 붙는다. 2026-10-28 실측
  (리더 eternity4): 기본값에서 push 16K 샘플의 82% 가 QuorumWait ≈253µs, 18% 가 25µs 로 갈려 Total 이
  destination 과 같게 나왔다(470 vs 465µs). 20 에서는 갈림이 사라졌고(≥150µs 샘플 0%) 결과는 아래와 같다.

  | Total p50 (µs) | 1K | 4K | 16K |
  |---|---|---|---|
  | destination | 341 | 365 | 399 |
  | push (비동기) | 175 | 200 | 258 |

  0(원본의 스핀)은 쓰지 않는다 — 루프가 바퀴마다 `mu` 를 잡아 AE 워커·Apply 를 굶긴다.
- C-state(SETUP.md "c-state off")와 `kSpinServeUs` — AENet 스톨이 이것으로 크게 바뀐다
- **리더.** 두 모드에서 같은 호스트가 리더여야 한다(sweep 로그의 `leader:` 줄로 확인). 다르면 다시 띄운다
- 스토리지 IB 링크 속도(`cat /sys/class/infiniband/*/ports/1/rate` — 2026-10-27 현재 SDR 10 Gb/s).
  push 의 데이터는 리더→스토리지로 패브릭을 타고, destination 의 복사는 스토리지 로컬이라 링크 속도가
  두 모드에 다르게 걸린다
- 매 모드 새 링 디렉터리 + sweep 워밍업(`WARMUP=1`, 기본값) — 링이 ZERO_RANGE 폴백이면 첫 바퀴의
  `LPersist` 가 부푼다(§7-1, U11)

**실행**

```bash
# 1) -mode destination 으로 띄운다 (-push-devices 없이)
ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001 SIZES="1024 4096 16384" REPS=3 \
  ./scripts/apply_latency_sweep.sh csv/lat_dest_$(date +%m%d)

# 2) 세 노드를 내리고 -mode push -push-devices ... 로 다시 띄운다 (새 링 디렉터리)
ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001 SIZES="1024 4096 16384" REPS=3 \
  ./scripts/apply_latency_sweep.sh csv/lat_push_$(date +%m%d)

# 3) 비교 (첫 번째가 기준. -v 면 mean/p99 열도)
./scripts/compare_latency.sh csv/lat_dest_<날짜> csv/lat_push_<날짜>
```

16 KiB 를 넘는 크기는 16 MiB 링에서 백프레셔가 섞이기 쉽다(sweep 머리 주석 (3)). 넣으려면 링을 키우고
`RING_BYTES` 를 맞춘다. compare 는 `busy_retries` 가 0 이 아닌 크기에 경고를 찍는다.

(선택) 동기 push(8 단계 1 부 이전, `b39e678`)를 별도 worktree 에서 빌드해 같은 절차로 `lat_pushsync_*`
를 만들어 세 번째 인자로 주면, 비동기화의 효과(LPersist = 자기+push 합 → 자기 쓰기만)를 따로 볼 수 있다.

**항을 읽는 법** (DECISIONS.md D20 "계측", §6-4 (4))

| 항 | destination (pull) | push (비동기) |
|---|---|---|
| `LPersist` | 리더 자기 링 쓰기 (패브릭) | 리더 자기 링 쓰기만 |
| `ReplNet` | 팔로워 → 스토리지 blockcopy RPC | 0 |
| `StorageIO` | 스토리지 노드 로컬 PCIe 복사 | push 대기 = 그 팔로워 push 끝 − Lock A 끝 (≥ 0) |
| `FHandler` | pull 을 포함한 팔로워 처리 | 메타데이터 처리만 — A안이 포기하는 몫 |
| `QuorumWait` | replicate − AE_RT | replicate − push 대기 − AE_RT |

중심으로 볼 것은 둘이다.
- **`Total` p50/p99** — 결론
- **`ReplNet+StorageIO(mean)`** — compare 가 덧붙이는 합성 행. 팔로워 데이터 경로 전체 비용이다.
  destination 은 (RPC + 스토리지 복사), push 는 push 대기다. 이 차이가 "팔로워의 복사를 리더가 대신
  했을 때" 얻은 몫이다. push 대기가 0 에 가까우면 push 가 자기 쓰기 뒤에 숨은 것이다.

compare 의 `d_*` 는 기준 대비 p50 차이이고, REPS>1 이면 반복 행을 평균한 값이다(latency.csv 에 rep 열이
없다). Apply 당 샘플이 1 건이므로(§7-5) REPS 는 3 이상으로 한다.

---

## 8. 정리

```bash
for h in eternity3 eternity5 eternity6; do ssh $h 'pkill -f "raft_node -id"'; done
ssh eternitystorage 'sudo pkill -f raft_blockcopy_server'
```

**재실행할 때는 링 파일을 지우고 깨끗하게 시작한다:**

```bash
for hi in eternity5:5 eternity6:6 eternity4:4; do
  h=${hi%%:*}; id=${hi##*:}
  ssh $h "rm -f /mnt/raftvol/node$id/raft-$id.ring"
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
