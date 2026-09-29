# Raft-oF v2 3-Node Runbook

## 1. 클러스터 구성

| 역할        | 서버              | Raft ID | Ethernet IP       | IPoIB IP    | Local NVMe-oF device |
| --------- | --------------- | ------: | ----------------- | ----------- | -------------------- |
| Compute 1 | eternity5       |       5 | `115.145.173.124` | `10.0.0.5`  | `/dev/nvme2n1`       |
| Compute 2 | eternity6       |       6 | `115.145.173.125` | `10.0.0.6`  | `/dev/nvme3n1`       |
| Compute 3 | eternity7       |       7 | `115.145.173.126` | `10.0.0.7`  | `/dev/nvme1n1`       |
| Storage   | eternitystorage |       - | `115.145.173.243` | `10.0.0.91` | physical NVMe 3개     |

Raft RPC port:

```
6000
```

BlockCopy RPC port:

```
5050
```

현재 transport mode는 세 가지를 사용한다.

```
rdma
→ RPC transport: RDMA
→ network address: 10.0.0.x IPoIB 주소

tcp-ib
→ RPC transport: TCP
→ network address: 10.0.0.x IPoIB 주소

tcp
→ RPC transport: TCP
→ network address: 115.145.173.x Ethernet 주소
```

즉:

```
rdma       = RDMA over InfiniBand
tcp-ib     = TCP over IPoIB
tcp        = TCP over Ethernet
```

---

# 2. 최초 세팅 확인

## 2.1 Compute node NVMe-oF 연결 확인

각 compute node:

```
sudo nvme list-subsys
```

필요하면:

```
sudo modprobe nvme_fabrics
sudo modprobe nvme_tcp
```

NVMe-oF discovery:

```
sudo nvme discover -t tcp -a 115.145.173.243 -s 4420
```

연결이 없는 경우 기존 subsystem 구성에 맞춰 connect한다.

예:

```
sudo nvme connect -t tcp -n node1 -a 115.145.173.243 -s 4420
sudo nvme connect -t tcp -n node3 -a 115.145.173.243 -s 4420
sudo nvme connect -t tcp -n node5 -a 115.145.173.243 -s 4420
```

다시 확인:

```
sudo nvme list-subsys
```

---

# 3. Compute node mount 확인

각 compute node:

```
mount | grep /mnt/raftvol
```

## ID 5

mount가 없다면:

```
sudo mkdir -p /mnt/raftvol
sudo mount /dev/nvme2n1 /mnt/raftvol
sudo mkdir -p /mnt/raftvol/node5
```

## ID 6

```
sudo mkdir -p /mnt/raftvol
sudo mount /dev/nvme3n1 /mnt/raftvol
sudo mkdir -p /mnt/raftvol/node6
```

## ID 7

```
sudo mkdir -p /mnt/raftvol
sudo mount /dev/nvme1n1 /mnt/raftvol
sudo mkdir -p /mnt/raftvol/node7
```

주의:

```
raft metadata 파일이 있는 filesystem과
cluster에 지정된 device_path는 같은 물리 NVMe-oF volume이어야 한다.
```

---

# 4. Build

## Compute node 3대

각 compute node:

```
cd /home/ryudb00/raftof_jongc

./build.sh node
./build.sh client
```

Raft core / timing 관련 파일을 수정했다면 최소:

```
./build.sh node
```

Client API 또는 출력도 수정했다면:

```
./build.sh client
```

---

## Storage node

Storage node:

```
cd /home/ryudb00/raftof_jongc

./build.sh blockcopy-server
```

다음 파일을 수정한 경우 재빌드 필요:

```
storage/raft_blockcopy_server.cpp
storage/raft_blockcopy_server.h
```

---

# 5. 사용하는 스크립트

현재 주요 스크립트:

```
env.sh
start_node.sh
stop_node.sh
clean_node.sh
start_blockcopy.sh
stop_blockcopy.sh
bench_latency.sh
```

실행 권한:

```
chmod +x env.sh
chmod +x start_node.sh
chmod +x stop_node.sh
chmod +x clean_node.sh
chmod +x start_blockcopy.sh
chmod +x stop_blockcopy.sh
chmod +x bench_latency.sh
```

---

# 6. Transport mode 정의

## RDMA

```
mode name:
rdma
```

실제 실행:

```
-transport rdma
```

사용 주소:

```
Raft:
10.0.0.5:6000
10.0.0.6:6000
10.0.0.7:6000

Storage:
10.0.0.91:5050
```

---

## TCP over IPoIB

```
mode name:
tcp-ib
```

실제 실행:

```
-transport tcp
```

사용 주소:

```
Raft:
10.0.0.5:6000
10.0.0.6:6000
10.0.0.7:6000

Storage:
10.0.0.91:5050
```

즉 RDMA와 동일한 IPoIB network path를 사용하지만 RPC 구현만 TCP로 바뀐다.

---

## TCP over Ethernet

```
mode name:
tcp
```

실제 실행:

```
-transport tcp
```

사용 주소:

```
Raft:
115.145.173.124:6000
115.145.173.125:6000
115.145.173.126:6000

Storage:
115.145.173.243:5050
```

---

# 7. Storage BlockCopy server 시작

사용법:

```
./start_blockcopy.sh <rdma|tcp-ib|tcp>
```

주의:

`tcp-ib`와 `tcp`는 둘 다 실제 binary에는:

```
-transport tcp
```

가 들어간다.

차이는 Raft node가 storage host로 어떤 IP를 사용하느냐이다.

---

## RDMA

Storage node:

```
./start_blockcopy.sh rdma
```

확인:

```
pgrep -af raft_blockcopy_server
```

로그:

```
tail -f raft_blockcopy_server_rdma.log
```

---

## TCP over IPoIB

Storage node:

```
./start_blockcopy.sh tcp-ib
```

확인:

```
pgrep -af raft_blockcopy_server
```

로그:

```
tail -f raft_blockcopy_server_tcp-ib.log
```

---

## TCP over Ethernet

Storage node:

```
./start_blockcopy.sh tcp
```

로그:

```
tail -f raft_blockcopy_server_tcp.log
```

---

# 8. Raft node 시작

사용법:

```
./start_node.sh <id> [destination|leader] [rdma|tcp-ib|tcp]
```

기본값:

```
mode      = destination
transport = rdma
```

---

## Destination + RDMA

eternity5:

```
./start_node.sh 5 destination rdma
```

eternity6:

```
./start_node.sh 6 destination rdma
```

eternity7:

```
./start_node.sh 7 destination rdma
```

---

## Destination + TCP over IPoIB

eternity5:

```
./start_node.sh 5 destination tcp-ib
```

eternity6:

```
./start_node.sh 6 destination tcp-ib
```

eternity7:

```
./start_node.sh 7 destination tcp-ib
```

---

## Destination + TCP Ethernet

eternity5:

```
./start_node.sh 5 destination tcp
```

eternity6:

```
./start_node.sh 6 destination tcp
```

eternity7:

```
./start_node.sh 7 destination tcp
```

---

## Leader + RDMA

```
./start_node.sh 5 leader rdma
./start_node.sh 6 leader rdma
./start_node.sh 7 leader rdma
```

---

## Leader + TCP over IPoIB

```
./start_node.sh 5 leader tcp-ib
./start_node.sh 6 leader tcp-ib
./start_node.sh 7 leader tcp-ib
```

---

## Leader + TCP Ethernet

```
./start_node.sh 5 leader tcp
./start_node.sh 6 leader tcp
./start_node.sh 7 leader tcp
```

한 실험에서는 세 노드 모두 동일한 replication mode와 transport mode를 사용한다.

---

# 9. Cluster 상태 확인

먼저:

```
source ./env.sh
```

그리고 transport mode를 선택한다.

## RDMA

```
select_transport_env rdma
```

Commit index:

```
./build/raft_client \
  -addrs "$ADDRS" \
  -op commit-index \
  -transport "$TRANSPORT"
```

AE stats:

```
./build/raft_client \
  -addrs "$ADDRS" \
  -op ae-stats \
  -transport "$TRANSPORT"
```

---

## TCP over IPoIB

```
select_transport_env tcp-ib
```

그 뒤 동일:

```
./build/raft_client \
  -addrs "$ADDRS" \
  -op commit-index \
  -transport "$TRANSPORT"

./build/raft_client \
  -addrs "$ADDRS" \
  -op ae-stats \
  -transport "$TRANSPORT"
```

이 경우 실제 값은:

```
TRANSPORT=tcp
ADDRS=10.0.0.x
```

이다.

---

## TCP Ethernet

```
select_transport_env tcp
```

그 뒤:

```
./build/raft_client \
  -addrs "$ADDRS" \
  -op commit-index \
  -transport "$TRANSPORT"
```

---

# 10. Smoke test

## RDMA

```
select_transport_env rdma

./build/raft_client \
  -addrs "$ADDRS" \
  -op apply \
  -n 200 \
  -size 512 \
  -batch 10 \
  -transport "$TRANSPORT"
```

---

## TCP over IPoIB

```
select_transport_env tcp-ib

./build/raft_client \
  -addrs "$ADDRS" \
  -op apply \
  -n 200 \
  -size 512 \
  -batch 10 \
  -transport "$TRANSPORT"
```

---

## TCP Ethernet

```
select_transport_env tcp

./build/raft_client \
  -addrs "$ADDRS" \
  -op apply \
  -n 200 \
  -size 512 \
  -batch 10 \
  -transport "$TRANSPORT"
```

---

# 11. Warm-up

성능 측정 전에는 동일한 warm-up 조건을 사용한다.

예:

```
n       = 5000
payload = 4064
batch   = 10
```

## RDMA

```
select_transport_env rdma

./build/raft_client \
  -addrs "$ADDRS" \
  -op apply \
  -n 5000 \
  -size 4064 \
  -batch 10 \
  -transport "$TRANSPORT"
```

---

## TCP over IPoIB

```
select_transport_env tcp-ib

./build/raft_client \
  -addrs "$ADDRS" \
  -op apply \
  -n 5000 \
  -size 4064 \
  -batch 10 \
  -transport "$TRANSPORT"
```

---

## TCP Ethernet

```
select_transport_env tcp

./build/raft_client \
  -addrs "$ADDRS" \
  -op apply \
  -n 5000 \
  -size 4064 \
  -batch 10 \
  -transport "$TRANSPORT"
```

Warm-up 결과는 실험 결과로 사용하지 않는다.

---

# 12. Latency benchmark

사용법:

```
./bench_latency.sh <mode> <transport-mode> [payload] [count] [batch]
```

transport-mode:

```
rdma
tcp-ib
tcp
```

---

## Destination + RDMA

```
./bench_latency.sh destination rdma 4064 10000 1
```

---

## Destination + TCP over IPoIB

```
./bench_latency.sh destination tcp-ib 4064 10000 1
```

---

## Destination + TCP Ethernet

```
./bench_latency.sh destination tcp 4064 10000 1
```

---

## Leader + RDMA

```
./bench_latency.sh leader rdma 4064 10000 1
```

---

## Leader + TCP over IPoIB

```
./bench_latency.sh leader tcp-ib 4064 10000 1
```

---

## Leader + TCP Ethernet

```
./bench_latency.sh leader tcp 4064 10000 1
```

---

# 13. Payload size별 측정

## 512 B

```
./bench_latency.sh destination rdma      512 10000 1
./bench_latency.sh destination tcp-ib 512 10000 1
./bench_latency.sh destination tcp       512 10000 1
```

## 약 4 KB

```
./bench_latency.sh destination rdma      4064 10000 1
./bench_latency.sh destination tcp-ib 4064 10000 1
./bench_latency.sh destination tcp       4064 10000 1
```

## 약 32 KB

```
./bench_latency.sh destination rdma      32736 10000 1
./bench_latency.sh destination tcp-ib 32736 10000 1
./bench_latency.sh destination tcp       32736 10000 1
```

Leader mode도 동일하게 첫 인자만 `leader`로 변경한다.

---

# 14. Benchmark 결과 저장

결과는:

```
results/
```

아래에 저장한다.

권장 파일명:

```
apply_timed_destination_rdma_4064B_n10000_b1_*.log

apply_timed_destination_tcp-ib_4064B_n10000_b1_*.log

apply_timed_destination_tcp_4064B_n10000_b1_*.log

apply_timed_leader_rdma_4064B_n10000_b1_*.log

apply_timed_leader_tcp-ib_4064B_n10000_b1_*.log

apply_timed_leader_tcp_4064B_n10000_b1_*.log
```

로그의 BENCH INFO에는 최소 다음을 기록한다.

```
mode
transport_mode
actual_transport
network_path
payload_bytes
count
batch
ring_pages
heartbeat_ms
ae_batch
copy_workers
raft_addrs
cluster
profiling
```

---

# 15. Transport 비교 해석

## RDMA vs TCP-over-IPoIB

비교:

```
rdma
vs
tcp-ib
```

둘 다:

```
10.0.0.x
```

IPoIB/InfiniBand 경로를 사용한다.

차이는 주로 RPC transport 구현이다.

```
rdma
→ RDMA transport

tcp-ib
→ TCP/IP stack
→ IPoIB
→ InfiniBand fabric
```

따라서 이 비교가 transport 자체의 비용 차이를 보는 데 더 적합하다.

예:

```
./bench_latency.sh destination rdma 4064 10000 1

./bench_latency.sh destination tcp-ib 4064 10000 1
```

---

## RDMA vs TCP Ethernet

비교:

```
rdma
vs
tcp
```

이 경우는 다음이 동시에 바뀐다.

```
transport implementation
physical/network path
NIC path
switch path
MTU
queueing
link latency
link bandwidth
```

따라서 이는 순수 transport 비교보다는:

```
system-level deployment comparison
```

으로 해석한다.

---

## TCP-over-IPoIB vs TCP Ethernet

비교:

```
tcp-ib
vs
tcp
```

둘 다 실제 RPC 구현은:

```
-transport tcp
```

이다.

차이는 network path다.

```
tcp-ib
→ 10.0.0.x
→ IPoIB / InfiniBand

tcp
→ 115.145.173.x
→ Ethernet
```

따라서 이 비교는 TCP 구현을 고정하고 network fabric/path 차이를 보는 데 유용하다.

---

# 16. ApplyTimed 주요 지표

```
Total
LHandler
LPersist
AENet
FHandler
ReplNet
StorageIO
QuorumWait
Mutex
CommitWait
residual
```

Destination mode:

```
Leader local
├─ LHandler
└─ LPersist

AppendEntries
├─ AENet
└─ Follower handler
   ├─ FHandler
   └─ WritePBABatch
      ├─ ReplNet
      └─ StorageIO

이후
└─ QuorumWait
```

현재 StorageIO는:

```
parallel pread
+ parallel pwrite
+ fdatasync
```

batch wall-clock이다.

---

# 17. Leader mode timing

Leader mode:

```
Leader-side WritePBABatch
├─ ReplNet
└─ StorageIO

그 다음 AppendEntries
├─ AENet
└─ FHandler

그 다음
└─ QuorumWait
```

따라서:

```
FHandler = R2

QuorumWait =
    Replicate
    - WritePBA_RT
    - AE_RT
```

형태의 mode-aware decomposition을 사용해야 한다.

---

# 18. 실험 종료

## Compute node

각 compute node:

```
./stop_node.sh
```

확인:

```
pgrep -af raft_node
```

---

## Storage node

```
./stop_blockcopy.sh
```

확인:

```
pgrep -af raft_blockcopy_server
```

---

# 19. Clean Start

Mode 또는 transport mode를 바꿔 완전히 새 실험을 시작할 때 권장한다.

## Step 1. Raft node 종료

각 compute node:

```
./stop_node.sh
```

## Step 2. BlockCopy server 종료

Storage node:

```
./stop_blockcopy.sh
```

## Step 3. Metadata clean

eternity5:

```
./clean_node.sh 5
```

eternity6:

```
./clean_node.sh 6
```

eternity7:

```
./clean_node.sh 7
```

주의:

```
/mnt/raftvol 전체를 삭제하지 않는다.

Storage node의 raw NVMe device에
mkfs / dd / blkdiscard를 실행하지 않는다.
```

---

# 20. Clean Start 후 실행 예

## Destination + RDMA

Storage:

```
./start_blockcopy.sh rdma
```

Compute:

```
./start_node.sh 5 destination rdma
./start_node.sh 6 destination rdma
./start_node.sh 7 destination rdma
```

Warm-up 후:

```
./bench_latency.sh destination rdma 4064 10000 1
```

---

## Destination + TCP over IPoIB

Storage:

```
./start_blockcopy.sh tcp-ib
```

Compute:

```
./start_node.sh 5 destination tcp-ib
./start_node.sh 6 destination tcp-ib
./start_node.sh 7 destination tcp-ib
```

Warm-up 후:

```
./bench_latency.sh destination tcp-ib 4064 10000 1
```

---

## Destination + TCP Ethernet

Storage:

```
./start_blockcopy.sh tcp
```

Compute:

```
./start_node.sh 5 destination tcp
./start_node.sh 6 destination tcp
./start_node.sh 7 destination tcp
```

Warm-up 후:

```
./bench_latency.sh destination tcp 4064 10000 1
```

---

# 21. 권장 Transport 실험 Matrix

| Replication mode | Transport mode | 실제 RPC transport | Network path               |
| ---------------- | -------------- | ---------------- | -------------------------- |
| Destination      | `rdma`         | RDMA             | InfiniBand / `10.0.0.x`    |
| Destination      | `tcp-ib`    | TCP              | IPoIB / `10.0.0.x`         |
| Destination      | `tcp`          | TCP              | Ethernet / `115.145.173.x` |
| Leader           | `rdma`         | RDMA             | InfiniBand / `10.0.0.x`    |
| Leader           | `tcp-ib`    | TCP              | IPoIB / `10.0.0.x`         |
| Leader           | `tcp`          | TCP              | Ethernet / `115.145.173.x` |

비교 시 다음 조건은 동일하게 유지한다.

```
payload
count
batch
ring_pages
heartbeat_ms
ae_batch
copy_workers
warm-up
leader placement
storage topology
```

---

# 22. 자주 쓰는 명령

## Destination / RDMA

```
./start_blockcopy.sh rdma

./start_node.sh 5 destination rdma
./start_node.sh 6 destination rdma
./start_node.sh 7 destination rdma

./bench_latency.sh destination rdma 4064 10000 1
```

---

## Destination / TCP-over-IPoIB

```
./start_blockcopy.sh tcp-ib

./start_node.sh 5 destination tcp-ib
./start_node.sh 6 destination tcp-ib
./start_node.sh 7 destination tcp-ib

./bench_latency.sh destination tcp-ib 4064 10000 1
```

---

## Destination / TCP Ethernet

```
./start_blockcopy.sh tcp

./start_node.sh 5 destination tcp
./start_node.sh 6 destination tcp
./start_node.sh 7 destination tcp

./bench_latency.sh destination tcp 4064 10000 1
```

---

# 23. 현재 알려진 계측 주의사항

## Per-Apply timing correlation

`ReplSink` 자체는 per-Apply이지만 sink가 비었을 때 사용하는 global `prof.sample_*` fallback은 request/log-index correlation이 없다.

따라서 concurrent client에서는 다른 Apply의 timing sample이 섞일 수 있다.

현재:

```
single-client detailed timing
→ 우선 사용 가능

multi-client detailed timing
→ correlation 수정 전에는 주의
```

---

## Leader mode decomposition

Leader mode에서는 WritePBABatch가 AppendEntries보다 먼저 실행된다.

따라서 destination mode와 동일한 timing decomposition을 사용하면 안 된다.

Mode-aware decomposition이 적용된 build를 사용한다.

---

# 24. 권장 기본 실험

현재 기본 latency 실험:

```
replication mode = destination
transport mode   = rdma
payload          = 4064
count            = 10000
batch            = 1
```

실행:

```
./bench_latency.sh destination rdma 4064 10000 1
```

Transport 비교:

```
./bench_latency.sh destination rdma      4064 10000 1
./bench_latency.sh destination tcp-ib 4064 10000 1
./bench_latency.sh destination tcp       4064 10000 1
```
