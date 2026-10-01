**c-state off**

'echo 1 | sudo tee /sys/devices/system/cpu/*/cpuidle/state3/disable'

**polling mode on/off**

raft_rdma_transport.cpp:242-250 constexpr int kSpinServeUs = 0; (off) or = -1; (polling)

**CLUSTER**

```bash
# IPoIB(RDMA)
CLUSTER="4@10.0.0.4:6001@/dev/nvme1n1@10.0.0.91:5050,\
5@10.0.0.5:6001@/dev/nvme2n1@10.0.0.91:5050,\
6@10.0.0.6:6001@/dev/nvme3n1@10.0.0.91:5050"

ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001

# TCP
CLUSTER="4@115.145.173.123:6001@/dev/nvme1n1@115.145.173.243:5050,\
5@115.145.173.124:6001@/dev/nvme2n1@115.145.173.243:5050,\
6@115.145.173.125:6001@/dev/nvme3n1@115.145.173.243:5050"

ADDRS=115.145.173.123:6001,115.145.173.124:6001,115.145.173.125:6001

```