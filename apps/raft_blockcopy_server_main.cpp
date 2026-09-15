/* server_random_main.go 대응: 스토리지 노드에서 도는 독립 blockcopy RPC
 * 서버. raft 노드(core/)와 별개 프로세스로 실행되며, "이 offset부터
 * nbytes만큼 다른 볼륨에서 복사해와라" 요청을 받아 실제 pread/pwrite를
 * 수행한다.
 *
 * Destination-Side(기본): follower의 server_random이 leader 볼륨을
 *   읽어 자기 볼륨에 씀 (-devices는 이 노드가 로컬로 갖는 디바이스만
 *   나열해도 됨, dst_dev는 항상 "나 자신").
 * Leader-Side(Server::ReplicationMode::LeaderSide, hpdc15dare 3.1.2
 *   정책): leader의 server_random이 자기 로그를 읽어 *원격* follower
 *   볼륨에 직접 씀. 이 경우 -devices는 클러스터 멤버 인덱스 순서와
 *   1:1로 맞아야 하고, 각 항목은 그 멤버의 볼륨이 이 노드에서도
 *   NVMe-oF로 이미 attach되어 열려있는 디바이스 경로여야 한다
 *   (DARE의 모든 서버 쌍이 log QP로 연결되어 서로의 로그를 MR로
 *   노출해두는 것과 동일한 토폴로지 가정 -- 로컬 개발/테스트
 *   단계에서는 loopback NVMe-oF export나 공유 파일로 흉내낼 수 있다).
 *
 * 사용법 (원본과 동일한 플래그):
 *   raft_blockcopy_server -addr 0.0.0.0:5050 \
 *       -devices /dev/nvme0n1,/dev/nvme1n1 -copy-workers 8
 */
#include "raft_blockcopy_server.h"
#include "raft_blockcopy_tcp_server.h"
#include "raft_rdma_transport.h"   /* rdma_devices_available */
#include "raft_cli.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <sstream>
#include <thread>

using namespace nvmeof_raft::cli;

namespace {

void usage(const char *prog) {
    std::fprintf(stderr,
        "usage: %s [-addr host:port] [-devices dev1,dev2,...] [-copy-workers N]\n"
        "             [-transport rdma|tcp]\n"
        "  -addr         listen address (default 0.0.0.0:5050)\n"
        "  -transport    rdma (default) | tcp\n"
        "  -devices      comma-separated NVMe device paths, one per cluster\n"
        "                member index (default /dev/nvme0n1)\n"
        "  -copy-workers parallel workers per WritePBABatch (default: nproc,\n"
        "                capped at 16)\n",
        prog);
}

} /* namespace */

int main(int argc, char **argv) {
    std::string addr = "0.0.0.0:5050";
    std::string transport = "rdma";
    std::string devices_str = "/dev/nvme0n1";
    unsigned hw = std::thread::hardware_concurrency();
    int copy_workers = static_cast<int>(hw == 0 ? 4 : hw);
    if (copy_workers > 16) {
        copy_workers = 16;   /* "so a 64-core box doesn't allocate W*BlockSize per RPC" */
    }

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-addr") {
            addr = next_arg_value(argc, argv, i, "-addr");
        } else if (arg == "-transport") {
            transport = next_arg_value(argc, argv, i, "-transport");
        } else if (arg == "-devices") {
            devices_str = next_arg_value(argc, argv, i, "-devices");
        } else if (arg == "-copy-workers") {
            copy_workers = std::atoi(next_arg_value(argc, argv, i, "-copy-workers").c_str());
        } else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "unknown flag: %s\n", arg.c_str());
            usage(argv[0]);
            return 1;
        }
    }

    std::vector<std::string> devices = split_trimmed(devices_str, ',');
    if (devices.empty()) {
        std::fprintf(stderr, "no devices specified\n");
        return 1;
    }

    std::vector<int> fds(devices.size());
    for (size_t i = 0; i < devices.size(); i++) {
        int fd = nvmeof_raft::blockcopy::open_device(devices[i]);
        if (fd < 0) {
            std::fprintf(stderr, "failed to open device[%zu] %s\n", i, devices[i].c_str());
            return 1;
        }
        fds[i] = fd;
        std::printf("  device[%zu]: %s (fd=%d)\n", i, devices[i].c_str(), fd);
    }

    if (copy_workers < 1) {
        copy_workers = 1;
    }
    std::printf("  copy-workers : %d (per WritePBABatch)\n", copy_workers);

    nvmeof_raft::blockcopy::BlockCopyServer server(fds, devices, copy_workers);

    nvmeof_raft::TransportKind transport_kind = nvmeof_raft::TransportKind::Rdma;
    if (!nvmeof_raft::parse_transport_kind(transport, &transport_kind)) {
        std::fprintf(stderr, "-transport must be 'rdma' or 'tcp'\n");
        return 1;
    }
    if (transport_kind == nvmeof_raft::TransportKind::Rdma &&
        !nvmeof_raft::rdma_devices_available()) {
        std::fprintf(stderr,
                     "-transport rdma: no RDMA device found on this host "
                     "(use -transport tcp)\n");
        return 1;
    }

    HostPort listen_at = parse_host_port(addr);
    if (!listen_at.has_port) {
        std::fprintf(stderr, "invalid -addr %s (expected host:port)\n", addr.c_str());
        return 1;
    }
    int port = listen_at.port;

    std::printf("raft_blockcopy_server listening (%s): %s  (%zu devices)\n",
                nvmeof_raft::transport_kind_name(transport_kind),
                addr.c_str(), devices.size());
    /* 전송은 -transport 로 고른다 (기본 rdma). 메서드 디스패치
     * (dispatch_blockcopy_method)와 스토리지 서버 로직
     * (storage/raft_blockcopy_server.h)은 전송을 전혀 모른다 -- 갈리는 것은
     * run_blockcopy_server 안의 리스너 한 줄뿐이다. */
    nvmeof_raft::run_blockcopy_server(transport_kind, port, &server);

    return 0;
}