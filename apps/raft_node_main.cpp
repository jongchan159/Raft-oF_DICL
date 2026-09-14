/* ============================================================
 * apps/raft_node_main.cpp -- Raft 노드 프로세스의 조립부
 *
 * **이 파일에 Raft 알고리즘은 없다.** 여기는 조립부(composition root)다.
 * 프로토콜 본체는 core/src/ 에 있고, 그 진입점은 raft_lifecycle.cpp의
 * Server::main_loop 다. 이 파일이 Server에 대해 부르는 것은 세 개뿐이다:
 *   init_storage() / start() / stop()
 * 나머지는 인자 파싱, 필드 채우기, 전송 구현체 주입, 상태 출력이다.
 * raft.current_term 등을 읽는 곳이 몇 군데 있지만 전부 -debug 덤프용
 * 읽기 전용이고, 상태 전이는 한 줄도 하지 않는다.
 *
 * 조립 순서:
 *   Server 생성 -> ring.configure() -> 필드 채우기
 *   -> transport / blockcopy 주입   <-- 이 프로젝트에서 "어떤 전송을
 *      쓸지" 결정하는 유일한 지점. RDMA로 바꿀 때 손댈 곳이 여기이고,
 *      core/ 는 건드리지 않는다 (core는 net/ 헤더를 하나도 모른다).
 *   -> init_storage()
 *   -> start()          (메인 루프 / apply 워커 / slot GC 워커)
 *   -> run_tcp_server() (AppendEntries / RequestVote / Client* 수신)
 *
 * 사용법:
 *   raft_node -id 1 \
 *     -cluster "1@127.0.0.1:6001@@127.0.0.1:5051,2@127.0.0.1:6002@@127.0.0.1:5052" \
 *     -metadata-dir /var/lib/raftof -heartbeat-ms 300
 *
 * -cluster 항목 형식: id@raft_addr@device_path@storage_host
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_statemachine_hash.h"
#include "raft_tcp_server.h"
#include "raft_tcp_clients.h"
#include "raft_cli.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace nvmeof_raft::cli;

namespace {

std::atomic<bool> g_stop{false};

void on_signal(int) { g_stop.store(true); }

struct MemberSpec {
    uint64_t id = 0;
    std::string address;
    std::string device_path;
    std::string storage_host;
};

/* "id@raft_addr@device@storage_host" 하나를 파싱.
 */
MemberSpec parse_member(const std::string &spec) {
    std::vector<std::string> f = split(spec, '@');
    if (f.size() < 4) {
        throw std::runtime_error(
            "invalid -cluster entry '" + spec +
            "' (expected id@raft_addr@device_path@storage_host)");
    }
    MemberSpec m;
    m.id = std::strtoull(f[0].c_str(), nullptr, 10);
    m.address = f[1];
    m.device_path = f[2];
    m.storage_host = f[3];
    if (m.id == 0) {
        throw std::runtime_error("invalid -cluster entry '" + spec + "': id must be > 0");
    }
    if (m.address.find(':') == std::string::npos) {
        throw std::runtime_error("invalid -cluster entry '" + spec +
            "': raft_addr must be host:port");
    }
    return m;
}

void usage(const char *prog) {
    std::fprintf(stderr,
      "usage: %s -id N -cluster SPEC[,SPEC...] [options]\n"
      "\n"
      "  SPEC = id@raft_addr@device_path@storage_host\n"
      "         device_path is the NVMe-oF volume this member's ring file\n"
      "         lives on; FIEMAP resolves PBAs against it.\n"
      "\n"
      "  -id N              this node's id (must appear in -cluster)\n"
      "  -metadata-dir DIR  where the ring file lives (default .)\n"
      "  -heartbeat-ms N    heartbeat interval (default 300)\n"
      "  -mode MODE         destination (default) | leader\n"
      "                     replication policy; 'leader' is the DARE-style\n"
      "                     policy for comparison (hpdc15dare 3.1.2)\n"
      "  -ring-pages N      ring size in 4KiB pages (default %llu = 32GiB).\n"
      "                     Ring file is N*4096 bytes and is fallocate'd.\n"
      "  -loop-sleep-us N   main loop pause per iteration (default 200, 0=spin)\n"
      "  -log-trim N        trim the in-memory log vector once N entries are\n"
      "                     reclaimable (default 8192, 0=never)\n"
      "  -ae-batch N        max_ae_batch override\n"
      "  -ae-batch-bytes N  max_ae_batch_bytes override\n"
      "  -debug             verbose logging\n",
      prog, static_cast<unsigned long long>(nvmeof_raft::DEFAULT_NUM_PAGES));
}

} /* namespace */

int main(int argc, char **argv) {
    using namespace nvmeof_raft;

    uint64_t my_id = 0;
    std::string cluster_str;
    std::string metadata_dir = ".";
    int heartbeat_ms = 300;
    std::string mode = "destination";
    uint64_t ring_pages = DEFAULT_NUM_PAGES;
    bool profile = false;
    bool debug = false;
    int loop_sleep_us = 200;
    uint64_t log_trim = 8192;
    uint64_t ae_batch = 0;
    uint64_t ae_batch_bytes = 0;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-id") {
            my_id = std::strtoull(next_arg_value(argc, argv, i, "-id").c_str(), nullptr, 10);
        } else if (arg == "-cluster") {
            cluster_str = next_arg_value(argc, argv, i, "-cluster");
        } else if (arg == "-metadata-dir") {
            metadata_dir = next_arg_value(argc, argv, i, "-metadata-dir");
        } else if (arg == "-heartbeat-ms") {
            heartbeat_ms = std::atoi(next_arg_value(argc, argv, i, "-heartbeat-ms").c_str());
        } else if (arg == "-mode") {
            mode = next_arg_value(argc, argv, i, "-mode");
        } else if (arg == "-ring-pages") {
            ring_pages = std::strtoull(next_arg_value(argc, argv, i, "-ring-pages").c_str(), nullptr, 10);
                } else if (arg == "-profile") {
            profile = true;
        } else if (arg == "-debug") {
            debug = true;
        } else if (arg == "-loop-sleep-us") {
            loop_sleep_us = std::atoi(next_arg_value(argc, argv, i, "-loop-sleep-us").c_str());
        } else if (arg == "-log-trim") {
            log_trim = std::strtoull(next_arg_value(argc, argv, i, "-log-trim").c_str(), nullptr, 10);
        } else if (arg == "-ae-batch") {
            ae_batch = std::strtoull(next_arg_value(argc, argv, i, "-ae-batch").c_str(), nullptr, 10);
        } else if (arg == "-ae-batch-bytes") {
            ae_batch_bytes = std::strtoull(next_arg_value(argc, argv, i, "-ae-batch-bytes").c_str(), nullptr, 10);
        } else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "unknown flag: %s\n", arg.c_str());
            usage(argv[0]);
            return 1;
        }
    }

    if (my_id == 0 || cluster_str.empty()) {
        usage(argv[0]);
        return 1;
    }

    std::vector<MemberSpec> members;
    try {
        for (const auto &spec : split(cluster_str, ',')) {
            if (!spec.empty()) {
                members.push_back(parse_member(spec));
            }
        }
    } catch (const std::exception &e) {
        std::fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    if (members.empty()) {
        std::fprintf(stderr, "-cluster is empty\n");
        return 1;
    }

    int my_index = -1;
    for (size_t i = 0; i < members.size(); i++) {
        if (members[i].id == my_id) {
            my_index = static_cast<int>(i);
        }
    }
    if (my_index < 0) {
        std::fprintf(stderr, "-id %llu not present in -cluster\n",
                     static_cast<unsigned long long>(my_id));
        return 1;
    }

    // 1. 서버 생성
    auto server = std::make_unique<Server>();

    /* 링 크기와 AE 배치 상한은 init_storage보다 먼저 확정해야 한다
     * (fallocate 이후에 링 크기를 바꾸면 슬롯 인덱스 해석이 깨진다).
     * 예전에는 프로세스 전역 configure_ring() + 전역 변수였다. */
    server->ring.configure(ring_pages);
    if (ae_batch > 0) {
        server->max_ae_batch = ae_batch;
    }
    if (ae_batch_bytes > 0) {
        server->max_ae_batch_bytes = ae_batch_bytes;
    }
    server->raft.id = my_id;
    server->raft.cluster_index = my_index;
    server->raft.address = members[static_cast<size_t>(my_index)].address;
    server->io.device_path = members[static_cast<size_t>(my_index)].device_path;
    server->io.metadata_dir = metadata_dir;
    server->raft.heartbeat_ms = heartbeat_ms;
    server->debug_enabled = debug;
    server->loop_sleep_us = loop_sleep_us;
    server->ring.log_trim_threshold = log_trim;
    server->statemachine = std::make_shared<HashStateMachine>();
    server->prof.enabled.store(profile ? 1 : 0);

    if (mode == "leader") {
        server->replication_mode = Server::ReplicationMode::LeaderSide;
    } else if (mode == "destination") {
        server->replication_mode = Server::ReplicationMode::DestinationSide;
    } else {
        std::fprintf(stderr, "-mode must be 'destination' or 'leader'\n");
        return 1;
    }

    server->raft.cluster.resize(members.size());
    for (size_t i = 0; i < members.size(); i++) {
        server->raft.cluster[i].id = members[i].id;
        server->raft.cluster[i].address = members[i].address;
        server->raft.cluster[i].device_path = members[i].device_path;
        server->raft.cluster[i].storage_host = members[i].storage_host;
    }

    // 2. 네트워크
    /* ---- 전송 구현체 주입 (core/include/raft_transport.h) ----
     * core/ 는 인터페이스만 알고, 여기서 TCP 구현을 꽂아 준다. RDMA로 갈
     * 때 바뀌는 곳은 이 두 줄이다 (그리고 새 구현체 파일 하나).
     * 커넥션 상태와 dial backoff는 구현체가 자기 안에 갖는다. */
    {
        std::vector<PeerEndpoint> peers;
        peers.reserve(members.size());
        for (const auto &m : members) {
            peers.push_back(PeerEndpoint{m.id, m.address});
        }
        server->transport = std::make_shared<TcpRaftTransport>(std::move(peers));
    }
    server->blockcopy = std::make_shared<TcpBlockCopyClient>(
        members[static_cast<size_t>(my_index)].storage_host);

    // 3. 스토리지
    try {
        server->init_storage();
    } catch (const std::exception &e) {
        std::fprintf(stderr, "init_storage failed: %s\n", e.what());
        return 1;
    }

    std::printf("raft_node id=%llu index=%d addr=%s\n",
                static_cast<unsigned long long>(my_id), my_index,
                server->raft.address.c_str());
    std::printf("  cluster      : %zu members\n", members.size());
    std::printf("  metadata     : %s/raft-%llu.ring (%llu bytes, %llu pages)\n",
                metadata_dir.c_str(), static_cast<unsigned long long>(my_id),
                static_cast<unsigned long long>(server->ring.file_size_bytes()),
                static_cast<unsigned long long>(server->ring.num_pages));
    std::printf("  ring slots   : %llu\n", static_cast<unsigned long long>(server->ring.ring_slots));
    std::printf("  storage host : %s\n",
                members[static_cast<size_t>(my_index)].storage_host.c_str());
    std::printf("  device       : %s\n",
                server->io.device_path.c_str());
    std::printf("  replication  : %s-side\n", mode.c_str());
    std::printf("  heartbeat    : %d ms\n", heartbeat_ms);
    std::printf("  term/tail    : term=%llu tail_log_index=%llu tail_slot=%llu\n",
                static_cast<unsigned long long>(server->raft.current_term),
                static_cast<unsigned long long>(server->ring.tail_log_index),
                static_cast<unsigned long long>(server->ring.tail_slot));
    std::fflush(stdout);

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    /* 상대 노드가 죽었을 때 write()가 SIGPIPE로 프로세스를 죽이지 않게 */
    std::signal(SIGPIPE, SIG_IGN);

    // 4. 서버 시작 - 상시 스레드 3개: main_loop / apply_worker_loop / slot_gc_worker_loop
    server->start();

    // 5. accept 루프 (별도 스레드)
    int port = port_of(server->raft.address);
    std::atomic<bool> listener_stop{false};
    std::thread listener([&]() {
        try {
            run_tcp_server(port, server.get(), &listener_stop);
        } catch (const std::exception &e) {
            std::fprintf(stderr, "run_tcp_server failed: %s\n", e.what());
            g_stop.store(true);
        }
    });

    /* -debug: 1초마다 Raft 상태 한 줄. 지금까지 debug_enabled는 필드만
     * 있고 실제로 아무것도 출력하지 않아서, 선거/복제가 멈췄을 때
     * 밖에서 들여다볼 방법이 없었다. */
    auto next_dump = std::chrono::steady_clock::now();
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!debug) {
            continue;
        }
        auto now = std::chrono::steady_clock::now();
        if (now < next_dump) {
            continue;
        }
        next_dump = now + std::chrono::seconds(1);

        std::string line;
        {
            std::lock_guard<std::mutex> lk(server->mu);
            line = "[state] " + to_string(server->raft.state) +
                   " term=" + std::to_string(server->raft.current_term) +
                   " tail=" + std::to_string(server->ring.tail_log_index) +
                   " commit=" + std::to_string(server->raft.commit_index) +
                   " applied=" + std::to_string(server->raft.last_applied) +
                   " log_len=" + std::to_string(server->raft.log.size());
            for (size_t i = 0; i < server->raft.cluster.size(); i++) {
                line += " | p" + std::to_string(server->raft.cluster[i].id) +
                        " vf=" + std::to_string(server->raft.cluster[i].voted_for) +
                        " next=" + std::to_string(server->raft.cluster[i].next_index) +
                        " match=" + std::to_string(server->raft.cluster[i].match_index);
            }
        }
        std::fprintf(stderr, "%s\n", line.c_str());
        std::fflush(stderr);
    }

    std::printf("shutting down...\n");
    std::fflush(stdout);
    listener_stop.store(true);
    if (listener.joinable()) {
        listener.join();
    }
    server->stop();
    return 0;
}
