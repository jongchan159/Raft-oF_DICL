/* ============================================================
 * raft_client_main.cpp -- Raft 노드에 Client* RPC를 쏘는 도구
 *
 * 리더를 찾아(모든 주소에 순서대로 붙어 "not leader"가 아닌 노드를
 * 고른다) 명령을 apply하고, 커밋 인덱스/해시/AE 통계를 읽는다.
 * 벤치마크 하네스의 최소 버전이자 e2e 스모크 테스트의 클라이언트다.
 *
 * 사용법:
 *   raft_client -addrs 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003 \
 *               -op apply -n 100 -size 512 -batch 10
 *   raft_client -addrs ... -op hash -at-count 100
 *   raft_client -addrs ... -op commit-index
 *   raft_client -addrs ... -op ae-stats
 * ============================================================ */
#include "raft_tcp_transport.h"
#include "raft_cli.h"
/* raft_proto_conv.h는 include하지 않는다: 이 바이너리는 변환 함수를 하나도
 * 쓰지 않으면서 core/raft_entry.h와 storage/raft_blockcopy_server.h(pread/pwrite
 * 버퍼 풀까지) 전체를 끌어오고 있었다. 필요한 것은 rpcproto.pb.h뿐이다. */
#include "rpcproto.pb.h"
#include "raft_rpc_methods.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace nvmeof_raft;
using namespace nvmeof_raft::cli;
using clock_type = std::chrono::steady_clock;

/* 하나의 노드로의 RPC 왕복. 실패 시 예외. */
template <typename ReqProtoT, typename RspProtoT>
RspProtoT call(RpcClientHandle *h, const std::string &method, const ReqProtoT &req) {
    std::vector<uint8_t> body(static_cast<size_t>(req.ByteSizeLong()));
    req.SerializeToArray(body.data(), static_cast<int>(body.size()));

    std::vector<uint8_t> rsp_body = rpc_invoke(h, method, body);
    RspProtoT rsp;
    if (!rsp.ParseFromArray(rsp_body.data(), static_cast<int>(rsp_body.size()))) {
        throw std::runtime_error("parse response failed");
    }
    return rsp;
}

void usage(const char *prog) {
    std::fprintf(stderr,
      "usage: %s -addrs host:port[,host:port...] -op OP [options]\n"
      "\n"
      "  -op apply         apply -n commands of -size bytes, -batch per RPC\n"
      "  -op apply-timed   same, but print the ApplyTimings breakdown\n"
      "  -op echo          codec/network round-trip only (no log write)\n"
      "  -op commit-index  print each node's commitIndex\n"
      "  -op hash          print each node's state machine hash/count\n"
      "  -op ae-stats      print leader AE batch counters\n"
      "\n"
      "  -n N        total commands (default 10)\n"
      "  -size N     bytes per command (default 512)\n"
      "  -batch N    commands per Apply RPC (default 1)\n"
      "  -at-count N for -op hash: wait until count reaches N\n"
      "  -timeout-s  leader discovery timeout (default 15)\n",
      prog);
}

/* 리더 찾기: 각 주소에 1개 명령짜리 빈 Apply(commands 없음)를 보내는
 * 대신, ClientApply를 빈 목록으로 보내면 성공하므로 판별이 안 된다.
 * 그래서 1바이트 no-op 명령을 보내 "not leader"가 아닌 노드를 찾는다.
 * 이 명령도 로그에 들어가므로, 이후 카운트에 포함해서 계산한다. */
struct Leader {
    size_t index = 0;
    std::shared_ptr<RpcClientHandle> handle;
    uint64_t probe_commands = 0;   /* 리더 탐색 중 실제로 apply된 명령 수 */
};

Leader find_leader(const std::vector<std::string> &addrs, int timeout_s) {
    auto deadline = clock_type::now() + std::chrono::seconds(timeout_s);
    uint64_t probes = 0;

    while (clock_type::now() < deadline) {
        for (size_t i = 0; i < addrs.size(); i++) {
            std::shared_ptr<RpcClientHandle> h;
            try {
                h.reset(tcp_dial_http(addrs[i]));
            } catch (const std::exception &) {
                continue;   /* 아직 안 떴을 수 있다 */
            }
            try {
                rpcproto::ClientApplyRequest req;
                req.add_commands("\x00", 1);
                auto rsp = call<rpcproto::ClientApplyRequest,
                                rpcproto::ClientApplyResponse>(
                    h.get(), rpc_method::kClientApply, req);
                if (rsp.err().empty() && !rsp.busy()) {
                    probes++;
                    return Leader{i, h, probes};
                }
                /* "not leader"면 다음 노드로. busy면 리더지만 링이 꽉 참 */
                if (rsp.busy()) {
                    return Leader{i, h, probes};
                }
            } catch (const std::exception &) {
                /* 커넥션 문제 -> 다음 노드 */
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    throw std::runtime_error("no leader found within timeout");
}

} /* namespace */

int main(int argc, char **argv) {
    std::string addrs_str;
    std::string op = "apply";
    int n = 10;
    int size = 512;
    int batch = 1;
    uint64_t at_count = 0;
    int timeout_s = 15;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-addrs") {
            addrs_str = next_arg_value(argc, argv, i, "-addrs");
        } else if (arg == "-op") {
            op = next_arg_value(argc, argv, i, "-op");
        } else if (arg == "-n") {
            n = std::atoi(next_arg_value(argc, argv, i, "-n").c_str());
        } else if (arg == "-size") {
            size = std::atoi(next_arg_value(argc, argv, i, "-size").c_str());
        } else if (arg == "-batch") {
            batch = std::atoi(next_arg_value(argc, argv, i, "-batch").c_str());
        } else if (arg == "-at-count") {
            at_count = std::strtoull(next_arg_value(argc, argv, i, "-at-count").c_str(), nullptr, 10);
        } else if (arg == "-timeout-s") {
            timeout_s = std::atoi(next_arg_value(argc, argv, i, "-timeout-s").c_str());
        } else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "unknown flag: %s\n", arg.c_str());
            usage(argv[0]);
            return 1;
        }
    }

    std::vector<std::string> addrs = split(addrs_str, ',', /*skip_empty=*/true);
    if (addrs.empty()) {
        usage(argv[0]);
        return 1;
    }
    if (batch < 1) batch = 1;

    try {
        /* ---- 노드별 조회 op은 리더 탐색 없이 전부에 물어본다 ---- */
        if (op == "commit-index" || op == "hash" || op == "ae-stats") {
            for (const auto &addr : addrs) {
                std::shared_ptr<RpcClientHandle> h;
                try {
                    h.reset(tcp_dial_http(addr));
                } catch (const std::exception &e) {
                    std::printf("%-22s unreachable (%s)\n", addr.c_str(), e.what());
                    continue;
                }
                if (op == "commit-index") {
                    rpcproto::ClientGetCommitIndexRequest req;
                    auto rsp = call<rpcproto::ClientGetCommitIndexRequest,
                                    rpcproto::ClientGetCommitIndexResponse>(
                        h.get(), rpc_method::kClientGetCommitIndex, req);
                    std::printf("%-22s commit_index=%llu\n", addr.c_str(),
                                static_cast<unsigned long long>(rsp.commit_index()));
                } else if (op == "hash") {
                    rpcproto::ClientGetHashRequest req;
                    req.set_at_count(at_count);
                    auto rsp = call<rpcproto::ClientGetHashRequest,
                                    rpcproto::ClientGetHashResponse>(
                        h.get(), rpc_method::kClientGetHash, req);
                    std::printf("%-22s hash=%s count=%llu%s%s\n", addr.c_str(),
                                rsp.hash().c_str(),
                                static_cast<unsigned long long>(rsp.count()),
                                rsp.err().empty() ? "" : "  err=",
                                rsp.err().c_str());
                } else {
                    rpcproto::ClientGetAEBatchStatsRequest req;
                    auto rsp = call<rpcproto::ClientGetAEBatchStatsRequest,
                                    rpcproto::ClientGetAEBatchStatsResponse>(
                        h.get(), rpc_method::kClientGetAEBatchStats, req);
                    std::printf("%-22s ae_count=%llu ae_entries=%llu\n", addr.c_str(),
                                static_cast<unsigned long long>(rsp.ae_count()),
                                static_cast<unsigned long long>(rsp.ae_entries()));
                }
            }
            return 0;
        }

        /* ---- echo: 리더 탐색 없이 첫 노드에 ---- */
        if (op == "echo") {
            std::shared_ptr<RpcClientHandle> h(tcp_dial_http(addrs[0]));
            std::string payload(static_cast<size_t>(size), 'e');
            auto t0 = clock_type::now();
            for (int i = 0; i < n; i++) {
                rpcproto::ClientEchoRequest req;
                req.add_commands(payload.data(), payload.size());
                auto rsp = call<rpcproto::ClientEchoRequest,
                                rpcproto::ClientEchoResponse>(
                    h.get(), rpc_method::kClientEcho, req);
                (void)rsp;
            }
            int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clock_type::now() - t0).count();
            std::printf("echo: %d rpcs, %.1f us/rpc\n", n,
                        static_cast<double>(ns) / 1000.0 / (n > 0 ? n : 1));
            return 0;
        }

        /* ---- apply / apply-timed ---- */
        Leader leader = find_leader(addrs, timeout_s);
        std::printf("leader: %s (probe applied %llu command)\n",
                    addrs[leader.index].c_str(),
                    static_cast<unsigned long long>(leader.probe_commands));

        std::string payload(static_cast<size_t>(size), 'x');
        uint64_t applied = leader.probe_commands;
        uint64_t busy_retries = 0;
        auto t0 = clock_type::now();

        int remaining = n;
        while (remaining > 0) {
            int this_batch = (remaining < batch) ? remaining : batch;

            if (op == "apply-timed") {
                rpcproto::ClientApplyTimedRequest req;
                for (int k = 0; k < this_batch; k++) {
                    req.add_commands(payload.data(), payload.size());
                }
                auto rsp = call<rpcproto::ClientApplyTimedRequest,
                                rpcproto::ClientApplyTimedResponse>(
                    leader.handle.get(), rpc_method::kClientApplyTimed, req);
                if (rsp.busy()) {
                    busy_retries++;
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(rsp.retry_after_ms() > 0
                                                   ? rsp.retry_after_ms() : 5));
                    continue;
                }
                if (!rsp.err().empty()) {
                    throw std::runtime_error("apply-timed: " + rsp.err());
                }
                std::printf("  batch=%d total=%.1fus LHandler=%.1f LPersist=%.1f "
                            "AENet=%.1f FHandler=%.1f ReplNet=%.1f StorageIO=%.1f "
                            "QuorumWait=%.1f Mutex=%.1f CommitWait=%.1f\n",
                            this_batch,
                            rsp.total_nanos() / 1000.0,
                            rsp.l_handler_nanos() / 1000.0,
                            rsp.l_persist_nanos() / 1000.0,
                            rsp.ae_net_nanos() / 1000.0,
                            rsp.f_handler_nanos() / 1000.0,
                            rsp.repl_net_nanos() / 1000.0,
                            rsp.replication_nanos() / 1000.0,
                            rsp.quorum_wait_nanos() / 1000.0,
                            rsp.mutex_nanos() / 1000.0,
                            rsp.commit_wait_nanos() / 1000.0);
            } else {
                rpcproto::ClientApplyRequest req;
                for (int k = 0; k < this_batch; k++) {
                    req.add_commands(payload.data(), payload.size());
                }
                auto rsp = call<rpcproto::ClientApplyRequest,
                                rpcproto::ClientApplyResponse>(
                    leader.handle.get(), rpc_method::kClientApply, req);
                if (rsp.busy()) {
                    busy_retries++;
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(rsp.retry_after_ms() > 0
                                                   ? rsp.retry_after_ms() : 5));
                    continue;
                }
                if (!rsp.err().empty()) {
                    throw std::runtime_error("apply: " + rsp.err());
                }
            }
            applied += static_cast<uint64_t>(this_batch);
            remaining -= this_batch;
        }

        int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            clock_type::now() - t0).count();
        std::printf("applied %llu commands (incl. 1 leader probe), "
                    "%.2f ms total, %.1f us/command, busy_retries=%llu\n",
                    static_cast<unsigned long long>(applied),
                    static_cast<double>(ns) / 1e6,
                    static_cast<double>(ns) / 1000.0 / (n > 0 ? n : 1),
                    static_cast<unsigned long long>(busy_retries));
        std::printf("TOTAL_APPLIED=%llu\n",
                    static_cast<unsigned long long>(applied));
        return 0;

    } catch (const std::exception &e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
