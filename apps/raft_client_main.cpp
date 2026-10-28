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
 *   raft_client -addrs ... -op kvs-lat  -reps 1000            (노드는 -statemachine kvs)
 *   raft_client -addrs ... -op kvs-loop -put-pct 50 -clients 4 -duration-s 10
 *
 * kvs op 들은 DARE 의 벤치마크 방식을 따른다 (kvs/README.md):
 *   kvs-lat  = DARE rtrace: 값 크기를 2배씩 키우며 PUT 을 -reps 번, GET 을
 *              -reps 번. 한 번에 하나만 보낸다 (closed loop, 1 outstanding).
 *   kvs-loop = DARE loop: -put-pct 확률로 PUT, 나머지는 GET. 클라이언트마다
 *              closed loop. -interval-ms 마다 처리량을 기록한다.
 * PUT 은 ClientApply(로그), GET 은 ClientRead(ReadIndex, 로그에 안 남음)로
 * 간다. 리더 탐색도 빈 ClientRead 로 해서 로그에 아무것도 남기지 않는다.
 * ============================================================ */
#include "raft_rpc_conn.h"
#include "raft_rdma_transport.h"   /* rdma_devices_available */
#include "raft_cli.h"
/* raft_proto_conv.h는 include하지 않는다: 이 바이너리는 변환 함수를 하나도
 * 쓰지 않으면서 core/raft_entry.h와 storage/raft_blockcopy_server.h(pread/pwrite
 * 버퍼 풀까지) 전체를 끌어오고 있었다. 필요한 것은 rpcproto.pb.h뿐이다. */
#include "rpcproto.pb.h"
#include "raft_rpc_methods.h"
#include "raft_stats.h"
#include "raft_kvs_wire.h"   /* 헤더 온리 -- raft_client 는 kvs 를 링크하지 않는다 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace nvmeof_raft;
using namespace nvmeof_raft::cli;
using namespace nvmeof_raft::stats;   /* summarize / print_stats_brief (raft_stats.h) */
using clock_type = std::chrono::steady_clock;

/* 하나의 노드로의 RPC 왕복. 실패 시 예외. */
template <typename ReqProtoT, typename RspProtoT>
RspProtoT call(RpcConn *h, const std::string &method, const ReqProtoT &req) {
    std::vector<uint8_t> body(static_cast<size_t>(req.ByteSizeLong()));
    req.SerializeToArray(body.data(), static_cast<int>(body.size()));

    std::vector<uint8_t> rsp_body = h->invoke(method, body);
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
      "  -op apply-timed   same, plus per-RPC ApplyTimings and a mean/p50/p99\n"
      "                    summary per term at the end\n"
      "  -op echo          codec/network round-trip only (no log write)\n"
      "  -op commit-index  print each node's commitIndex\n"
      "  -op hash          print each node's state machine hash/count\n"
      "  -op ae-stats      print leader AE batch counters\n"
      "  -op kvs-lat       KVS latency sweep (DARE rtrace): for each value size\n"
      "                    -min-size..-max-size (doubling), PUT x -reps then\n"
      "                    GET x -reps, one request in flight\n"
      "  -op kvs-loop      KVS mixed closed loop (DARE loop): -clients threads,\n"
      "                    PUT with probability -put-pct else GET, for\n"
      "                    -duration-s; ops per -interval-ms\n"
      "  (kvs ops need the nodes started with -statemachine kvs. PUT goes\n"
      "   through the log, GET through ReadIndex and never touches the log.)\n"
      "\n"
      "  -n N        total commands (default 10)\n"
      "  -size N     bytes per command (default 512)\n"
      "  -batch N    commands per Apply RPC (default 1)\n"
      "  -at-count N for -op hash: wait until count reaches N\n"
      "  -timeout-s  leader discovery timeout (default 15)\n"
      "  -transport  rdma (default) | tcp\n"
      "\n"
      "  kvs options:\n"
      "  -reps N        kvs-lat: samples per op and size (default 1000)\n"
      "  -warmup N      kvs-lat: unrecorded requests per op and size (default 10)\n"
      "  -min-size N    kvs-lat: smallest value (default 8)\n"
      "  -max-size N    kvs-lat: largest value (default %zu = 128KiB command)\n"
      "  -size N        kvs-loop: value size (default 512)\n"
      "  -put-pct P     kvs-loop: percent of PUTs, 0..100 (default 50)\n"
      "  -keys K        kvs-loop: key space, preloaded before the run (default 1000)\n"
      "  -clients C     kvs-loop: concurrent closed-loop clients (default 1)\n"
      "  -duration-s S  kvs-loop: run time (default 10)\n"
      "  -interval-ms M kvs-loop: throughput sampling period (default 10)\n"
      "  -seed S        kvs-loop: RNG seed (default 1)\n"
      "  -csv FILE      kvs-lat: raw samples; kvs-loop: per-interval series\n",
      prog, kvs::kMaxValueBytes);
}

/* 리더 찾기: 각 주소에 1개 명령짜리 빈 Apply(commands 없음)를 보내는
 * 대신, ClientApply를 빈 목록으로 보내면 성공하므로 판별이 안 된다.
 * 그래서 1바이트 no-op 명령을 보내 "not leader"가 아닌 노드를 찾는다.
 * 이 명령도 로그에 들어가므로, 이후 카운트에 포함해서 계산한다. */
struct Leader {
    size_t index = 0;
    std::shared_ptr<RpcConn> handle;
    uint64_t probe_commands = 0;   /* 리더 탐색 중 실제로 apply된 명령 수 */
};

Leader find_leader(const std::vector<std::string> &addrs, int timeout_s,
                    const RpcDial &dial) {
    auto deadline = clock_type::now() + std::chrono::seconds(timeout_s);
    uint64_t probes = 0;

    while (clock_type::now() < deadline) {
        for (size_t i = 0; i < addrs.size(); i++) {
            std::shared_ptr<RpcConn> h;
            try {
                h = dial(addrs[i]);
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

/* ============================================================
 * KVS 벤치마크 (-op kvs-lat / kvs-loop)
 * ============================================================ */

/* 리더 찾기 (kvs op 용): 빈 ClientRead 는 리더만 성공하고 로그에 아무것도
 * 남기지 않는다. find_leader 의 1바이트 probe 와 달리 상태머신 카운트를
 * 건드리지 않는다. */
Leader find_leader_read(const std::vector<std::string> &addrs, int timeout_s,
                        const RpcDial &dial) {
    auto deadline = clock_type::now() + std::chrono::seconds(timeout_s);
    while (clock_type::now() < deadline) {
        for (size_t i = 0; i < addrs.size(); i++) {
            std::shared_ptr<RpcConn> h;
            try {
                h = dial(addrs[i]);
            } catch (const std::exception &) {
                continue;
            }
            try {
                rpcproto::ClientReadRequest req;
                auto rsp = call<rpcproto::ClientReadRequest, rpcproto::ClientReadResponse>(
                    h.get(), rpc_method::kClientRead, req);
                if (rsp.err().empty()) {
                    return Leader{i, h, 0};
                }
            } catch (const std::exception &) {
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    throw std::runtime_error("no leader found within timeout");
}

/* 요청 하나의 측정값 (ns). server 쪽 분해는 GET 에만 있다. */
struct KvsSample {
    int64_t rtt_ns = 0;
    int64_t server_ns = 0;
    int64_t confirm_ns = 0;
    int64_t apply_wait_ns = 0;
    int64_t sm_ns = 0;
};

void sleep_retry(int retry_after_ms) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(retry_after_ms > 0 ? retry_after_ms : 5));
}

/* PUT: ClientApply 로 명령 하나. busy 면 재시도한다 (재시도 시간도 rtt 에
 * 포함된다 -- 클라이언트가 실제로 기다린 시간이므로). */
KvsSample kvs_put(RpcConn *h, const std::vector<uint8_t> &cmd, uint64_t *busy_retries) {
    rpcproto::ClientApplyRequest req;
    req.add_commands(reinterpret_cast<const char *>(cmd.data()), cmd.size());
    KvsSample s;
    auto t0 = clock_type::now();
    for (;;) {
        auto rsp = call<rpcproto::ClientApplyRequest, rpcproto::ClientApplyResponse>(
            h, rpc_method::kClientApply, req);
        if (rsp.busy()) {
            (*busy_retries)++;
            sleep_retry(rsp.retry_after_ms());
            continue;
        }
        if (!rsp.err().empty()) {
            throw std::runtime_error("kvs PUT: " + rsp.err());
        }
        break;
    }
    s.rtt_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        clock_type::now() - t0).count();
    return s;
}

/* GET: ClientRead 로 질의 하나. *value 에 결과를 담는다. */
KvsSample kvs_get(RpcConn *h, const std::vector<uint8_t> &query, std::string *value,
                  uint64_t *busy_retries) {
    rpcproto::ClientReadRequest req;
    req.add_queries(reinterpret_cast<const char *>(query.data()), query.size());
    KvsSample s;
    auto t0 = clock_type::now();
    for (;;) {
        auto rsp = call<rpcproto::ClientReadRequest, rpcproto::ClientReadResponse>(
            h, rpc_method::kClientRead, req);
        if (rsp.busy()) {
            (*busy_retries)++;
            sleep_retry(rsp.retry_after_ms());
            continue;
        }
        if (!rsp.err().empty()) {
            throw std::runtime_error("kvs GET: " + rsp.err());
        }
        if (rsp.results_size() != 1) {
            throw std::runtime_error("kvs GET: expected 1 result");
        }
        s.server_ns = rsp.latency_nanos();
        s.confirm_ns = rsp.confirm_nanos();
        s.apply_wait_ns = rsp.apply_wait_nanos();
        s.sm_ns = rsp.sm_nanos();
        *value = rsp.results(0);
        break;
    }
    s.rtt_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        clock_type::now() - t0).count();
    return s;
}

struct KvsOptions {
    int reps = 1000;
    int warmup = 10;
    size_t min_size = 8;
    size_t max_size = kvs::kMaxValueBytes;
    size_t size = 512;
    int put_pct = 50;
    int keys = 1000;
    int clients = 1;
    int duration_s = 10;
    int interval_ms = 10;
    unsigned seed = 1;
    std::string csv;
};

/* 값 크기 목록: min, 2*min, ... 를 max 이하로, 마지막은 max 로 맞춘다 */
std::vector<size_t> kvs_sizes(size_t min_size, size_t max_size) {
    std::vector<size_t> sizes;
    for (size_t sz = min_size; sz < max_size; sz *= 2) {
        sizes.push_back(sz);
    }
    sizes.push_back(max_size);
    return sizes;
}

int run_kvs_lat(RpcConn *h, const KvsOptions &o) {
    FILE *csv = nullptr;
    if (!o.csv.empty()) {
        csv = std::fopen(o.csv.c_str(), "w");
        if (csv == nullptr) {
            throw std::runtime_error("cannot open -csv " + o.csv);
        }
        std::fprintf(csv, "op,size,rep,rtt_ns,server_ns,confirm_ns,apply_wait_ns,sm_ns\n");
    }

    uint64_t busy = 0;
    uint64_t mismatches = 0;
    std::printf("kvs-lat: reps=%d warmup=%d (client round trip, microseconds)\n",
                o.reps, o.warmup);
    for (size_t sz : kvs_sizes(o.min_size, o.max_size)) {
        const std::string key = "key" + std::to_string(sz);
        const std::string value(sz, 'x');
        const auto put_cmd = kvs::encode_put(key, value);
        const auto get_cmd = kvs::encode_get(key);

        std::vector<KvsSample> puts, gets;
        puts.reserve(static_cast<size_t>(o.reps));
        gets.reserve(static_cast<size_t>(o.reps));
        for (int i = 0; i < o.warmup + o.reps; i++) {
            KvsSample s = kvs_put(h, put_cmd, &busy);
            if (i >= o.warmup) {
                puts.push_back(s);
            }
        }
        std::string got;
        for (int i = 0; i < o.warmup + o.reps; i++) {
            KvsSample s = kvs_get(h, get_cmd, &got, &busy);
            if (got != value) {
                mismatches++;
            }
            if (i >= o.warmup) {
                gets.push_back(s);
            }
        }

        auto col = [](const std::vector<KvsSample> &v, int64_t KvsSample::*f) {
            std::vector<int64_t> out;
            out.reserve(v.size());
            for (const auto &s : v) {
                out.push_back(s.*f);
            }
            return out;
        };
        std::printf("size=%zuB (command %zuB)\n", sz, sz + kvs::kHeaderBytes);
        print_stats("PUT", summarize(col(puts, &KvsSample::rtt_ns)));
        print_stats("GET", summarize(col(gets, &KvsSample::rtt_ns)));
        std::printf("  GET server breakdown:\n");
        print_stats_brief("server", summarize(col(gets, &KvsSample::server_ns)));
        print_stats_brief("confirm", summarize(col(gets, &KvsSample::confirm_ns)));
        print_stats_brief("apply_wait", summarize(col(gets, &KvsSample::apply_wait_ns)));
        print_stats_brief("sm", summarize(col(gets, &KvsSample::sm_ns)));

        if (csv != nullptr) {
            for (size_t i = 0; i < puts.size(); i++) {
                std::fprintf(csv, "PUT,%zu,%zu,%lld,0,0,0,0\n", sz, i,
                             static_cast<long long>(puts[i].rtt_ns));
            }
            for (size_t i = 0; i < gets.size(); i++) {
                const KvsSample &g = gets[i];
                std::fprintf(csv, "GET,%zu,%zu,%lld,%lld,%lld,%lld,%lld\n", sz, i,
                             static_cast<long long>(g.rtt_ns),
                             static_cast<long long>(g.server_ns),
                             static_cast<long long>(g.confirm_ns),
                             static_cast<long long>(g.apply_wait_ns),
                             static_cast<long long>(g.sm_ns));
            }
        }
    }
    if (csv != nullptr) {
        std::fclose(csv);
    }
    std::printf("busy_retries=%llu get_mismatches=%llu\n",
                static_cast<unsigned long long>(busy),
                static_cast<unsigned long long>(mismatches));
    return mismatches == 0 ? 0 : 2;
}

int run_kvs_loop(const std::string &leader_addr, RpcConn *leader, const RpcDial &dial,
                 const KvsOptions &o) {
    const std::string value(o.size, 'x');
    auto key_of = [](int k) { return "key" + std::to_string(k); };

    /* 미리 채운다 -- GET 이 항상 값을 찾도록 (DARE loop 도 PUT 한 키를 GET 한다) */
    uint64_t busy = 0;
    for (int k = 0; k < o.keys; k++) {
        kvs_put(leader, kvs::encode_put(key_of(k), value), &busy);
    }
    std::printf("kvs-loop: preloaded %d keys of %zuB; clients=%d put-pct=%d duration=%ds\n",
                o.keys, o.size, o.clients, o.put_pct, o.duration_s);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> n_puts{0}, n_gets{0}, n_mismatch{0}, n_busy{0};
    std::mutex agg_mu;
    std::vector<int64_t> put_lat, get_lat, get_confirm;
    std::string first_error;

    std::vector<std::thread> threads;
    for (int c = 0; c < o.clients; c++) {
        threads.emplace_back([&, c]() {
            std::vector<int64_t> my_put, my_get, my_confirm;
            try {
                /* 클라이언트마다 커넥션 하나 -- 서버는 커넥션마다 스레드를 띄운다 */
                std::shared_ptr<RpcConn> h = dial(leader_addr);
                std::mt19937 rng(o.seed + static_cast<unsigned>(c));
                std::uniform_int_distribution<int> pct(1, 100);
                std::uniform_int_distribution<int> key(0, o.keys - 1);
                uint64_t my_busy = 0;
                std::string got;
                while (!stop.load(std::memory_order_relaxed)) {
                    const std::string k = key_of(key(rng));
                    if (pct(rng) <= o.put_pct) {
                        my_put.push_back(kvs_put(h.get(), kvs::encode_put(k, value), &my_busy).rtt_ns);
                        n_puts.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        KvsSample s = kvs_get(h.get(), kvs::encode_get(k), &got, &my_busy);
                        if (got != value) {
                            n_mismatch.fetch_add(1, std::memory_order_relaxed);
                        }
                        my_get.push_back(s.rtt_ns);
                        my_confirm.push_back(s.confirm_ns);
                        n_gets.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                n_busy.fetch_add(my_busy);
            } catch (const std::exception &e) {
                std::lock_guard<std::mutex> lk(agg_mu);
                if (first_error.empty()) {
                    first_error = e.what();
                }
                stop.store(true);
            }
            std::lock_guard<std::mutex> lk(agg_mu);
            put_lat.insert(put_lat.end(), my_put.begin(), my_put.end());
            get_lat.insert(get_lat.end(), my_get.begin(), my_get.end());
            get_confirm.insert(get_confirm.end(), my_confirm.begin(), my_confirm.end());
        });
    }

    /* 처리량 시계열: DARE 의 "<t_ms> <requests in last period>" 와 같은 뜻 */
    FILE *series = stdout;
    if (!o.csv.empty()) {
        series = std::fopen(o.csv.c_str(), "w");
        if (series == nullptr) {
            stop.store(true);
            for (auto &t : threads) t.join();
            throw std::runtime_error("cannot open -csv " + o.csv);
        }
    }
    std::fprintf(series, "t_ms,ops,puts,gets\n");
    const auto t0 = clock_type::now();
    const auto end = t0 + std::chrono::seconds(o.duration_s);
    uint64_t last_p = 0, last_g = 0;
    auto next = t0;
    while (!stop.load() && clock_type::now() < end) {
        next += std::chrono::milliseconds(o.interval_ms > 0 ? o.interval_ms : 10);
        std::this_thread::sleep_until(next);
        const uint64_t p = n_puts.load(), g = n_gets.load();
        const long long t_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            clock_type::now() - t0).count();
        std::fprintf(series, "%lld,%llu,%llu,%llu\n", t_ms,
                     static_cast<unsigned long long>((p - last_p) + (g - last_g)),
                     static_cast<unsigned long long>(p - last_p),
                     static_cast<unsigned long long>(g - last_g));
        last_p = p;
        last_g = g;
    }
    stop.store(true);
    for (auto &t : threads) {
        t.join();
    }
    const double secs = std::chrono::duration<double>(clock_type::now() - t0).count();
    if (series != stdout) {
        std::fclose(series);
    }
    if (!first_error.empty()) {
        throw std::runtime_error(first_error);
    }

    const uint64_t p = n_puts.load(), g = n_gets.load();
    std::printf("kvs-loop: %llu ops in %.2fs = %.0f ops/s (PUT %llu, GET %llu), "
                "busy_retries=%llu get_mismatches=%llu\n",
                static_cast<unsigned long long>(p + g), secs,
                static_cast<double>(p + g) / secs,
                static_cast<unsigned long long>(p), static_cast<unsigned long long>(g),
                static_cast<unsigned long long>(n_busy.load() + busy),
                static_cast<unsigned long long>(n_mismatch.load()));
    std::printf("latency (client round trip, microseconds):\n");
    print_stats("PUT", summarize(put_lat));
    print_stats("GET", summarize(get_lat));
    print_stats("GET.conf", summarize(get_confirm));
    return n_mismatch.load() == 0 ? 0 : 2;
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
    std::string transport = "rdma";
    KvsOptions kvs_opt;
    bool size_given = false;

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
            size_given = true;
        } else if (arg == "-batch") {
            batch = std::atoi(next_arg_value(argc, argv, i, "-batch").c_str());
        } else if (arg == "-at-count") {
            at_count = std::strtoull(next_arg_value(argc, argv, i, "-at-count").c_str(), nullptr, 10);
        } else if (arg == "-timeout-s") {
            timeout_s = std::atoi(next_arg_value(argc, argv, i, "-timeout-s").c_str());
        } else if (arg == "-transport") {
            transport = next_arg_value(argc, argv, i, "-transport");
        } else if (arg == "-reps") {
            kvs_opt.reps = std::atoi(next_arg_value(argc, argv, i, "-reps").c_str());
        } else if (arg == "-warmup") {
            kvs_opt.warmup = std::atoi(next_arg_value(argc, argv, i, "-warmup").c_str());
        } else if (arg == "-min-size") {
            kvs_opt.min_size = std::strtoull(next_arg_value(argc, argv, i, "-min-size").c_str(), nullptr, 10);
        } else if (arg == "-max-size") {
            kvs_opt.max_size = std::strtoull(next_arg_value(argc, argv, i, "-max-size").c_str(), nullptr, 10);
        } else if (arg == "-put-pct") {
            kvs_opt.put_pct = std::atoi(next_arg_value(argc, argv, i, "-put-pct").c_str());
        } else if (arg == "-keys") {
            kvs_opt.keys = std::atoi(next_arg_value(argc, argv, i, "-keys").c_str());
        } else if (arg == "-clients") {
            kvs_opt.clients = std::atoi(next_arg_value(argc, argv, i, "-clients").c_str());
        } else if (arg == "-duration-s") {
            kvs_opt.duration_s = std::atoi(next_arg_value(argc, argv, i, "-duration-s").c_str());
        } else if (arg == "-interval-ms") {
            kvs_opt.interval_ms = std::atoi(next_arg_value(argc, argv, i, "-interval-ms").c_str());
        } else if (arg == "-seed") {
            kvs_opt.seed = static_cast<unsigned>(std::strtoul(next_arg_value(argc, argv, i, "-seed").c_str(), nullptr, 10));
        } else if (arg == "-csv") {
            kvs_opt.csv = next_arg_value(argc, argv, i, "-csv");
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

    TransportKind transport_kind = TransportKind::Rdma;
    if (!parse_transport_kind(transport, &transport_kind)) {
        std::fprintf(stderr, "-transport must be 'rdma' or 'tcp'\n");
        return 1;
    }
    if (transport_kind == TransportKind::Rdma && !rdma_devices_available()) {
        std::fprintf(stderr, "-transport rdma: no RDMA device found "
                             "(use -transport tcp)\n");
        return 1;
    }
    RpcDial dial = dialer_for(transport_kind);

    try {
        /* ---- 노드별 조회 op은 리더 탐색 없이 전부에 물어본다 ---- */
        if (op == "commit-index" || op == "hash" || op == "ae-stats") {
            for (const auto &addr : addrs) {
                std::shared_ptr<RpcConn> h;
                try {
                    h = dial(addr);
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
            std::shared_ptr<RpcConn> h = dial(addrs[0]);
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

        /* ---- kvs-lat / kvs-loop (노드는 -statemachine kvs) ---- */
        if (op == "kvs-lat" || op == "kvs-loop") {
            if (size_given) {
                kvs_opt.size = static_cast<size_t>(size);
            }
            if (kvs_opt.min_size < 1 || kvs_opt.max_size > kvs::kMaxValueBytes ||
                kvs_opt.min_size > kvs_opt.max_size || kvs_opt.size < 1 ||
                kvs_opt.size > kvs::kMaxValueBytes) {
                std::fprintf(stderr, "kvs value sizes must be 1..%zu (min <= max)\n",
                             kvs::kMaxValueBytes);
                return 1;
            }
            if (kvs_opt.put_pct < 0 || kvs_opt.put_pct > 100 || kvs_opt.keys < 1 ||
                kvs_opt.clients < 1 || kvs_opt.reps < 1 || kvs_opt.warmup < 0) {
                std::fprintf(stderr, "invalid kvs options (see -h)\n");
                return 1;
            }
            Leader leader = find_leader_read(addrs, timeout_s, dial);
            std::printf("leader: %s\n", addrs[leader.index].c_str());
            if (op == "kvs-lat") {
                return run_kvs_lat(leader.handle.get(), kvs_opt);
            }
            return run_kvs_loop(addrs[leader.index], leader.handle.get(), dial, kvs_opt);
        }

        /* ---- apply / apply-timed ---- */
        Leader leader = find_leader(addrs, timeout_s, dial);
        std::printf("leader: %s (probe applied %llu command)\n",
                    addrs[leader.index].c_str(),
                    static_cast<unsigned long long>(leader.probe_commands));

        std::string payload(static_cast<size_t>(size), 'x');
        uint64_t applied = leader.probe_commands;
        uint64_t busy_retries = 0;
        auto t0 = clock_type::now();

        /* -op apply-timed 요약용 표본 (ns 원시값). 순서는 per-RPC 줄과 같고,
         * 마지막 residual 은 항등식 검산이다:
         *   Total ~= LHandler + LPersist + AENet + FHandler + ReplNet
         *            + StorageIO + QuorumWait        (core/include/raft_timings.h)
         * Mutex / CommitWait / SRead / SWrite 는 진단용이라 합에 안 들어간다.
         * SRead / SWrite 는 StorageIO 중 스토리지 서버의 pread / pwrite 합이다
         * -- StorageIO 와의 차이가 서버가 디스크 밖에서 쓴 시간이다.
         * 항들이 전부 timings_clamp0 을 거치므로 0 이 아닐 수 있고, 음수도
         * 나올 수 있다 -- 그 부호 자체가 정보다.
         *
         * busy 재시도는 아래에서 continue 로 빠지므로 표본이 안 쌓인다.
         * 즉 표본 수 = **성공한 RPC 수**이고 n/batch 와 다를 수 있다. */
        static const char *kTimedTerms[] = {
            "Total", "LHandler", "LPersist", "AENet", "FHandler",
            "ReplNet", "StorageIO", "QuorumWait", "Mutex", "CommitWait",
            "SRead", "SWrite", "residual",
        };
        constexpr size_t kNTimedTerms = sizeof(kTimedTerms) / sizeof(kTimedTerms[0]);
        std::vector<int64_t> timed[kNTimedTerms];

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

                /* StorageIO 는 proto 필드명만 replication_nanos 다 (원본
                 * raft.go의 Replication 이름을 유지). C++ 쪽은 서버·클라이언트
                 * 모두 storage_io_ns 이고 라벨도 StorageIO 다 --
                 * net/include/raft_proto_conv.h 주석 참고. */
                const int64_t sum7 =
                    rsp.l_handler_nanos() + rsp.l_persist_nanos() + rsp.ae_net_nanos() +
                    rsp.f_handler_nanos() + rsp.repl_net_nanos() + rsp.replication_nanos() +
                    rsp.quorum_wait_nanos();
                const int64_t vals[kNTimedTerms] = {
                    rsp.total_nanos(),      rsp.l_handler_nanos(), rsp.l_persist_nanos(),
                    rsp.ae_net_nanos(),     rsp.f_handler_nanos(), rsp.repl_net_nanos(),
                    rsp.replication_nanos(), rsp.quorum_wait_nanos(),
                    rsp.mutex_nanos(),      rsp.commit_wait_nanos(),
                    rsp.storage_read_nanos(), rsp.storage_write_nanos(),
                    rsp.total_nanos() - sum7,
                };
                for (size_t k = 0; k < kNTimedTerms; k++) {
                    timed[k].push_back(vals[k]);
                }

                std::printf("  batch=%d total=%.1fus LHandler=%.1f LPersist=%.1f "
                            "AENet=%.1f FHandler=%.1f ReplNet=%.1f StorageIO=%.1f "
                            "QuorumWait=%.1f Mutex=%.1f CommitWait=%.1f "
                            "SRead=%.1f SWrite=%.1f\n",
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
                            rsp.commit_wait_nanos() / 1000.0,
                            rsp.storage_read_nanos() / 1000.0,
                            rsp.storage_write_nanos() / 1000.0);
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

        if (op == "apply-timed" && !timed[0].empty()) {
            std::printf("\napply-timed summary (n=%zu samples, microseconds):\n",
                        timed[0].size());
            for (size_t k = 0; k < kNTimedTerms; k++) {
                print_stats_brief(kTimedTerms[k], summarize(timed[k]));
            }
            std::printf("  (residual = Total - sum of the 7 identity terms)\n\n");
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
