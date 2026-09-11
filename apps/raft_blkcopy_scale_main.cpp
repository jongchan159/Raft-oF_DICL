/* ============================================================
 * raft_blkcopy_scale_main.cpp -- 스토리지 노드의 blockcopy 처리량/포화점 측정
 *
 * `BlockCopyServer.HandleWritePBABatch` 를 직접 호출한다. Raft 를 전혀 거치지
 * 않으므로 선거/quorum/persist 노이즈 없이 pread+pwrite 만 남는다.
 *
 * 재는 것: **처리량**이 워커 수(W = 서버의 -copy-workers)와 배치 크기
 * (B = -batch)에 따라 어떻게 확장되고 어디서 포화하는가.
 *
 * 이 도구는 지연 전용이던 raft_blkcopy_bench 를 **대체**한다 (2026-09-10).
 * 그쪽은 -batch 1 / -copy-workers 1 을 계약으로 고정하고 있어 스케일링
 * 스윕과 양립할 수 없었다. 지연이 필요하면 W=1,B=1 코너를 보면 된다 --
 * wall 의 p50/p99 를 그대로 보고한다.
 *
 * **한 지점(W, B, chunk 고정)만 측정한다.** 스윕은 scripts/blkcopy_scaling.sh
 * 가 돈다 -- W 는 서버 시작 플래그라 지점마다 서버를 다시 띄워야 하고, 그
 * 수명 관리는 스크립트 몫이다.
 *
 * 링크는 raft_client 와 같이 wire + proto 뿐이다 -- tcp_dial_http / rpc_invoke
 * 가 WIRE_SRCS 에 있어서 core 심볼이 필요한 raft_net_obj 를 끌어올 이유가
 * 없다 (계층 규약: CMake 의 isolation_* 테스트가 강제한다).
 *
 * 사용법:
 *   raft_blkcopy_scale -storage 127.0.0.1:5060 -src-dev 0 -dst-dev 1 \
 *       -chunk 65536 -batch 16 -workers 8 -yes-destroy-dst
 *
 * 주의 두 가지:
 *   - **`copy_nanos` 는 워커별 시간의 합이다** (raft_blockcopy_server.cpp:284).
 *     병렬 구간에서 경과시간이 아니므로 처리량 분모로 쓰면 안 된다.
 *     처리량은 언제나 wall 기준으로 낸다.
 *   - **`-workers` 는 기록 전용이다.** 이 도구는 서버의 실제 -copy-workers 를
 *     알 수 없다. 드라이버가 채우는 라벨일 뿐이므로, 서버 기동 로그의
 *     `copy-workers : N` 과 대조해서 쓸 것.
 *
 * dst 장치/파일의 해당 구간을 **덮어쓴다.** -yes-destroy-dst 없이는 거부한다.
 * ============================================================ */
#include "raft_tcp_transport.h"
#include "raft_cli.h"
/* raft_proto_conv.h 는 include 하지 않는다 (raft_client 와 같은 이유):
 * 변환 함수를 하나도 안 쓰면서 core/raft_entry.h 와
 * storage/raft_blockcopy_server.h 전체를 끌어온다. 필요한 건 pb.h 뿐이다. */
#include "rpcproto.pb.h"
#include "raft_rpc_methods.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace nvmeof_raft;
using namespace nvmeof_raft::cli;
using clock_type = std::chrono::steady_clock;

/* 정렬 단위. blockio::kPageSize 와 같은 값이지만 blockio 를 링크하지 않으므로
 * (헤더 하나 때문에 의존을 늘리지 않는다) 여기서 다시 쓴다. O_DIRECT 요구다. */
constexpr uint64_t kAlign = 4096;

/* 측정 1회분. 전부 나노초. */
struct Sample {
    int64_t wall_ns = 0;    /* 클라이언트가 rpc_invoke 앞뒤로 잰 값 -- 처리량의 분모 */
    int64_t copy_ns = 0;    /* 서버 보고: read+write, **워커별 합** */
    int64_t read_ns = 0;    /* 서버 보고: pread 만, 워커별 합 */
    int64_t write_ns = 0;   /* 서버 보고: pwrite 만, 워커별 합 */
};

void usage(const char *prog) {
    std::fprintf(stderr,
      "usage: %s -storage host:port -src-dev N -dst-dev N -chunk BYTES [options]\n"
      "\n"
      "  -storage host:port  blockcopy server (raft_blockcopy_server -addr)\n"
      "  -src-dev N          source index into the server's -devices list\n"
      "  -dst-dev N          destination index (overwritten!)\n"
      "  -chunk BYTES        bytes per chunk, multiple of 4096\n"
      "\n"
      "  -batch N        chunks per WritePBABatch RPC (default 1)\n"
      "  -workers N      the server's -copy-workers. RECORD ONLY: this tool\n"
      "                  cannot verify it. Cross-check the server's startup\n"
      "                  log line 'copy-workers : N' (default 0 = unset)\n"
      "  -target-bytes B move about this much per point, then stop\n"
      "                  (default 2 GiB). iterations are derived from it\n"
      "  -min-iters N    lower clamp on derived iterations (default 30)\n"
      "  -max-iters N    upper clamp (default 2000)\n"
      "  -warmup N       unmeasured iterations first (default max(10, iters/10))\n"
      "  -src-off B      source region start, default 4 GiB\n"
      "  -dst-off B      destination region start, default 4 GiB\n"
      "  -region B       region length on each device, default 32 GiB\n"
      "  -stride B       offset advance per chunk, default = -chunk\n"
      "  -csv FILE       write per-iteration raw samples here\n"
      "  -label NAME     tag written into the CSV and the summary line\n"
      "  -arm NAME       arm tag (e.g. A-local, B-rdma) for the CSV\n"
      "  -yes-destroy-dst   required: acknowledges the destination is overwritten\n"
      "\n"
      "Throughput is always computed from wall time. copy_nanos is a SUM across\n"
      "the server's workers, so it is not an elapsed time once -batch > 1 or the\n"
      "server runs -copy-workers > 1; it is reported only as a diagnostic.\n",
      prog);
}

/* 하나의 스토리지 노드로의 RPC 왕복. 실패 시 예외. (raft_client 의 call 과
 * 같은 형태지만 그 파일을 include 할 수 없어 -- 둘 다 main 이다 -- 재정의한다) */
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

uint64_t parse_bytes(const std::string &s, const char *flag) {
    /* 10진수만 받는다. KiB/MiB 접미사는 지원하지 않는다 -- 스크립트가
     * $((4*1024*1024)) 로 넘기면 되고, 파서를 늘리면 오해 여지만 는다. */
    errno = 0;
    char *end = nullptr;
    unsigned long long v = std::strtoull(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') {
        std::fprintf(stderr, "%s: not a number: %s\n", flag, s.c_str());
        std::exit(1);
    }
    return static_cast<uint64_t>(v);
}

/* 백분위수. 정렬된 v 에 대해 nearest-rank. */
int64_t pct(const std::vector<int64_t> &sorted, double p) {
    if (sorted.empty()) {
        return 0;
    }
    size_t rank = static_cast<size_t>(std::ceil(p / 100.0 * static_cast<double>(sorted.size())));
    if (rank == 0) {
        rank = 1;
    }
    if (rank > sorted.size()) {
        rank = sorted.size();
    }
    return sorted[rank - 1];
}

struct Stats {
    size_t n = 0;
    double mean = 0, stddev = 0;
    int64_t min = 0, p50 = 0, p90 = 0, p99 = 0, p999 = 0, max = 0;
};

Stats summarize(std::vector<int64_t> v) {
    Stats s;
    if (v.empty()) {
        return s;
    }
    std::sort(v.begin(), v.end());
    s.n = v.size();
    double sum = 0;
    for (int64_t x : v) {
        sum += static_cast<double>(x);
    }
    s.mean = sum / static_cast<double>(v.size());
    double acc = 0;
    for (int64_t x : v) {
        double d = static_cast<double>(x) - s.mean;
        acc += d * d;
    }
    s.stddev = std::sqrt(acc / static_cast<double>(v.size()));
    s.min = v.front();
    s.max = v.back();
    s.p50 = pct(v, 50);
    s.p90 = pct(v, 90);
    s.p99 = pct(v, 99);
    s.p999 = pct(v, 99.9);
    return s;
}

void print_stats(const char *name, const Stats &s) {
    std::printf("  %-8s n=%-6zu mean=%10.1f sd=%10.1f  p50=%9lld p90=%9lld "
                "p99=%9lld p99.9=%9lld  min=%9lld max=%9lld\n",
                name, s.n, s.mean / 1000.0, s.stddev / 1000.0,
                static_cast<long long>(s.p50 / 1000), static_cast<long long>(s.p90 / 1000),
                static_cast<long long>(s.p99 / 1000), static_cast<long long>(s.p999 / 1000),
                static_cast<long long>(s.min / 1000), static_cast<long long>(s.max / 1000));
}

/* bytes/ns -> MiB/s. ns 가 0이면 0을 돌려준다 (분모 보호). */
double mib_per_s(uint64_t bytes, int64_t ns) {
    if (ns <= 0) {
        return 0.0;
    }
    return static_cast<double>(bytes) * 1e9 /
           (static_cast<double>(ns) * 1024.0 * 1024.0);
}

} /* namespace */

int main(int argc, char **argv) {
    std::string storage_addr, csv_path, label = "unnamed", arm = "unnamed";
    int src_dev = -1, dst_dev = -1;
    uint64_t chunk = 0;
    uint64_t src_off = 4ull << 30, dst_off = 4ull << 30;
    uint64_t region = 32ull << 30;
    uint64_t stride = 0;                      /* 0 = chunk 와 같게 */
    uint64_t target_bytes = 2ull << 30;
    int batch = 1, workers = 0;
    int min_iters = 30, max_iters = 2000;
    int warmup = -1;                          /* -1 = 자동 */
    bool confirmed = false;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "-storage") {
            storage_addr = next_arg_value(argc, argv, i, "-storage");
        } else if (a == "-src-dev") {
            src_dev = std::atoi(next_arg_value(argc, argv, i, "-src-dev").c_str());
        } else if (a == "-dst-dev") {
            dst_dev = std::atoi(next_arg_value(argc, argv, i, "-dst-dev").c_str());
        } else if (a == "-chunk") {
            chunk = parse_bytes(next_arg_value(argc, argv, i, "-chunk"), "-chunk");
        } else if (a == "-batch") {
            batch = std::atoi(next_arg_value(argc, argv, i, "-batch").c_str());
        } else if (a == "-workers") {
            workers = std::atoi(next_arg_value(argc, argv, i, "-workers").c_str());
        } else if (a == "-target-bytes") {
            target_bytes = parse_bytes(next_arg_value(argc, argv, i, "-target-bytes"),
                                        "-target-bytes");
        } else if (a == "-min-iters") {
            min_iters = std::atoi(next_arg_value(argc, argv, i, "-min-iters").c_str());
        } else if (a == "-max-iters") {
            max_iters = std::atoi(next_arg_value(argc, argv, i, "-max-iters").c_str());
        } else if (a == "-warmup") {
            warmup = std::atoi(next_arg_value(argc, argv, i, "-warmup").c_str());
        } else if (a == "-src-off") {
            src_off = parse_bytes(next_arg_value(argc, argv, i, "-src-off"), "-src-off");
        } else if (a == "-dst-off") {
            dst_off = parse_bytes(next_arg_value(argc, argv, i, "-dst-off"), "-dst-off");
        } else if (a == "-region") {
            region = parse_bytes(next_arg_value(argc, argv, i, "-region"), "-region");
        } else if (a == "-stride") {
            stride = parse_bytes(next_arg_value(argc, argv, i, "-stride"), "-stride");
        } else if (a == "-csv") {
            csv_path = next_arg_value(argc, argv, i, "-csv");
        } else if (a == "-label") {
            label = next_arg_value(argc, argv, i, "-label");
        } else if (a == "-arm") {
            arm = next_arg_value(argc, argv, i, "-arm");
        } else if (a == "-yes-destroy-dst") {
            confirmed = true;
        } else if (a == "-h" || a == "--help") {
            usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "unknown flag: %s\n", a.c_str());
            usage(argv[0]);
            return 1;
        }
    }

    if (storage_addr.empty() || src_dev < 0 || dst_dev < 0 || chunk == 0) {
        usage(argv[0]);
        return 1;
    }
    if (chunk % kAlign != 0 || src_off % kAlign != 0 || dst_off % kAlign != 0) {
        std::fprintf(stderr, "-chunk/-src-off/-dst-off must be multiples of %llu "
                             "(O_DIRECT alignment)\n", static_cast<unsigned long long>(kAlign));
        return 1;
    }
    if (batch < 1) {
        std::fprintf(stderr, "-batch must be >= 1\n");
        return 1;
    }
    if (min_iters < 1 || max_iters < min_iters) {
        std::fprintf(stderr, "-min-iters must be >= 1 and <= -max-iters\n");
        return 1;
    }
    if (stride == 0) {
        stride = chunk;
    }
    if (stride % kAlign != 0) {
        std::fprintf(stderr, "-stride must be a multiple of %llu\n",
                     static_cast<unsigned long long>(kAlign));
        return 1;
    }
    if (chunk > region) {
        std::fprintf(stderr, "-chunk (%llu) exceeds -region (%llu)\n",
                     static_cast<unsigned long long>(chunk),
                     static_cast<unsigned long long>(region));
        return 1;
    }

    /* 반복 횟수를 전송량으로 맞춘다. 고정 iters 는 축을 넘나들 때 안 맞는다 --
     * B=64 x 1Mi 는 RPC 하나가 64MiB 라 1000회면 64GiB 를 옮기게 된다. */
    const uint64_t bytes_per_rpc = static_cast<uint64_t>(batch) * chunk;
    uint64_t derived = target_bytes / bytes_per_rpc;
    if (derived < static_cast<uint64_t>(min_iters)) {
        derived = static_cast<uint64_t>(min_iters);
    }
    if (derived > static_cast<uint64_t>(max_iters)) {
        derived = static_cast<uint64_t>(max_iters);
    }
    const int iters = static_cast<int>(derived);
    if (warmup < 0) {
        warmup = iters / 10;
        if (warmup < 10) {
            warmup = 10;
        }
    }

    /* 파괴적 작업이라 명시적 동의를 요구한다. 어디를 덮어쓰는지 먼저 보여준다. */
    if (!confirmed) {
        std::fprintf(stderr,
            "REFUSING: this overwrites the destination.\n"
            "  storage    : %s\n"
            "  dst-dev    : %d (index into that server's -devices list)\n"
            "  dst region : [%llu, %llu) = %llu MiB\n"
            "Re-run with -yes-destroy-dst once you have confirmed dst-dev is a "
            "scratch namespace or file.\n",
            storage_addr.c_str(), dst_dev,
            static_cast<unsigned long long>(dst_off),
            static_cast<unsigned long long>(dst_off + region),
            static_cast<unsigned long long>(region >> 20));
        return 1;
    }

    std::shared_ptr<RpcClientHandle> h;
    try {
        h.reset(tcp_dial_http(storage_addr));
    } catch (const std::exception &e) {
        std::fprintf(stderr, "connect %s failed: %s\n", storage_addr.c_str(), e.what());
        return 1;
    }

    /* 반복마다 stride 만큼 전진해 매번 새 LBA 를 건드린다. 같은 LBA 를 계속
     * 재기록하면 SSD/FTL 이 그 패턴에 맞춰 움직여 수치가 편향된다.
     * region 끝을 넘으면 처음으로 되돌아온다. */
    uint64_t span = region - chunk + stride;   /* 마지막 청크가 region 안에 들어오는 시작점 범위 */
    if (span < stride) {
        span = stride;
    }
    uint64_t cursor = 0;

    const int total = warmup + iters;
    std::vector<Sample> samples;
    samples.reserve(static_cast<size_t>(iters));

    std::printf("blkcopy-scale arm=%s label=%s storage=%s src_dev=%d dst_dev=%d\n"
                "  W=%d(reported) B=%d chunk=%llu  bytes/rpc=%llu\n"
                "  stride=%llu region=%llu src_off=%llu dst_off=%llu\n"
                "  target=%lluMiB -> warmup=%d n=%d\n",
                arm.c_str(), label.c_str(), storage_addr.c_str(), src_dev, dst_dev,
                workers, batch,
                static_cast<unsigned long long>(chunk),
                static_cast<unsigned long long>(bytes_per_rpc),
                static_cast<unsigned long long>(stride),
                static_cast<unsigned long long>(region),
                static_cast<unsigned long long>(src_off),
                static_cast<unsigned long long>(dst_off),
                static_cast<unsigned long long>(target_bytes >> 20), warmup, iters);
    std::fflush(stdout);

    for (int it = 0; it < total; it++) {
        rpcproto::WritePBABatchRequest req;
        req.set_src_dev(src_dev);
        req.set_dst_dev(dst_dev);
        for (int b = 0; b < batch; b++) {
            uint64_t rel = cursor % span;
            if (rel + chunk > region) {
                rel = 0;
                cursor = 0;
            }
            req.add_pba_srcs(src_off + rel);
            req.add_pba_dsts(dst_off + rel);
            req.add_nbytes(chunk);
            cursor += stride;
        }

        auto t0 = clock_type::now();
        rpcproto::WritePBABatchResponse rsp;
        try {
            rsp = call<rpcproto::WritePBABatchRequest,
                       rpcproto::WritePBABatchResponse>(
                h.get(), rpc_method::kWritePBABatch, req);
        } catch (const std::exception &e) {
            std::fprintf(stderr, "iteration %d: RPC failed: %s\n", it, e.what());
            return 1;
        }
        int64_t wall = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           clock_type::now() - t0).count();

        /* err 는 치명적으로 다룬다. DECISIONS.md U6 대로 스토리지 커넥션이
         * 한 번 죽으면 이후 전부 실패하는데, 조용히 0을 기록하면 그 구간이
         * "아주 빠른 복사"로 통계에 섞인다. */
        if (!rsp.err().empty()) {
            std::fprintf(stderr, "iteration %d: server error: %s\n", it, rsp.err().c_str());
            return 1;
        }

        if (it >= warmup) {
            samples.push_back(Sample{wall, rsp.copy_nanos(), rsp.read_nanos(), rsp.write_nanos()});
        }
    }

    if (samples.empty()) {
        std::fprintf(stderr, "no samples collected\n");
        return 1;
    }

    std::vector<int64_t> wall_v, copy_v, read_v, write_v;
    wall_v.reserve(samples.size());
    copy_v.reserve(samples.size());
    read_v.reserve(samples.size());
    write_v.reserve(samples.size());
    int64_t wall_sum = 0, copy_sum = 0;
    for (const Sample &s : samples) {
        wall_v.push_back(s.wall_ns);
        copy_v.push_back(s.copy_ns);
        read_v.push_back(s.read_ns);
        write_v.push_back(s.write_ns);
        wall_sum += s.wall_ns;
        copy_sum += s.copy_ns;
    }

    const Stats wall_s = summarize(wall_v);
    const uint64_t total_bytes = bytes_per_rpc * static_cast<uint64_t>(samples.size());

    /* 집계 처리량은 총바이트/총시간이다. 반복별 처리량의 산술평균이 아니다 --
     * 비율의 평균은 느린 반복을 과소평가한다. */
    const double thr_aggregate = mib_per_s(total_bytes, wall_sum);
    const double thr_p50 = mib_per_s(bytes_per_rpc, wall_s.p50);
    const double thr_p99 = mib_per_s(bytes_per_rpc, wall_s.p99);

    /* 실효 워커 수: copy_nanos 가 워커별 합이므로 wall 로 나누면 "평균 몇 개가
     * 동시에 바빴는가" 가 된다.
     *
     * min(W,B) 는 **상한**이지 목표치가 아니다. wall 에는 RPC 코덱/전송/스케줄링이
     * 함께 들어 있어서, 복사가 짧으면 W=1 에서도 1 보다 작게 나온다(정상).
     * 판단은 상대적으로 한다 -- B 를 키우며 이 값이 min(W,B) 를 따라 올라가면
     * 병렬화가 먹는 것이고, min(W,B) 를 올려도 제자리면 안 먹는 것이다
     * (파일 기반이면 ext4 직렬화를 먼저 의심할 것). */
    const double eff_workers = (wall_sum > 0)
        ? static_cast<double>(copy_sum) / static_cast<double>(wall_sum) : 0.0;
    const int expected_par = (workers > 0) ? std::min(workers, batch) : batch;

    std::printf("throughput (wall-based):\n"
                "  aggregate = %10.1f MiB/s   (%llu MiB over %.3f s)\n"
                "  at p50    = %10.1f MiB/s\n"
                "  at p99    = %10.1f MiB/s\n",
                thr_aggregate,
                static_cast<unsigned long long>(total_bytes >> 20),
                static_cast<double>(wall_sum) / 1e9,
                thr_p50, thr_p99);
    std::printf("parallelism:\n"
                "  effective workers = %6.2f   (copy_ns/wall_ns; upper bound min(W,B) = %d)\n"
                "                              below 1.0 just means RPC overhead is visible;\n"
                "                              judge by whether it tracks min(W,B) as B grows\n",
                eff_workers, expected_par);

    std::printf("microseconds (n=%zu):\n", samples.size());
    print_stats("read", summarize(read_v));
    print_stats("write", summarize(write_v));
    print_stats("copy", summarize(copy_v));
    print_stats("wall", summarize(wall_v));

    if (!csv_path.empty()) {
        std::FILE *f = std::fopen(csv_path.c_str(), "w");
        if (f == nullptr) {
            std::fprintf(stderr, "cannot open %s for writing\n", csv_path.c_str());
            return 1;
        }
        std::fprintf(f, "label,arm,workers,batch,chunk,iter,wall_ns,copy_ns,read_ns,write_ns\n");
        for (size_t i = 0; i < samples.size(); i++) {
            std::fprintf(f, "%s,%s,%d,%d,%llu,%zu,%lld,%lld,%lld,%lld\n",
                         label.c_str(), arm.c_str(), workers, batch,
                         static_cast<unsigned long long>(chunk), i,
                         static_cast<long long>(samples[i].wall_ns),
                         static_cast<long long>(samples[i].copy_ns),
                         static_cast<long long>(samples[i].read_ns),
                         static_cast<long long>(samples[i].write_ns));
        }
        std::fclose(f);
        std::printf("raw samples -> %s\n", csv_path.c_str());
    }

    /* 스크립트가 파싱하는 한 줄. 형식을 바꾸면 blkcopy_scaling.sh 의
     * 요약 집계가 깨진다. */
    std::printf("SUMMARY arm=%s workers=%d batch=%d chunk=%llu "
                "thr_mibs=%.1f thr_p50_mibs=%.1f eff_workers=%.2f "
                "wall_p50_us=%lld wall_p99_us=%lld n=%zu\n",
                arm.c_str(), workers, batch,
                static_cast<unsigned long long>(chunk),
                thr_aggregate, thr_p50, eff_workers,
                static_cast<long long>(wall_s.p50 / 1000),
                static_cast<long long>(wall_s.p99 / 1000),
                samples.size());
    return 0;
}
