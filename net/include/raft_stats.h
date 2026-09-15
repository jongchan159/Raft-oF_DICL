#ifndef RAFT_STATS_H
#define RAFT_STATS_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <vector>

/* ============================================================
 * 나노초 표본 벡터의 요약 통계.
 *
 * 원래 apps/raft_blkcopy_scale_main.cpp 의 익명 네임스페이스에 있었다.
 * raft_client 의 -op apply-timed 요약이 같은 계산을 필요로 하면서, 두 main 에
 * 같은 코드가 복사되는 것(이 저장소에는 call<> 템플릿이 이미 그렇게 두 벌
 * 있다)을 피하려고 header-only 로 뽑았다.
 *
 * **core/ 나 net/ 의 다른 헤더를 include 하지 않는다.** 이걸 쓰는 두
 * 바이너리(raft_client / raft_blkcopy_scale)는 core 를 링크하지 않고, CTest
 * isolation_raft_client / isolation_raft_blkcopy_scale 이 그 바이너리에
 * nvmeof_raft::Server:: 심볼이 없음을 nm 으로 강제한다.
 *
 * 단위 규약: **저장은 나노초, 출력은 마이크로초.** 두 print_* 함수가 나눈다.
 * ============================================================ */

namespace nvmeof_raft {
namespace stats {

/* 백분위수. 정렬된 v 에 대해 nearest-rank. */
inline int64_t pct(const std::vector<int64_t> &sorted, double p) {
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

/* 값 전달이다 -- 안에서 정렬하므로 호출자의 벡터를 건드리지 않는다.
 * 표준편차는 모집단 기준(n 으로 나눔)이다. */
inline Stats summarize(std::vector<int64_t> v) {
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

/* 전체 출력 (n / mean / sd / p50 / p90 / p99 / p99.9 / min / max).
 * **이 형식은 scripts/blkcopy_scaling.sh 가 파싱한다 -- 바꾸지 말 것.** */
inline void print_stats(const char *name, const Stats &s) {
    std::printf("  %-8s n=%-6zu mean=%10.1f sd=%10.1f  p50=%9lld p90=%9lld "
                "p99=%9lld p99.9=%9lld  min=%9lld max=%9lld\n",
                name, s.n, s.mean / 1000.0, s.stddev / 1000.0,
                static_cast<long long>(s.p50 / 1000), static_cast<long long>(s.p90 / 1000),
                static_cast<long long>(s.p99 / 1000), static_cast<long long>(s.p999 / 1000),
                static_cast<long long>(s.min / 1000), static_cast<long long>(s.max / 1000));
}

/* 좁은 출력 (mean / p50 / p99). 항이 열 개를 넘는 곳에서 쓴다 --
 * print_stats 는 항목당 아홉 개 수치라 표가 너무 넓어진다. */
inline void print_stats_brief(const char *name, const Stats &s) {
    std::printf("  %-12s mean=%9.1f  p50=%9.1f  p99=%9.1f\n",
                name, s.mean / 1000.0,
                static_cast<double>(s.p50) / 1000.0,
                static_cast<double>(s.p99) / 1000.0);
}

} /* namespace stats */
} /* namespace nvmeof_raft */

#endif /* RAFT_STATS_H */
