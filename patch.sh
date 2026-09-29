#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$ROOT"

python3 - <<'PY'
from pathlib import Path

# 1) proto: add one lightweight server-side latency field.
p = Path("proto/rpcproto.proto")
s = p.read_text()
old = """message ClientApplyResponse {
  string err = 1;
  // Backpressure: server asks client to retry after this many milliseconds.
  bool busy = 2;
  int32 retry_after_ms = 3;
}"""
new = """message ClientApplyResponse {
  string err = 1;
  // Backpressure: server asks client to retry after this many milliseconds.
  bool busy = 2;
  int32 retry_after_ms = 3;
  // Plain apply only: server->apply() wall-clock latency.
  int64 latency_nanos = 4;
}"""
if "latency_nanos = 4" not in s:
    if old not in s:
        raise SystemExit("rpcproto.proto: ClientApplyResponse pattern not found")
    p.write_text(s.replace(old, new, 1))

# 2) internal response struct.
p = Path("core/include/raft_entry.h")
s = p.read_text()
old = """struct ClientApplyResponse {
    std::string error;
    bool busy = false;
    int retry_after_ms = 0;
};"""
new = """struct ClientApplyResponse {
    std::string error;
    bool busy = false;
    int retry_after_ms = 0;
    int64_t latency_ns = 0;
};"""
if "int64_t latency_ns = 0;" not in s:
    if old not in s:
        raise SystemExit("raft_entry.h: ClientApplyResponse pattern not found")
    p.write_text(s.replace(old, new, 1))

# 3) proto conversion both ways.
p = Path("net/include/raft_proto_conv.h")
s = p.read_text()
if "p->set_latency_nanos(g.latency_ns);" not in s:
    s = s.replace(
"""    p->set_retry_after_ms(g.retry_after_ms);
}""",
"""    p->set_retry_after_ms(g.retry_after_ms);
    p->set_latency_nanos(g.latency_ns);
}""", 1)
if "g.latency_ns = p.latency_nanos();" not in s:
    s = s.replace(
"""    g.retry_after_ms = p.retry_after_ms();
}""",
"""    g.retry_after_ms = p.retry_after_ms();
    g.latency_ns = p.latency_nanos();
}""", 1)
p.write_text(s)

# 4) server: only two steady_clock timestamps around server->apply().
p = Path("net/src/raft_tcp_server.cpp")
s = p.read_text()
old = """        bool busy = false;
        ApplyResult res = server->apply(req.commands, &busy);

        ClientApplyResponse rsp;
        rsp.error = res.error;
        rsp.busy = busy;
        rsp.retry_after_ms = busy ? apply_busy_retry_after_ms() : 0;
"""
new = """        bool busy = false;
        const auto apply_t0 = std::chrono::steady_clock::now();
        ApplyResult res = server->apply(req.commands, &busy);
        const int64_t apply_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - apply_t0).count();

        ClientApplyResponse rsp;
        rsp.error = res.error;
        rsp.busy = busy;
        rsp.retry_after_ms = busy ? apply_busy_retry_after_ms() : 0;
        rsp.latency_ns = apply_ns;
"""
if "rsp.latency_ns = apply_ns;" not in s:
    if old not in s:
        raise SystemExit("raft_tcp_server.cpp: apply handler pattern not found")
    p.write_text(s.replace(old, new, 1))

# 5) client: collect server-side latency samples for plain apply and summarize.
p = Path("apps/raft_client_main.cpp")
s = p.read_text()

if "std::vector<int64_t> apply_latencies;" not in s:
    anchor = """        std::vector<int64_t> timed[kNTimedTerms];

        int remaining = n;
"""
    repl = """        std::vector<int64_t> timed[kNTimedTerms];
        std::vector<int64_t> apply_latencies;

        int remaining = n;
"""
    if anchor not in s:
        raise SystemExit("raft_client_main.cpp: latency vector anchor not found")
    s = s.replace(anchor, repl, 1)

if "apply_latencies.push_back(rsp.latency_nanos());" not in s:
    anchor = """                if (!rsp.err().empty()) {
                    throw std::runtime_error("apply: " + rsp.err());
                }
            }
            applied += static_cast<uint64_t>(this_batch);
"""
    repl = """                if (!rsp.err().empty()) {
                    throw std::runtime_error("apply: " + rsp.err());
                }
                apply_latencies.push_back(rsp.latency_nanos());
            }
            applied += static_cast<uint64_t>(this_batch);
"""
    if anchor not in s:
        raise SystemExit("raft_client_main.cpp: plain apply response anchor not found")
    s = s.replace(anchor, repl, 1)

if "plain apply internal latency summary" not in s:
    anchor = """        if (op == "apply-timed" && !timed[0].empty()) {
            std::printf("\\napply-timed summary (n=%zu samples, microseconds):\\n",
                        timed[0].size());
            for (size_t k = 0; k < kNTimedTerms; k++) {
                print_stats_brief(kTimedTerms[k], summarize(timed[k]));
            }
            std::printf("  (residual = Total - sum of the 7 identity terms)\\n\\n");
        }

        int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
"""
    repl = """        if (op == "apply-timed" && !timed[0].empty()) {
            std::printf("\\napply-timed summary (n=%zu samples, microseconds):\\n",
                        timed[0].size());
            for (size_t k = 0; k < kNTimedTerms; k++) {
                print_stats_brief(kTimedTerms[k], summarize(timed[k]));
            }
            std::printf("  (residual = Total - sum of the 7 identity terms)\\n\\n");
        }

        if (op == "apply" && !apply_latencies.empty()) {
            Stats st = summarize(apply_latencies);
            std::vector<int64_t> sorted = apply_latencies;
            std::sort(sorted.begin(), sorted.end());
            const int64_t p95 = pct(sorted, 95);
            std::printf("\\nplain apply internal latency summary "
                        "(n=%zu samples, microseconds):\\n",
                        apply_latencies.size());
            std::printf("  avg=%.1f  p50=%.1f  p95=%.1f  p99=%.1f\\n\\n",
                        st.mean / 1000.0,
                        static_cast<double>(st.p50) / 1000.0,
                        static_cast<double>(p95) / 1000.0,
                        static_cast<double>(st.p99) / 1000.0);
        }

        int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
"""
    if anchor not in s:
        raise SystemExit("raft_client_main.cpp: summary anchor not found")
    s = s.replace(anchor, repl, 1)

p.write_text(s)

# 6) Fix tcp-ipoib environment selection bug in start_node.sh.
p = Path("start_node.sh")
s = p.read_text()
old = """TRANSPORT_MODE="${3:-rdma}"

case "$TRANSPORT_MODE" in
    rdma)
        TRANSPORT="rdma"
        select_transport_env rdma
        ;;
    tcp-ipoib)
        TRANSPORT="tcp"
        select_transport_env rdma
        ;;
    tcp)
        TRANSPORT="tcp"
        select_transport_env tcp
        ;;
esac

select_transport_env "$TRANSPORT"
"""
new = """TRANSPORT_MODE="${3:-rdma}"

case "$TRANSPORT_MODE" in
    rdma)
        select_transport_env rdma
        ;;
    tcp-ipoib)
        select_transport_env tcp-ipoib
        ;;
    tcp)
        select_transport_env tcp
        ;;
esac
"""
if old in s:
    p.write_text(s.replace(old, new, 1))
elif 'select_transport_env "$TRANSPORT"' in s:
    raise SystemExit("start_node.sh: transport block differs; inspect manually")

print("source patch complete")
PY

# Regenerate protobuf C++ then rebuild.
./build.sh regen-proto
./build.sh

echo
echo "done"
echo "plain -op apply now returns server-side apply latency only."
