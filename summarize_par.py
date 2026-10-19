#!/usr/bin/env python3
"""par_* 결과 폴더(c*.txt, ae_before/after.txt)를 요약한다.
사용:  summarize_par.py DIR [DIR2] [--elapsed 초] [--batch N] [--size 바이트]
  - DIR2를 주면 두 실행을 나란히 + 차이(B-A)로 출력
  - --elapsed 생략 시 DIR/summary.txt의 'elapsed=..s'를 읽음
  - --batch 생략 시 환경변수 BATCH, 없으면 1
출력 값의 단위: 요청(RPC) 하나당 평균. LHandler/LPersist는 그룹 커밋 묶음 값,
AENet~StorageIO는 AE 값이 요청마다 복제된 것 (엔트리당 값이 아님)."""
import glob, os, re, statistics as st, sys

KEYS = ["Total", "LHandler", "LPersist", "AENet", "FHandler", "ReplNet", "StorageIO",
        "QuorumWait", "Mutex", "MutexA", "MutexB", "MutexC", "CommitWait", "residual"]
IDENT = ["LHandler", "LPersist", "AENet", "FHandler", "ReplNet", "StorageIO", "QuorumWait", "residual"]
NAN = float("nan")

def ae_sum(path):
    try: t = open(path).read()
    except OSError: return 0, 0
    c = e = 0
    for m in re.finditer(r"ae_count=(\d+) ae_entries=(\d+)", t):
        c += int(m[1]); e += int(m[2])
    return c, e

def leader_of(d):
    def per_addr(p):
        try: t = open(p).read()
        except OSError: return {}
        return {m[1]: int(m[2]) for m in re.finditer(r"^(\S+)\s+ae_count=(\d+)", t, re.M)}
    b, a = per_addr(d + "/ae_before.txt"), per_addr(d + "/ae_after.txt")
    best = max(a.items(), key=lambda kv: kv[1] - b.get(kv[0], 0), default=("?", 0))
    return best[0] if best[1] - b.get(best[0], 0) > 0 else "?"

def replication_check(d, n_clients, cmds, warmup=1000):
    """엔트리가 각 팔로워에 정확히 한 번씩 갔는지, 노드들의 commit_index가 수렴했는지 확인"""
    def per_addr(p):
        try: t = open(p).read()
        except OSError: return None
        return {m[1]: (int(m[2]), int(m[3])) for m in re.finditer(r"^(\S+)\s+ae_count=(\d+)\s+ae_entries=(\d+)", t, re.M)}
    b, a = per_addr(d + "/ae_before.txt"), per_addr(d + "/ae_after.txt")
    out = []
    if a is None or b is None or not a:
        out.append("복제 개수 검사: ae-stats 없음(건너뜀)")
    else:
        followers = len(a) - 1
        got = sum(a[k][1] - b.get(k, (0, 0))[1] for k in a)
        want = followers * (cmds + warmup + n_clients + 1)
        diff = got - want
        # 리더 선출 직후 no-op 엔트리 1개(raft_election.cpp become_leader)가 측정 구간에 끼면 팔로워당 +1
        if diff == 0:
            verdict = "일치"
        elif diff == followers:
            verdict = "일치 (리더 선출 no-op 1개 포함)"
        else:
            verdict = f"불일치 (차이 {diff:+d})"
        out.append(f"복제 개수 검사: {verdict}  entries={got}  기대={want} "
                   f"(= 팔로워 {followers} × (명령 {cmds} + 워밍업 {warmup} + probe {n_clients + 1}))")
    try:
        t = open(d + "/commit_after.txt").read()
        ci = [int(m[1]) for m in re.finditer(r"commit_index=(\d+)", t)]
        if ci:
            ok = (max(ci) - min(ci) <= 1)
            out.append(f"commit_index 수렴: {'일치' if ok else '불일치'}  값={ci}  (리더가 다음 AE 전까지 1 앞설 수 있음)")
    except OSError:
        out.append("commit_index 수렴: commit_after.txt 없음(스크립트에 한 줄 추가하면 검사됨)")
    return out

def summarize(d, elapsed, batch):
    rows, tp, cmds = [], 0.0, 0
    for f in sorted(glob.glob(d + "/c*.txt")):
        t = open(f).read(); r = {}
        for k in KEYS:
            m = re.search(r"^\s+" + k + r"\s+mean=\s*([\d.]+)\s+p50=\s*([\d.]+)\s+p99=\s*([\d.]+)", t, re.M)
            if m: r[k] = (float(m[1]), float(m[2]), float(m[3]))
        m = re.search(r"applied (\d+) commands.*?([\d.]+) ms total", t)
        if m and r:
            k_ = int(m[1]) - 1                      # leader probe 1건 제외
            tp += k_ / (float(m[2]) / 1000); cmds += k_; rows.append(r)
    if not rows:
        sys.exit(f"{d}: 집계할 c*.txt 없음")
    if elapsed is None:
        try: elapsed = float(re.search(r"elapsed=([\d.]+)s", open(d + "/summary.txt").read())[1])
        except Exception: elapsed = NAN
    o = {"leader": leader_of(d), "N": len(rows), "batch": batch, "cmds": cmds, "elapsed": elapsed,
         "tput_sum": tp, "tput_wall": cmds / elapsed if elapsed == elapsed else NAN}
    for k in KEYS:
        v = [r[k] for r in rows if k in r]
        for i, s in enumerate(("", "_p50", "_p99")):
            o[k + s] = st.mean(x[i] for x in v) if v else NAN
    b, a = ae_sum(d + "/ae_before.txt"), ae_sum(d + "/ae_after.txt")
    dc, de = a[0] - b[0], a[1] - b[1]
    o["ae_count"], o["ae_entries"] = dc, de
    o["followers"] = max(1, round(de / cmds)) if cmds and de > 0 else NAN
    o["entries_per_ae"] = de / dc if dc > 0 else NAN
    o["ae_cycle_ms"] = (o["followers"] * elapsed / dc * 1000) if dc > 0 and elapsed == elapsed else NAN
    o["chain_us"] = sum(o[k] for k in ("AENet", "FHandler", "ReplNet", "StorageIO"))
    o["checks"] = replication_check(d, len(rows), cmds)
    o["entry_serial_us"] = 1e6 / tp
    o["in_flight"] = tp * o["Total"] / 1e6
    return o

def f(x, w=9, p=1):
    return f"{'n/a':>{w}}" if x != x else f"{x:{w}.{p}f}"

def show_one(o, size=None):
    if size is not None:   # 기존 형식의 첫 줄 (이전 summary.txt와 호환)
        tw = o['tput_wall']
        print(f"N={o['N']} size={size} cmds={o['cmds']} elapsed={o['elapsed']:.2f}s throughput={tw:.0f} ops/s (wall 기준)")
    print(f"N={o['N']} batch={o['batch']} cmds={o['cmds']} elapsed={f(o['elapsed'],0,2).strip()}s")
    print(f"leader={o['leader']}   (ae-stats 증가분이 있는 노드. 실행마다 바뀔 수 있어 비교 전에 확인)")
    print(f"throughput(클라이언트별 합) = {o['tput_sum']:.0f} ops/s   wall = {f(o['tput_wall'],0,0).strip()} ops/s  (실제값은 두 값 사이, 구간이 길수록 수렴)")
    print(f"{'':12s}{'mean':>9s}{'p50':>9s}{'p99':>9s}{'%Total':>8s}   (요청 하나당 µs)")
    for k in KEYS:
        if o[k] != o[k]: continue
        share = f"{100*o[k]/o['Total']:7.1f}%" if k in IDENT else f"{'':>8s}"
        tag = "  ← 가산 아님(대기시간 진단)" if k in ("Mutex", "MutexA", "MutexB", "MutexC", "CommitWait") else ""
        print(f"{k:12s}{f(o[k])}{f(o[k+'_p50'])}{f(o[k+'_p99'])}{share}{tag}")
    print("-- 엔트리당 환산 (엔트리 단위로 읽는 값) --")
    print(f"엔트리당 직렬 비용      = {o['entry_serial_us']:.1f} µs  (1/처리량. 락 A 등 직렬 단계의 상한)")
    exp = o['N'] * o['batch']
    print(f"서버 안 동시 엔트리     = {o['in_flight']:.1f}  (처리량×Total, 기대 N×batch={exp}; 낮으면 클라이언트 쪽 시간)")
    if o['ae_count'] > 0:   # 기존 형식의 AE 줄
        print(f"AE: count={o['ae_count']} entries={o['ae_entries']} entries/AE={o['entries_per_ae']:.2f} AE/cmd={o['ae_count']/o['cmds']:.3f}")
    print(f"AE: {o['ae_count']}회, 엔트리/AE={f(o['entries_per_ae'],0,1).strip()}, 팔로워 {f(o['followers'],0,0).strip()}대 추정")
    print(f"AE 사이클(팔로워당)     = {f(o['ae_cycle_ms'],0,2).strip()} ms   전송체인(AENet+FHandler+ReplNet+StorageIO) = {o['chain_us']/1000:.2f} ms")
    if o['ae_cycle_ms'] == o['ae_cycle_ms']:
        over = o['ae_cycle_ms'] - o['chain_us'] / 1000
        print(f"  └ 체인 밖 대기          = {over:.2f} ms ({100*over/o['ae_cycle_ms']:.0f}%)  ← 락 대기/파이프라인 대기")
    if o['entries_per_ae'] == o['entries_per_ae']:
        print(f"StorageIO/엔트리         = {o['StorageIO']/o['entries_per_ae']:.1f} µs  (AE 값 ÷ 엔트리/AE)")
    print("※ LHandler/LPersist는 그룹 커밋 묶음 하나의 값(요청 가중 평균). 엔트리당 환산은 iostat의 묶음 크기가 필요.")
    print("-- 정합성 점검 --")
    for line in o["checks"]:
        print(line)

def main():
    a = sys.argv[1:]
    elapsed = batch = size = None; dirs = []
    i = 0
    while i < len(a):
        if a[i] == "--elapsed": elapsed = float(a[i+1]); i += 2
        elif a[i] == "--batch": batch = int(a[i+1]); i += 2
        elif a[i] == "--size": size = a[i+1]; i += 2
        else: dirs.append(a[i]); i += 1
    if not dirs: sys.exit(__doc__)
    if batch is None: batch = int(os.environ.get("BATCH", "1"))
    res = [summarize(d, elapsed, batch) for d in dirs[:2]]
    if len(res) == 1:
        show_one(res[0], size); return
    A, B = res
    print("A =", dirs[0], " leader=" + A["leader"]); print("B =", dirs[1], " leader=" + B["leader"])
    if A["leader"] != B["leader"]:
        print("경고: 두 실행의 리더가 다름 → 노드별 SSD/경로 차이가 섞여 있음")
    print(f"{'':22s}{'A':>11s}{'B':>11s}{'B-A':>11s}{'B/A':>8s}")
    items = [("tput_sum","처리량 합"),("tput_wall","처리량 wall"),("entry_serial_us","엔트리당 직렬µs"),
             ("in_flight","서버 안 동시"),("entries_per_ae","엔트리/AE"),("ae_cycle_ms","AE 사이클 ms"),("chain_us","체인 µs")]
    items += [(k, k + " µs") for k in KEYS if A[k] == A[k]]
    for k, lab in items:
        x, y = A[k], B[k]
        ratio = f"{y/x:8.2f}" if x == x and y == y and x else f"{'n/a':>8s}"
        print(f"{lab:22s}{f(x,11)}{f(y,11)}{f(y-x if x==x and y==y else NAN,11)}{ratio}")

main()