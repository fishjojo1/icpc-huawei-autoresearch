#!/usr/bin/env python3
"""Fill generated tests with statement-derived scoring constants.

`tp_base` and `dist_base` are measured by running the fully serialized
one-request-at-a-time scheduler in `tools/ref.cpp`. `tp_UB` is the total output
divided by the largest unavoidable aggregate load among the local computer,
remote pool, UP link, and DOWN link, minimized over legal decode group sizes.

Calibration is independent of `sol/main.cpp` and is idempotent.

    python3 tools/calibrate.py bench/suites/dev/*.txt
"""
import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERACTOR = os.path.join(ROOT, "tools", "interactor")
REF = os.path.join(ROOT, "tools", "ref")


# ------------------------------------------------------------------ the test

class Test:
    """Just enough of a test file to compute the work bound."""

    def __init__(self, path):
        with open(path) as fh:
            toks = fh.read().split()
        i = 0

        def nxt():
            nonlocal i
            v = toks[i]
            i += 1
            return v

        self.K = int(nxt())
        self.S = float(nxt())
        self.lat = float(nxt())
        self.bw = float(nxt())
        self.bpt = float(nxt())
        self.num_layers = int(nxt())
        for _ in range(7):          # the scoring line, which we are rewriting
            nxt()
        n = int(nxt())
        self.cols = [[] for _ in range(6)]
        for _ in range(n):
            b = float(nxt())
            for c in range(6):
                v = float(nxt())
                if v >= 0:
                    self.cols[c].append((b, v))
        for c in self.cols:
            c.sort()
        self.R = int(nxt())
        self.lin, self.lout = [], []
        for _ in range(self.R):
            nxt()                   # arrival time: the bound ignores arrivals
            self.lin.append(int(nxt()))
            self.lout.append(int(nxt()))
        self.sum_lout = sum(self.lout)

    def col(self, c, x):
        """Piecewise-linear lookup, clamped outside the listed range."""
        pts = self.cols[c]
        if not pts:
            return 1.0
        if x <= pts[0][0]:
            return pts[0][1]
        if x >= pts[-1][0]:
            return pts[-1][1]
        lo, hi = 0, len(pts) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if pts[mid][0] <= x:
                lo = mid
            else:
                hi = mid
        if pts[lo][0] == x:
            return pts[lo][1]
        t = (x - pts[lo][0]) / (pts[hi][0] - pts[lo][0])
        return pts[lo][1] + t * (pts[hi][1] - pts[lo][1])

    def xfer(self, length):
        return self.lat + 8.0 * length * self.bpt / (self.bw * 1e6)


def tp_upper_bound(t):
    """sum_lout / max over resources of the total work that must run on it.

    Every task pays the schedule cost S once, so the work a resource must do is
    a property of the instance and the group sizes, not of the schedule. Prefill
    is unbatchable (one request per P PRE / P PROC / P POST), so its work is
    a constant; decode work depends on the group size B, and the bound is taken
    at the B that makes the busiest resource lightest, capped by R because a
    group cannot contain more requests than exist.

    On Example 1 (K=1, S=1, one request, L_in=4, L_out=1) the four loads are
    local 11, remote 16, UP 9, DOWN 9, giving 1/16 = 0.0625 -- the judge's value
    to nine decimals.
    """
    pre_local = sum(t.S * 2 + t.col(0, L) + t.col(2, L) for L in t.lin)
    pre_rem = sum(t.S + t.col(1, L) for L in t.lin)
    pre_link = sum(t.xfer(L) for L in t.lin)

    cands = {1.0, float(t.R)}
    for c in (3, 4, 5):
        for b, _ in t.cols[c]:
            cands.add(b)
    best = 0.0
    for b in sorted(cands):
        b = min(max(1.0, b), float(t.R))
        local = pre_local + t.sum_lout * (2 * t.S + t.col(3, b) + t.col(5, b)) / b
        rem = (pre_rem + t.sum_lout * (t.S + t.col(4, b)) / b) / t.K
        link = pre_link + t.sum_lout * t.xfer(b) / b
        span = max(local, rem, link)          # UP and DOWN carry the same load
        if span > 0:
            best = max(best, t.sum_lout / span)
    return best


# ------------------------------------------------------------- calibration

def run_json(path, sol, tl):
    res = subprocess.run(
        [INTERACTOR, path, "--tl", str(tl), "--", sol],
        capture_output=True, text=True, timeout=3600,
    )
    try:
        return json.loads(res.stdout.strip().splitlines()[-1]), None
    except Exception:
        return None, f"interactor produced no JSON: {res.stdout[-300:]} {res.stderr[-300:]}"


def calibrate_one(path, tl):
    t = Test(path)
    tp_ub = tp_upper_bound(t)

    j, err = run_json(path, REF, tl)
    if err:
        return path, None, err
    if j["verdict"] != "OK":
        return path, None, f"reference scheduler failed: {j['verdict']} {j.get('error','')}"

    tp_base = j["tp"]
    dist_base = j["dist"]
    # The statement guarantees tp_UB > tp_base, and the work bound always does:
    # the serial makespan is the SUM of the four resource loads plus any waiting
    # for arrivals, so it strictly exceeds the largest of them whenever more than
    # one resource has work. Keep the guard anyway; it should never fire.
    if tp_ub <= tp_base:
        tp_ub = tp_base * 1.5 + 1e-9
        j["ub_guard"] = True
    j["tp_ub_used"] = tp_ub

    with open(path) as fh:
        lines = fh.read().splitlines()
    f = lines[1].split()
    lines[1] = " ".join([f[0], f[1], f"{tp_ub:.9f}", f"{tp_base:.9f}",
                         f"{dist_base:.9f}", f[5], f[6]])
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return path, j, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tests", nargs="+")
    ap.add_argument("--tl", type=int, default=600000,
                    help="CPU limit for the reference run; it is slow by design")
    ap.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 4)
    args = ap.parse_args()

    failures = 0
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        for path, j, err in ex.map(lambda p: calibrate_one(p, args.tl), args.tests):
            name = os.path.basename(path)
            if err:
                failures += 1
                print(f"{name:24s} FAILED  {err}")
            else:
                print(f"{name:24s} tp_base={j['tp']:.6g} dist_base={j['dist']:.6g} "
                      f"tp_UB={j['tp_ub_used']:.6g} "
                      f"(work bound{', GUARDED' if j.get('ub_guard') else ''}) "
                      f"(R={j['R']} tokens={j['sum_lout']} ref_makespan={j['makespan']:.4g}ms)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
