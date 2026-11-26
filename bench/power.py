#!/usr/bin/env python3
"""How big does a delta have to be before it means anything?

The simulator is deterministic: re-running a test gives a bit-identical score,
so there is no measurement noise and no reason to repeat a run. All the
variance is *instance sampling* — a suite is a finite draw from gen.py, and a
different seed draws different instances.

Comparisons are paired (both solutions see the same instances), so the quantity
that matters is the mean per-test delta and its standard error. This script
measures both, plus how much the mean delta itself moves across seeds, which is
the honest answer to "would a different accept seed have flipped this
decision?".

    python3 bench/power.py --sol base=./sol/main --sol cand=/tmp/variant \
        --seeds 8 --per-family 3

Reports, per seed: mean delta, wins/losses, and the number of tests that moved
at all. Then across seeds: the spread of the mean delta, and a paired t-like
statistic pooled over every test of every seed.
"""

import argparse
import glob
import json
import os
import random
import statistics
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERACTOR = os.path.join(ROOT, "tools", "interactor")
GEN = os.path.join(ROOT, "tools", "gen.py")
CALIB = os.path.join(ROOT, "tools", "calibrate.py")


def build_suite(seed, per_family, out_dir, jobs):
    subprocess.run([sys.executable, GEN, "--suite", out_dir, "--seed", str(seed),
                    "--per-family", str(per_family)], check=True, stdout=subprocess.DEVNULL)
    tests = sorted(glob.glob(os.path.join(out_dir, "*.txt")))
    subprocess.run([sys.executable, CALIB, "-j", str(jobs), *tests], check=True,
                   stdout=subprocess.DEVNULL)
    return tests


_serial = threading.Lock()


def _once(test, sol):
    try:
        r = subprocess.run([INTERACTOR, test, "--tl", "15000", "--read-timeout", "300000",
                            "--", sol], capture_output=True, text=True, timeout=3600)
        j = json.loads(r.stdout.strip().splitlines()[-1])
        return j["score"], j["verdict"], j.get("score_unclamped", 0.0)
    except Exception:
        return 0.0, "HARNESS", 0.0


def score(test, sol):
    # Confirm any failure with a serial re-run: parallel load inflates the
    # child's system time and produces spurious TLEs.
    v = _once(test, sol)
    if v[1] != "OK":
        with _serial:
            v = _once(test, sol)
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sol", action="append", required=True, metavar="NAME=PATH")
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--seed-list", type=int, nargs="*")
    ap.add_argument("--per-family", type=int, default=3)
    ap.add_argument("-j", "--jobs", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    ap.add_argument("--json", help="write raw per-test deltas here")
    args = ap.parse_args()

    sols = []
    for spec in args.sol:
        n, _, p = spec.partition("=")
        sols.append((n, p))
    if len(sols) != 2:
        sys.exit("need exactly two --sol arguments: a base and a candidate")
    for n, p in sols:
        if not (os.path.isfile(p) and os.access(p, os.X_OK)):
            sys.exit(f"not an executable file: {p}")

    seeds = args.seed_list or [random.randrange(1, 2**31 - 1) for _ in range(args.seeds)]
    (bn, bp), (cn, cp) = sols

    print(f"{bn} vs {cn}   {len(seeds)} seeds x {args.per_family}/family\n")
    print(f"{'seed':>12} {'n':>4} {'mean ' + bn:>12} {'mean ' + cn:>12} "
          f"{'delta':>9} {'W/L':>9} {'moved':>7} {'notOK':>6}")
    print("-" * 78)

    per_seed_delta = []
    all_deltas = []
    all_udeltas = []
    rows = []
    for seed in seeds:
        with tempfile.TemporaryDirectory() as d:
            tests = build_suite(seed, args.per_family, d, args.jobs)
            jobs = [(s, t) for s in (bp, cp) for t in tests]
            with ThreadPoolExecutor(max_workers=args.jobs) as ex:
                res = list(ex.map(lambda j: score(j[1], j[0]), jobs))
            half = len(tests)
            base = res[:half]
            cand = res[half:]

        deltas = [c[0] - b[0] for b, c in zip(base, cand)]
        udeltas = [c[2] - b[2] for b, c in zip(base, cand)]
        all_udeltas.extend(udeltas)
        notok = sum(1 for v in base + cand if v[1] != "OK")
        bm = sum(b[0] for b in base) / half
        cm = sum(c[0] for c in cand) / half
        wins = sum(1 for d in deltas if d > 1e-6)
        losses = sum(1 for d in deltas if d < -1e-6)
        moved = wins + losses
        per_seed_delta.append(cm - bm)
        all_deltas.extend(deltas)
        rows.append({"seed": seed, "deltas": deltas,
                     "tests": [os.path.basename(t) for t in tests]})
        print(f"{seed:>12} {half:>4} {bm:>12.2f} {cm:>12.2f} {cm-bm:>+9.2f} "
              f"{f'{wins}/{losses}':>9} {moved:>7} {notok:>6}")

    print("-" * 78)
    n = len(all_deltas)
    mean_d = statistics.fmean(all_deltas)
    sd_d = statistics.stdev(all_deltas) if n > 1 else 0.0
    se = sd_d / (n ** 0.5) if n else 0.0
    moved = [d for d in all_deltas if abs(d) > 1e-6]

    print(f"\npooled over {n} test comparisons ({len(moved)} moved, "
          f"{n - len(moved)} pinned/identical)")
    print(f"  mean per-test delta   {mean_d:+.2f}")
    print(f"  sd of per-test delta  {sd_d:.2f}")
    print(f"  standard error        {se:.2f}   -> 95% CI [{mean_d-1.96*se:+.2f}, {mean_d+1.96*se:+.2f}]")
    if sd_d > 0:
        print(f"  t = {mean_d/se:+.2f}")

    if all_udeltas:
        um = statistics.fmean(all_udeltas)
        usd = statistics.stdev(all_udeltas) if len(all_udeltas) > 1 else 0.0
        use = usd / (len(all_udeltas) ** 0.5)
        umoved = sum(1 for d in all_udeltas if abs(d) > 1e-6)
        print(f"\nunclamped (search signal only, not the contest metric): "
              f"{umoved}/{len(all_udeltas)} moved")
        print(f"  mean {um:+.2f}   se {use:.2f}"
              + (f"   t = {um/use:+.2f}" if use > 0 else ""))

    if len(per_seed_delta) > 1:
        m = statistics.fmean(per_seed_delta)
        s = statistics.stdev(per_seed_delta)
        print(f"\nacross {len(per_seed_delta)} seeds, the *suite mean delta* itself:")
        print(f"  mean {m:+.2f}   sd {s:.2f}   range [{min(per_seed_delta):+.2f}, "
              f"{max(per_seed_delta):+.2f}]")
        sign = sum(1 for d in per_seed_delta if d > 0)
        print(f"  positive on {sign}/{len(per_seed_delta)} seeds")
        print(f"\n  A single suite of {args.per_family * 12} tests measures the mean delta "
              f"to about +/-{1.96*s:.0f} (95%).")
        print("  Treat any delta smaller than that as unproven, however good the mean looks.")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"sols": [bn, cn], "per_family": args.per_family,
                       "per_seed_delta": per_seed_delta, "rows": rows}, fh, indent=1)


if __name__ == "__main__":
    main()
