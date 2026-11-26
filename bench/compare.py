#!/usr/bin/env python3
"""Rank several solutions head-to-head on one fresh evaluation suite.

No benchmark instances are committed. Tune against a development seed, then
make the accept/reject decision on a separate seed drawn here. This avoids
evaluating a candidate only on the tests used to develop it.

Compare candidates in one call — omit `--seed` to draw and print a fresh one:

    python3 bench/compare.py --fresh --per-family 3 \
        --sol base=./sol/main \
        --sol a=./wt/a/sol/main --sol b=./wt/b/sol/main

Re-validate a single winner on yet another unseen seed before accepting. Picking
the best of N candidates on one suite is winner's curse; this is the correction:

    python3 bench/compare.py --fresh --sol base=./sol/main --sol winner=./wt/b/sol/main

The first `--sol` is the reference; everything else is reported as a delta
against it with a 95% confidence interval and a call of WIN / LOSS /
INCONCLUSIVE / ILLEGAL. Accept on the confidence call, not the win/loss count:
many small wins can be outweighed by a few large losses.

Scores are comparable only *within* one seed, since calibration constants
differ per suite.
"""

import argparse
import glob
import json
import os
import statistics
import subprocess
import sys
import random
import threading
import tempfile
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERACTOR = os.path.join(ROOT, "tools", "interactor")
GEN = os.path.join(ROOT, "tools", "gen.py")
CALIB = os.path.join(ROOT, "tools", "calibrate.py")


def make_fresh(seed, per_family, out_dir, jobs):
    os.makedirs(out_dir, exist_ok=True)
    for f in glob.glob(os.path.join(out_dir, "*.txt")):
        os.remove(f)
    subprocess.run([sys.executable, GEN, "--suite", out_dir, "--seed", str(seed),
                    "--per-family", str(per_family)], check=True,
                   stdout=subprocess.DEVNULL)
    tests = sorted(glob.glob(os.path.join(out_dir, "*.txt")))
    subprocess.run([sys.executable, CALIB, "-j", str(jobs), *tests], check=True,
                   stdout=subprocess.DEVNULL)
    return tests


# The seven scoring constants calibrate.py writes on line 2 of every test file.
# They are embedded in --json so that a pooled screen can weight each test by
# the judge's own objective (`w_tp`) without needing the suite itself, which
# --fresh deletes on exit. Read-only: this changes no score and no verdict.
def test_params(path):
    keys = ("SLO1", "SLO2", "tp_UB", "tp_base", "dist_base", "w_tp", "w_c")
    try:
        with open(path) as fh:
            fh.readline()
            parts = fh.readline().split()
        if len(parts) < len(keys):
            return {}
        return dict(zip(keys, (float(x) for x in parts[:len(keys)])))
    except Exception:
        return {}



# A parallel run inflates the child's system time (pipe churn, context switches
# on a contended box), so a test can report several times the CPU it uses
# serially and be recorded as a spurious TLE. Any non-OK verdict is re-run
# alone, under this lock, before it is believed.
_serial = threading.Lock()


def confirm(result, rerun):
    if result.get("verdict") == "OK":
        return result
    with _serial:
        second = rerun()
    second["confirmed_by_serial_rerun"] = True
    if second.get("verdict") == "OK":
        second["first_pass_verdict"] = result.get("verdict")
    return second


def _score_once(test, sol, tl):
    try:
        res = subprocess.run([INTERACTOR, test, "--tl", str(tl), "--read-timeout", "300000",
                              "--", *sol], capture_output=True, text=True, timeout=3600)
        return json.loads(res.stdout.strip().splitlines()[-1])
    except Exception as e:
        return {"verdict": "HARNESS", "error": str(e)[:120], "score": 0.0}


def run_one(test, sol, tl):
    return confirm(_score_once(test, sol, tl), lambda: _score_once(test, sol, tl))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sol", action="append", required=True,
                    metavar="NAME=PATH", help="repeatable; the first is the reference")
    ap.add_argument("--suite", default=os.path.join(ROOT, "bench", "suites", "dev"))
    ap.add_argument("--fresh", action="store_true",
                    help="generate and calibrate a new suite instead of using --suite")
    ap.add_argument("--seed", type=int,
                    help="seed for --fresh; drawn at random and printed if omitted")
    # 12 families x 8 = 96 tests. Generation and calibration cost ~70s, which
    # is cheap next to halving the confidence interval on the mean delta.
    ap.add_argument("--per-family", type=int, default=8)
    ap.add_argument("--keep", help="with --fresh, keep the generated suite here")
    ap.add_argument("--tl", type=int, default=15000)
    ap.add_argument("-j", "--jobs", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    ap.add_argument("--json", help="write full results here")
    args = ap.parse_args()

    sols = []
    for spec in args.sol:
        name, _, path = spec.partition("=")
        if not path:
            name, path = os.path.basename(os.path.dirname(spec)) or spec, spec
        sols.append((name, path.split() if " " in path else [path]))

    # A missing or unbuilt binary would otherwise score 0 on every test and be
    # reported as a legitimate result -- which silently makes every other
    # candidate look like a huge win. Fail loudly instead.
    missing = [(n, c[0]) for n, c in sols
               if not (os.path.isfile(c[0]) and os.access(c[0], os.X_OK))]
    if missing:
        for n, p in missing:
            print(f"error: solution '{n}' is not an executable file: {p}", file=sys.stderr)
        sys.exit("refusing to run: fix the paths or `make` the missing worktrees")

    tmp = None
    if args.fresh:
        if args.seed is None:
            args.seed = random.randrange(1, 2**31 - 1)
        out_dir = args.keep
        if not out_dir:
            tmp = tempfile.TemporaryDirectory()
            out_dir = tmp.name
        print(f"=== accept seed {args.seed} ({args.per_family}/family) ===\n"
              f"record this seed in experiments/LOG.md; scores below are "
              f"comparable only within it")
        tests = make_fresh(args.seed, args.per_family, out_dir, args.jobs)
    else:
        tests = sorted(glob.glob(os.path.join(args.suite, "*.txt")))
    if not tests:
        sys.exit("no tests to run")

    jobs = [(si, ti) for si in range(len(sols)) for ti in range(len(tests))]
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        flat = list(ex.map(lambda j: run_one(tests[j[1]], sols[j[0]][1], args.tl), jobs))
    grid = {}
    for (si, ti), r in zip(jobs, flat):
        grid[(si, ti)] = r

    names = [n for n, _ in sols]
    w = max(10, max(len(n) for n in names) + 2)
    hdr = f"{'test':24s}" + "".join(f"{n:>{w}s}" for n in names)
    if len(sols) > 1:
        hdr += "".join(f"{'d:' + n:>{w}s}" for n in names[1:])
    print(hdr)
    print("-" * len(hdr))
    for ti, t in enumerate(tests):
        line = f"{os.path.basename(t):24s}"
        for si in range(len(sols)):
            r = grid[(si, ti)]
            cell = f"{r['score']:.1f}" if r["verdict"] == "OK" else r["verdict"]
            line += f"{cell:>{w}s}"
        for si in range(1, len(sols)):
            d = grid[(si, ti)]["score"] - grid[(0, ti)]["score"]
            line += f"{d:>+{w}.1f}"
        print(line)
    print("-" * len(hdr))

    means = [sum(grid[(si, ti)]["score"] for ti in range(len(tests))) / len(tests)
             for si in range(len(sols))]
    line = f"{'MEAN':24s}" + "".join(f"{m:>{w}.2f}" for m in means)
    line += "".join(f"{means[si] - means[0]:>+{w}.2f}" for si in range(1, len(sols)))
    print(line)

    print()
    for si, (name, _) in enumerate(sols):
        bad = [os.path.basename(tests[ti]) for ti in range(len(tests))
               if grid[(si, ti)]["verdict"] != "OK"]
        note = "ALL OK" if not bad else f"NOT OK: {', '.join(bad[:5])}"
        if si == 0:
            print(f"{name:12s} mean {means[si]:8.2f}  {note}   (reference)")
            continue
        deltas = [grid[(si, ti)]["score"] - grid[(0, ti)]["score"] for ti in range(len(tests))]
        wins = sum(1 for d in deltas if d > 1e-6)
        losses = sum(1 for d in deltas if d < -1e-6)
        wi = min(range(len(deltas)), key=lambda i: deltas[i])
        md = statistics.fmean(deltas)
        sd = statistics.stdev(deltas) if len(deltas) > 1 else 0.0
        se = sd / (len(deltas) ** 0.5) if deltas else 0.0
        lo, hi = md - 1.96 * se, md + 1.96 * se
        # The comparison is paired, so the mean per-test delta and its standard
        # error determine the call. Win/loss counts are diagnostic only.
        if bad:
            call = "ILLEGAL"
        elif lo > 0:
            call = "WIN"
        elif hi < 0:
            call = "LOSS"
        else:
            call = "INCONCLUSIVE"
        print(f"{name:12s} mean {means[si]:8.2f}  delta {md:+7.2f} "
              f"95% CI [{lo:+7.2f},{hi:+7.2f}]  {call:12s} "
              f"{wins}W/{losses}L  worst {os.path.basename(tests[wi])} "
              f"{deltas[wi]:+.1f}  {note}")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"suite": args.suite if not args.fresh else f"fresh:{args.seed}",
                       "sols": names, "means": dict(zip(names, means)),
                       "tests": [dict(name=os.path.basename(t), **test_params(t))
                                 for t in tests],
                       "results": {names[si]: [grid[(si, ti)] for ti in range(len(tests))]
                                   for si in range(len(sols))}}, fh, indent=1)
    if tmp:
        tmp.cleanup()
    # Non-zero if any candidate is not fully legal.
    return 1 if any(grid[(si, ti)]["verdict"] != "OK"
                    for si in range(len(sols)) for ti in range(len(tests))) else 0


if __name__ == "__main__":
    sys.exit(main())
