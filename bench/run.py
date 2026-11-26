#!/usr/bin/env python3
"""Run a solution over a benchmark suite and report per-test and mean scores.

    make suite SEED=$SEED                         # build an experiment suite first
    python3 bench/run.py                          # score ./sol/main on it
    python3 bench/run.py --sol ./worktree/sol/main --json out.json
    python3 bench/run.py --baseline base.json     # show per-test deltas

Suites are generated from recorded seeds and are never committed, so scores
are comparable only within one seed. Use
bench/compare.py for any accept/reject decision.

Exit status is non-zero if any test did not finish cleanly, so this doubles as
a correctness gate: a VIOLATION / STUCK / TLE anywhere means the submission
would score 0 on that test.

The headline number is the mean normalized score over the suite, which mirrors
how the contest aggregates ("arithmetic mean of the 20 frozen final-test
scores"). Raw tp / tdr / tpot are also reported because tp_UB is only an
estimate locally - if a change moves raw tp up but the score down, trust tp.
"""

import argparse
import glob
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERACTOR = os.path.join(ROOT, "tools", "interactor")



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


def _score_once(test, sol, tl, read_timeout):
    cmd = [INTERACTOR, test, "--tl", str(tl), "--read-timeout", str(read_timeout), "--"] + sol
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        return json.loads(res.stdout.strip().splitlines()[-1])
    except Exception as e:
        return {"verdict": "HARNESS", "error": str(e)[:200], "score": 0.0}


def run_one(test, sol, tl, read_timeout):
    j = confirm(_score_once(test, sol, tl, read_timeout),
                lambda: _score_once(test, sol, tl, read_timeout))
    j["test"] = os.path.basename(test)
    return j


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default=os.path.join(ROOT, "bench", "suites", "dev"),
                    help="a suite built by `make suite SEED=...`; never a committed one")
    ap.add_argument("--sol", default=os.path.join(ROOT, "sol", "main"),
                    help="solution command (quote if it has arguments)")
    ap.add_argument("--tl", type=int, default=15000, help="CPU limit in ms, mirrors the judge")
    ap.add_argument("--read-timeout", type=int, default=60000)
    ap.add_argument("-j", "--jobs", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    ap.add_argument("--json", help="write full results here")
    ap.add_argument("--baseline", help="a previous --json file; print per-test deltas")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    tests = sorted(glob.glob(os.path.join(args.suite, "*.txt")))
    if not tests:
        print(f"no tests in {args.suite}.\n"
              f"Suites are generated per experiment, not committed:\n"
              f"    make suite SEED=<seed> DIR={args.suite}",
              file=sys.stderr)
        return 2
    sol = args.sol.split() if " " in args.sol else [args.sol]
    # Guard against scoring a nonexistent binary as a legitimate run of zeros.
    if not (os.path.isfile(sol[0]) and os.access(sol[0], os.X_OK)):
        print(f"error: not an executable file: {sol[0]}  (did you run `make`?)", file=sys.stderr)
        return 2

    base = {}
    if args.baseline:
        with open(args.baseline) as fh:
            for r in json.load(fh)["results"]:
                base[r["test"]] = r

    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        results = list(ex.map(lambda t: run_one(t, sol, args.tl, args.read_timeout), tests))

    bad = [r for r in results if r["verdict"] != "OK"]
    mean = sum(r["score"] for r in results) / len(results)

    if not args.quiet:
        hdr = f"{'test':24s} {'verdict':10s} {'score':>9s}"
        if base:
            hdr += f" {'delta':>9s}"
        hdr += f" {'tp':>11s} {'tdr':>11s} {'tpot':>10s} {'cpu_ms':>8s}"
        print(hdr)
        print("-" * len(hdr))
        for r in results:
            line = f"{r['test']:24s} {r['verdict']:10s} {r['score']:9.2f}"
            if base:
                d = r["score"] - base.get(r["test"], {}).get("score", 0.0)
                line += f" {d:+9.2f}"
            line += (f" {r.get('tp',0):11.5g} {r.get('tdr',0):11.5g} "
                     f"{r.get('tpot',0):10.5g} {r.get('sol_cpu_ms',0):8.0f}")
            if r["verdict"] != "OK":
                line += "  " + str(r.get("error", ""))[:80]
            print(line)
        print("-" * len(hdr))

    umean = sum(r.get("score_unclamped", 0.0) for r in results) / len(results)
    print(f"MEAN SCORE {mean:.3f}   over {len(results)} tests   "
          f"({len(bad)} not OK)")
    print(f"MEAN UNCLAMPED {umean:.3f}   (gradient only - tests pinned at 0 or "
          f"1000 still move here; never quote this as a contest score)")
    if base:
        bmean = sum(base.get(r["test"], {}).get("score", 0.0) for r in results) / len(results)
        print(f"BASELINE   {bmean:.3f}   DELTA {mean - bmean:+.3f}")
        # A change that lifts the mean while tanking one family is a trap.
        worst = min(results, key=lambda r: r["score"] - base.get(r["test"], {}).get("score", 0.0))
        wd = worst["score"] - base.get(worst["test"], {}).get("score", 0.0)
        print(f"WORST REGRESSION {worst['test']} {wd:+.3f}")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"mean": mean, "sol": sol, "suite": args.suite, "results": results}, fh, indent=1)

    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
