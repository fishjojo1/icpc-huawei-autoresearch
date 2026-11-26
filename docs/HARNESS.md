# Harness design and limits

The official tests are hidden and there is no official local testing kit.
`tools/interactor.cpp` implements the protocol and scoring rules from the
statement, while `tools/gen.py` supplies proxy workloads. Local results rank
candidates against one another; the judge remains ground truth.

## Protocol fidelity

`tools/interactor.cpp` implements:

- piecewise-linear task costs with the statement's boundary behavior;
- setup cost `S` for every task;
- prefill-piece duration proportional to its layer range;
- independent FIFO UP and DOWN links;
- automatic transfer creation and event coalescing;
- throughput, TDR, TPOT, distance, and score computation;
- protocol, stuck-state, CPU, and memory failure verdicts.

`tests/example1.txt` is the protocol fixture from the statement. Once a
candidate exists, `make check` must report `OK` with score `500.000003`.

## Calibration

Generated tests initially contain placeholder score constants.
`tools/calibrate.py` replaces them using two statement-derived objects:

- `tp_base` and `dist_base` come from the fully serialized
  one-request-at-a-time scheduler in `tools/ref.cpp`;
- `tp_UB` is the total output divided by the largest unavoidable aggregate
  load among the local computer, remote pool, UP link, and DOWN link, minimized
  over legal decode group sizes.

Calibration does not depend on `sol/main.cpp`. On every freshly calibrated
test, `tools/ref` must score zero. The interactor also prints `tp_ub_est`; this
diagnostic is not the calibrated `tp_UB` and should not be used for decisions.

## Generated workloads are a proxy

`tools/gen.py` samples several workload families so a policy is exercised
across arrival patterns, model depths, output lengths, remote counts, and link
bottlenecks. The statement does not reveal the final workload distribution or
cost-table distribution, so a local win may not transfer to the judge.

Prefer raw `tp`, `tdr`, and `tpot` when diagnosing why a score moved. If local
and judge directions disagree materially, record the disagreement and evaluate
the harness separately before changing the solution again.

## Seed isolation

No generated suite is committed. Use distinct seeds for development and final
evaluation:

| tier | visibility | purpose |
| --- | --- | --- |
| dev | current agent | implementation and tuning |
| evaluation | unused during tuning | candidate selection |
| revalidation | unused during selection | selected-candidate confirmation |
| preliminary judge | external | feedback and calibration evidence |

Scores are comparable only within one seed. Always record the seed beside a
score, and use paired per-test deltas when a baseline exists. Repeating the same
deterministic suite provides no new evidence; use a new seed instead.

When no baseline exists, establish a legal scheduler and validate it on a fresh
suite. Later candidates must beat it with a paired confidence interval whose
lower bound clears zero.

## Tail and legality gates

A positive mean does not excuse a protocol failure or catastrophic family
loss. For every candidate:

1. require every verdict to be `OK`;
2. inspect the worst per-test and per-family regression;
3. inspect raw throughput and latency movement;
4. confirm the intended workload families moved for the stated reason;
5. reject an inconclusive comparison.

Parallel runs can inflate reported system CPU through pipe and scheduling
overhead. The runners therefore rerun non-OK results serially before accepting
the verdict.

## Performance budget

The judge limit is 15 seconds and 256 MB per test, with up to two million
frames and 2000 requests. Scheduler work per frame must be proportional to
changed state. Maintain event-updated ready structures; an `O(R)` scan on every
frame is not viable.

Read interactive input with `read()`, not `fread()`. Buffered `fread()` may wait
for more bytes while the interactor waits for the solution's response.

## Harness changes

Keep `tools/` and `bench/` frozen while evaluating a solution change. Change
generation, calibration, or statistics only in a separate harness experiment
after repeated inconclusive results or a material local-versus-judge
disagreement. Record the exact reason and comparability boundary, rebuild all
suites, establish a fresh baseline, and do not claim a simultaneous solution
improvement.

Useful checks after a harness change:

```bash
make -s tools
make -s suite SEED=<fresh-seed>
./tools/interactor bench/suites/dev/mixed_00.txt --quiet -- ./tools/ref
