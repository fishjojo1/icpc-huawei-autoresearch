# Autoresearch workflow

The orchestrator runs each round against the current accepted scheduler.
Use the [harness notes](docs/HARNESS.md) and [experiment log](experiments/LOG.md)
to choose where to investigate next.

1. Freeze the baseline executable and share one development suite across four
   isolated Git worktrees. Save a separate evaluation seed for the tournament.
2. Run one shared research agent. Ask for relevant work, transferable ideas,
   and the assumptions behind them. Give its brief to all four planners.
3. Launch four planners concurrently, one per lane. Each chooses a hypothesis,
   identifies the expected throughput or latency effect, and writes a concrete
   implementation and validation plan.
4. Hand each plan to a separate implementer in the same worktree. Build and
   test against the baseline on the development suite. Record failures and
   regressions alongside gains.
5. Compare the valid candidates and the baseline on the same unseen suite.
   Look at paired score differences, protocol verdicts, and the worst
   per-family regressions.
6. Start with the strongest candidate. Try compatible ideas from other winners
   one at a time. Keep an addition only if it improves the combined solver on
   fresh tests. Finally, compare the full combination against the original
   baseline on another unseen suite.
7. Adopt the validated solver for the next round. When no candidate passes
   validation, carry the existing baseline forward. Record the seeds, results,
   rejected ideas, and the next question in the experiment log.

Use fresh seeds for development, candidate selection, and revalidation.
If the generator needs work, treat that as a separate round so a changed
benchmark cannot be mistaken for a solver improvement.
