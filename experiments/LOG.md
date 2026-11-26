# Selected experiments

## Opportunistic decode batching

The first event-driven scheduler processed singleton decode steps. The next
version batched a ready FIFO prefix and picked the size that reduced setup cost
per request. It dispatched immediately. On unseen seed `2026082103`, the mean
rose from 564.92 to 704.45 with all 36 cases legal. A second fresh seed confirmed
the gain.

## Score-aware wave scheduling

The model chose how many requests to admit to decoding and how to group them
into waves. It weighed greater throughput against longer gaps between tokens.
Two fresh local seeds showed mean gains of 1.43 and 3.39 points per case. The
22 preliminary cases gained 15.829 points in aggregate.

## Admission during live arrivals

On some workloads the model projected a makespan up to 50 times too short
while requests were still arriving. For throughput-heavy cases, a minimum
decode population reduced the cost of admitting too few requests too early.

## Sparse arrivals on slow links

A 10,080-case validation sweep found near-zero scores on three legal runs.
Disabling the admission floor for sparse streams had also disabled the small
batches needed to amortize transfers on very slow links. Restoring the floor
for those cases fixed the largest regressions. Preliminary scores stayed flat.
