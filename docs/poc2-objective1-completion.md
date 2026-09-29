# POC2 Objective 1 completion

Objective 1 established the central ShapeFM research workflow for 100
deterministically selected M4 Daily series using AutoARIMA, Chronos-2,
equal-weight combinations, and official GIFT-Eval evaluation. The approved
implementation checkpoint is `92f9d75f045b3ac17da6965a2cdd72be1c573f92`.

The implementation follows the approved architecture: JSON defines a new
experiment, DuckDB preserves and controls it, and the coordinator supplies each
process and task only the settings it needs. New databases are created from
`config/experiments/poc2_m4_daily_100.json`; after creation, DuckDB is the sole
configuration authority. The stored reproducibility seed is `1234`.

Final acceptance used exactly one Mac CPU worker and one Ubuntu GPU worker on
the RTX 5090; the scheduler was not counted as a worker. The initial run took
591.04 seconds and the restart took 21.05 seconds. It produced:

- Process 01: 100 series;
- Processes 02–06: 200, 400, 800, 1,200, and 12 tasks respectively;
- 1,200 forecast rows and 12 official evaluations;
- zero failures, retries, duplicate forecasts, or duplicate evaluations.

The scientific fingerprint is
`7feb7ae5738328ceacb72604fdb3f6719943ca202638063f7a0911a6f9393d8d`.
The configuration-integrity fingerprint is
`472a4afba63ca1fd91a9630dbbe27e221c06e758390b8dd1bbe47675ee46ddc1`.
On restart, Process 01 skipped all 100 completed imports, Processes 02–06 each
selected no work, all result counts remained unchanged, and both fingerprints
were preserved.

Resource-safety checks passed with no spilling, swap growth, worker replacement,
sampling error, or unsafe condition. GPU execution may still exhibit
library- or hardware-level nondeterminism despite the central seed.

Objective 2 has not started. Its direction is only partially defined; its exact
results, models, tolerances, import order, acceptance criteria, and definition
of done remain unapproved.

## Chief Developer QA closure — 29 September 2026

The Chief Developer's human QA review approved the source documentation, human
readability, process naming, and wrapper/substep convention. The final source
structure uses `00_main.py` as the sole researcher entry point, numbered
Process 01–06 Python wrappers, numbered specialised substeps, and unnumbered
shared utilities.

Final local Mac validation passed Python compilation, parsing of all three R
sources, both relocated R worker contracts, all 88 fast tests, the isolated
GIFT-Eval semantics test, all three remaining integration tests, every
researcher-action help check, stale-reference checks, and `git diff --check`.
Semantic-equivalence review and regression evidence confirm that scientific
behaviour and persistent DuckDB contracts are unchanged. The earlier
two-machine acceptance therefore remains valid evidence for the scientific
architecture; the final structural candidate was validated locally on Mac.

Ubuntu was offline and intentionally was not synchronized. Mac `main` and
GitHub `origin/main` are authoritative until Ubuntu fetches and fast-forwards
to that revision before any execution or development. POC2 Objective 1 is
closed. POC2 Objective 2 has not started, and its detailed scope remains
subject to researcher approval.
