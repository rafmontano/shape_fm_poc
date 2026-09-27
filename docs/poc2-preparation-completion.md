# POC2 Preparation completion record

## Scope

POC2 Preparation establishes one source tree, one researcher interface,
six-process orchestration, a single DuckDB writer, specialised R workers,
official GIFT-Eval evaluation, distributed provenance, and restart evidence.
It does not import scientific code from the previous project; that work belongs
to the later POC2 Import phase.

## Historical evidence

- Accepted functional baseline revision: `597232a2b9e0ad67550480607b44ec4cbe542f43`.
- The accepted first-100-series baseline produced 100 series, 100 forecast
  instances, process counts 200/400/800/1,200/12, 1,200 forecasts, and 12
  official evaluations across 21 Dask workers, then passed restart validation.
- The isolated one-versus-15 calibration found the 15-logical-worker candidate
  scientifically equivalent, faster, and resource-safe on one physical RTX
  5090. Sustained warm GPU utilisation was inconclusive because the 15-process
  workload completed in approximately 0.55 seconds and produced only two
  telemetry samples per warm repetition. Peak utilisation, power draw and warm
  throughput increased.
- The Chief Developer accepted 15 logical processes as the integrated target,
  with later recalibration permitted.

Historical generated databases, full reports, telemetry, logs, caches, and
model files remain ignored and are not release artifacts.

## Final integrated acceptance

The fresh two-pass acceptance ran at candidate revision
`7302fb537cbf3339bca5664cec06b0cf7bb863c1`, with GIFT-Eval revision
`4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a` and Chronos-2 model revision
`29ec3766d36d6f73f0696f85560a422f50e8498c`. Both invocations used the same
isolated database and report through `00_main.py test` with the locked existing
environment and `--no-sync`.

### Complete pipeline

- Runtime: 210.388 seconds for import through official evaluation.
- Topology: 5 Mac CPU workers, 15 Ubuntu CPU workers, and 15 logical Ubuntu GPU
  workers sharing one physical NVIDIA GeForce RTX 5090; 35 Dask workers total.
- Data: exactly the first 100 official M4 Daily series and 100 forecast
  instances.
- Completed tasks for Processes 02–06: 200 / 400 / 800 / 1,200 / 12.
- Results: 1,200 forecasts and 12 official evaluations, with no duplicate rows.
- Contribution: 572 Mac CPU tasks, 828 Ubuntu CPU tasks, and all 400 Chronos
  tasks on all 15 Ubuntu `CHRONOS_GPU_SLOT` workers.
- Reliability: zero failed attempts, retries, worker losses, or replacements.
- Forecast fingerprint:
  `485629183c5352ffba9c68174967488761d3016b41a978cfe126c369902af4f9`.
- Evaluation fingerprint:
  `dad15b2a0d7b3336015799122894c4f9da270c72824974c8bbb6cf36ac1dbcc6`.

Resource checks passed with no Dask spill or swap growth. During the complete
run, minimum available memory was 4,563,107,840 bytes on the Mac,
103,708,561,408 bytes on Ubuntu, and 14,890,827,776 bytes on the physical GPU.
The configured ceilings were 5 × 2 GiB Mac CPU, 15 × 2 GiB Ubuntu CPU, and
15 × 4 GiB logical Ubuntu GPU worker memory.

### GPU calibration evidence

The earlier isolated calibration established scientific equivalence,
throughput improvement, and resource safety for fifteen logical GPU workers.
Sustained warm GPU utilisation remained inconclusive because each approximately
0.55-second warm repetition yielded only two telemetry samples; this does not
alter the measured complete-pipeline acceptance above.

### Restart

The identical second command completed in 73.518 seconds. It found the prior
successful run, skipped all 100 imported series, selected zero tasks for every
Process 02–06 stage, retained all row counts, and preserved both fingerprints
exactly. The report's final `acceptance_passed` value is `true`.

Generated acceptance databases, reports, scheduler/worker logs, telemetry,
caches, and model files remain ignored and are not release artifacts.
