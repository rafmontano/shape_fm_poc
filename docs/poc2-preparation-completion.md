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

Pending the fresh two-pass 35-worker acceptance. This section must be updated
only from the generated final report after both the initial and restart runs
pass.
