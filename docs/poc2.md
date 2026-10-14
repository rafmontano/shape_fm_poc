# POC2 scope and status

## Purpose

POC2 proves that ShapeFM's architecture and building blocks work on M4 Daily
before the complete M4 scale-up. It is about trustworthy mechanisms and
reproduction, not improving forecast accuracy.

## Scientific scope

- Canonical M4 Daily data from GIFT-Eval.
- Six processes: Import, Preprocess, Transform, Forecast, Combine and Evaluate.
- FFORMA-compatible R forecast pool, Chronos-2, DTW and Mantis/Random Forest.
- Stored individual forecasts and approved combinations/adjustments.
- Complete M4 Daily Table 1.
- Daily-only Table 2 calculation using the future POC3 path.
- Approved Figure 2 outputs for applicable implemented models.
- Same-current-code sequential and distributed execution must preserve accepted
  scientific results for identical inputs/settings under the approved numerical
  policy; this is not exact equivalence to historical implementations.
- Active DTW: direct aeon one-NN, float64 standardised length 64, fixed proportion
  0.10/effective width 6 across horizons 1–14, one neighbour's labels reused and
  shared configured reference cap 10,000. Calibrated/uncapped v12 is historical.

## Current status

POC2 Part B is scientifically accepted: the reproduced results were sufficiently
similar for the POC2 objective, all 1,057 sensitivity candidates completed, the
tables/exports completed, and a no-new-work restart passed. This accepts the
research result, not the current software structure.

POC2 remains open for the exit backlog:

| ID | Exit item | Required outcome |
| --- | --- | --- |
| X01 | Five-layer MVP architecture | Implement the mandatory approved hierarchy/subflow order and DuckDB/Python-R/Dask/Prefect/MLflow ownership; remove superseded paths and speculative behaviour. Open for researcher review. |
| X08 | Exact visual reproduction | Match approved names, fonts, colours, dimensions and separate figure files; discuss any discrepancy. |
| X18 | Researcher usability and workspace | One obvious command, readable progress, fixed paths and a clean comprehensible root/results layout. |
| X19 | Failure containment | Bounded retry and restart from committed work without repeating completed science. |
| X20 | Ubuntu R-worker qualification | Diagnose and qualify the native R path independently of scientific acceptance. |
| QA | Standards and reproducibility | Verify current contracts, six-process ownership, clean installation and reproduction from an empty generated workspace. |

The researcher decides whether newly found work belongs to POC2 or the POC3
backlog. Execution logs are not living documentation.

The cleanup checkpoint `c498dc6` and its recorded structural baseline are
preserved in [X01](poc2-x01-architecture-transformation.md). Increments A–B passed
bounded developer acceptance; C–F remain planned. Do not rerun cleanup.
Approval of the five-layer design does not close X01.

## Exit criteria

POC2 closes when the accepted science remains reproducible through the simplified
architecture, the researcher can understand and resume an experiment, outputs
match the approved presentation contract, the workspace is clean, and the
standards/clean-install QA passes. Publication and synchronization occur only
after explicit approval.

Historical comparison is approximate aggregate similarity supporting the same
research interpretation (also for POC3), not forced exact historical equality.
No calibration comparison or tuning to reproduce retired v12 is required or
permitted. Record ordinary numerical differences and continue; only material
differences in rankings, conclusions or interpretations require researcher
review. X01 remains open until final researcher acceptance.
