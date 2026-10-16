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
- Calculate each scientific atomic item once on its assigned worker; resume
  accepted DuckDB state. Hardware/backend are operational provenance, not identity
  or grounds for recalculation. Ordinary valid float differences are accepted;
  no parity gate, workload or tolerance gate.
- Active DTW: direct aeon one-NN, float64 standardised length 64, fixed proportion
  0.10/effective width 6 across horizons 1–14, one neighbour's labels reused and
  shared configured reference cap 10,000. Calibrated/uncapped v12 is historical.

## Current status

POC2 Part B is scientifically accepted: the reproduced results were sufficiently
similar for the POC2 objective, all 1,057 sensitivity candidates completed, the
tables/exports completed, and a no-new-work restart passed. This accepts the
research result; the architecture transformation was accepted separately.

The sensitivity execution issue is closed for POC2: the full Daily experiment
completed through the supported parallel Mac placement, published its declared
outputs and passed the no-new-work restart. The underlying Ubuntu crash cause
was not established. This is a limitation, not an investigation, backlog item or
acceptance blocker; the operational policy is in [evaluation](evaluation.md#parallel-execution-and-restart).

The five-layer architecture transformation is researcher-accepted. It delivered
the approved workflow hierarchy, readable process ownership, canonical paths,
minimal validation boundaries, restartable item state and pragmatic MLflow
visibility. POC2 remains open for the reduced exit backlog:

| ID | Exit item | Required outcome |
| --- | --- | --- |
| 06 | Pragmatic execution tuning | Use observed utilisation and throughput to select sensible worker settings without making host contribution a scientific gate. |
| 07 | Golden-output inventory | Finish mapping every in-scope POC2 table, CSV and figure to its current output and owner. |
| 08 | Output reproduction | Match approved names, fonts, colours, dimensions, layouts and separate figure files; discuss any material discrepancy. |
| 10 | Standards QA | Complete the remaining output, documentation and reproducibility checks against the accepted standards. |
| 12 | Clean-room reconstruction | Recreate the accepted POC2 experiment and outputs from a clean checkout and empty generated workspace. |
| 14 | Final documentation and sync | Record final evidence and limitations, obtain sign-off and synchronize Mac, Ubuntu and GitHub. |
| 19 | Failure containment | Confirm restart preserves committed work; decide whether an additional bounded transient retry adds MVP value. |

The researcher decides whether newly found work belongs to POC2 or the POC3
backlog. Execution logs are not living documentation.

Canonical DuckDB state, fitted artifacts, tables, figures and the existing MLflow
run are delivered at the repository root. Git history preserves the transformation
evidence; it is not retained as a competing living design document.

## Exit criteria

POC2 closes when the accepted science remains reproducible through the simplified
architecture, the researcher can understand and resume an experiment, outputs
match the approved presentation contract, the workspace is clean, and the
standards/clean-install QA passes. Publication and synchronization occur only
after explicit approval.

Present final produced tables/figures to the researcher; do not run automatic
historical comparison or a second scientific workload for final review. Stop for
a material final-result issue only when the researcher identifies it. No
calibration or tuning to prior results.
