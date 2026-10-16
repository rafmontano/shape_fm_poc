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

The fresh full M4 Daily operational test is completed and technically accepted:
Processes 01–06 and all thirty process/subflow milestones are complete, all
4,227 series and 1,057 sensitivity candidates are accepted, zero scientific items
are pending, and five CSVs and six figures are published. The no-new-work restart
submitted zero scientific calculation tasks and preserved accepted rows and
timestamps, all fifteen fitted models and published file hashes/mtimes. The two
canonical databases and existing MLflow run are retained; execution services and
database writers are stopped on both machines. The operational capacity and
failure-containment requirements (exit items 06 and 19) are satisfied for this
test; no additional tuning or retry mechanism is required for its closeout.

The sensitivity execution issue is closed for POC2: the full Daily experiment
completed through the supported parallel Mac placement, published its declared
outputs and passed the no-new-work restart. The underlying Ubuntu crash cause
was not established. This is a limitation, not an investigation, backlog item or
acceptance blocker; the operational policy is in [evaluation](evaluation.md#parallel-execution-and-restart).

Captured Ubuntu failures occurred in Python parameter handling and native memory
cleanup; the originating defect was not established. After accepted closeout,
the four test-owned core dumps, debugger/native/launcher logs, temporary transport
and request probes, obsolete investigation snapshots/reports and stale Dask
scratch files were removed. Final execution/restart evidence, scientific state,
model assets and MLflow state were preserved. Ubuntu-only execution is deferred
to a future researcher decision; no further qualification is required here.

The five-layer architecture transformation is researcher-accepted. It delivered
the approved workflow hierarchy, readable process ownership, canonical paths,
minimal validation boundaries, restartable item state and pragmatic MLflow
visibility. POC2 remains open for the reduced exit backlog:

| ID | Exit item | Required outcome |
| --- | --- | --- |
| 07 | Golden-output inventory | Review and sign off the produced five-CSV/six-figure inventory and its Process 06 ownership. |
| 08 | Output reproduction | Researcher presentation review of approved names, fonts, colours, dimensions, layouts and separate figure files; discuss any material discrepancy. |
| 10 | Standards QA | Complete the remaining output, documentation and reproducibility checks against the accepted standards. |
| 12 | Clean-room reconstruction | Recreate the accepted POC2 experiment and outputs from a clean checkout and empty generated workspace. |
| 14 | Final documentation and sync | Obtain remaining POC2 sign-off and maintain final documentation/synchronization; the operational test closeout is already documented and synchronized. |

The researcher decides whether newly found work belongs to POC2 or the POC3
backlog. Completed operational execution does not by itself establish clean-room
dependency reconstruction or researcher presentation acceptance. Execution logs
are not living documentation. No remaining exit item begins as part of closeout.

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
