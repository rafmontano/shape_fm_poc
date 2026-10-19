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

The approved runtime simplification keeps fitted models as files, uses ordinary
SSH/rsync delivery and local session loading, and removes redundant internal
qualification and content attestation. The fresh Ubuntu-coordinated 4,227-series
Daily experiment has completed Processes 01–06, including all 1,057 sensitivity
candidates and publication. Its two canonical databases, fifteen fitted directional
model files, five CSVs and six figure files are available at the repository root.
The final no-new-work restart submitted zero work groups and left accepted
scientific records and model/output files unchanged. Scientific configuration and
calculations remain unchanged; sensitivity uses coordinator CPU workers. This
run and its delivered corrected CSVs and figures are researcher-accepted. Any
changes arising from subsequent review are a new action; this acceptance does
not establish clean-room dependency reconstruction.

The targeted Naive2/OWA correction has been applied and its delivered CSVs
reviewed: Naive2 reference errors and SMYL/FFORMA OWA match the previous paper
results at displayed precision. Selected lambda pairs are unchanged.
The researcher has approved the raw-history Chronos baseline correction under
[forecasting](forecast-black-box-contract.md#chronos-2-paper-baseline), with only
its dependent recalculation/publication, plus final publication copying to worker
hosts under [evaluation](evaluation.md#figures-and-files). Implementation and the
targeted correction/publication are complete. Completed-state resume schedules
zero scientific work and retries existing-file delivery without rendering.
Refreshed outputs and approved source/documentation are synchronized on both
hosts. The researcher accepts the correction and delivered outputs, including
the reported residual Chronos–Mantis difference and unresolved native-failure
limitation. Subsequent review changes require a new action. No fresh experiment,
full six-process rerun or further crash investigation is authorized by closeout.

The five-layer architecture transformation is researcher-accepted. It delivered
the approved workflow hierarchy, readable process ownership, canonical paths,
minimal validation boundaries, restartable item state and pragmatic MLflow
visibility. POC2 remains open for the reduced exit backlog:

| ID | Exit item | Required outcome |
| --- | --- | --- |
| 10 | Standards QA | Complete the remaining output, documentation and reproducibility checks against the accepted standards. |
| 12 | Clean-room reconstruction | Recreate the accepted POC2 experiment and outputs from a clean checkout and empty generated workspace. |
| 14 | Final documentation and sync | Maintain documentation/synchronization for the remaining backlog; the delivered correction, output inventory and presentation are accepted and synchronized. |

The researcher decides whether newly found work belongs to POC2 or the POC3
backlog. Completed operational execution does not by itself establish clean-room
dependency reconstruction. The five-CSV/six-figure inventory, Process 06 ownership
and delivered presentation are researcher-accepted. Execution logs are not living
documentation. No remaining exit item begins as part of closeout.

Canonical DuckDB state, fitted artifacts, tables, figures and the new MLflow
run belong at the repository root. Git history preserves superseded execution
and transformation evidence, not a competing living design document.

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
