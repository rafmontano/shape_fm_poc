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
- Approved DTW target: historical one-NN via dtw-python, symmetric2, integer window 6
  for Daily length 64, one neighbour's labels reused across horizons 1–14,
  DTW-only reference cap 10,000 with seed 123. Mantis training is uncapped.
  The correction and bounded integration are complete; fresh-run delivery is blocked.

## Current status

The acceptance paragraphs below concern the preceding experiment, not the new
methodology run. Its generated state was disposed of once under the subsequent
fresh-run authorization; those outputs are not the current canonical delivery.

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
full six-process rerun or further crash investigation was authorized by closeout.

The researcher has subsequently approved the following methodology decisions.
These supersede the earlier cleaning-only instruction to preserve the shared cap,
training boundary and aeon DTW. Production code/configuration now implement
these decisions. Focused checks and the isolated 100-series Processes 01–06 run
pass, including a restart that changed no accepted scientific records or
model/output files. Fresh canonical execution is blocked as detailed below;
saved-result comparison and final scientific delivery remain pending.

| ID | Decision | Owning contract |
| --- | --- | --- |
| 1 | Keep GIFT-Eval observations and M4comp2018 archived forecasts. | Data |
| 2 | Clean applicable history once before window creation; no per-window cleaning. Keep Chronos/Naive2 raw. | Data |
| 3 | Use all pre-test history for training windows; remove the extra withheld horizon. | Data |
| 4 | Keep current tsai S1 membership, seed 123, and 80/20 policy. | Data |
| 5 | Uncapped Mantis training; DTW alone capped at 10,000 using historical stratified sampling with seed 123. | Directional models |
| 6 | Confirmed: restore previous dtw calculation and tie order using dtw-python. Keep fixed width; Bakeoff reconsideration is deferred to later research. | Directional models |
| 7 | Restore prepared-origin direct-classifier evaluation and separate raw-origin sensitivity/table evaluation. | Evaluation |

The researcher authorizes a fresh full M4 Daily Processes 01–06 run after
implementing these decisions, not a cleaning-only rerun. After focused
checks, dispose of prior generated scientific databases, fitted experiment
models, results and experiment runtime/log state on both hosts, with writers
stopped and exact targets resolved. Preserve source data, pretrained downloads,
environments, dependency caches, source, Git history and user stashes. Start one
fresh canonical experiment on Ubuntu, retain the approved execution profile and
raw-history Chronos/Naive2 policies, and deliver final tables/figures to worker
hosts. Resume progress within this new run after recoverable operational failure;
do not repeatedly reset it. Preserve the previous project's reference results.
The researcher requests a saved-result comparison under the evaluation contract
to establish similarity and explain differences. Acceptance of the new run
remains pending; successful execution is not scientific acceptance.

The new 4,227-series experiment has completed Processes 01–02 and subflows
03.01–03.02, preserving 129,482 modelling inputs. Its persisted cohorts contain
10,000 DTW references and 100,026 Mantis training windows. Subflow 03.03 has
accepted 392 feature vectors. No new fitted models or publication outputs have
yet been produced. The one-time reset must not be repeated.

The initial full attempt failed while reading child JSON values in DuckDB;
fresh read-only streaming and exact fetch/decode checks subsequently succeeded
for every modelling input. An unchanged-profile resume then stopped on an Ubuntu
R feature-worker SIGSEGV in the JIT compiler. A focused read-only reproduction
of that unaccepted eight-item request succeeded without storing results. Neither
failure has an established cause, and there is no evidence establishing a Mac
IP change as their cause. The full run is paused under the researcher's stop
condition for persistent operational blockage, with accepted state retained.
The remaining feature work, Processes 04–06, saved-result comparison and final
no-new-work restart require recovery; scientific acceptance remains separate.

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

Present final produced tables/figures and the currently authorized saved-result
comparison to the researcher. Do not execute a second scientific workload for
review. No calibration or tuning to prior results; further methodology changes
or scientific recalculation require researcher direction.
