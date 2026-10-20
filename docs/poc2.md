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
  The correction and bounded integration are complete; fresh-run delivery is pending.

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
model/output files. The fresh canonical execution and saved-result comparison
are complete as detailed below; researcher scientific acceptance remains pending.

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

The researcher renewed authorization to dispose of the interrupted experiment
and start Processes 01–06 from the beginning. Both hosts' generated scientific
databases, experiment models, publication outputs and Prefect/Dask/MLflow state
were cleared with writers stopped. Canonical data, pretrained checkpoints,
environments, caches, authored files, Git state and previous-project references
were preserved. Source revisions matched on both hosts; 59 Python tests, the R
cohort test, Mac DTW checks and Ubuntu setup verification passed before launch.

One fresh 4,227-series experiment started on Ubuntu with the unchanged
`mvp_two_machine` profile. Retain accepted work within this run on recovery;
this renewed authorization is not standing permission for repeated resets.
Processes 01–03 and all 67,632 individual forecasts (including 8,454 imported
archives) are accepted. The child contains 129,482 modelling inputs; the parent
contains 14,227 feature vectors. Training populations are 10,000 ordered DTW
references and 100,026 uncapped Mantis windows. All 104,253 training/official
representations and all 15 decision models are accepted: the DTW reference and
Mantis Random Forest horizons 1–14. All accepted model files are present.

An Ubuntu R NNETAR simulation SIGSEGV interrupted forecasting. Its unaccepted
seven-item request succeeded in isolation; unchanged-profile recovery completed
all remaining individual forecasts. The native crash cause remains unresolved.
Recovery then stopped before representation execution because only 44 of the
required 50 workers remained: all six Mac workers had disconnected following
macOS-recorded sleep. All sixteen logical GPU workers were present; no allocation
or scientific setting was reduced. The researcher subsequently confirmed the Mac
was awake and authorized automatic sleep protection and recovery. Mac workers now
use process-scoped `caffeinate -is`, with both a bounded native check and the live
worker's system-sleep assertion verified. Nine researcher-interface tests pass.
Recovery completed representations, then the Ubuntu coordinator Python process
terminated with SIGSEGV during 04.03. The execution and kernel logs establish
the native crash, not its underlying cause. The researcher authorized continued
diagnosis and recovery. Read-only planning and serialization of the full pending
training inputs did not reproduce the native crash; all 59 Python tests pass.
Recovery with coordinator Python fault tracing, unchanged scientific settings
and the existing executor ran for 4,897.27 seconds. The classifier subprocess
reported a JSON decode failure for horizon 11. The executor accepted the failed
group's completed horizons 8–10, drained the other group and accepted its DTW
reference and horizons 1–7, then exited with status 1. Scientific completion
remains at 04.03; neither directional prediction nor later processes ran.
No new segmentation fault was reported during this invocation. The JSON
failure's underlying cause remains unverified. The researcher authorized a
read-only horizon-11 transport diagnosis and recovery even if the cause remains
unresolved. The reconstructed 519,341,858-character request decoded locally and
crossed the existing subprocess transport with an identical SHA-256 checksum;
no model was fitted in that diagnostic. Normal recovery successfully fitted
horizons 11–14 without repeating the JSON failure. The researcher then needed
to disconnect the Mac and authorized Ubuntu-only continuation, operational
corrections and bounded retries without scientific or process changes. The six
idle Mac workers were stopped; after all fits were accepted, remote model
preparation was interrupted before prediction tasks ran. Recovery now uses
`mvp_ubuntu_only`, retaining 28 Ubuntu CPU and sixteen logical GPU workers,
the same 24-group bound and the existing executor. Local services use loopback;
no Mac worker, model transfer or publication delivery is required. All 60 Python
tests pass, including a real local-only scheduler/worker task. Mac synchronization
and publication delivery wait for the host's return. The proposed one-fit-per-group
scheduling change has not been implemented. Accepted science is retained.
Do not reset or recalculate accepted work. The earlier DuckDB read and R JIT
failures remain unexplained and are not attributed to Mac movement.

Ubuntu-only continuation completed Processes 04–05, all 71,859 forecast-accuracy
evaluations and all 80,313 directional evaluations. After directional prediction
finished, the coordinator closed 25 idle native classifier sessions through their
existing shutdown interface, releasing their cached fitted models from memory.
This was an operational action, not automatic lifecycle cleanup or a refit.
Sensitivity accepted 168 candidates before native Python worker segmentation
faults and an R heap-corruption abort ended the invocation. A fresh-process,
unchanged-profile retry retained those candidates and reached 344 of 1,057 before
another Python worker segmentation fault and an R JSON-parsing/garbage-collection
failure. The executor drained successful groups and preserved their commits.
The two invocations have stopped; no coordinator writer or associated workers
remain active. There are 713 sensitivity candidates still pending.

Read-only inspection also found an unresolved fitted-artifact discrepancy:
the horizon-04 Random Forest file's current SHA-256 differs from two earlier
snapshots. Repeated current reads, including a direct disk read, agree with one
another; file modification/change timestamps precede both earlier snapshots.
The other fourteen model-file hashes and the rolling-input logical digest match
the pre-Ubuntu-only snapshot. No model was replaced or refitted. This does not
establish a cause, prove stored predictions wrong, or attribute the failures to
the Mac, hardware or any particular library. Logs, snapshots and the discrepancy
record are preserved for review. The researcher subsequently authorized
continuation, deferring checksum and native-failure diagnosis to a post-mortem.
The current objective is end-to-end completion and comparison with saved
scientific results, not establishing environment integrity through retries.
The researcher requires unchanged configurations. The temporary eight-group
recovery limit was withdrawn; recovery uses the original 24-group bound and
resumes only missing candidates. Worker counts, scientific batches, populations
and process logic are unchanged. The Mac is online again; computation remains on
Ubuntu, with final source/output delivery to the Mac after completion. Following
the reboot, unchanged-profile continuation retained another 24 sensitivity
candidates before a numeric-array decoding failure. Read-only reconstruction
passed the production decoder without reproducing that failure; the original
failed bytes were unavailable. The unchanged retry completed all 1,057 candidates,
selection and publication on 21 October 2026. All six process milestones through
06.07 are accepted in DuckDB; five CSVs and six figure files are produced.
The saved-result comparison covers 103 corresponding values and 28 per-series
classifier localization summaries. Selected lambda pairs remain (1.015, 1.0).
Baseline aggregate directional accuracies match exactly, with OWA differences
below 1.8e-8. Adjusted-method and direct-classifier differences remain real and
are reported without tuning or an invented acceptance threshold in
`results/run_handover.md`. Scientific acceptance of this new methodology run
remains the researcher's decision. Do not reset, refit or tune results to match
the references; successful completion does not resolve the native failures or
the historical fitted-file discrepancy. The final completed-state restart
exited successfully with zero scientific work groups; all atomic records,
milestones, experiment/tracking identities, fifteen model files and eleven
publication files were unchanged. It used the existing two-machine delivery
profile solely to retry copying existing publication files, not to recalculate
science. Final restart and Ubuntu/Mac delivery evidence live under `results/`.

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
