# POC2 capped training cohort and fixed DTW: 12 October 2026

Status: **Part B accepted and closed by the researcher on 14 October 2026**;
scientific results are sufficiently similar for the POC2 objective. This document
owns the implementation contract; the [authoritative completion record](poc2-id038-041-implementation-evidence.md#authoritative-part-b-acceptance-and-closure-14-october-2026)
owns acceptance, execution/restart evidence and open closure findings. **POC2
remains open**, including exact visual reproduction under POC2-X08. Historical
ID 021 and IDs 026–028 closure, accepted data and the tuned bakeoff remain
untouched. The new design is not a numerical reproduction claim for those results.

## New experiment and shared cohort

Use configuration version 13 and a fresh experiment/database:
`config/experiments/poc2_m4_daily_full_capped_fixed_dtw.json`, experiment name
`poc2_m4_daily_full_capped_fixed_dtw_20261012`. Include all 4,227 official M4
Daily series. Preserve the normal **uncapped S1 split first**. Apply the cap
only to eligible S1 training references, once, before either provider branches:
select exactly 10,000 when more are eligible; otherwise use all. Test membership
and all 4,227 official inputs are uncapped.

Sampling uses seed 1234 and the historical proportional first-horizon label
stratification from `rafmontano/m4_tsc_fmts_2026` revision
`839b5b6dc4334b5f088adce610f6d35935cbbca1`, `src/r/08_split_index.R`:

1. Compute proportional floor quotas, with a minimum of one per class.
2. Allocate the remainder cyclically in descending class-count order, **not**
   largest-fraction order; preserve historical ordering and capacity handling.
3. Use class-local R `sample`, then the final R permutation.
4. When the cap is a no-op, retain eligible order and consume no RNG draws.

Use the pinned R RNG, not a Python approximation. Persist the actual ordered
selected members, full eligible first-horizon-label fingerprint, cohort
fingerprint, R version and `RNGkind`. The seed alone is not reproducibility
evidence. Restart must verify the exact cohort and its source fingerprint before
reusing dependent work. DTW and Mantis consume these same canonical Process 03
members, prepared inputs and labels; provider-specific conversion starts only
after that boundary. Official future labels remain protected from workers.

## Fixed DTW and unchanged Mantis

Direct `aeon.distances.dtw_distance` at aeon 1.6.0 remains the sole DTW engine.
Use float64 length-64 inputs, `window=0.10`, effective width
`int(0.10 * 64)=6` for every horizon. Perform one 1-NN query per official input
and reuse that neighbour's stored labels at horizons 1–14. Preserve squared
Euclidean local cost and equal-distance selection of the lowest stable
reference-window identity. Do not introduce a calibration grid, calibration
tasks, calibration scores or same-series calibration exclusion.

Persist fourteen configuration-derived width rows with
`calibration_accuracy=NULL` and `tie_rule=fixed_configuration`. Parent schema
13 requires a nullable-accuracy migration; it must preserve historical rows and
their scientific meaning. Retain the shared model store and one complete DTW
multi-horizon artifact. Prediction loads fitted artifacts and never trains.

Mantis uses the same capped cohort without changing its pinned frozen encoder,
representation, 200-tree per-horizon Random Forest or classifier seed 42.
Sampling seed 1234 remains distinct from seed 42. Keep fourteen fitted classifier
artifacts and existing provider-owned serialization boundaries.

## Workflow, durability and entry point

```text
01 Import all 4227 -> 02 Preprocess -> 03 normal S1 split + bounded inputs
                                      -> one persisted shared cap10000 cohort
04 Prefect/Dask: cohort -> fixed aeon DTW / frozen Mantis -> RF(seed42)
                       -> bounded prediction outcomes -> immediate Mac commits
05 stored forecast combination / directional no-work -> 06 evaluate + tables
```

The six-process architecture is unchanged: Prefect owns orchestration, Dask
schedules eligible work, 8 Mac CPU workers and 15 Ubuntu CPU workers use one
internal computational thread each, and one Ubuntu GPU worker serves Mantis
and Chronos through their existing provider lifecycles. The Mac is the sole research DuckDB writer. Workers receive
immutable scientific inputs, never database connections. No alternate runner,
nested scheduler or second scientific store is authorised.

Yield bounded outcomes as they complete; the coordinator validates and commits
each immediately with insert-or-verify semantics. A later failed batch must not
erase already completed predictions. Restart verifies the cohort exactly and
skips complete identities, scheduling only missing work. Operational completion
order, host and batch size do not alter scientific identity.

During the fresh run on 13 October, all 59,178 DTW horizon predictions and all
fifteen fitted directional artifacts were persisted before a Mantis dataset
staging interruption. Dask's simultaneous full-dataset broadcast failed with
macOS `OSError: [Errno 55] No buffer space available`; the scheduler log records
the failed TCP broadcasts. This was not a DTW subprocess/native crash. The
operational correction stages the unchanged fingerprint-verified classification
dataset one worker at a time, without changing computation concurrency, worker
counts, models or membership. The interruption databases and logs are preserved
under the experiment's `evidence/interruption-01/`. Supported restart must reuse
completed scientific work; the later restart completed as recorded below. The
49 comparison-pipeline, experiment-execution and Mantis-component regressions
passed. A disposable real two-worker Dask fixture exercised the production
staging block twice, verifying bounded transfer, unchanged payload fingerprint
and existing-cache reuse. No dependency, model or configuration changed.

The existing entry point remains authoritative. The following is the historical
fresh-run command, not an instruction to recreate the now-accepted experiment:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database results/poc2_m4_daily_full_capped_fixed_dtw_20261012.duckdb \
  --configuration config/experiments/poc2_m4_daily_full_capped_fixed_dtw.json \
  --processes 1-6 --execution-profile poc2_seasonal_recovery
```

For an existing database, omit `--configuration` on restart. This documented
command records the original invocation; no scientific rerun is authorised by
the Part B documentation closure.

## Implementation validation

On 12 October, the locked focused/regression selection passed **112 tests**,
plus **65 established fast execution, experiment and paper-table tests**,
and the pinned classifiers interpreter passed **13 DTW tests**. These cover
exact 10,000 membership, class proportions, deterministic fingerprints and
restart conflict rejection; real Process 03 shared DTW/Mantis inputs; fixed
proportion and cross-horizon labels; fourteen NULL-accuracy configuration rows,
zero scores, and persistence through a later failure followed by missing-only
restart. A stale hardcoded schema-12 assertion was changed to the current
`SCHEMA_VERSION` (13); historical configuration and result rows remain intact.

A disposable two-worker LocalCluster exercised the actual Prefect/Dask and
isolated fitted-model prediction boundaries: **28 predictions** matched the
independently expected neighbours, zero distances, labels and width 6 exactly
in sequential and distributed modes. R historical sampling/RNG checks passed,
as did the existing forecast-method checks for nine registered methods and all
four paper-metric/three JSON-worker groups. No dependency or model lock changed.
The bounded fixture is not a claim of full two-machine scientific acceptance.

The implementation uses cohesive existing coordinator/provider objects and the
small `TrainingCohort` persistence object. The stateless R sampling kernel and
small fixture functions are bounded OOP exceptions; no new runner or scheduler
was introduced. Previous failed experiments were not opened for writes.

The accepted full-run evidence accounts for all 4,227 official inputs and 59,178
predictions per directional model, fourteen widths with no calibration scores,
unchanged scientific hashes on restart, model artifacts, host/resource
contribution and Process 06 outputs. Researcher acceptance applies to the Part B
scientific objective, not exact visual reproduction or final POC2 closure.

## Mac-only Process 06 recovery and X18 QA follow-up

On 13 October the researcher authorised correction of the `sequential_safe`
entry-point routing, then one Process 06-only recovery of the existing database.
`ProcessAction._execution()` previously treated every explicit profile without
a local-heavy exception as distributed: `sequential_safe` supplied zero Dask
workers, so `ExecutionSettings.dask_expected_workers` failed before cluster
startup, candidate scheduling or database writes. The GPU count was zero, not
negative; the combined validation message obscured the actual failing field.

The profile's existing name is the explicit local sequential routing rule (no
configuration schema change). It now returns sequential settings with one local
calculation in flight, zero retries and no cluster. Process 06 uses the existing
Prefect local CPU/R kernel and unchanged scientific identities. Distributed
profiles retain their topology, positive-worker validation and managed lifecycle;
heavy Process 04 approval checks remain intact. No GPU is enabled by this route.

Seven focused tests passed, including CLI Process 06 selection, local routing,
invalid distributed topology rejection, existing distributed propagation and
the real synthetic sequential/local-Dask 1,057-candidate kernel/identity parity.
The Dask fixture was local CPU-only, not the production recovery. Isolated
pre-execution routing left the authoritative database byte-identical and returned
no cluster, no accelerator and one in-flight calculation.

For later **POC2-X18 pipeline/usability QA**, review sequential versus distributed
routing, profile-to-execution consistency, field-specific validation messages,
and whether researcher-facing status makes the selected mode explicit. This is
a recorded follow-up, not authorisation to begin the broader X18 refactor.
The recovery console does not announce the selected execution mode; the execution
record does. All 46 scientific tasks can be completed while Process 06 still
shows `running` during integrity validation. That validation rebuilds all 1,057
candidate input identities without candidate-level console progress. Distinguish
calculation, persistence and validation in the later usability review; do not
remove integrity checks to make the status appear faster.

The single recovery completed all Process 06 substeps in 11,072 seconds:
278 stored candidates were preserved, 779 missing candidates were calculated,
and all 1,057 candidates and 46 tasks completed without failure. Stored outputs
include nine selections, 12,681 diagnostic means, 126 paper directional rows,
28 DTW/Mantis evaluations and 17 official evaluations. Table 1 has seven rows;
Daily Table 2 panels A/B/C have 8/7/7 rows. Processes 01–05 were not rerun.
They had completed through the approved distributed Mac/Ubuntu execution.
Before recovery, Process 06 stored 278 candidates and then stopped when an
Ubuntu R subprocess suffered a native memory-access failure during `jsonlite`
worker-request decoding. That crash was neither diagnosed nor fixed.
Production recovery used local sequential CPU/R execution, no Dask or GPU,
and no Ubuntu contact. Sampled available memory stayed above 3.06 GiB, peak
swap was 1.99 GiB and no below-2-GiB warning samples occurred.

Declared CSV/PDF/PNG outputs and the manifest were exported beneath the existing
experiment results directory. Export left the database byte-identical. One
Mac-only restart returned `skipped_completed`; scientific counts/fingerprints,
task attempts, fitted artifacts, cohort/windows checksum and exports were
unchanged. Detailed generated evidence remains untracked in that experiment's
`evidence/`, including `mac-sequential-final-verification.json`. The two named
standalone CD exports are byte-identical: the current renderer calls the same
plot for both names. Record distinct presentation and Daily/axis labeling as
X08 follow-up, not a renderer change within this recovery.

The read-only stored-forecast alternative-source comparison completed without
refitting or reselection. GIFT-Eval remains authoritative and its accepted
informational reconciliation is unchanged. SMAPE/MASE/OWA differences were below
3.4e-8, with unchanged model ordering. A material reporting caveat remains:
Naive2 horizon-14 directional accuracy changes from 32.1031% to 45.7298% under
M4comp2018 observations because tiny origin differences cross frozen-forecast
direction boundaries; 4,117 of 4,227 stored Naive2 paths equal their GIFT origin
at every horizon. DTW/Mantis correct counts and tie-aware CD ranks are unchanged.
This sensitivity remains a disclosed interpretation caveat, not replacement of
authoritative data or a failed-recovery classification. Generated comparison
evidence is retained under `evidence/alternative-source-*`. Researcher acceptance
on 14 October closes Part B only; it does not establish exact historical accuracy
or visual reproduction, repair of the Ubuntu path or POC2 closure. The Ubuntu
native-crash cause remains unconfirmed. POC2-X19 (bounded work-item retry/failure
containment) and POC2-X20 (bounded Ubuntu R-worker qualification) remain open in
the authoritative record; X18 retains visibility/usability. No backlog
implementation, crash diagnosis, scientific rerun or cleanup accompanies closure.
