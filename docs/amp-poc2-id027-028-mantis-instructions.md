# AMP implementation instructions: POC2 IDs 027 and 028

## Authority and boundary

Implement the approved architecture in
[`poc2-id027-028-mantis-directional.md`](poc2-id027-028-mantis-directional.md).
Read that document completely before changing code, together with `AGENTS.md`,
the research vision, code standards, workflow decision and execution policy.

This is one bounded Mantis directional increment. It does not authorise a broad
model framework, new research method, Mantis fine-tuning, latest Mantis
representation variant, another dataset/frequency, forecast adjustment, model
artifact service or unrelated refactor. Preserve the closed ID 018 and ID 021
scientific behaviour and evidence.

Do not commit, push, synchronize Ubuntu or begin the full two-machine run until
the Chief Developer approves the local candidate and evidence. Dependency-lock
changes require an explicit inventory and review before synchronization.

## Checkpoint 1: read-only mapping

1. Verify Mac branch, revision and cleanliness. Stop on unrelated changes.
2. Confirm GitHub and Ubuntu status read-only if Ubuntu is available; do not
   modify either.
3. Trace the current version-10 directional planner, Process 03 preparation,
   Process 04 DTW path, Process 05 no-work path, Process 06 evaluation,
   storage validation, distributed worker contract and restart checks.
4. Re-read the historical `mantis_models.py`, `run_mantis_experiment.py`, its
   data bridge, model I/O and historical foundation-model environment.
5. Inspect the pinned Mantis 1.1.0 package and exact local checkpoint without
   network download. Confirm the legacy-compatible final-layer CLS output shape.
6. Report the exact files/tables/contracts that need change, including how the
   existing DTW-only mandatory columns and float64 check will be handled without
   reinterpreting closed databases.
7. Confirm the smallest safe source structure before editing. Use the approved
   names unless a concrete conflict is reported:
   `04_05_encode_mantis.py`, `04_06_classify_random_forest.py`,
   `util/p04_05_directional_mantis.py`, and
   `util/shared_classification.py`.

Stop after this mapping if the approved design conflicts with current durable
contracts or cannot preserve version-10 restart/read behaviour.

## Checkpoint 2: component implementation

Implement the incrementally testable components; do not copy the two historical
files.

1. Add typed, documented input/output objects for prepared identities,
   representation records, classifier specifications/jobs/results and standard
   directional predictions. Use dataclasses or similarly lightweight cohesive
   objects; do not introduce inheritance or a generic workflow language.
2. Implement `MantisRepresentationProvider` in the native worker boundary:
   pinned local checkpoint only, length-64 finite input, float32 conversion,
   linear resize to 512 with `align_corners=false`, frozen legacy final-layer
   CLS representation, bounded batches and explicit device validation.
3. Implement the model-neutral `RandomForestClassifierProvider`. It must accept
   ordinary finite feature matrices and labels, expose small describe/fit/predict
   methods and contain no Mantis, time-series, DuckDB, Prefect or Dask logic.
4. Use `environments/classifiers` as the exclusive Random Forest execution
   environment. Its existing direct lock pins scikit-learn 1.7.2 for Mac ARM64
   and Ubuntu x86_64, matching the historical environment. Do not use Mantis's
   transitive scikit-learn 1.9.1 and do not change either lock without first
   reporting a concrete incompatibility. Record every resolved Random Forest
   parameter. Use 200 trees, seed 42 and one internal thread.
5. Compose the components through `p04_05_directional_mantis.py`. Calculate each
   selected source representation once, then construct fourteen independent
   horizon jobs from the same training/evaluation representation sets and the
   appropriate training-label column.
6. Keep native Torch/Mantis and fitted scikit-learn objects inside their worker
   processes. Return bounded JSON-compatible values and provenance. Do not
   serialize fitted model files in this increment.
7. Add concise Purpose, Inputs, Outputs and usage documentation to every new or
   materially changed script, class and function under the living standard.

Focused tests must cover resize, representation contract, generic Random Forest
use without Mantis, horizon-label selection, fixed-seed reproducibility,
composition, malformed values, explicit device failure and absence of official
labels from Process 04 requests.

## Checkpoint 2 correction: contracts and common-input boundary

Complete this correction before starting Checkpoint 3. Preserve the current
component candidate and make only the following bounded changes.

1. Introduce one model-neutral immutable classification-dataset contract (or
   an equivalent cohesive object) containing the training/evaluation
   representation identities, matrices and fingerprints once. Horizon jobs
   contain the classifier specification, horizon, matching label column and a
   stable reference to that dataset. Do not construct fourteen complete copies
   of the representation matrices in the coordinator.
2. Make result composition accept the expected submitted jobs as well as the
   returned results. Require an exact one-to-one match for job ID, horizon,
   classifier ID and resolved parameters, training/evaluation fingerprints and
   evaluation identities. Reject missing, extra, stale, swapped or internally
   self-consistent results from another request.
3. Validate every returned representation against the requested approved
   representation-definition ID, not merely against the other returned
   records. Preserve the input identity and fingerprint through the adapter.
4. Keep worker/runtime provenance once in a validated response envelope when
   several results share it; persist its association with each classifier run.
   Do not duplicate large provenance structures merely to satisfy the object
   shape.
5. Add an independent historical Random Forest reference test using the legacy
   200-tree, seed-42 configuration on fixed embeddings and labels. Prove its
   binary predictions equal the approved one-thread provider. Repeating the new
   provider twice proves determinism but is not by itself historical parity.
6. Record and test the common-input rule: DTW and Mantis must consume the same
   Process 03 S1 membership, prepared input identities, length-64
   `standardise_sample_v1` values, training labels and protected official
   labels. Mantis-specific float32 conversion and 64-to-512 resize begin only
   inside Process 04.05. Do not add another split, preparation path, table or
   model-specific copy.
7. Retain the approved utility naming convention: `pNN_MM_` for process-owned
   helpers and `shared_` for genuinely reusable components. The six anticipated
   Checkpoint 3 `shared_...` files already exist and are files to modify, not new
   files to add. Do not rename unrelated utilities.
8. Run the focused component suite and the established repository suites with
   their correct locked environments and required `PYTHONPATH`, using
   `--no-sync`. Report broad-discovery limitations separately; do not change an
   unrelated fixture merely to make an ad-hoc discovery command green.
9. Keep scientific identity separate from operational execution provenance.
   Retain hostname, platform, runtime versions, device, worker address and
   elapsed time in validated response/representation evidence, but exclude them
   from representation fingerprints, classification-dataset identities and
   directional-prediction identities, scientific metadata and content hashes.
   An identical retry on another eligible worker must preserve every scientific
   hash. Checkpoint 3 storage must retain the operational association separately.

Stop for Chief Developer review after this correction. Do not start schema,
configuration, planning, Prefect/Dask routing or Process 04-06 integration. Do
not contact Ubuntu, commit, push, install or alter either lockfile.

## Checkpoint 3: workflow and storage integration

Local implementation status (6 October 2026): candidate implemented for Chief
Developer review as configuration version 11. It preserves version 10, plans
one shared Process 03, 1,416 Process-04 tasks, one Process-05 no-work task and
28 Process-06 evaluations. Scientific rows use normalized model-neutral storage;
DTW and classifier lineage remain separate, while host/device/runtime evidence
is retained only in operational execution records. Checkpoint 4 has not started.

1. Create a new experiment configuration version and file for one fresh first-
   100 M4 Daily experiment containing both closed DTW and the new
   `directional_mantis_rf` model. Do not edit or reinterpret the accepted v10
   configuration.
2. Define representation and classifier components centrally and reference them
   from the Mantis composite model. Store the original/resolved documents and
   scientific/operational fingerprints through the existing configuration
   authority.
3. Generalise directional planning and execution narrowly so Process 03 runs
   once, Process 04 dispatches both selected directional providers, Process 05
   stores one deterministic no-work identity, and Process 06 evaluates every
   configured model/horizon.
   Both providers must reference the same stored Process 03 preparation,
   membership, input identities, transformed values and labels. Provider
   adapters begin only after that shared boundary.
4. Extend DuckDB with the minimum normalized definitions, representations,
   classifier runs and model-neutral prediction lineage needed by the approved
   document. Do not fabricate DTW fields for Mantis. Preserve exact v10 DTW
   behaviour and existing-database read/restart compatibility.
5. The Mac remains the sole writer. Workers receive bounded immutable payloads,
   never database paths or connections. Validate complete identities and
   contents before insert-or-verify commits.
   Reuse/scatter the immutable classification dataset rather than materialising
   fourteen complete matrix copies.
6. Establish stable task identities so representation batches, individual
   horizons and evaluations resume independently. A failed horizon must not
   invalidate accepted embeddings or other completed horizons.
7. Route dependencies through the existing Prefect flow and eligible work
   through Dask. Do not add a second scheduler, manual multiprocessing pool or
   model-owned retry loop.

Run focused and established fast/integration tests locally with locked,
no-synchronization environments. Use isolated temporary databases only. Report
exact counts, schema changes, locks, source paths, legacy compatibility and any
deviation. Stop for Chief Developer review before committing or remote work.

## Checkpoint 4: bounded execution calibration

After local candidate approval, synchronize the exact candidate revision to the
available Ubuntu checkout without GitHub publication. Verify clean source,
locks, checkpoint revision/checksums, Python/runtime versions, GPU readiness,
ports, memory floors and absence of stale owned processes.

Run a bounded real-data fixture that proves:

- the checkpoint loads offline once on the approved Ubuntu GPU;
- embedding batches are finite and have the expected dimension;
- Mac and Ubuntu can run eligible one-thread Random Forest horizon jobs;
- fixed embeddings/labels yield identical predictions across execution modes;
- sequential, local-Dask and distributed composition agree; and
- resource monitoring and safe cleanup work.

Start with one Mantis GPU process. Increase GPU-process concurrency only if
measured throughput improves and accelerator/host memory floors remain safe.
Do not create fifteen Mantis model copies solely to occupy configured logical
GPU capacity. Present calibration evidence and the proposed final settings for
approval before the full experiment.

## Checkpoint 5: full acceptance and closure

Only after explicit approval, run a fresh isolated first-100 M4 Daily experiment
through Processes 01-06 on Mac and Ubuntu. It must use every S1 training window,
the exact approved component definitions and the existing accepted DTW method.

Required durable counts are:

- two directional model definitions;
- 495 S1 training and 100 official Mantis representations;
- fourteen Mantis classifier-run records;
- 1,400 Mantis predictions;
- 1,400 DTW predictions;
- one Process 05 no-work record; and
- 28 directional evaluations.

Accuracy is recorded only. Acceptance concerns correct scientific contracts,
execution, storage, coordination and restart—not which model is more accurate.

Run the supported restart from DuckDB authority without the creation JSON. It
must submit zero new preparation, representation, classification, DTW,
prediction or evaluation work and preserve all identities, counts, values and
fingerprints.

Report source/lock/checkpoint equality, task and host contributions, runtimes,
resource minima/peaks, failures/retries, exact counts, duplicate checks,
fingerprints and limitations. Stop for Researcher/Chief Developer acceptance.
Commit, push and final machine synchronization require a separate explicit
closure approval; do not infer it from successful tests.
