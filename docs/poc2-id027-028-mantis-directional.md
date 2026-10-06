# POC2 IDs 027 and 028 Mantis directional architecture

Status: Approved by the Researcher and Chief Architect on 6 October 2026.
Checkpoint 2 is accepted. The local Checkpoint 3 version-11 workflow and
additive-storage candidate is implemented for Chief Developer review. ID 026
subsequently approved fitted-meta-learner persistence and must be integrated
before bounded execution and scientific acceptance.

## Decision

IDs 027 and 028 form one ShapeFM increment: the Mantis directional baseline.
The historical `mantis_models.py` scientific method is migrated selectively;
the historical `run_mantis_experiment.py` runner is not imported. ShapeFM owns
configuration, orchestration, parallel execution, storage, restart and
evaluation.

The researcher-facing model is `directional_mantis_rf`. It is one black box at
the directional-model boundary, composed internally from two reusable parts:

```text
Mantis representation provider -> Random Forest classifier
```

Mantis is a frozen representation provider. Random Forest is a general ShapeFM
classifier and must not depend on Mantis. Prefect owns dependencies, Dask owns
eligible computation, and the Mac coordinator is the sole DuckDB writer.

## Scientific definition

For every prepared input, preserve the historical method:

- input: one finite `standardise_sample_v1` array of length 64;
- resize: linear interpolation to 512 with `align_corners=false`;
- encoder: pinned `paris-noah/Mantis-8M` checkpoint and revision;
- representation: legacy-compatible final-transformer-layer CLS token;
- representation dtype and size: float32, 256 values;
- adaptation: frozen embeddings only; no Mantis fine-tuning;
- classifier: scikit-learn Random Forest, one independent binary model per
  horizon;
- horizons: 1 through 14;
- trees: 200;
- classifier seed: 42 for every horizon;
- training membership: every S1 training window, without cap or sampling; and
- output: one `directional_strict_v1` prediction in `{0, 1}` per official input
  and horizon.

The experiment seed remains 1234. The classifier seed 42 is a separate explicit
scientific model setting. Record the complete resolved Random Forest parameters
and pin scikit-learn 1.7.2, matching the historical environment. Operational
thread count is one; Dask, not scikit-learn, owns parallelism.

The native Mantis worker runs only in the locked `environments/mantis`
environment. The model-neutral Random Forest worker runs only in
`environments/classifiers`, whose existing direct lock pins scikit-learn 1.7.2
for Mac ARM64 and Ubuntu x86_64. Mantis's transitive scikit-learn 1.9.1 is not
the Random Forest authority and is never imported by the classifier worker.
This separation requires no Mantis-environment or lockfile change.

Do not silently replace this representation with the newer MantisV1
intermediate-layer/combined-token recommendation. That would be a different
research model. Do not train one multi-output forest or derive different seeds
per horizon.

## Process flow

| Process | Responsibility | Durable output |
| --- | --- | --- |
| 01 Import | Import the selected M4 Daily records from pinned GIFT-Eval data. | Canonical series and benchmark identity in the experiment DuckDB. |
| 02 Preprocess | Preserve the existing experiment preprocessing stage. | Existing preprocessing records. |
| 03 Prepare | Reuse the accepted bounded directional preparation, S1 membership and labels. | S1 windows/labels in the linked windows DuckDB; official inputs and protected actual labels in the experiment DuckDB. |
| 04.05 Represent | Resize prepared arrays and calculate frozen Mantis embeddings once. | Representation definitions and one verified representation per selected source object. |
| 04.06 Train | Fit and publish one Random Forest per horizon. | Fourteen classifier-run and verified fitted-artifact records. |
| 04.08 Predict | Load every required fitted classifier and predict every official representation. | Standard binary directional predictions with fitted-artifact lineage. |
| 04 output boundary | Expose the composed Mantis/Random-Forest result as one directional model. | Model-neutral `DirectionalPrediction` records. |
| 05 Combine | Perform no directional combination in this increment. | One deterministic no-work record. |
| 06 Evaluate | Compare stored predictions with protected actual labels. | Accuracy per directional model and horizon. |

The S1 test partition remains available but is not calculated merely to reproduce
the historical diagnostic report. It neither trains nor tunes the historical
Mantis model and is not needed for the approved official Figure 2 result.

## Common upstream comparison boundary

DTW and Mantis/Random Forest must branch from the same accepted Process 03
objects. Process 03 runs once and owns:

- the S1 membership and its fingerprint;
- every selected S1 training window;
- the same final 64 observations for every official input;
- the same bounded robust cleaning and `standardise_sample_v1` result;
- the same `directional_strict_v1` training and protected actual labels; and
- the same source-series, preparation, forecast-origin and input identities.

No Mantis-specific split, cleaning, standardisation, label calculation,
official-input table or Process 03 rerun is permitted. The current S1 contract
persists train and test membership but no separate validation partition. Both
models use the complete S1 training membership. DTW's leave-one-source-out
width calibration is an internal training calculation; Mantis uses fixed
approved settings and therefore has no corresponding tuning requirement. The
S1 test membership remains unused for this Figure 2 comparison.

Provider-specific work begins only after the common Process 03 boundary:

```text
common float64 length-64 standardised input
    |-- DTW adapter: direct aeon distance calculation
    `-- Mantis adapter: float32 -> linear resize 64 to 512
                       -> frozen 256-value embedding -> Random Forest
```

The Mantis dtype conversion and resize are conscious, historical
model-interface requirements. They must retain the common input identity and
fingerprint and record their own representation definition and fingerprint.
They do not authorise a different research population or upstream preparation.
Any future exception requires a separate approved architecture decision.

## Object contracts

### PreparedDirectionalInput

Process 03 owns this model-neutral input. It contains the stable input and
source-series identities, role (`training` or `official_evaluation`), finite
standardised values, label reference, permitted training labels, preparation
definition and fingerprint. Official future labels never enter a Process 04
worker payload.

### MantisRepresentationProvider

This cohesive provider owns the native Mantis boundary. Its small operations
describe and validate the provider, load the pinned local checkpoint, reshape
and resize bounded batches, calculate embeddings, and release resources. Torch
tensors, `MantisTrainer` and the loaded network remain inside the provider.

It does not read or write DuckDB, train classifiers, schedule work, calculate
accuracy, download changing weights during scientific execution or silently
substitute another device.

### RepresentationRecord

This portable result contains representation and source identities, role,
definition identity, the finite 256-value embedding, dtype/dimension, input and
embedding fingerprints, and worker provenance. Persist all 495 S1 training
representations and 100 official representations for the accepted 100-series
experiment. This avoids fourteen repeated Mantis passes and provides a durable
restart boundary.

### ClassifierSpec and RandomForestClassifierProvider

`ClassifierSpec` contains a classifier identity, implementation/version,
complete resolved parameters, seed, accepted feature shape/dtype and output
contract. `RandomForestClassifierProvider` accepts any valid finite two-
dimensional feature matrix and labels; it must not import or interpret Mantis,
time-series arrays, rolling windows, Torch or DuckDB.

The provider exposes small `describe`, `fit`, `save`, `load` and `predict`
operations. ID 026 provides the approved use case and separate decision for
serialization: one fitted Random Forest per horizon is published through the
shared model store as a trusted `joblib` artifact outside DuckDB. Training and
prediction are separate Process 04 substeps. Definitions, training
identities/fingerprints, embeddings, artifact checksums and predictions remain
durable research evidence.

### Classifier training and prediction jobs

One immutable classification dataset owns the training and official
representation identities/values once, with complete dataset fingerprints.
One independent training job owns one horizon, its classifier specification
and matching training-label column. Its result identifies the fitted artifact
and training fingerprint but contains no official predictions. Do not
materialise fourteen coordinator-side copies of the representation matrices.

After every required fitted artifact is available, one independent prediction
job per horizon loads it and uses the official representations. Before
composition, the Mac validates a one-to-one match between every submitted job,
artifact and result: job identity, horizon, classifier definition, training and
evaluation fingerprints, expected evaluation identities and parameters must
agree, with no omissions or extras. A set of internally valid but stale or
unrelated results must be rejected.

The validated response envelope retains operational evidence such as hostname,
platform, runtime versions, device, worker address and elapsed time. Its response
identity and provenance fingerprint may therefore differ across workers or
retries. Those operational fields do not enter classifier-run or directional-
prediction scientific identities, metadata or content hashes. Checkpoint 3
storage must associate the envelope with its execution while keeping scientific
and operational metadata separate.

### DirectionalPrediction

This is the common external result for DTW and Mantis/Random Forest. It contains
experiment, model, evaluation-input and horizon identities, the binary
prediction, scientific metadata and content hash. DTW neighbour/width evidence
and Mantis representation/classifier lineage remain provider-specific metadata;
Process 06 consumes the common fields only.

Mantis representation values and fingerprints likewise exclude worker hostname,
platform, device and duration. Worker provenance remains operational evidence.
Retrying identical representation or classification work on another eligible
worker must preserve representation, dataset and directional prediction hashes.

## Implementation structure

Use real process substeps and the existing naming standard after the ID 026
lifecycle integration:

```text
src/python/04_05_encode_mantis.py
src/python/04_06_train_random_forest.py
src/python/04_08_predict_random_forest.py
src/python/util/p04_05_directional_mantis.py
src/python/util/shared_classification.py
```

- `04_05_encode_mantis.py` is the isolated native Mantis worker.
- `04_06_train_random_forest.py` is the isolated model-neutral training worker.
- `04_08_predict_random_forest.py` loads fitted classifiers and predicts; it
  never fits.
- `p04_05_directional_mantis.py` composes and validates the Process 04 work
  without becoming another scheduler.
- `shared_classification.py` owns the reusable classifier contracts and Random
  Forest adapter.

These names follow the approved utility standard. `p04_05_` identifies the
Process 04-owned composer. `shared_classification.py` is genuinely reusable
outside Mantis, as required for the general Random Forest component. The six
`shared_...` integration files named for Checkpoint 3 already exist; they are
integration owners to modify, not six new utility files to create. Do not
perform a repository-wide naming migration in this increment.

Reuse the existing entry point, Prefect/Dask workflow, configuration authority,
directional preparation and storage components. Do not add `mantis_models.py`,
`run_mantis_experiment.py`, RDS/rpy2 bridges, CSV result stores, manual loops or
a second orchestration path.

## Configuration and storage

A new experiment configuration must preserve the closed version-10 DTW
experiment and define one fresh experiment containing both `directional_dtw`
and `directional_mantis_rf` under the same Process 03 inputs. Define reusable
representation and classifier components centrally, then reference them from
the composite model. Original and resolved configuration remain authoritative
in DuckDB.

Extend the current directional storage only as needed to represent:

- representation definitions;
- stored representations;
- classifier definitions;
- fourteen classifier-run identities;
- composite directional model definitions;
- model-neutral predictions; and
- per-model, per-horizon evaluations.

The apparently generic current model/prediction tables contain mandatory DTW-
specific fields and a float64-only model check. Generalise future schemas so
Mantis does not fabricate neighbour, distance, width or dtype values. Preserve
closed version-10 databases and DTW behaviour; migrate neither historical
scientific content nor accepted evidence silently.

## Execution and recovery

The Mac coordinator reads accepted state and constructs bounded payloads; no
worker receives a database connection. Process/environment boundaries use the
approved JSON-compatible contract. Ordinary in-process Python uses typed objects
and arrays rather than unnecessary JSON conversions.

Run Mantis representation batches on the approved Ubuntu accelerator resource.
Load the checkpoint once for the complete representation phase. Begin with one
GPU process and increase only if measured calibration proves a benefit within
the approved memory floor. Do not launch fifteen checkpoint copies merely
because fifteen logical GPU slots exist.

Submit the fourteen horizon training jobs independently across Mac and Ubuntu,
then submit prediction work only after all required fitted artifacts are
available through the ID 026 model store. Each job uses one internal
computational thread. Scatter or otherwise reuse the immutable classification
dataset instead of creating fourteen complete matrix copies or
rereading/retransmitting it unnecessarily. The Mac validates exact
request/response and fitted-artifact lineage and performs every durable write.

Stable identities and insert-or-verify semantics apply at each durable boundary.
A restart skips completed preparation and representation work, reports existing
fitted classifiers as `skipped_existing`, and loads them for any missing
prediction work. If one horizon fails, retry that horizon, not Mantis embedding
or completed classifier horizons.

The version-11 candidate keeps operational response and representation-worker
provenance in separate execution records. Host, worker address, device and
elapsed runtime do not enter representation, classifier-run, prediction or
evaluation scientific hashes. DTW neighbour/distance/width evidence and Mantis
representation/classifier evidence use separate provider-lineage tables behind
one model-neutral prediction contract. Version 10 remains readable and retains
its original DTW tables and task identities.

## Acceptance

First prove component contracts on bounded fixtures:

- resize parity with the historical helper;
- embedding parity using the exact pinned checkpoint and device;
- generic Random Forest operation without Mantis;
- historical Random Forest prediction parity from fixed embeddings/labels,
  comparing the historical 200-tree, seed-42 configuration with the approved
  one-thread provider;
- composed Mantis/Random-Forest directional output;
- rejected malformed/non-finite identities and outputs; and
- absence of official future labels from Process 04 payloads.

Integration tests must also prove that DTW and Mantis reference the same
Process 03 preparation, membership, input identities, transformed values and
labels, and that provider-specific conversion starts only in Process 04.

Then run one fresh two-machine M4 Daily first-100 experiment through Processes
01-06. It must use the complete S1 training membership and produce:

- two directional model definitions: DTW and Mantis/Random Forest;
- 495 training plus 100 official Mantis representations;
- fourteen Mantis classifier-run records;
- one verified DTW multi-horizon artifact and fourteen verified Mantis/Random-
  Forest horizon artifacts outside DuckDB;
- exactly 1,400 Mantis and 1,400 DTW predictions;
- one Process 05 no-work record; and
- 28 directional evaluations.

Both machines must perform real eligible work. Record source, environment,
checkpoint, component, task, host, resource and fingerprint evidence; require
zero duplicate or failed accepted records. Accuracy is a research result, not
an acceptance threshold. A supported restart must perform no DTW calibration
or Random-Forest fitting and must preserve every count, prediction, evaluation
and fingerprint.
