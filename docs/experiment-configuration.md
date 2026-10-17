# Experiment configuration and storage

## Active configuration

The repository has one active bootstrap:

```text
config/
├── dependencies/
│   ├── gift_eval.json
│   └── models.json
├── execution_profiles.json
├── experiment.json
└── machines.json
```

- `experiment.json` defines the research data, processes, models, metrics and
  seed for the experiment being created.
- `machines.json` defines stable host names, project roots, capabilities and
  service ports.
- `execution_profiles.json` selects the coordinator, workers and operational
  capacities.
- dependency files and lockfiles pin external data/model/software assets.

Do not create experiment-named JSON files. Tests derive variants in temporary
directories. Git history and completed DuckDB files preserve older definitions.

## Bootstrap and authority

Before creation, edit `config/experiment.json`. Process 01 resolves and stores
the complete configuration in `results/experiment.duckdb`. From that point the
database—not the JSON filename—is the experiment authority and restart source.
Deleting or changing the JSON does not redefine an existing database.

Scientific configuration and operational execution are separate. Data,
preprocessing, transformations, methods, model parameters, horizon, seed,
metric profile and cohort membership affect scientific identity. Host, worker
count, addresses, batch size, completion order, elapsed time, hardware and backend
do not. These are operational provenance, not grounds to recalculate accepted
DuckDB work. Ordinary valid floating-point differences are accepted.

## Fixed generated layout

```text
results/
├── experiment.duckdb
├── windows.duckdb              created by Process 03
├── tables/
└── figures/

models/
├── directional_dtw/daily/model.pickle
└── directional_mantis_rf/daily/horizon_01.pickle ... horizon_14.pickle

.runtime/
├── prefect/                   operational orchestration state
├── dask/                      operational scheduling state
└── mlflow/
    ├── tracking.sqlite        operational tracking, not scientific authority
    └── artifacts/<run-id>/artifacts/
```

There is no experiment name, date or retry number in generated filenames. Those
details live inside DuckDB. No third research database or sibling evidence tree
is authorised. Logs/caches are runtime material and remain untracked.

Normal researcher commands write directly to the repository's canonical paths,
not a review workspace. Temporary test roots clean up after themselves. Fitted
paths stored in DuckDB are relative to this root; MLflow local artifact URIs
resolve under canonical `.runtime/mlflow/`. No symlink or `.amp` copy is needed.
Visible model paths contain no scientific experiment IDs. Path normalization
moves fitted files byte-for-byte and changes only operational model-path fields
in the accepted database; scientific identities, settings and stored
configuration remain unchanged. No MLflow model artifacts exist, so this model
path normalization requires no MLflow metadata changes.

The parent owns resolved configuration, canonical series, process/item state,
forecasts, combinations and evaluations. The child owns high-volume preparation
data. Before Process 03 it is legitimately absent; after Process 03 a missing or
incompatible child is a recovery error.

## Experiment lifecycle

- **New:** both databases and derived outputs are absent; Process 01 reads the
  bootstrap.
- **Running/resuming:** use stored configuration and submit only uncommitted
  work for the requested process range.
- **Extending:** unchanged completed model/results remain; newly approved models
  or work calculate only their delta.
- **Replacing:** stop writers, then explicitly clear both databases, generated
  tables/figures and experiment-owned fitted models as one experiment package.
  There is no implicit reset in `run`.
- **Archiving:** optional NAS copies are point-in-time researcher evidence;
  code/configuration remain recoverable from Git.

## Fitted models

Only expensive reusable meta-learner state is stored outside DuckDB: currently
DTW fitted reference state and one Mantis/Random-Forest classifier per horizon.
Ordinary forecast-pool fits are disposable. Provider environments serialize and
load their own objects from local files. The coordinator publishes complete
canonical files and stores model paths and lineage without deserializing them.

Default behaviour is reuse: accepted training items are not fitted again.
Pending items publish their files before completion is committed. A missing
worker copy may be recopied; a lost authoritative accepted file is a recovery
error requiring researcher direction, not grounds to delete completion records
or automatically retrain. See [directional models](directional-models.md) for
ordinary rsync delivery and session-local loading.

## Researcher status

Status reads DuckDB without modification. It shows experiment/run identity,
accepted process/subflow milestones and atomic result counts. MLflow supplies
operational milestone/error visibility. Status does not validate scientific
outputs, repair state, contact workers or infer an unrecorded ETA.
