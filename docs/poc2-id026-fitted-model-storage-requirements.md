# POC2 ID 026 fitted-model storage requirements

Status: Requirements approved by the Researcher and Chief Architect on
6 October 2026. Architecture design and implementation are not yet approved.

## Purpose

ShapeFM needs one simple mechanism for saving and retrieving fitted models. It
must avoid unnecessary retraining, train only newly added models, permit
deliberate replacement, separate training from prediction, and hide physical
storage details from scientific scripts.

ID 026 starts from the intent of the historical
`src/python/model_io.py`, but that module is not imported directly. The design
must consider training, storage, loading, prediction, restart and distributed
execution end to end under the current ShapeFM standards.

## Scope

This increment applies to expensive fitted meta-learners, initially the
accepted directional DTW baseline and the Mantis/Random-Forest baseline. It
covers:

- fitted-model storage outside DuckDB;
- one storage abstraction for Python and R model providers;
- model existence checks, saving and loading;
- deliberate overwriting;
- missing or unreadable model handling; and
- incremental addition of meta-learners to otherwise unchanged experiment
  inputs; and
- retrofit of DTW and Mantis/Random Forest to the same fitted-model lifecycle.

Ordinary forecast-pool methods such as AutoARIMA, ETS, NNETAR, Naive2,
Chronos-2 and the other accepted ID 018 methods remain bounded fit-and-forecast
operations. Their fitted per-series objects are not persisted by ID 026.

It does not cover a researcher user interface, spreadsheet, dashboard, global
cross-experiment registry, automatic retention or archival workflow, storage
quotas, multiple retained generations of one logical model, or automatic
scientific compatibility inference. The need for a simple human-friendly view
of experiment intent, progress and requested changes remains open; its solution
is not selected by ID 026.

## Storage boundary

Fitted model binaries live outside DuckDB. DuckDB may retain experiment
configuration, task state, predictions, evaluations and execution evidence,
but it is not the model-file store.

Every ShapeFM script uses one model-storage interface. Scientific scripts do
not construct paths, know the storage root, implement separate existence
checks, or overwrite files directly. Each language may retain its suitable
native serialization format while following the same workflow contract.

The storage root is centrally configured. Mac and Ubuntu may use different
physical roots while resolving the same logical model identity. The detailed
transfer mechanism belongs to the architecture design.

## Minimum logical identity

The current workflow locates a fitted model by:

- model or researcher-defined variant identifier;
- frequency; and
- forecast or classification horizon, or one explicit multi-horizon scope when
  the fitted state is naturally shared across horizons.

No researcher-facing training key or global artifact catalogue is required for
this increment. Scientifically different models that must coexist use distinct
variant identifiers. Mantis/Random Forest owns one fitted classifier per
horizon. DTW owns one frequency-level fitted artifact containing the common
reference library and every selected horizon width so the reference data is not
duplicated fourteen times.

## Training contract

The training process evaluates every required
`model x frequency x horizon-or-approved-multi-horizon-scope` combination.

| Stored state | Overwrite | Required behaviour |
| --- | --- | --- |
| Missing | Either | Train, save and mark the training task complete. |
| Present | `false` | Do not train; report `skipped_existing` and continue. |
| Present | `true` | Train a replacement and replace the saved model only after the new save succeeds. |

`overwrite=false` is the default. A failed training or save must not destroy a
previous valid fitted model. Overwriting one model scope requires its dependent
predictions and evaluations to be recalculated; unrelated models remain
unchanged.

For unchanged data and conditions, adding a model trains only the missing
model/frequency/horizon combinations. Existing models and results remain.

## Prediction contract

Prediction does not train models. Every required fitted model must have been
created by the preceding training process.

| Stored state | Required behaviour |
| --- | --- |
| Present and readable | Load and predict. |
| Missing | Stop the affected prediction work and report the exact model, frequency and horizon. |
| Invalid or unreadable | Stop the affected prediction work and report the failure. |

Prediction must not train a missing model, substitute another model or silently
continue with incomplete coverage.

## Changed inputs and manual deletion

The model-storage component does not decide whether changed data,
preprocessing or labels invalidate an existing model. The experiment plan owns
that scientific decision. A material input change uses a fresh model-storage
location, requests overwrite for every affected model, or removes the affected
files before training. Automatic dependency invalidation is outside this MVP.

The researcher may manually delete model files or directories. The filesystem
state is authoritative for availability: an absent file means the model is not
available. Training recreates it when required; prediction stops and reports it
missing. A historical DuckDB completion record must never make a missing file
appear loadable. No database-repair workflow is required solely because a file
was manually deleted.

## Serialization and distributed execution

Providers use suitable native formats, such as scikit-learn `joblib`, XGBoost
UBJ or JSON, R-native serialization, or an appropriate neural-weight format.
The shared abstraction standardises the lifecycle rather than imposing one
binary format.

A model trained on either machine must become available through the same
logical storage interface. Partial or failed saves are never valid models.
The Mac remains the sole DuckDB writer; the design must specify how a worker's
completed model reaches authoritative storage without placing a large binary
inside the normal JSON request/result contract.

## Minimal execution evidence

Training records one of:

- `trained`;
- `overwritten`;
- `skipped_existing`; or
- `failed`.

Loading records one of:

- `loaded`;
- `missing`; or
- `invalid_or_unreadable`.

This is execution evidence, not a new researcher interface.

## MVP boundary

The MVP must be functional, tested and conform to all approved architecture,
code, documentation, orchestration, storage and cross-language standards. MVP
does not permit lower quality or a parallel legacy path.

The MVP must not add speculative generations, promotion, global discovery,
automatic quotas, archival services, or a user interface. Its required outcome
is simple:

1. existing models are skipped unless overwrite is requested;
2. missing models are trained and saved;
3. deliberate overwrite safely replaces the selected fitted model;
4. prediction only loads previously fitted models and fails clearly if one is
   unavailable; and
5. adding one model to unchanged experiment inputs calculates only that model's
   missing work and downstream results.
