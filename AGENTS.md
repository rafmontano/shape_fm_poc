# ShapeFM development guidance

## Authority

The researcher owns scientific scope, standards and final acceptance. The Chief
Architect owns design and technical review. AMP implements and tests approved
scope. Do not infer approval from historical messages or Git history.

Read the living documentation index at [docs/README.md](docs/README.md). Before
changing workflow, storage or scientific behaviour, read:

- [research vision](docs/research-vision.md);
- [architecture](docs/architecture.md);
- [code standards](docs/code-standards.md);
- [working agreement](docs/working-agreement.md);
- the relevant scientific contract.

## Standing architecture

- One researcher entry point and six processes: Import, Preprocess, Transform,
  Forecast, Combine and Evaluate.
- Five layers, bottom to top: DuckDB, Python/R science, Dask execution, Prefect
  orchestration and MLflow research tracking. The mandatory hierarchy is
  Experiment → Process → meaningful subflow → work group → atomic item, with
  exactly the approved subflows and order in `docs/architecture.md`.
- Prefect owns orchestration and operational attempts.
- Dask owns eligible local/distributed scheduling.
- Python/R application objects own scientific behaviour.
- The selected coordinator alone writes DuckDB.
- DuckDB owns accepted scientific state and restart; Prefect history does not.
- GIFT-Eval is the sole canonical benchmark-data authority.
- The entry point is `src/python/experiment.py`; process flows are importable
  `p01_import/flow.py` through `p06_evaluate/flow.py`. Shared adapters are
  `util/runtime.py`, `util/distributed_execution.py` and `util/research_tracking.py`.
- Process-only Python code belongs in its owning process folder; `util/`
  contains only code used by at least two processes.
- Exactly two scientific databases: `results/experiment.duckdb` and
  `results/windows.duckdb`; create the child only when Process 03 needs it.
  Membership, reusable features and representations belong in the parent;
  rolling modelling inputs belong in the child.
- MLflow is coordinator-only: one resumable `ShapeFM` run per experiment, its ID
  stored in the parent, supported SQLite under `.runtime/mlflow/`. Tracking is
  non-scientific; failures warn without invalidating science.

Do not implement a second scheduler, queue, retry system, transaction protocol,
validation framework or model-specific experiment runner. Do not preserve
obsolete paths through empty wrappers or aliases.

## Scientific boundaries

- Preserve the standard forecast black-box contract and approved provider
  settings/fallbacks.
- Store individual forecasts before combination; combinations use stored
  components and preserve their lineage.
- DTW and Mantis use the same canonical directional input/cohort/output boundary.
- Only expensive reusable directional fitted state is persisted outside DuckDB.
- Process 06 evaluates stored results; it does not refit models.
- Calculate each scientific atomic item once on its assigned worker; resume
  accepted DuckDB state. Hardware/backend are operational provenance, not identity
  or grounds for recalculation. Ordinary valid float differences are accepted;
  no parity gate, workload or tolerance gate. Provider boundaries check shape
  and scientifically usable values. Preserve the Mantis TF32/float32 correction.

The current contracts are in [data](docs/data-contract.md),
[forecasting](docs/forecast-black-box-contract.md),
[directional models](docs/directional-models.md), and
[evaluation](docs/evaluation.md).

## Implementation rules

Scope is closed by default. Behaviour not named in the approved MVP is absent.
If an unrequested capability is genuinely required to deliver approved scope,
explain why and obtain researcher approval before implementing it. Do not add
calibration, automatic tuning, benchmark/environment probes, speculative
validation/conversion, compatibility paths, monitoring frameworks, worker or
nested MLflow runs, autologging or system metrics. New capabilities need approval.

Prefer the simplest cohesive implementation. Use objects when they own state or
an interchangeable responsibility and small functions for stateless numerical
work. Validate at external input, provider return, transaction and completion
boundaries—not every internal handoff.

Preserve unrelated user work. Do not delete generated material while a writer is
active. Heavy or distributed execution, dependency changes, publication and
machine synchronization require the applicable researcher approval.

Validate a changed execution path with focused tests and a fresh isolated
100-series run only through the affected process. Prove restart at the smallest
durable unit. Do not routinely execute later processes.

The architecture transformation is researcher-accepted. Use canonical root
databases, models and outputs; temporary test packages are not final delivery.
Do not recalculate accepted scientific items merely to validate implementation.

## Documentation

Audit all retained active first-party Python/R, not only new files: module
responsibility, owning process and principal inputs/outputs; public purpose and
contract; Prefect hierarchy and durable restart boundary; formula meaning,
assumptions and authority. Comments explain why, not syntax or history. Use
descriptive typed private helpers without ceremony. Exclude vendored/generated
files and locks. See `docs/code-standards.md` for the complete standard.

`docs/` contains current guidance only. Update the owning living document; do
not add AMP instruction, acceptance, calibration, failure-history or checkpoint
documents. Durable evidence belongs in DuckDB and derived outputs under
`results/`; superseded text remains available in Git history.
