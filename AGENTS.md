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
- the relevant scientific contract; and
- [POC2-X01](docs/poc2-x01-architecture-transformation.md) while the current
  transformation is active.

## Standing architecture

- One researcher entry point and six processes: Import, Preprocess, Transform,
  Forecast, Combine and Evaluate.
- Prefect owns orchestration and operational attempts.
- Dask owns eligible local/distributed scheduling.
- Python/R application objects own scientific behaviour.
- The selected coordinator alone writes DuckDB.
- DuckDB owns accepted scientific state and restart; Prefect history does not.
- GIFT-Eval is the sole canonical benchmark-data authority.
- Process-only Python code belongs in its numbered process folder; `util/`
  contains only code used by at least two processes.

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
- Operational topology must not enter scientific identity or alter results.

The current contracts are in [data](docs/data-contract.md),
[forecasting](docs/forecast-black-box-contract.md),
[directional models](docs/directional-models.md), and
[evaluation](docs/evaluation.md).

## Implementation rules

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

## Documentation

`docs/` contains current guidance only. Update the owning living document; do
not add AMP instruction, acceptance, calibration, failure-history or checkpoint
documents. Durable evidence belongs in DuckDB and derived outputs under
`results/`; superseded text remains available in Git history.
