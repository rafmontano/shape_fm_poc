# ShapeFM code standards

These standards apply to ShapeFM and are the baseline for later research
projects. They serve research clarity and reliable change; they are not a reason
to reproduce production-enterprise ceremony.

## Precedence

1. Approved scientific meaning and research scope.
2. Architecture and durable data contracts.
3. Simple, readable and testable implementation.
4. Performance and operational optimization supported by evidence.

When rules conflict, stop and ask the researcher rather than silently choosing.

## MVP scope control

Scope is closed by default. The MVP contains only outcomes and capabilities the
researcher has approved. An unrequested feature, check, abstraction, report or
operational mechanism is not part of the implementation merely because it might
be useful.

If an unrequested capability is genuinely necessary to deliver an approved
outcome, explain the need, effect and simplest option to the researcher and wait
for scope approval before implementing it. Do not disguise scope expansion as
an implementation detail. Optional robustness, future reuse and hypothetical
users are not necessity.

The default response to a nice-to-have is omission. The default response to a
new material choice is a recommendation, not code.

## Simplicity rule

Add a file, class, wrapper, validation or state record only when it owns a clear
responsibility that cannot be expressed more simply. Do not add architecture for
hypothetical consumers: the researcher is the only current user and the system
will continue to change.

Remove:

- pass-through wrappers and forwarding aliases;
- duplicate state machines, schedulers, retries and transaction logic;
- repeated validation of trusted internal objects;
- compatibility APIs used only by obsolete tests;
- model-specific experiment runners; and
- inactive code from the active source tree.

Inactive or superseded code is removed. Git history preserves it for possible
future reuse.

## Research value and structural evidence

Code exists to produce trustworthy research results. A change adds value when it
improves approved scientific capability, necessary reproducibility, clarity,
restart, scalability or execution efficiency. Code volume, test count and
validation count are not measures of quality.

For a material architecture change, report before/after files, folders, lines
and active call paths. These measurements expose whether complexity actually
moved or disappeared; they are evidence, not reduction quotas. Acceptance also
requires that the researcher can follow the process flow and locate its inputs,
state, models and outputs without reconstructing implementation history.

## Object and function design

Use objects for cohesive responsibilities with state or interchangeable
implementations: source, importer, provider, repository, model store, evaluator
and exporter. Prefer small pure functions for stateless numerical calculations.
OOP is not mandatory when it adds ceremony without clarity or reuse.

- One object owns one concept.
- Public methods express domain actions, not transport mechanics.
- Constructors establish valid dependencies; methods do not rediscover global
  configuration or paths.
- Scientific functions accept ordinary values/arrays and return ordinary
  results; orchestration is outside them.
- A long function or file triggers a responsibility review. Splitting text
  without separating responsibility does not improve the design.

## Workflow implementation

`experiment.py` is the researcher entry point. Prefect owns experiment,
process and meaningful-subflow orchestration. The mandatory hierarchy is
Experiment → Process → meaningful subflow → work group → atomic item; preserve
the exact subflows/order in [architecture](architecture.md). Use the supported
Prefect/Dask integration and one shared executor, invisible to process flows.
Scientific functions do not import Prefect, Dask or MLflow, choose placement or
open tracking runs. MLflow is the fifth/top research-tracking layer, not science.

The coordinator is the sole DuckDB writer. Workers receive bounded read-only
inputs and return results. DuckDB transactions own accepted scientific item and
completion state. Restart queries stored identities and schedules the delta.

## Python ownership

```text
src/python/experiment.py                experiment Prefect flow
src/python/p01_import/flow.py            real process flow (through p06_evaluate)
src/python/pNN_process/<name>.py         code used by that process only
src/python/util/runtime.py              minimal shared runtime facade
src/python/util/distributed_execution.py shared work-group executor
src/python/util/research_tracking.py     coordinator MLflow adapter
src/python/util/<name>.py                code used by two or more processes
src/python/tests/...                     tests by process/component
```

Use descriptive `snake_case` names. A `shared_` prefix is unnecessary inside
`util/`. Do not create empty process wrappers or one file per trivial class.
Shared transformations live in `src/python/util/transformations.py`. Process
folders use implicit namespace packages; retain `util/__init__.py` and
`tests/__init__.py` for their shared utility and fixture namespaces.

## R ownership

R follows the same `pNN_process` ownership and descriptive `snake_case` naming
as Python. Workers and helpers used by one process live together:

```text
src/r/p03_transform/feature_worker.R
src/r/p03_transform/features.R
src/r/p03_transform/training_cohort.R
src/r/p04_forecast/forecast_worker.R
src/r/p01_import/archived_forecast_worker.R
src/r/p04_forecast/forecast_methods.R
src/r/p06_evaluate/evaluation_worker.R
src/r/p06_evaluate/diagnostic_worker.R
src/r/p06_evaluate/publication_worker.R
src/r/p06_evaluate/adjustments.R
src/r/p06_evaluate/metrics.R
src/r/util/preprocess_worker.R
src/r/util/time_series_input.R
src/r/tests/
```

`src/r/util/` contains only code used by at least two processes;
`preprocess_worker.R` is shared because Process 02 prepares full histories while
Process 03 performs block-local cleaning without future leakage.
`time_series_input.R` supplies the common input conversion for Processes 02–04.
The single worker-path constant lives beside `run_r` in Python `util/providers.py`;
neither process imports the other's application. Tests stay central under `src/r/tests/`,
just as Python tests stay under `src/python/tests/`. R workers read one request
and return one response; they do not write DuckDB or start nested parallel
execution.

## Configuration and globals

Scientific settings live in `config/experiment.json`; stable machine facts in
`config/machines.json`; operational profiles in `config/execution_profiles.json`;
dependency pins under `config/dependencies/` and lockfiles. Do not duplicate
these values in module globals, environment variables or model-specific files.

Runtime-derived endpoints and paths are operational values. They do not enter
scientific identity. After experiment creation, the resolved configuration in
DuckDB is authoritative.

## Repository and artifact hygiene

- Keep one obvious canonical location for configuration, source data, scientific
  databases, fitted models, outputs and runtime state.
- Use stable semantic filenames. Experiment identity, date and execution detail
  belong inside DuckDB or MLflow, not duplicated in paths.
- Python and R mirror the approved process ownership and naming conventions.
  A process owns its code; `util/` is reserved for code used by two or more
  processes; tests stay in the language's central test folder.
- Generated packaging metadata, IDE state, caches, scratch work, runtime logs and
  obsolete experiment copies are not authored source and do not remain in the
  visible project structure.
- Derived CSV and figure outputs live only under `results/`; fitted artifacts live
  only under `models/`; operational state lives only under `.runtime/`.
- Do not retain compatibility copies, aliases or historical folders in the active
  tree. Git is the archive for removed source and documentation.

## Validation policy

Validate once at a real trust boundary. Normal checks are proportional:

- external source/configuration shape and identity;
- provider request/result compatibility;
- finite/dimensional scientific output where required;
- database constraints and transaction outcome; and
- prerequisite/completion count.

Do not validate the same contract at every internal handoff. MVP behaviour not
named in the approved design is absent by default: no calibration, automatic
tuning, benchmark/environment probes, speculative validation/conversion,
compatibility wrappers or monitoring frameworks. Normal assertions and clear
trust-boundary failures are allowed. Completion checks only planned identities,
accepted predecessors and the required output reference; it does not revalidate
scientific content. Binary direction has no near-zero dead zone.

Provider return checks concern output shape and scientifically usable values.
Ordinary valid floating-point differences are accepted, without parity gates,
workloads or tolerance gates. Preserve approved precision, including disabling
CUDA TF32 matmul/convolution for Mantis float32 calculations.

## Errors, retry and fallback

Failures identify process, substep (if any), batch/item and underlying cause.
Infrastructure retry must not change scientific settings. Model fallback exists
only when the scientific contract explicitly defines it. Never hide a failed
model behind a generic forecast substitution.

## Identity and provenance

Scientific identity contains data, method, approved settings, horizon and
transformation/model lineage. Operational evidence contains host, worker,
runtime, duration, retry, batching, hardware and backend. Operational differences
must not change scientific IDs or trigger recalculation of accepted results.

Record provenance needed to reproduce or interpret a result; do not turn every
internal object into a manifest. Documentation text and execution location are
not scientific identity.

## Tests and QA

Use the smallest test that proves the changed behaviour:

1. focused unit/contract tests;
2. a fresh isolated 100-series run only through the affected process; and
3. interruption/restart at the smallest durable boundary when execution changed.

The accepted architecture experiment is authoritative in the canonical root locations.
Completion checks are non-scientific: compilation, mocked interfaces and read-only
inspection. Do not rerun processes, scientific calculations, no-new-work restarts
or output generation for final review. Temporary test roots must clean up and
must never be presented as the final researcher experiment.
Calculate each scientific atomic item once on its assigned worker and resume
accepted DuckDB state. Final review uses produced tables/figures, not automatic
historical comparison or a second scientific workload; stop for a material
final-result issue only when the researcher identifies it.

Do not routinely run later processes or the complete historical matrix. Broader
tests need a demonstrated dependency or researcher approval. A fixture tests the
canonical source contract; it does not add a second data authority.

At review, show the active call path before/after, responsibilities removed or
moved, files/functions/classes/lines, validation removed or retained, and the
bounded evidence. Architecture compliance is judged from the active path, not
from test count or folder names.

## Documentation

All retained active first-party Python/R under `src/` is audited at final
acceptance, not just newly created code. Vendored dependencies, generated files
and lockfiles are excluded.

- Every module/R worker states its domain responsibility, owning process and
  principal inputs/outputs in a concise header or module docstring.
- Every public flow, task, class and function documents research purpose,
  contract and durable output where relevant.
- Prefect flows/subflows document hierarchy level and durable restart boundary;
  tasks document atomic-item or work-group responsibility.
- Formulas and non-obvious transformations state meaning, assumptions and the
  authoritative paper/equation or accepted contract where available.
- Comments explain why or scientific meaning, not obvious syntax, change
  history or duplicated type hints. No commented-out code or ceremonial prose.
- Simple private helpers use descriptive names and type hints as primary
  documentation. Reconcile stale comments/docstrings when moving retained code.

Keep living rules in the small canonical set listed in `docs/README.md`. Update
the owning document; do not create a new AMP-instruction or acceptance file.
Git and DuckDB preserve history/evidence. Links and diagrams must describe the
current system. Generated reports never enter source documentation.
