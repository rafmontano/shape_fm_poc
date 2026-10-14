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

`00_main.py` is the only researcher entry point. Prefect owns experiment and
process dependencies. Use a meaningful substep only when it has distinct inputs,
durable outputs and restart value. Use Prefect's Dask integration for eligible
parallel tasks; do not manage a parallel scheduler inside application objects.

The coordinator is the sole DuckDB writer. Workers receive bounded read-only
inputs and return results. DuckDB transactions own accepted scientific item and
completion state. Restart queries stored identities and schedules the delta.

## Python ownership

```text
src/python/<NN_process>/process.py       real Prefect process flow
src/python/<NN_process>/<name>.py        code used by that process only
src/python/util/<name>.py                code used by two or more processes
src/python/tests/...                     tests by process/component
```

Use descriptive `snake_case` names. A `shared_` prefix is unnecessary inside
`util/`. Do not create empty process wrappers or one file per trivial class.

## R ownership

Process-facing R workers use `<NN>_<action>.R`. Reusable numerical functions live
under `src/r/util/`; tests under `src/r/tests/`; manual inspection helpers under
`src/r/qa/`. R workers read one request and return one response; they do not
write DuckDB or start nested parallel execution.

## Configuration and globals

Scientific settings live in `config/experiment.json`; stable machine facts in
`config/machines.json`; operational profiles in `config/execution_profiles.json`;
dependency pins under `config/dependencies/` and lockfiles. Do not duplicate
these values in module globals, environment variables or model-specific files.

Runtime-derived endpoints and paths are operational values. They do not enter
scientific identity. After experiment creation, the resolved configuration in
DuckDB is authoritative.

## Validation policy

Validate once at a real trust boundary. Normal checks are proportional:

- external source/configuration shape and identity;
- provider request/result compatibility;
- finite/dimensional scientific output where required;
- database constraints and transaction outcome; and
- prerequisite/completion count.

Do not validate the same contract at every internal handoff. Deep parity,
environment, checksum, cross-host, corruption and historical reproduction
checks belong to named QA actions. Binary direction uses its exact definition;
near-zero values are not a special validation policy.

## Errors, retry and fallback

Failures identify process, substep (if any), batch/item and underlying cause.
Infrastructure retry must not change scientific settings. Model fallback exists
only when the scientific contract explicitly defines it. Never hide a failed
model behind a generic forecast substitution.

## Identity and provenance

Scientific identity contains data, method, approved settings, horizon and
transformation/model lineage. Operational evidence contains host, worker,
runtime, duration, retry and batching. Operational differences must not change
scientific IDs or content hashes.

Record provenance needed to reproduce or interpret a result; do not turn every
internal object into a manifest. Documentation text and execution location are
not scientific identity.

## Tests and QA

Use the smallest test that proves the changed behaviour:

1. focused unit/contract tests;
2. a fresh isolated 100-series run only through the affected process; and
3. interruption/restart at the smallest durable boundary when execution changed.

Do not routinely run later processes or the complete historical matrix. Broader
tests need a demonstrated dependency or researcher approval. A fixture tests the
canonical source contract; it does not add a second data authority.

At review, show the active call path before/after, responsibilities removed or
moved, files/functions/classes/lines, validation removed or retained, and the
bounded evidence. Architecture compliance is judged from the active path, not
from test count or folder names.

## Documentation

Keep living rules in the small canonical set listed in `docs/README.md`. Update
the owning document; do not create a new AMP-instruction or acceptance file.
Git and DuckDB preserve history/evidence. Links and diagrams must describe the
current system. Generated reports never enter source documentation.
