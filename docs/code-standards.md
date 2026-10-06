# ShapeFM code structure and naming standards

## Status and purpose

This is the living code standard agreed during POC2 Preparation. It applies to
work performed by the researcher, ChatGPT, AMP, or any other development tool.

ShapeFM is one evolving research project and one repository. POC1, POC2, and
later POCs are Agile working increments of the same codebase. They do not
create copied source trees, parallel implementations, or new repositories.

The priorities are scientific correctness, human readability, traceability,
modularity, and efficient development by one researcher. Start with the
simplest useful structure. Add a file or folder only when an actual need
justifies it.

## Pragmatic implementation

Approved by the researcher on 3 October 2026: development effort must serve the
research outcome. For POC2 consolidation, prioritise required methods and readable
connected workflows over polishing temporary or obsolete infrastructure.

- Use known, controlled input/output contracts. Validate at the authoritative
  external, provider, storage and restart boundaries; avoid repeating the same
  defensive checks between internal components that already share that contract.
- Consolidate cohesive utilities and remove unnecessary forwarding layers,
  speculative branches and obsolete implementations after checking real callers
  and replacement behaviour. Do not retain executable scaffolding merely for a
  hypothetical future need; keep future research options in documentation.
- Document genuine fixed constants without making each one configurable.
  Scientific choices and operational settings retain their central authority.
- OOP, small reusable functions and documentation remain the standard, applied
  proportionately. No class, module, test matrix or exception framework exists
  solely to satisfy a checklist.
- Test the changed behaviour and relevant connected path; reuse valid evidence
  and group related checks. Do not repeat full campaigns for internal edits.

This relaxes unnecessary structural and defensive machinery, not scientific
correctness, explicit errors, required data contracts, accepted-data preservation,
single-writer/restart integrity, provenance or approved memory/resource limits.
Record deferred work honestly in the existing acceptance record. A researcher
may accept a bounded increment with explicit limitations without claiming that
unperformed tests passed.

## Mandatory object oriented implementation

Researcher direction clarified on 2 October 2026: object-oriented design, with
small reusable methods and functions, is the standing code standard for the
entire ShapeFM project and all future projects, not only POC2. Apply it using
each language's appropriate mechanisms; the current implementation uses R and
Python. The POC2 migration brings existing code into this standard, not just new
code or `00_main.py`. The standard remains an acceptance requirement, with the
bounded practical exceptions below; it does not require classes for their own sake.

### Purpose and precedence

Clarified by the researcher on 2 October 2026: OOP serves the code standards;
it does not outrank them. Its purpose is to manage growing complexity and make
components easier to reuse, maintain and extend without rewriting the system.
Class counts, inheritance depth and fewer lines are not measures of success.

Scientific correctness comes first. Structural changes must not silently change
calculations, inputs, transformations, fallbacks or experiment meaning. Apply
OOP alongside simplicity and proven library capabilities, human readability,
mandatory documentation, cohesive responsibilities and shared implementation,
consistent naming, central configuration, explicit data contracts,
reproducibility and safety, and incremental evidence-based development.

Use the simplest adequate design under this standard and its practical
exceptions. An abstraction must earn its place through a clear responsibility
or demonstrated reuse or maintenance benefit, not speculative future needs.

### Object responsibilities

Objects encapsulate a cohesive responsibility, its relevant data/state and the
small methods that implement its behaviour. Reuse existing suitable classes and
language-native model objects. A large procedural coordinator renamed as a class,
or data records passed into unchanged monolithic functions, is not compliance.
Prefer composition and clear interfaces; avoid deep inheritance, universal
context objects, unnecessary forwarding classes and a new object framework.

| Object responsibility | Required boundary |
| --- | --- |
| Configuration | Load and validate authoritative stored settings; expose the relevant resolved settings to consumers |
| Dataset source | Read the configured source and expose its records through a small, consistent interface |
| Research storage | Own database access, transactions and durable research state on the coordinator |
| Scientific provider | Apply configured preprocessing, transformation, forecasting or evaluation behaviour using native libraries and small reusable functions |
| Request, job and result | Carry explicit identity, required settings, bounded values/references and provenance between components |

Prefect flows/tasks orchestrate operations on these objects; Dask schedules
eligible computation. Domain objects must not become competing workflow engines,
schedulers or infrastructure-retry controllers. Thin Prefect callables invoke
object methods. Small pure scientific/validation helpers may support those
methods; they are not an alternative procedural application architecture.
Use Python classes and documented native R class/method contracts, preserving
existing library objects rather than adding a framework solely for OOP.

Configuration is stored, loaded through the configuration object, then consumed
by the responsible objects. Preserve creation-time JSON validation/storage and
DuckDB authority on resume. Consumers must not independently reread configuration
files, reinterpret fields or maintain competing settings. Operational overrides
still follow the execution policy and are recorded without changing science.

GIFT-Eval import must follow this simple pattern: load configuration, construct
the configured source object, read bounded records, validate and persist through
the storage object. Acquisition/cache/source-version details belong behind the
source boundary; sequencing and parallel work belong to Prefect/Dask. Reuse the
pinned official reader where it meets our contract; justify necessary adaptation.
Remove redundant discovery, conversion and configuration handling without losing
source identity, required validation, streaming or restart guarantees.

Functions and methods must be minimal and reusable: one clear operation at one
level of abstraction, with explicit inputs, outputs and effects. Combine these
operations to perform a task; do not create tiny forwarding functions merely to
inflate abstraction. Do not start jobs or mutate research data in constructors.
Serialise bounded job/result data at real boundaries, not live storage/service
objects or writable connections. Reconstruct native provider objects where used.

### Practical exceptions

Use a simpler functional implementation when object-oriented structure would
add complexity without a useful benefit, for example a small self-contained
test, diagnostic or one-off utility. Keep functions minimal, readable, documented
and reusable where useful; do not add classes merely to satisfy a style count.

Simple calculations and performance-sensitive scientific kernels may remain
small functions supporting the object interfaces. Do not add heavy object
structures around them without a demonstrated benefit. Claims that one design
is faster require proportionate measurement, not an assumption that procedural
or object-oriented code is inherently faster.

This is an authorised part of the standard, not a new approval request for every
small test. Briefly record the reason and bounded scope in the relevant source
comment or existing handoff; no separate exception document is required. It does
not waive scientific correctness, configuration authority, documentation, safety
or execution policy, or permit a second production orchestration path.

Reassess the design if the work grows, gains substantial state or becomes a
shared production component. Broader departures affecting the application's
architecture still require researcher approval. The aim is maintainable,
proportionate design, not maximum class usage.

### Enforcement and reuse

Enforce this standard through code review, object/interface tests and targeted
architecture checks. Every refactor handoff must map changed responsibilities to
their object, methods, Prefect owner where applicable, and tests; record any
unmigrated code and justified practical exceptions explicitly. Acceptance requires
coverage of the agreed increment with no unexplained departures; the current
POC2 migration still covers all active first-party components. Passing numerical
tests, adding decorators, or reducing lines does not waive the standard. The
researcher approves changes beyond the practical exceptions above. Every later
project must reference or include this baseline and its exception policy rather
than re-establishing them from chat memory. This includes the precedence rules
and two-reviewer quality assurance process below.

### Two reviewer quality assurance

At the agreed implementation review checkpoint, use two complementary reviews
of the same identified source version and test evidence:

1. **ChatGPT technical review.** Inspect the actual implementation, tests and
   documentation, not only AMP's completion report. Provide evidence-backed
   findings and recommendations for R, Python and their boundaries, including
   Prefect/Dask ownership where applicable.
2. **Researcher independent review.** Assess human readability, research intent
   and whether the processes and modules make sense. The researcher retains
   final acceptance authority; a technical pass is not automatic approval.

Both reviews assess whether:

- Scientific behaviour, data contracts and experiment meaning are preserved.
- Objects and modules have clear responsibilities, use small reusable methods
  and appropriate libraries, and simplify maintenance without unnecessary layers.
- Names, documentation, inputs/outputs and central configuration make the
  process understandable; R/Python native objects preserve the same meaning.
- Tests support the claimed behaviour, reproducibility, restart and safety;
  obsolete implementations are removed only after replacement coverage.

Report **required corrections**, **recommended improvements** and **acceptable
exceptions**, with relevant source references and evidence. Distinguish verified
behaviour from untested areas and state remaining work. Reuse the existing
handoff/acceptance record; do not create another review registry.

Reviews recommend changes; they do not automatically implement them or expand
approved scope. Address agreed corrections, retest affected behaviour and update
documentation together before requesting acceptance. Apply this process to the
current staged refactor and carry it into the baseline for future projects.

## Mandatory workflow architecture

The researcher approved the
[workflow standard and software-layer diagram](poc2-workflow-orchestration-decision.md)
on 2 October 2026. It is mandatory for this migration and later workflow work
until explicitly superseded. The initial implementation has reported test
evidence. Stage 1 is now accepted for progression with recorded limitations;
the remaining required workflow integration and simplification proceed in
Stage 2 under the pragmatic implementation rule. The
[acceptance record](poc2-workflow-orchestration-acceptance.md) retains prior evidence.

Use Prefect for experiment/gate/substep orchestration, Dask for eligible compute
scheduling, native R/Python adapters for scientific work, and the Mac's single
DuckDB writer for validated research results. Keep scientific functions usable
without workflow-service dependencies. Do not introduce independent schedulers,
competing retry loops, duplicate resource settings or a second research store.
Use normal source contracts, not a custom workflow-description framework.

Keep workflow definitions high-level: named tasks, dependencies and results.
Follow the decision's shared handoff convention, reusing existing identities,
payload validation and native adapters. Prefer framework execution facilities
over repeated per-script retry/timeout machinery, retaining proven scientific
and process-safety responsibilities. Simplicity is reduced maintenance, not
shorter files achieved by hidden logic or removed documentation.

This migration must record before/after R/Python script counts, per-script line
counts and reconciled totals under the
[measurement instructions](amp-poc2-workflow-orchestration-instructions.md#script-and-line-count-comparison).
Include new integration code, tests and QA explicitly. A lower line count is a
goal to measure, not evidence on its own that the architecture is correct.

Every workflow change must document its owning layer, inputs/outputs,
prerequisites, validation, retry/restart behaviour and authoritative execution
settings, and test the real entry-to-output path. Preserve read-only utilities
and the single researcher entry point. Update architecture and affected
researcher guidance in the same increment; distinguish approved, implemented
and tested status. Record temporary compatibility paths explicitly.

The [execution policy](execution-policy.md) still governs limits and two-machine
testing. Scientific behaviour remains governed by approved experiment decisions.
Any conflict or material departure requires researcher approval before coding;
an implementation convenience does not silently amend the standard.

## Agile development approach

Each POC follows the same small cycle:

1. Define the outcome, scope, and acceptance criteria.
2. Agree on design decisions before implementation.
3. Implement one bounded item at a time.
4. Test the affected end-to-end behaviour.
5. Review the implementation and scientific result.
6. Record the accepted increment and continue with the same codebase.

Git history, project documentation, configuration, and DuckDB preserve project
history. Source code and tests are not copied merely to preserve a POC.

## One researcher entry point

ShapeFM has one researcher-facing entry point:

```text
src/python/00_main.py
```

Python is the control layer because it owns Dask execution, the single DuckDB
writer, task orchestration, restart state, GIFT-Eval integration, and most
foundation-model execution.

The entry point exposes only the actions with demonstrated researcher workflows:

```text
python src/python/00_main.py plan
python src/python/00_main.py run
python src/python/00_main.py prepare-windows
python src/python/00_main.py status
python src/python/00_main.py results
python src/python/00_main.py test
```

There is no implicit default that starts an experiment, installs software, or
modifies data. A mutating action must be explicit.

## Readable modules and object boundaries

Researcher direction recorded on 2 October 2026: the current `00_main.py`
requires refactoring. This is a coding requirement, not a claim that the
refactoring or workflow migration has been accepted.

The entry point must be extremely simple: obtain a request through the CLI
component, dispatch the requested action, and present its result or failure.
It must not implement argument definitions, SQL, database transactions, cluster
lifecycle, scheduling, retry loops or scientific calculations. A researcher
must be able to understand the top-level flow without reading those details.

| Component | Responsibility behind its interface |
| --- | --- |
| Entry point | Connect request, action and presentation through a few clear calls |
| CLI and presentation | Parse arguments, validate command syntax, format results/errors and exit status |
| Configuration | Resolve the authoritative settings into documented configuration objects |
| Workflows and action handlers | Express the requested operation as named steps with explicit dependencies |
| Storage utilities | Own database access, transactions and persisted-state operations |
| Execution utilities and adapters | Own service/cluster lifecycle, resource handling and native worker boundaries under the approved architecture |
| Scientific functions | Calculate results from their documented inputs |

Reuse cohesive existing modules under `util/`; add specifically named modules
only where responsibilities need separating. This rule applies recursively:
database and execution modules must also expose simple operations over smaller
components, not absorb all of `main` into another monolith. Avoid catch-all
utilities, circular imports and layers that merely forward every call.

Pass small, documented request, configuration, job and result objects between
components, reusing existing types and contracts. Each component receives only
what it needs. Implement component behaviour in cohesive classes under the
[mandatory object-oriented standard](#mandatory-object-oriented-implementation),
with small methods and supporting functions. Preserve native R/Python model
objects within their adapters and serialize only at actual process, language or
storage boundaries. Never send writable database connections to workers.

Names and structure must make intent evident. Keep each function at one level
of detail; high-level functions compose meaningful operations rather than
mixing workflow decisions with low-level implementation. Encapsulate detail,
but keep dependencies, failure propagation and important side effects explicit.

Review acceptance requires a readable top-level flow, cohesive module ownership,
documented object contracts, and tests of the unchanged public commands and
affected boundaries. Preserve scientific behaviour, storage compatibility and
safety guarantees during structural refactoring. File/line counts are supporting
evidence, not an arbitrary limit or permission to remove documentation or tests.
Apply this standard to R and Python and retain it for future projects. The POC2
object-oriented migration covers all active components, delivered in the reviewed
stages of the AMP instructions. Report remaining work; do not silently exclude
existing modules or expand scientific scope.

## Experiment configuration evolution

One complete, versioned JSON document is the only creation-time experiment
definition. Database creation validates and stores both the original and
resolved documents. Thereafter DuckDB is authoritative and the source JSON is
neither required nor accepted for resume.

Any later POC that adds or changes a scientific or operational workflow field
must update together: the JSON contract, validation, configuration version,
stored schema/configuration, typed Python interface, affected coordinator and
task payloads, Python/R consumers, tests, architecture, and researcher
documentation. Machine addresses and credentials remain environment settings;
protocol and schema constants remain code constants. An incompatible scientific
change starts a new experiment database and never mutates an existing one.

## Global settings

A global setting is a value that is used by more than one function or script,
changes the scope of an experiment, changes how the workflow executes, or must
remain consistent across machines, processes, tasks, or restarts.

Every active production global setting belongs to one of four classes:

1. **Experiment global.** Affects scientific scope or intended results. It is
   defined in the experiment JSON, stored in DuckDB, and immutable after the
   experiment database is created.
2. **Execution global.** Affects how work is performed but not the intended
   scientific result. Its creation default is defined centrally and the value
   actually used is recorded with the DuckDB execution event. Approved
   execution globals may change when an experiment resumes.
3. **Machine environment.** Identifies a host, address, port, installation,
   device, or cache. Stable shared machine facts live in the approved
   [machine inventory](machine-environment.md); execution profiles select the
   coordinator and enabled workers. Environment variables are explicit temporary
   overrides, not ordinary hidden defaults. Effective non-secret values are
   recorded as execution evidence.
4. **Code constant.** Defines a local implementation, schema, or protocol and
   is not a researcher-controlled setting. It remains documented in code.

Derived values are calculated from authoritative configuration and are not
additional editable settings. Test fixtures and calibration search grids are
not production globals unless the researcher-facing workflow uses them.

Do not copy every third-party library default into the experiment JSON. A
default remains governed by the pinned dependency and implementation version
until ShapeFM deliberately chooses to control it. When a later POC or imported
script needs that value to be shared or varied, promote it through the complete
JSON → validation → DuckDB → configuration interface → consumer path.

No production script may maintain a competing copy, hidden override, or
fallback for an authoritative global setting. Each process and worker receives
only the settings it needs.

The README documents only this entry point. R files, supporting Python files,
shell commands, and internal command functions are implementation details and
are not alternative researcher interfaces.

## Pipeline order

The entry point makes the six scientific processes visible and runs them in a
defined order:

```text
01 import
02 preprocess
03 transform
04 forecast
05 combine
06 evaluate
```

The order must also be recorded in DuckDB task and invocation state. A process
may be implemented by Python, R, or both, but Python remains the coordinator
and only database writer.

## Minimal target structure

```text
src/python/
├── 00_main.py
├── 01_import.py
├── 02_preprocess.py
├── 03_transform.py
├── 04_forecast.py
├── 04_02_forecast_chronos.py
├── 05_combine.py
├── 06_evaluate.py
├── 06_01_evaluate_gift_eval.py
├── util/
└── tests/

src/r/
├── 02_01_preprocess_series.R
├── 04_01_forecast_auto_arima.R
└── util/
    └── time_series_input.R
```

The structure starts flat. Do not create source folders for individual POCs,
workflows, integrations, bridges, or models until a real collection of files
requires one.

There is no additional `src/python/shapefm/` directory. The repository is the
ShapeFM project; Python source lives directly under `src/python/`.

## Source-file classification

Every source file has one necessary and explainable responsibility.

### Process-specific files

The ordered research flow must be visible from the source filenames. The
coordinating language has exactly one wrapper for each scientific process:

```text
NN_action_subject.ext
```

Additional scripts that implement a real substep, language boundary, model
branch, or isolated environment within that process use:

```text
NN_MM_action_subject.ext
```

Process wrappers use `NN_name`; executable process substeps use `NN_MM_name`.
Importable Python components under `util` follow the approved
[utility naming standard](#python-utility-file-organisation) below; shared
components do not acquire an arbitrary process number.

- `NN` identifies the scientific process.
- `MM` identifies a real, separately identifiable substep or parallel branch.
  It does not imply sequential execution when branches may run concurrently.
- Names use lowercase snake case.
- Words such as `gate`, `phase`, `script`, and `shapefm` are not repeated when
  the number, action, subject, or repository already provides that context.
- `00_main.py` is the only researcher-facing entry point. Numbered wrappers and
  substeps are internal and are called through that entry point.

Python is the coordinating language, so ShapeFM has one wrapper for every
scientific process:

```text
src/python/01_import.py
src/python/02_preprocess.py
src/python/03_transform.py
src/python/04_forecast.py
src/python/05_combine.py
src/python/06_evaluate.py
```

R or any later language has files only for processes or substeps implemented in
that language. ShapeFM currently has these specialised substeps:

```text
src/r/02_01_preprocess_series.R
src/r/04_01_forecast_auto_arima.R
src/python/04_02_forecast_chronos.py
src/python/06_01_evaluate_gift_eval.py
```

A process wrapper is concise but not cosmetic. It documents the process
purpose, inputs, outputs, caller, specialised substeps, and shared utilities,
and owns the high-level hand-off to the central coordinator. It does not
duplicate scientific calculations, database transactions, distributed
execution, retries, or provenance implemented by shared utilities.

Existing numbered Python wrappers are loaded internally from their explicit
repository paths by one standard-library loader. Preserve that wrapper interface;
do not add a loader or subprocess for the importable utility modules below.
Loading must preserve the in-process coordinator and single-writer architecture.

Do not create extra wrappers or substeps merely to make a directory appear
complete. The six Python wrappers exist because they are the six defined
scientific process boundaries. Further files require a real responsibility.

This convention applies to every programming language and to future research
projects unless a later approved architecture decision explicitly replaces it.

### Python utility file organisation

Approved by the researcher on 3 October 2026. This is the continuing Python
file-naming standard for ShapeFM and future projects, not a one-off POC cleanup.
The purpose is to expose ownership and responsibility through filenames while
preserving behaviour. The current migration is Python-only; it does not rename
R workers or change public process wrappers.

Keep `src/python/util/` flat and use ordinary Python imports:

| Responsibility | Naming rule | Example |
| --- | --- | --- |
| Helper owned by one process | `pNN_MM_existing_descriptive_name.py` | `p04_02_forecast_provider.py` |
| Existing coordinating flow within that group | First position, `pNN_01_...` | `p04_01_forecast_flow.py` |
| Genuinely shared component | `shared_descriptive_name.py` | `shared_configuration.py` |
| Independent workflow without an approved numeric parent | Retain its descriptive name and document its owning command | `window_preparation.py`, owned by `prepare-windows` |

`NN` is the existing parent process number, including `00` for components owned
by the researcher entry point. `MM` is a stable two-digit navigation position,
not a new task identifier or execution order; Prefect still defines dependencies.
The `p` permits normal Python imports. Do not extend the wrapper loader to helpers.
Keep assigned positions stable; gaps are harmless and additions do not require
renumbering every existing file. Keep Python special files such as `__init__.py`
and established `test_...` discovery names unchanged.

Retain the recognisable current name. Add a purpose word only if needed to make
its responsibility clear; do not append redundant words such as `script` or
`helper`. Existing suffixes such as `flow`, `provider` and `storage` already explain
purpose. Each affected header states its parent or shared role and its purpose,
inputs, outputs and execution context under the source-documentation standard.

Use the existing coordinating flow as the group's first helper where one exists.
The numbered process wrapper remains the gate entry. Do not create another
coordinator solely to fill position 01. If coordination currently lives in a
cross-gate module, identify that honestly in the process map; renaming does not
authorise splitting its implementation. In particular, window preparation is not
assigned to Gate 3 merely because it uses transformations.

Keep cohesive objects and related small functions together. There is no
one-file-per-object rule, target file count, new package hierarchy or permission
to rewrite working logic. Renaming and relocating files requires corresponding
imports, launch commands, tests and documentation to follow the same ownership
map. Preserve scientific identities and historical experiment definitions.

### Inactive code holding area

During incomplete migration, code confirmed unnecessary to the active supported
paths may be moved, not deleted, to `tmp/inactive/` at repository root. This is a
Git-tracked pool available for later reactivation, not permanently retired code,
generated scratch data or a folder that cleanup/install commands may remove.
Do not name it `retired`, ignore it, or delete it as routine temporary storage.

Check ordinary and dynamic imports, setup/subprocess/configuration references,
stored paths, tests and requested manual QA before moving a file. Infrequent use
or the absence of an import is not proof of inactivity. If usage is uncertain,
leave it active and identify the question rather than changing a supported path.
No file has to be moved merely to make this folder nonempty.

When needed, preserve the original relative path below `tmp/inactive/`, retain
the source contents, and add one short index recording original location,
purpose, reason for holding and what must be checked before reactivation. Keep
inactive code out of active runtime discovery and ordinary test collection;
do not remove active tests to make a dependency appear unused. Restore through
the then-current naming standard and focused validation when it is needed again.
Report active and inactive file counts separately without presenting relocation
as deletion or a reduction in total retained code.

### Shared foundation code

Code used by more than one process, wrapper, or substep, or by the overall
experiment foundation, belongs in the appropriate utility folder:

```text
src/python/util/
src/r/util/
```

Shared database, configuration, provenance, Dask, restart, task lifecycle, and
cross-process functions remain under `src/python/util/` with `shared_` names.
Not every file in `util` is shared: use the ownership rules above for process
helpers and independent workflows. R utility names remain unchanged by this
Python-only migration. Foundation responsibilities include:

```text
configuration
database
distributed execution
experiment execution
forecasting
GIFT-Eval integration
provenance
restart and task orchestration
transformations
rolling-window preparation and read-only retrieval
```

Each utility file has one cohesive responsibility and a specific name. Avoid
catch-all names such as `utils`, `helpers`, `common`, and `misc`.

Moving existing foundation code into `util/` is not itself simplification.
Split demonstrated responsibilities under the
[readability and object-boundary standard](#readable-modules-and-object-boundaries),
with tests demonstrating unchanged behaviour. Do not create speculative layers
or split files solely to reach a line-count target.

### Process-to-code map

`src/python/00_main.py` is the single researcher-facing entry point. Shared
coordinators use `shared_` names under `util`; process-owned helpers use `pNN_MM_`.
Each numbered Python wrapper exposes its process boundary, and numbered substeps
expose specialised execution boundaries. These naming changes do not move the
object responsibilities in the map below.

| Process | Python wrapper | Specialised substep | Shared coordinator/utility | Principal DuckDB input → output |
|---|---|---|---|---|
| 01 import | `src/python/01_import.py` | — | `ImportCoordinator`, configuration, database, GIFT-Eval source, provenance | stored configuration → datasets, series, windows, import task state |
| 02 preprocess | `src/python/02_preprocess.py` | `src/r/02_01_preprocess_series.R` | `ExperimentCoordinator`, distributed execution | forecast instances and benchmark metadata → preprocessed series and task state |
| 03 transform | `src/python/03_transform.py` | — | `ExperimentCoordinator`, transformations, distributed execution | preprocessed series and variants → transformed series and task state |
| 04 forecast | `src/python/04_forecast.py` | `src/r/04_01_forecast_auto_arima.R`; `src/python/04_02_forecast_chronos.py` | `ExperimentCoordinator`, execution profiles, transformations, distributed execution | transformed series and model settings → base forecasts and task state |
| 05 combine | `src/python/05_combine.py` | — | `ExperimentCoordinator`, forecast combination, distributed execution | base forecasts and combination settings → candidate forecasts, components and task state |
| 06 evaluate | `src/python/06_evaluate.py` | `src/python/06_01_evaluate_gift_eval.py` | `ExperimentCoordinator`, GIFT-Eval bridge | complete candidate forecasts → official evaluations and task state |

## R boundary

R is a specialised computation worker, not the pipeline coordinator.

Python passes ordinary serializable inputs to R and receives ordinary results.
R workers do not open the writable DuckDB database or manage distributed task
state.

R is retained where its implementation is scientifically required, for
example:

```text
src/r/02_01_preprocess_series.R
src/r/04_01_forecast_auto_arima.R
```

If more than one R process needs identical time-series construction, frequency
handling, validation, or serialization, that code belongs in a specifically
named file under `src/r/util/`.

The Python-to-R contract must explicitly preserve values, missingness,
frequency, start/end indices, horizon, identifiers, and error information.

## Common scientific inputs and model adapters

Models compared in one experiment use the same canonical upstream research
objects wherever their scientific definitions permit. A shared process owns
dataset membership, train/test partitions, forecast origins, preprocessing,
transformations, labels and evaluation targets. Model branches reference those
accepted objects; they do not silently create model-specific copies or repeat
the shared preparation under different settings.

A provider-specific adapter may begin only after the shared process boundary.
It may perform the minimum representation change required by the model, such
as converting an array dtype, resizing an accepted array, creating an R `ts`
object or constructing a framework tensor. The adapter must:

- preserve the source identity and common preparation fingerprint;
- document and fingerprint the exact conversion;
- avoid changing cohort membership, split assignment, cleaning,
  standardisation, labels, forecast origin or evaluation target; and
- expose a common output contract when models are compared by the same
  research question.

Different model internals do not by themselves make a comparison unequal. A
fixed model may have no validation or tuning step while another model performs
training-only calibration. Any departure from the common upstream population
or preparation is a scientific architecture decision and requires explicit
researcher approval before implementation.

## Distributed execution

Python controls Dask and the experiment lifecycle.

The [execution policy](execution-policy.md), approved on 30 September 2026,
governs current heavy testing and supersedes smaller historical profiles below.
All heavy paths must share the resolved execution configuration and preflight.
Do not silently switch to local heavy execution if Ubuntu is unavailable.
Implementation must test mismatch rejection and real cross-host computation;
documentation or connected-worker counts alone do not establish compliance.

- The execution profile selects one coordinator; the current profile selects the
  MacBook Pro, and a future approved profile may select the Mac Studio.
- The selected coordinator runs Prefect and Dask scheduling and is the only
  DuckDB writer.
- Workers receive ordinary tasks and return ordinary results.
- Execution mode and worker count do not alter scientific identity.
- Completed tasks are not repeated after restart.
- R operations execute inside bounded worker calls controlled by Python.
- Foundation-model GPU tasks use `CHRONOS_GPU_SLOT`, an explicitly declared
  logical execution resource that is distinct from physical GPU count.

## Historical Objective 1 acceptance case

The Objective 1 end-to-end acceptance case is retained for its specific scope
and compatibility. It is controlled by:

```text
python src/python/00_main.py test
```

That historical test configuration uses:

- pinned GIFT-Eval M4 Daily data;
- the first 100 official time series in deterministic GIFT-Eval order;
- a fresh isolated DuckDB database;
- the complete import-to-evaluation pipeline;
- one Mac CPU worker;
- one Ubuntu GPU worker on the physical RTX 5090;
- two Dask workers in total; the scheduler is not a worker;
- official GIFT-Eval evaluation;
- a restart run that proves completed work is skipped;
- recorded task counts, row counts, fingerprints, provenance, host
  contribution, resource use, failures, and retries.

The test must not overwrite `data/shapefm.duckdb` or accepted results.

Its two-worker counts are not the current heavy-test default. Follow the
execution policy for new heavy work; gate-local changes need only their scoped
acceptance, not an automatic full import-to-evaluation rerun.

The acceptance logic lives under:

```text
src/python/tests/
```

It is invoked through `00_main.py`; it is not another public entry point.

For the current incomplete migration, follow the
[inactive code holding rule](#inactive-code-holding-area) rather than deleting
code that is not currently needed. The earlier POC1 cleanup is historical and
does not authorise deletions in the approved file-organisation pass.

## Configuration and documentation

### Source documentation

Source documentation is mandatory for every project-authored source and test
file, regardless of programming or scripting language. This standard applies
to Python, R, shell, and any language introduced by a later POC. Vendored
external code is excluded.

Each file uses its language's normal documentation or comment syntax for this
header:

```text
==============================================================================
filename

Purpose: One or two short lines explaining why the file exists.
Inputs:  Files, objects, arguments, environment or standard input consumed.
Outputs: Files, database changes, returned objects or standard output produced.
Run from: Exact repository-root command, or “Imported; not run directly.”
==============================================================================
```

- Each file begins with a concise header naming the filename and its
  **Purpose**, **Inputs**, **Outputs**, and **Run from** context. Preserve useful
  language-native module documentation. Utility, package, and test-support
  modules state `Imported; not run directly.` Executable files give an exact
  repository-root command. Worker files describe their standard-input,
  subprocess, or scheduler context accurately.
- **Run from** applies to files, not functions. It records how an executable is
  launched or states that a module is imported. A function does not repeat the
  file's command or maintain a caller list.
- Every named function or method, including private helpers and test methods,
  has a language-native docstring or an immediately preceding contract comment.
  A non-trivial function documents its **Purpose**, **Inputs**, and **Outputs**.
  Outputs include returned values and important side effects such as database,
  file, task-state, standard-output, or process changes.
- A small, obvious function may use one accurate sentence when separate input
  and output sections would only repeat its signature. Add **Raises**,
  **Notes**, or an **Example** only when needed to use or verify the function
  correctly.
- Function documentation explains the contract rather than listing current
  callers or narrating the implementation. Parameter descriptions add research
  meaning, units, shapes, allowed values, object types, or data origin instead
  of merely repeating names and type annotations.
- Every class, data object, or research object documents its purpose, important
  inputs or fields, state or outputs, and ownership of side effects. Individual
  methods follow the same function standard.
- Important scientific fields, identifiers, units, and allowed values are
  explicit.
- Every important module global has an adjacent concise comment explaining its
  purpose and approved classification: experiment global, execution global,
  bootstrap/interface default, machine environment, code constant, or
  test/calibration value. Override and provenance behaviour are stated when
  relevant. Related constants may share a block only when every name is clear.
- Do not document imports or every local variable.
- Keep documentation simple, concise, and technical. Prefer one short line per
  field. Explain contracts rather than restating names; do not use descriptions
  such as “helper function” or “process data.”
- Do not add essays, history, generic generated text, or speculative
  explanations inside source files. Documentation must match the implementation.
- All **Run from** commands assume the repository root. Utility modules state
  that they are imported and are not direct entry points. Only
  `src/python/00_main.py` is researcher-facing.

Configuration describes scientific choices and execution controls; it does not
hide orchestration logic. Documentation describes the single entry point and
the current accepted experiment.

Folders remain flat until their contents create a real navigation problem.
Environment subfolders are allowed when incompatible or separately locked
environments require them.

## POC2 objectives and definition of done

POC2 has two objectives. Objective 1 is approved below. Objective 2 is being
approved and implemented component by component; the item-specific decisions
record the approved methods and contracts. Completion of infrastructure alone
does not constitute scientific Objective 2 completion.

### Preparation

1. Establish `src/python/00_main.py` over the existing POC1 implementation.
2. Add the deterministic 100-series acceptance action.
3. Run it through both machines and retain baseline evidence.
4. Refactor existing implementation code behind the stable entry point.
5. Rerun the identical acceptance case after each meaningful structural group.
6. Remove obsolete POC1 interfaces and tests only after equivalence is proven.
7. Run the acceptance case one final time.

Preparation changes structure and interfaces, not scientific mathematics.

### Objective 1: centralised research workflow

The following are the historical Objective 1 completion criteria. Its limited
worker topology does not override the current execution policy.

Objective 1 is complete when:

1. One readable, versioned JSON document is the only creation-time experiment
   authority.
2. DuckDB is the experiment authority after creation and the source JSON is not
   required for resume.
3. Every active production global affecting scope or execution is centrally
   configured, machine-environment controlled, or documented as a code
   constant.
4. No production script maintains a competing value.
5. The coordinator gives every process and task only the settings it needs.
6. Processes 01–03 can execute first and Processes 04–06 can resume later in
   the same database.
7. Restart repeats no completed valid scientific work.
8. Configuration, process status, execution evidence, and results remain
   queryable for later research review.
9. The deterministic 100-series M4 Daily experiment completes with AutoARIMA
   and Chronos-2 using one Mac CPU worker and one Ubuntu GPU worker.
10. A documented configuration-evolution procedure governs every later POC
    and imported script.

### Objective 2: selected previous-project reproduction

The approved direction is to reproduce only relevant M4 Daily results from
`m4_tsc_fmts_2026` under the ShapeFM architecture and code standards, importing
and validating one necessary component at a time. Irrelevant previous-project
methods are not imported merely for completeness.

Use the recorded item-specific approvals, including the approved R forecast pool;
do not reopen them merely because this overview is high-level. Do not invent
remaining scientific result targets, comparison tolerances or training decisions.
Obtain the Chief Developer's decision where an item genuinely lacks one.

The working discipline remains: agree, change one bounded item, test, review,
and then continue.
