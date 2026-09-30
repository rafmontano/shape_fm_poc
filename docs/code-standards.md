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
python src/python/00_main.py status
python src/python/00_main.py results
python src/python/00_main.py test
```

There is no implicit default that starts an experiment, installs software, or
modifies data. A mutating action must be explicit.

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
   device, or cache. It remains in a machine profile or environment variable;
   its effective non-secret value is recorded as execution evidence.
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

In short, process wrappers use `NN_name`; process substeps use `NN_MM_name`;
shared utilities are unnumbered and live under `util`.

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

Numbered Python filenames are loaded internally from their explicit repository
paths by one standard-library loader. Do not rename them to create ordinary
Python identifiers, add a competing filename convention, or start a new
subprocess merely to load a wrapper. Loading must preserve the in-process
coordinator and single-writer DuckDB architecture.

Do not create extra wrappers or substeps merely to make a directory appear
complete. The six Python wrappers exist because they are the six defined
scientific process boundaries. Further files require a real responsibility.

This convention applies to every programming language and to future research
projects unless a later approved architecture decision explicitly replaces it.

### Shared foundation code

Code used by more than one process, wrapper, or substep, or by the overall
experiment foundation, belongs in the appropriate utility folder:

```text
src/python/util/
src/r/util/
```

Shared database, configuration, provenance, Dask, restart, task lifecycle, and
cross-process functions remain under `src/python/util/`. Utility filenames are
descriptive and never carry a process number. Shared responsibilities include:

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
```

Each utility file has one cohesive responsibility and a specific name. Avoid
catch-all names such as `utils`, `helpers`, `common`, and `misc`.

Moving existing foundation code into `util/` does not require splitting it
prematurely. A module is divided only when a current POC creates a clear
responsibility boundary and tests can demonstrate unchanged behaviour.

### Process-to-code map

`src/python/00_main.py` is the single researcher-facing entry point. Shared
coordinator functions remain unnumbered under `util`; each numbered Python
wrapper exposes its process boundary, and numbered substeps expose specialised
execution boundaries.

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

## Distributed execution

Python controls Dask and the experiment lifecycle.

The [execution policy](execution-policy.md), approved on 30 September 2026,
governs current heavy testing and supersedes smaller historical profiles below.
All heavy paths must share the resolved execution configuration and preflight.
Do not silently switch to local heavy execution if Ubuntu is unavailable.
Implementation must test mismatch rejection and real cross-host computation;
documentation or connected-worker counts alone do not establish compliance.

- The Mac runs the coordinator and Dask scheduler.
- Only the coordinator writes to DuckDB.
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

Obsolete POC1 workflows, console commands, shell entry points, root R wrappers,
and compatibility packages are removed after replacement coverage. Git
preserves their history, so no archive copy is created.

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

POC2 has two objectives. Objective 1 is approved below. Objective 2 has only
provisional high-level approval; its detailed scope, target results, comparison
rules, and completion criteria require later Chief Developer approval.

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

This direction is not yet a definition of done. Do not treat a particular
result list, model list, comparison tolerance, import order, or completion
criterion as approved until the Chief Developer supplies and approves the
detailed Objective 2 scope.

The working discipline remains: agree, change one bounded item, test, review,
and then continue.
