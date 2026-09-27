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
python src/python/00_main.py status
python src/python/00_main.py results
python src/python/00_main.py test
```

There is no implicit default that starts an experiment, installs software, or
modifies data. A mutating action must be explicit.

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
shape_fm_poc/
|-- README.md
|-- config/
|-- docs/
|-- environments/
|-- src/
|   |-- python/
|   |   |-- 00_main.py
|   |   |-- util/
|   |   `-- tests/
|   `-- r/
|       `-- util/
|-- data/
`-- results/
```

The structure starts flat. Do not create source folders for individual POCs,
workflows, integrations, bridges, or models until a real collection of files
requires one.

There is no additional `src/python/shapefm/` directory. The repository is the
ShapeFM project; Python source lives directly under `src/python/`.

## Source-file classification

Every source file has one necessary and explainable responsibility.

### Process-specific files

If an executable script belongs to one process, its filename identifies that
process:

```text
NN_action_subject.ext
NN_MM_action_subject.ext
```

- `NN` is the process number from `01` to `06`.
- `MM` is a substep number used only when a separate substep is necessary.
- Names use lowercase snake case.
- Words such as `gate`, `phase`, `script`, and `shapefm` are not repeated when
  the number, action, subject, or repository already provides that context.

Examples include:

```text
src/python/04_forecast_chronos.py
src/r/02_preprocess_series.R
src/r/04_forecast_auto_arima.R
```

Do not create six Python or six R files merely to display the pipeline. Create
a process-specific file only when the implementation needs it.

### Shared foundation code

Code used by more than one process or by the overall experiment foundation
belongs in the appropriate utility folder:

```text
src/python/util/
src/r/util/
```

The current Python foundation is expected to place most orchestration code in
`src/python/util/`, including responsibilities such as:

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

## R boundary

R is a specialised computation worker, not the pipeline coordinator.

Python passes ordinary serializable inputs to R and receives ordinary results.
R workers do not open the writable DuckDB database or manage distributed task
state.

R is retained where its implementation is scientifically required, for
example:

```text
src/r/02_preprocess_series.R
src/r/04_forecast_auto_arima.R
```

If more than one R process needs identical time-series construction, frequency
handling, validation, or serialization, that code belongs in a specifically
named file under `src/r/util/`.

The Python-to-R contract must explicitly preserve values, missingness,
frequency, start/end indices, horizon, identifiers, and error information.

## Distributed execution

Python controls Dask and the experiment lifecycle.

- The Mac runs the coordinator and Dask scheduler.
- Only the coordinator writes to DuckDB.
- Workers receive ordinary tasks and return ordinary results.
- Execution mode and worker count do not alter scientific identity.
- Completed tasks are not repeated after restart.
- R operations execute inside bounded worker calls controlled by Python.
- Foundation-model GPU tasks use `CHRONOS_GPU_SLOT`, an explicitly declared
  logical execution resource that is distinct from physical GPU count.

## One acceptance case

POC2 uses one authoritative end-to-end acceptance case controlled by:

```text
python src/python/00_main.py test
```

The acceptance case uses:

- pinned GIFT-Eval M4 Daily data;
- the first 100 official time series in deterministic GIFT-Eval order;
- a fresh isolated DuckDB database;
- the complete import-to-evaluation pipeline;
- five Mac CPU workers;
- fifteen Ubuntu CPU workers;
- fifteen logical Ubuntu GPU workers sharing one physical RTX 5090;
- 35 Dask workers in total;
- official GIFT-Eval evaluation;
- a restart run that proves completed work is skipped;
- recorded task counts, row counts, fingerprints, provenance, host
  contribution, resource use, failures, and retries.

The test must not overwrite `data/shapefm.duckdb` or accepted results.

The acceptance logic lives under:

```text
src/python/tests/
```

It is invoked through `00_main.py`; it is not another public entry point.

Obsolete POC1 workflows, console commands, shell entry points, root R wrappers,
and compatibility packages are removed after replacement coverage. Git
preserves their history, so no archive copy is created.

## Configuration and documentation

Configuration describes scientific choices and execution controls; it does not
hide orchestration logic. Documentation describes the single entry point and
the current accepted experiment.

Folders remain flat until their contents create a real navigation problem.
Environment subfolders are allowed when incompatible or separately locked
environments require them.

## POC2 implementation sequence

POC2 has two successive parts.

### Preparation

1. Establish `src/python/00_main.py` over the existing POC1 implementation.
2. Add the deterministic 100-series acceptance action.
3. Run it through both machines and retain baseline evidence.
4. Refactor existing implementation code behind the stable entry point.
5. Rerun the identical acceptance case after each meaningful structural group.
6. Remove obsolete POC1 interfaces and tests only after equivalence is proven.
7. Run the acceptance case one final time.

Preparation changes structure and interfaces, not scientific mathematics.

### Import

1. Define the exact M4 Daily result required from `m4_tsc_fmts_2026`.
2. Identify only the components necessary for that result.
3. Import one component at a time into its final process-specific or utility
   location.
4. Run the same end-to-end acceptance path after each component is integrated.
5. Compare results with the previous project before importing the next item.

The working discipline remains: agree, change one bounded item, test, review,
and then continue.
