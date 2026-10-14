# POC2-X01 Increment 3 implementation evidence

14 October 2026. **Implemented and boundedly verified; researcher and Chief
Architect acceptance remains open.** Scope: Process 01 only, on the continuing
`poc2-x01-architecture-transformation` branch. Starting revision:
[`14ed2f5`](https://github.com/rafmontano/shape_fm_poc/commit/14ed2f502df453bd69f42d3c183fff501d710597).
Main and safety remain at
[`b8efcba`](https://github.com/rafmontano/shape_fm_poc/commit/b8efcbae4776409184417849a3fb6f63eb5ce6eb).
No Ubuntu contact, dependency changes, later scientific processes or full
historical acceptance run were performed. The [governing transformation](poc2-x01-architecture-transformation.md)
records the pre-edit inventory and approved target.

## Complexity and responsibility comparison

Physical lines include comments and blanks, not formatting-compressed logical
statements. Before counts are from the starting revision; the process-owned
file footprint includes its superseded legacy APIs, separately identified below.
After counts include all code in the five execution-path files, not only lines
hit by this fixture. Generic schema/infrastructure is disclosed separately.

| Measure | Increment 2 | Increment 3 |
| --- | --- | --- |
| Application frames to an item insert, excluding Prefect internals/DuckDB call | 14 | 10 |
| Process-owned files on execution path | 5 | 5 |
| Process-owned file footprint | 1,422 lines, plus embedded import branches | 551 lines; 36 inline routing/progress lines = 587 |
| Whole shared-module footprint entered by import/restart/status | 10,930 lines in corresponding baseline modules | 10,878 lines across 13 modules |
| Largest active import method | `_commit_result`: 112 lines; legacy production `import_m4_daily`: 165 | `ImportStore.begin`: 35; largest function, Arrow iterator: 57; `ImportProcess.run`: 17 |
| Responsibility objects | 3 import owners plus generic storage validation/state | 3: source, process, store; 2 immutable data carriers |
| Prefect flows/subflows | 1 experiment + 4 descendants | 1 experiment + 1 process |
| Application task types / fresh 100-item executions | 5 / 104 | 0 / 0 |
| Full-data checking/fingerprint passes on fresh import | 2 source pins, membership scan, 4 output digests | 1 source pin, 1 final output digest; one final count query |
| Completed restart full-data passes | 2 output digests | 0 |
| Durable restart | process, artificial substeps, items with recalculation | process and atomic items; no completed-item recalculation |
| Human progress | artificial 01.01/01.02 states | process state, completed/expected items, failed identities/errors |

Dedicated file lines: `01_import.py` 5, `p01_01_import_flow.py` 46,
`p01_02_import_execution.py` 53, `p01_03_gift_eval_source.py` 221,
`p01_05_import_store.py` 226. Inline allocation: 25 reader/presentation lines,
3 binding lines, 8 workflow routing/signature lines. Generic import-validation
code retained for explicit downstream consumers is not executed by Process 01;
it now compares the final stored digest instead of recalculating source science.

Shared whole-module lines (before → after), including status: `00_main.py`
35→35; `p00_01_researcher_actions.py` 833→835; `p00_02_researcher_cli.py`
195→195; `p00_03_researcher_request.py` 32→32; `p00_04_progress.py` 260→270;
`shared_active_paths.py` 57→62; `shared_configuration.py` 1577→1577;
`shared_database.py` 1330→1314; `shared_execution_event_storage.py` 125→125;
`shared_experiment_execution.py` 5210→5210; `shared_process_storage.py` 709→647;
`shared_provenance.py` 55→55; `shared_workflow_orchestration.py` 512→521.
These totals deliberately expose large existing shared modules rather than
claiming their full contents execute. A Python call profile during fault/restart
and status observed only `configuration_status` and `latest_experiment_id` in
the 5,210-line coordinator, and only constructor/`process_rows` in generic process
storage. Later provider modules are import-time dependencies, not executed work.
No scientific work or import complexity was moved into these shared modules.

## Final application and workflow maps

```text
00_main.main
  ResearcherActions.dispatch → _run
    ProcessAction.run → _run          [shared bootstrap/lock/execution event]
      experiment_flow
        import_flow                  [Process 01 Import]
          ImportProcess.run
            ImportStore.completed_summary → return immediately if complete
            GiftEvalSource.fingerprint / metadata / records(exclude=completed)
            execute_local(convert_series, record, settings)
            ImportStore.start_item / accept → insert_or_verify → DuckDB
            ImportStore.finish       [count + one digest + final transaction]
```

Prefect has only `ShapeFM experiment → Process 01 Import`. Its fresh root run
`a4829c81-4694-402c-b0aa-ee73b3a65555` has one framework-generated subflow marker;
child `989237a1-b7ec-4655-aa8f-1fc2d66fbae8` has **zero task runs**. A framework
marker is not an application task. No inspect/execute/validate or per-series task
is created. Process 01's local execution callable has no scheduler/host knowledge.

Removed: `ImportCoordinator`, `ImportState`, task/result wrapper APIs,
`worker_entry`, ProcessPool dispatch, `import_m4_daily`, `import_configured`,
the nested import flow, both artificial subflows and repeated acceptance/source
checks. Historical tests preserving those APIs were removed/replaced by public
entry-point tests. The reusable `repository_root` helper moved unchanged into
the shared path boundary; six later consumers changed imports only.

`ImportStore` atomically commits series, evaluation-window boundaries and item
completion. Failed conversion/storage leaves earlier commits intact; finalization
failure does not invalidate those commits. No new tables are added. Fresh schema
15 omits artificial substep state; existing historical tables are not dropped.
Configuration remains parent-authoritative. The source, canonical float32 hash
encoding, missing-value handling and end-exclusive windows are unchanged.

## Bounded validation results

- Changed Python compiled. The focused command
  `PYTHONPATH=src/python .venv/bin/python -m unittest -v -f tests.test_active_import tests.test_progress tests.test_import`
  passed **17 tests in 24.312 seconds**. This covers atomic rollback, finalization
  retry, partial restart, failure context, pinned-source/conversion contracts,
  fixed paths, no overwrite, JSON compatibility and read-only status.
- Removed legacy mock-only Process 01 tests were replaced by real completion/
  failure event assertions in `test_active_import`. Later-process mock fixtures
  were updated without executing later science. Historical acceptance was not run.
- Verified no active writer, preserved the Increment 2 database privately, and
  confirmed both active database paths absent before fresh execution. The unchanged
  `config/experiment.json` is byte-identical to the approved 100-series paper-table
  configuration. Ran `.venv/bin/python src/python/00_main.py run --processes 1`.
- Real pinned GIFT-Eval revision `30841734ac5cfddbd0c3bad6d09d2b6b32becbb0` produced
  **100 series, 100 evaluation windows, 57,235 observations**. Every scientific
  column in `series`, `evaluation_windows` and `datasets` exactly matches the
  preserved Increment 2 rows; original/resolved configuration also matches.
- Final output fingerprint matches both Increment 2 accepted fingerprints:
  `df51315d178dbfd29e5e4c87c1999dd06f67d9322f45f52304c264a6b2e12d14`.
- An isolated temporary-path run used the same real pinned 100-series source and
  unchanged configuration. Injected conversion failure at identity `37` left
  **37 committed items**. The configured retry encountered the same fault. After
  removing the fault, the normal entry point converted **exactly identities
  37–99 (63 items)**; none of 0–36 was recalculated. All final scientific rows and
  the fingerprint match the fresh run and Increment 2. The fixture was removed.
- With the real bootstrap JSON temporarily withheld, repeated Process 01 returned
  `skipped_completed`; all 100 item attempts were unchanged. Guards rejected any
  source read/hash, conversion or output fingerprint call. JSON was restored.
- Two human and two JSON status requests returned identical status content and
  unchanged parent bytes. SHA-256 both before and after:
  `7d3df826cd04a7e193af56f433dfa1d0619057e8587c1231f70197c382e389fc`.
  Restart legitimately adds operational events before this hash check.
- Dask constructors were guarded during controlled recovery/completed restart;
  an audit guard rejected SSH/SCP/rsync. No cluster or remote operation occurred.
  Fresh normal-run Prefect evidence shows only the two local flows.
- Process 01 complete; 02–06 pending. `results/` contains only
  `experiment.duckdb`; no child, experiment-named output directory or root-level
  CSV/PDF/PNG was created. Status correctly reports the child pending Process 03.
- Changed-document local link targets and whitespace checks passed. After all
  writers stopped, temporary Prefect SQLite/WAL/shared-memory files, memo data,
  test lock files and the scratch validation driver were removed. The accepted
  Increment 2 database is preserved privately for comparison; concise review
  logs remain under ignored `.amp/in/artifacts/`. No generated file is staged.

```text
Main database: results/experiment.duckdb — available
Windows database: results/windows.duckdb — not created (Process 03 pending)
Process 01 Import: completed
  Items: 100 / 100
  Failed items: 0
```

## Exact candidate paths

Modified:

```text
docs/architecture.md
docs/code-standards.md
docs/local-execution.md
docs/poc2-workflow-orchestration-decision.md
docs/poc2-x01-architecture-transformation.md
src/python/01_import.py
src/python/tests/acceptance.py
src/python/tests/test_acceptance.py
src/python/tests/test_active_import.py
src/python/tests/test_import.py
src/python/tests/test_main.py
src/python/tests/test_progress.py
src/python/tests/test_table_configuration.py
src/python/tests/test_workflow_orchestration.py
src/python/util/execution_calibration.py
src/python/util/feature_extraction.py
src/python/util/p00_01_researcher_actions.py
src/python/util/p00_04_progress.py
src/python/util/p01_01_import_flow.py
src/python/util/p01_02_import_execution.py
src/python/util/p01_03_gift_eval_source.py
src/python/util/shared_active_paths.py
src/python/util/shared_database.py
src/python/util/shared_execution_profiles.py
src/python/util/shared_experiment_execution.py
src/python/util/shared_machine_environment.py
src/python/util/shared_process_storage.py
src/python/util/shared_workflow_orchestration.py
src/python/util/window_preparation.py
```

Added: this document and `src/python/util/p01_05_import_store.py`.
Deleted: `src/python/util/p01_04_import_state.py` and
`src/python/tests/integration/test_import_pipeline.py`.

## Remaining boundaries and review limitations

The parent owns configuration, canonical data, process/item state and later
scientific outputs. The required child owns Process 03 rolling-window material;
both represent one experiment. Process 03 fixed-child-path wiring and later
export/model path transformations remain deferred, as does Process 02 work.
No claims about later execution or old acceptance harness success are made.
Explicit downstream validation expects the new final import digest; migrating
old accepted substep-era experiments into later execution was not part of this
fresh-experiment increment and is not claimed.

One pinned-source hash on incomplete invocations and one final scientific digest
remain necessary identity boundaries. Pinned source files must not be modified
concurrently; there is no redundant before/after source hashing. Completed-item
state is trusted after atomic commit; completed restart does not audit external
tampering. Null/NaN hash distinctions remain even though persisted target NaNs
map to SQL NULL, exactly as before. Small pure functions and the callable executor
are intentional exceptions to classes where state would add no responsibility.

Status is operational evidence, not a scientific validator or liveness monitor.
Later processes still lack substep progress/heartbeat/ETA. The removed JSON
`substeps` field is intentionally not preserved; useful existing JSON fields
remain, with additive `import_progress`. The final stored summary describes the
completion invocation, not new work during a skipped restart.

Concurrent diagram/image changes were discovered after clean starting-state
verification. They are not this increment and are excluded: both image files
and the unrelated technical-layer rewrite in `docs/architecture.md` remain in
the user's worktree. Do not revert or include them in this commit. Only this
increment's reviewed documentation hunks/source/tests are authorised for publication.
