# POC2-X01 architecture transformation

Status: **in progress — Increment 1 only**, authorised by the researcher on
14 October 2026. Implementation and evidence remain subject to ChatGPT technical
review and independent researcher approval under the
[working agreement](working-agreement.md). This target does **not** authorise a
wholesale rewrite, scientific changes, or starting another increment.

## Current and approved target architecture

Current map (accepted Part B baseline; migration remains incomplete):

```text
00_main.py → ResearcherCLI → ResearcherActions
  run → ProcessAction → Prefect experiment/gate flows → numbered wrappers 01–06
    → ImportCoordinator / ExperimentCoordinator + process-specific components
    → Dask eligible batches → native R/Python providers
    → coordinator validation/storage → DuckDB + external fitted-model artifacts
  status → stored configuration/process/experiment reads → JSON
  export → validated stored Process 06 results → derived files

Prefect: operational history; DuckDB: scientific state and restart authority
Legacy recovery: process/task records plus specialised checkpoints, not a
uniform durable substep hierarchy; execution and bookkeeping remain intertwined.
```

Approved target (destination, not a claim of implementation):

```text
Single researcher entry point: 00_main.py + central configuration
  ├─ run → thin Prefect process/substep dependencies
  │          → cohesive preparation/provider/storage objects
  │          → bounded Dask batches → native scientific item calculations
  │          → trust-boundary validation → sole coordinator DuckDB writer
  │          ← durable accepted outputs and restart identities
  └─ status → read-only state reader → progress interpretation → JSON/human

Restart hierarchy: process → meaningful substep → bounded batch → scientific item
Prefect history links operational attempts; it is not scientific result storage.
```

Incrementally separate responsibilities behind the existing contracts. Preserve
accepted databases, artifacts, configurations, calculations and historical
records. Replace only the scoped path after bounded evidence; retain necessary
compatibility until its replacement is proven. Do not duplicate an execution
authority, add speculative frameworks, or split files merely to change counts.
Follow the [object and pragmatic standards](code-standards.md), existing
[architecture](architecture.md), and
[workflow ownership decision](poc2-workflow-orchestration-decision.md).

## Stable contracts and ownership

Processes remain **01 import, 02 preprocess, 03 transform, 04 forecast,
05 combine, 06 evaluate**, with existing numbered wrappers and one researcher
entry point. Existing commands, scientific settings, canonical membership,
provider mathematics, fallbacks, model artifacts and output contracts remain
stable. Optional preparation workflows are not new compulsory processes.

- Prefect owns dependencies, operational attempts and configured retries.
- Dask schedules eligible bounded computation using the approved profile.
- Native providers own calculations and native model serialization.
- The profile-selected coordinator is the sole research DuckDB writer.
- Creation-time JSON bootstraps configuration; thereafter DuckDB is authoritative.
  Prefect history and derived human reports are not competing research stores.
- Validate at external configuration/data, provider-return, storage and restart
  trust boundaries. Reuse authoritative checks; do not repeatedly validate trusted
  internal rows or recalculate science merely to present status.

Scientific identity describes the research inputs, methods, settings and outputs.
Operational identity describes attempts, hosts, source revision, worker placement,
batching and framework runs. Record and link both; operational changes must not
silently alter historical scientific identity. Preserve existing fingerprints
and explicit source/restart compatibility checks rather than weakening them.

The target restart hierarchy is process → substep → batch → item. A meaningful
substep has explicit prerequisites and accepted outputs; a batch is bounded
dispatch, not a substitute for scientific item identity. A required failed child
cannot make its parent complete. Restart uses durably committed scientific work,
not an in-memory future or cached Prefect success. Persist outputs atomically
through their storage owner: **insert absent output, verify equivalent existing
output, fail on conflicting identity/content**; never silently overwrite accepted
science. Commit-before-acknowledgement recovery must reuse accepted output.
Substep persistence and changes to execution/restart are **not Increment 1 work**.

## Researcher progress and Increment 1 implementation boundary

`status --database results/<experiment>.duckdb --format human` is presentation
only. Omitted format and `--format json` retain JSON status. The read-only branch
adds `ProgressReader`, `ProgressInterpretation` and `HumanProgress` in
`src/python/util/p00_04_progress.py`; existing status readers remain in place.
No execution coordinator refactoring, schema migration, new tables, second state
store, polling, monitoring or dashboard is authorised.

Show stored name/description/configuration/source revision, Processes 01–06 in
order, statuses/timestamps, task counts, recorded active process, latest committed
activity, latest recorded error/context and Process 06 candidate counts where
available. Distinguish stored experiment status from the process-derived pipeline
state. Missing substeps and ETA must say `not recorded`. Never infer liveness,
an expected-work denominator, or remaining time from legacy task counts.

Status opens DuckDB read-only, never evaluates or validates scientific outputs,
schedules work, repairs records or writes database state. The former JSON status
call to the validating Process 06 table summary is replaced with a plain read
retaining its existing fields; `results`/export validation remains unchanged.
Legacy timestamps are record timestamps, not a complete commit/error history.
Process/execution state is database-wide; downstream tasks are selected-experiment
specific and import counts use the latest import run. An interrupted process may
still say running; a locked database may refuse inspection rather than be repaired.

## Governing incremental validation rule

Every execution-bearing transformation increment uses a **fresh, isolated
100-series experiment starting with no experiment DuckDB**. Documentation-only
changes need no scientific run. Use the earliest adequate process boundary:

| Changed behaviour | Highest permitted process without further justification |
| --- | --- |
| Researcher status/reporting | 01 unless later state is specifically required |
| Process 02 | 02 (run 01–02) |
| Process 03 | 03 (run 01–03) |
| Process 04 | 04 (run 01–04) |
| Process 05 | 05 (run 01–05) |
| Process 06 | 06 (run 01–06) |
| Shared component | Earliest process adequately exercising the change |

**Routine execution beyond the affected process “for completeness” is prohibited.**
Broader execution requires a demonstrated dependency or separate researcher
authorisation. Focused fixtures may represent later, failed, running or interrupted
states without executing those scientific processes. Do not run the historical
acceptance/regression matrix for Increment 1.

Increment 1 compiles changed Python files and checks focused reader/formatting,
existing JSON compatibility, repeated-status byte preservation, pending/running/
completed/failed/legacy fixtures, changed-path documentation links and whitespace.
Then commit/push the candidate normally before the fresh run:

```sh
.venv/bin/python src/python/00_main.py run \
  --configuration config/experiments/poc2_m4_daily_100_paper_tables.json \
  --database results/poc2_m4_daily_100_paper_tables.duckdb --processes 1
.venv/bin/python src/python/00_main.py status \
  --database results/poc2_m4_daily_100_paper_tables.duckdb --format json
.venv/bin/python src/python/00_main.py status \
  --database results/poc2_m4_daily_100_paper_tables.duckdb --format human
```

Check both the database and sibling results directory are absent beforehand.
The existing output contract requires this configuration-matching filename at
the root of `results/`; create its new sibling directory for Increment 1 evidence.
Preserve outputs there, hash the database, repeat both status calls and require
the same hash. Confirm Process 01 completed, 02–06 pending, 100 imported series,
and no invented substeps/ETA. No no-new-work rerun is required. Ubuntu must not be
contacted or modified for this local increment. Processes 02–06 must not run.

On implementation or fresh Process 01 validation failure: stop, preserve the
isolated database and concise evidence, report the exact failure, and do not
change scientific settings or refactor unrelated execution to bypass it.

## Recoverable delivery boundary

Baseline: `b8efcbae4776409184417849a3fb6f63eb5ce6eb`, local `main` and
`origin/main`, verified clean and without ShapeFM-owned execution before changes.
Push safety branch `archive/poc2-part-b-accepted-2026-10-14` at that exact revision;
an existing safety branch must match. Create and push
`poc2-x01-architecture-transformation` from the same baseline with an upstream.
Do not create a tag, change/push/merge into main, synchronise Ubuntu, or begin
Increment 2. Stage only reviewed source/tests/governance, never generated results.
Leave a clean pushed working branch and unchanged main/safety references.
Researcher review and acceptance remain open after successful validation.
