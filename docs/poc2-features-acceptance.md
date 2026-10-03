# POC2 optional reusable features acceptance record

Implementation date: 3 October 2026. Scope: approved ID 014 only.
Status: implementation, bounded-memory correction, focused Mac/Ubuntu checks,
and small two-host integration independently reviewed and accepted by the
researcher on 3 October 2026.

## Researcher acceptance and closure

The researcher approved technical acceptance and the final ID 013/014 closure
checkpoint on 3 October 2026. The bounded-memory implementation, retained
failure/restart evidence, two-host evidence, limitations and non-goals below are
accepted as recorded. This closure authorises scoped publication; it does not
enable the eight directional features, start ID 015 or authorise another
migration item.

## Implementation

- `fforma_base_v1` exposes 42 explicit ordered names from one R provider. It
  preserves per-series `tsfeatures(scale = TRUE)`, named nonseasonal insertion,
  final NA-to-zero treatment and the established entropy, heterogeneity and
  Holt-Winters fallbacks.
- The eight ARIMA/ETS directional features are documented but disabled because
  legacy training futures and prediction holdout inputs are inconsistent.
- `prepare-features` is opt-in. The normal window and forecast paths do not
  create feature tables or calculate features.
- The Mac reads compact prepared inputs and is the only DuckDB writer. Native R
  jobs run in bounded Prefect batches; the approved distributed route preflights
  every worker's source and provider dependencies before accepting results.
- Rows store only named feature values and exact feature/source/dependency,
  transformed-input, transformation, period and worker provenance against the
  existing window identity. Exact accepted rows are reused; provenance changes
  recompute; failures remain explicit.
- The approved correction counts and rejects an oversized local request before
  any prepared array is read. Exact pending work is filtered in DuckDB and read
  in immutable keyset pages bounded by configured batch × maximum in-flight
  batches. Completion uses database counts; accepted identities are not loaded
  into an unbounded Python set.

No accepted database or legacy repository was modified. No Mantis/classifier
training, prediction, directional feature, GPU or forecast campaign was run.

## Changed files and purpose

| File | Purpose |
| --- | --- |
| `src/r/util/features.R` | Cohesive named 42-feature scientific calculation and fallbacks |
| `src/r/feature_provider.R` | Native describe/extract JSON adapter with per-job failures |
| `src/python/util/shared_distributed_execution.py` | Bounded R feature batch and worker provenance adapter |
| `src/python/util/feature_extraction.py` | Optional coordinator, additive child schema, exact cache identity, persistence and retrieval |
| `src/python/util/p00_01_researcher_actions.py` | Local/distributed action, preflight, sole-writer and results integration |
| `src/python/util/p00_02_researcher_cli.py` | Explicit `prepare-features` and named retrieval options |
| `src/r/tests/test_features.R` | Legacy parity, schema, period, edge, failure and batch checks |
| `src/python/tests/test_feature_extraction.py` | Disposable connected persistence/restart/failure/isolation/resource checks |
| `src/python/tests/test_main.py` | Public command, dispatch and retrieval contracts |
| `docs/poc2-features.md` | Approved scientific design, consumers and non-goals |
| `docs/architecture.md`, `docs/poc2-window-training-architecture.md`, `README.md` | Current architecture and researcher instructions |
| `AGENTS.md`, `docs/amp-poc2-features-instructions.md` | Current-task reference |

The bounded-memory correction changes only
`src/python/util/feature_extraction.py`, its focused test, this decision and the
existing architecture/acceptance documentation. It preserves ID 013, all 42
feature definitions, provider adapters, storage schema and public commands.

## Bounded-memory correction evidence

The disposable regression fixture contains seven prepared windows with a
configured page size of two. It reads four pages of sizes 2, 2, 2 and 1,
submits seven distinct IDs exactly once, and reports a maximum of two live
payload rows. Its immediate restart reuses all seven, reads zero array rows,
submits zero tasks and leaves prepared-window hashes, directional labels and S1
membership unchanged. A separate five-window fixture with a local limit of four
raises before feature-table creation or a pending-page read.

The connected two-window native-R test still enforces a 120-second/192-MiB
traced-Python bound and now also proves zero array reads on restart. The complete
focused Python suites passed on both hosts after the correction:

| Host | Focused Python | R feature/provider | R transformation/labels |
| --- | ---: | ---: | ---: |
| Mac | 57 passed in 92.048 s | 9 passed, including unchanged-legacy parity | all passed |
| Ubuntu | 57 passed in 107.202 s | 9 passed, including unchanged-legacy parity | all passed |

Python compilation, R parsing and `git diff --check` passed. The Ubuntu R
environment retained the pre-existing warning that some unrelated lockfile
packages are not installed; all provider dependencies were present and used.
The reviewed 112-file runtime manifests matched at
`8455708c0c2c18f7ed556194daa5e59a75cf9774734bd7963fd7d2e14047ff25`.

## Small real distributed acceptance

A fixed disposable fixture was prepared through the existing window workflow
and expanded to 46 feature-only test identities. The normal `prepare-features`
entry point used `poc2_seasonal_recovery` v3: 8 Mac and 15 Ubuntu CPU workers,
23 maximum in-flight batches, one R thread per task, existing 3/16-GiB host
floors and no GPU workers.

The initial invocation used Prefect's localhost-only ephemeral API. Eight Mac
rows completed and were committed, while the first Ubuntu task timed out trying
to reach that API. The cluster stopped cleanly. Following the repository's
previously accepted two-host method, a test-owned private-LAN Prefect service was
started and was reachable from both hosts; the same public command then resumed
without deleting or rewriting the eight accepted rows.

| Evidence | Result |
| --- | --- |
| Resume selection | 46 total; 8 reused; 38 arrays read/submitted; 0 failures |
| Bounded coordinator payload | batch 1; page 23; 2 pages; maximum 23 rows / 11,776 value bytes |
| Completed work during resume | Mac 13 rows; Ubuntu 25 rows; simultaneous native R processes observed on both hosts |
| Complete persisted work | Mac 21 rows; Ubuntu 25 rows; 46 distinct successes; maximum attempt count 1 |
| Resource evidence | minimum available during fits: Mac 3.681 GiB, Ubuntu 116.757 GiB; maximum owned R RSS 0.229 GiB; 9.0 s maximum admission throttle; 0 safety responses |
| Sequential reference | 2 rows through the same public local path; absolute tolerance 1e-10; maximum difference 1.59e-11 |
| Full restart | 46 reused; 0 arrays read; 0 submissions; 0 live payload rows |
| Retrieval and isolation | public retrieval returned 42 ordered values; 46 source windows, 2 label rows and 2-member S1 allocation unchanged |

The private-LAN Prefect service and both managed clusters were stopped after the
test. This service requirement is an existing two-host Prefect operational
contract, not a new feature setting. No GPU, training, forecast or accuracy
campaign ran, and no accepted database/result was touched.

## Mac focused evidence

- `src/r/tests/test_features.R`: 9 checks passed. Period-1 and period-12 rows
  matched the unchanged legacy source; names/order, constant and short inputs,
  period resolution, failure isolation, directional exclusion and individual vs
  mixed-constant batch equivalence passed.
- `tests.test_feature_extraction`: 2 connected tests passed. Two eligible
  windows among a disposable 100-series parent were prepared; no feature tables
  existed before the request. The first bounded batch persisted and retrieved
  42 values for varying and constant inputs, restart submitted zero rows, a
  synthetic failed provenance remained explicit, changed dependency provenance
  was not reused, and parent content/S1 membership stayed unchanged. The test
  enforces less than 120 seconds and 192 MiB traced Python allocation.
- `tests.test_main`: public CLI/action/retrieval checks passed with the existing
  entry-point suite.
- Combined `tests.test_main tests.test_feature_extraction tests.test_labels
  tests.test_window_preparation`: 55 tests passed in 83.492 seconds.
- Python compilation, R parsing and `git diff --check` passed.

## Ubuntu focused evidence

The complete reviewed overlay was synchronized to the clean matching
`dfc1af0` base without copying databases, results, environments or the Mac
legacy repository. The unchanged legacy `features.R` was copied only to the
excluded `.amp/in/id014/` test area.

- The same 55 combined Python tests passed in 97.279 seconds, including the
  connected feature persistence/retrieval/restart/failure/resource case.
- The same 9 R feature checks passed against the byte-identical unchanged legacy
  source, and the R transformation/label suite passed.
- Python compilation, R parsing and `git diff --check` passed. R emitted the
  existing warning that the broader renv lock is not fully installed; every
  dependency used by the focused provider was present and exercised.
- Mac and Ubuntu matched for the 25 reviewed source/test/contract paths at
  combined SHA-256 `8d2baf475d375e708307cab2b4c617a6fce67a215c3f97061365052ce4069744`.
  This evolving ID 014 acceptance record is excluded from its own hash. No
  distributed cluster or GPU was needed for the focused acceptance.

## Manual QA

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py prepare-features \
  --database /path/to/disposable-parent.duckdb \
  --windows-database /path/to/disposable-windows.duckdb \
  --local-max-windows 2

.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --database /path/to/disposable-parent.duckdb \
  --windows-database /path/to/disposable-windows.duckdb \
  --dataset-id dataset/features --series-id 0 --window-ordinal 0 \
  --feature-set-id fforma_base_v1
```

Expected: the first command reports two submitted/successful rows; an immediate
repeat reports two reused, zero submitted, `array_rows_read: 0`, and
`page_count: 0`. Retrieval reports exactly 42
ordered names/values plus its provenance fingerprint, period, transformation,
R dependencies and worker identity.

## Script and line counts

The count scope is first-party `*.py`/`*.R` below `src`, using physical lines.
The ID 013 corrected checkpoint is the before-state for ID 014.

| Affected script | Before | After | Change |
| --- | ---: | ---: | ---: |
| `src/r/util/features.R` | 0 | 125 | +125 |
| `src/r/feature_provider.R` | 0 | 97 | +97 |
| `src/python/util/feature_extraction.py` | 0 | 640 | +640 |
| `src/python/util/shared_distributed_execution.py` | 1,527 | 1,606 | +79 |
| `src/python/util/p00_01_researcher_actions.py` | 539 | 735 | +196 |
| `src/python/util/p00_02_researcher_cli.py` | 156 | 164 | +8 |
| `src/r/tests/test_features.R` | 0 | 146 | +146 |
| `src/python/tests/test_feature_extraction.py` | 0 | 252 | +252 |
| `src/python/tests/test_main.py` | 467 | 531 | +64 |

Production/support changed by +1,145 lines and automated tests by +462 lines.
Overall source changed from the ID 013 checkpoint of 81 scripts/31,112 lines to
86 scripts/32,719 lines: +5 scripts and +1,607 lines. The largest addition is
the restart/provenance coordinator and its connected disposable evidence; no
legacy script or accepted scientific artifact was deleted.

The bounded-memory correction leaves the script count at 86. Relative to the
first ID 014 implementation, `feature_extraction.py` changes from 640 to 709
lines (+69) and `test_feature_extraction.py` from 252 to 442 (+190). Overall
first-party Python/R source changes from 32,719 to 32,978 physical lines (+259):
production/support +69 and tests +190. No R script or scientific kernel changed.

## Scientific decisions and limitations

No unresolved decision blocks the approved base provider. Enabling the eight
directional fields still requires a separate scientific decision defining one
information-consistent training/prediction input policy. ID 014 deliberately
does not choose it. The researcher accepted ID 014 with those fields disabled.

Two-host runs require the established private-LAN Prefect service configuration;
the default localhost ephemeral API cannot be reached by Ubuntu workers. This is
an operational prerequisite, not a scientific decision or a bounded-memory
limitation. The failed first invocation is retained as restart evidence rather
than hidden or relabelled as success.
