# POC2 centralised directional labels acceptance record

Implementation date: 3 October 2026. Scope: approved ID 013 only.
Status: corrected implementation verified on Mac and Ubuntu, independently
reviewed and accepted by the researcher on 3 October 2026.

## Researcher acceptance and closure

The researcher approved technical acceptance and the final ID 013/014 closure
checkpoint on 3 October 2026. The exact-reference correction, cross-platform
cleaning limitation, focused evidence and stated non-goals below are accepted as
recorded. This closure authorises scoped publication; it does not authorise a
new migration item, a cleaning change or regeneration of accepted preparation
data.

## Exact-reference correction checkpoint

The researcher-approved correction supersedes only the initial inverse-derived
schema-v2 compatibility policy recorded below. New windows still atomically save
the exact final cleaned input returned by R and calculate strict labels from it.
Historical schema-v2 windows, missing label rows and rows whose provenance says
their reference was inverse-derived remain readable but return labels/reference
as unavailable with an explicit reason. Resume reports label preparation as
incomplete and does not backfill, regenerate or rewrite accepted rows.

The regression fixture covers the reported boundary directly: future `8.76`
against exact reference `8.76` is 0, while the same future against reconstructed
`8.759999999999998` is 1. A connected disposable database additionally proves
that an exact constant-window tie remains 0, an emulated inverse-derived row is
reported unavailable and preserved across restart, and schema-v2 retrieval keeps
all 100 windows and S1 membership while reporting 100 unavailable labels.

Mac correction evidence:

- `tests.test_labels`: 5 tests passed, including the exact reported tie.
- `tests.test_window_preparation`: 17 tests passed, including exact retrieval,
  inverse-derived retrieval/restart, schema-v2 unavailability, missing-target
  distinction and direct cleaner-reference capture.
- Python compilation, R label/transformation checks and `git diff --check` passed.

Ubuntu correction evidence used the same reviewed overlay:

- The same 22 combined Python label/window tests passed in 67.330 seconds.
- The same R checks, Python compilation, R parsing and `git diff --check` passed.
- Mac and Ubuntu matched for the 13 reviewed ID 013 source/test/contract paths at
  combined SHA-256 `ac24270d3ec365f77194f91b5faec8ea18d8dd0ca6fc12ffb44b92bcd6d19c37`;
  this evolving evidence record is excluded from its own hash.

The bounded cleaning diagnostic used robust mode, centrally resolved period 1
and the exact 64-value input `[9999, 1, ..., 63]` (canonical payload SHA-256
`e09024692c12feca5deff189e5ca491517d9dfbd325f3da19ebb437196974d80`).
Both hosts used R 4.6.1, forecast 8.24.0, jsonlite 2.0.0 and tsfeatures 1.1.1.
Mac changed positions 1-10 and 60-64, returning final reference 58; Ubuntu
changed positions 1-14 and 59-64, returning final reference 57. Inspection of
`forecast::tsoutliers` established that native `stats::supsmu` produces different
floating residuals and quartile limits on ARM macOS versus x86 Ubuntu, changing
the thresholded outlier positions. This is a confirmed preprocessing difference,
not a label-calculation difference. No algorithm, setting or dependency changed.
The connected test independently captures the R result and proves the persisted
reference equals its final cleaned value on either host.

Detailed disposable outputs and host records are retained under the excluded
`.amp/in/artifacts/id013-cleaning-diagnostic/` review-artifact directory.

## Initial implementation evidence

The following records the initial ID 013 implementation. Statements describing
inverse-based backfill/read-only compatibility are superseded by the correction
checkpoint above; the remaining numerical, storage and safety evidence stands.

## Implementation

- R and Python implement `directional_strict_v1`: strict value > reference,
  equality 0, nullable future labels, finite explicit references, vector or
  row-major matrix shapes, and no implicit recycling/broadcasting.
- The window worker returns one original-scale cleaned reference. The Mac
  coordinator reads untouched parent futures in bounded chunks and atomically
  stores compact labels with each existing window identity.
- Child schema version 3 adds definition and label tables only. Resume reuses
  accepted exact-reference labels. The initial inverse-derived schema-v2
  compatibility path is superseded by the correction checkpoint above.
- Retrieval validates parent/child lineage, definition, exact-reference
  provenance and labels against the canonical future. Unmigrated schema-v2
  children remain readable with labels explicitly unavailable.

No W/H/stride, cleaning, transformation, S1, Mantis, prediction, probability,
metric, padding or failure behaviour changed. No accepted research database was
used for mutation testing.

## Changed files and purpose

| File | Purpose |
| --- | --- |
| `src/python/util/shared_labels.py` | Authoritative vectorised Python calculation and persisted definition constants |
| `src/r/util/labels.R` | Matching R calculation plus the delegating historical adapter |
| `src/python/util/shared_distributed_execution.py` | Return the final cleaned input value without exposing future observations to R |
| `src/python/util/window_preparation.py` | Bounded label calculation, additive schema, atomic persistence, retrieval and restart compatibility |
| `src/python/tests/test_labels.py` | Shared R/Python numerical and shape fixtures |
| `src/python/tests/test_window_preparation.py` | Connected missing-target, persistence, retrieval, restart and bounded resource checks |
| `src/r/tests/test_transformations.R` | Direct R vector/matrix/missing/invalid checks |
| `docs/poc2-directional-labels.md` | Approved scientific, caller, storage and legacy-difference contract |
| `docs/architecture.md`, `docs/poc2-window-training-architecture.md`, `docs/poc2-standardisation.md` | Affected architecture and historical-decision references |
| `AGENTS.md`, `docs/amp-poc2-directional-labels-instructions.md` | Current approved task reference |

## Verification evidence

Mac focused checks completed on the final implementation:

- `tests.test_labels`: 5 tests passed, including R/Python shared vector/matrix
  fixtures, ties, negatives, constants, representable near-ties, all configured
  horizon lengths, one-element horizons, missing futures, invalid references,
  infinity and incompatible shapes.
- `src/r/tests/test_transformations.R`: all checks passed, including direct R
  matrix/missing/invalid label cases and the delegating legacy adapter.
- Combined `tests.test_labels tests.test_window_preparation`: 21 tests passed in
  53.169 seconds. The 16 window tests include a connected disposable
  case persisted 100 windows and 100 labels, retained one missing future as a
  missing label, retrieved the cleaned original-scale reference, reused all
  accepted rows on restart, and rebuilt only labels when emulating schema v2.
  Prepared windows and S1 membership were byte-for-byte/query-for-query
  unchanged across restart and the then-current compatibility migration. The
  correction checkpoint replaces that migration's label result, not its safety
  evidence.
- The connected case is bounded to 100 series/100 windows. Its local run is
  required to remain below 120 seconds and 192 MiB of traced Python allocations.
  That assertion passed; the complete connected test including initial write,
  two normal resumes, schema-v2 label backfill and retrieval took 23.889 seconds.
- General-environment discovery ran 244 tests: 243 passed and the known isolated
  GIFT-Eval semantics module could not import `gift_eval`. The same module passed
  2 tests in the pinned `environments/gift-eval/.venv`. This dependency boundary
  predates ID 013 and no GIFT-Eval file changed.
- Python compilation, R parsing and `git diff --check` passed.

Ubuntu was clean at the same `dfc1af0` base and GIFT-Eval submodule revision
before the reviewed 14-path overlay. Existing untracked `.amp/`, local
environments, databases and results were untouched. Final focused evidence:

- The same 21 Python label/window tests passed in 65.355 seconds.
- The connected persistence/restart/resource test passed in 29.304 seconds,
  including its 120-second and 192-MiB bounds.
- The same R transformation/label checks, Python compilation, R parsing and
  `git diff --check` passed.
- Mac and Ubuntu have matching bytes for the 13-path source/test/contract overlay:
  combined SHA-256 `d14f7b3959a4e74f042114937f50c0b10c891b6798902e6ee4ca5111c0646b99`.
  This evidence record is excluded from its own hash. No distributed cluster or
  GPU was needed for this focused calculation/storage increment.

## Manual QA example

```python
from util.shared_labels import directional_labels

directional_labels([7.0, 5.0, None, 4.0], 5.0).tolist()
# [1.0, 0.0, nan, 0.0]

directional_labels([[2.0, 1.0], [-1.0, -3.0]], [1.0, -2.0]).tolist()
# [[1.0, 0.0], [1.0, 0.0]]
```

## Script and line counts

The count scope is first-party `*.py`/`*.R` files below `src`, physical lines:

| Affected script | Before | After | Change |
| --- | ---: | ---: | ---: |
| `src/python/util/shared_labels.py` | 0 | 61 | +61 |
| `src/r/util/labels.R` | 21 | 68 | +47 |
| `src/python/util/shared_distributed_execution.py` | 1,525 | 1,527 | +2 |
| `src/python/util/window_preparation.py` | 1,235 | 1,489 | +254 |
| `src/python/tests/test_labels.py` | 0 | 162 | +162 |
| `src/python/tests/test_window_preparation.py` | 704 | 913 | +209 |
| `src/r/tests/test_transformations.R` | 118 | 129 | +11 |

Production/support changed by +364 lines and automated tests by +382 lines.
Overall source changed from 79 scripts/30,366 lines to 81 scripts/31,112 lines:
+2 scripts and +746 lines. The correction itself reduced production code by
removing unsafe inverse backfill while adding regression evidence. No script was
removed or moved inactive. The storage,
restart and cross-language tests account for most of the increase; no duplicate
window/future/split representation was added.

## Scientific decisions

The cross-platform robust-cleaning difference is now explained and recorded; it
does not require an ID 013 label decision because each host persists its actual
cleaner output. Any request to make cleaning bit-identical across architectures
would be a separate consequential scientific correction. The researcher has
accepted ID 013 with this limitation; no such cleaning change is authorised by
this closure.
