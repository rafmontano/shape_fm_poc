# POC2 Standardisation Evidence

Recorded: 30 September 2026; prerequisite status updated 1 October 2026.
Related [approved decision](poc2-standardisation.md).

This document has two dated evidence phases. The opening sections preserve the
decision basis and small read-only observations recorded before implementation;
those observations alone did not establish implementation or acceptance. The
sections dated 1 October 2026 separately record the later implementation,
focused tests, and bounded 100-series Gate 1–3 acceptance. No section records a
full forecasting pipeline, GPU, full-dataset, or forecast-accuracy acceptance.

## Literature and implementation evidence

| Source | Verified finding | Limit on interpretation |
| --- | --- | --- |
| [UCR archive paper, version 2, section III-C](https://arxiv.org/html/1810.07758v2) | Per-series z-normalisation removes offset and scale; the expanded archive supplies raw new data where possible and records donor normalisation. | Not a requirement to normalise every task, and not a specification of ShapeFM inverses or constant policy. |
| [Original Bake Off paper, introduction and experimental design](https://link.springer.com/article/10.1007/s10618-016-0483-9) | Controlled normalised datasets were compared through a common framework and repeated splits. Coffee demonstrates how inconsistent normalisation can weaken a baseline. | Does not establish one constant-series rule for every original experiment or guarantee universal accuracy gains. |
| [TSML legacy normaliser at revision f885bf3](https://github.com/time-series-machine-learning/tsml-java/blob/f885bf327e7ddf7ebf9c71dd5840e7b7654704c2/src/main/java/tsml/filters/NormalizeCase.java) | Uses population variance. For zero variance, the dataset overload warns and leaves the series unchanged by default; the array overload throws an exception. | This inspected revision is from 2019, not proof of the exact original Bake Off runtime. |
| [Later TSML row normaliser](https://github.com/time-series-machine-learning/tsml-java/blob/master/src/main/java/tsml/transformers/RowNormalizer.java) | Uses population variance in the Weka-instance path and returns zeros for variance treated as nearly zero. | The master URL is not a historical paper snapshot; the inspected behaviour was read on 30 September 2026. |
| [Mantis original paper, sections 2.2 and 2.3](https://arxiv.org/html/2502.15637v1) | Internally standardises inputs and also encodes statistics from before that internal normalisation. | External scaling is not automatically redundant; this decision does not modify the pretrained architecture or establish a new Mantis accuracy result. |
| [R standard deviation](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/sd.html) and [NumPy standard deviation](https://numpy.org/doc/stable/reference/generated/numpy.std.html) | R uses n − 1; NumPy exposes the denominator choice through ddof, with ddof=1 for n − 1. | Language/library defaults must not determine the cross-language scientific contract. |

The chosen fallback divisor of 1, portable fitted state and affine inverse are ShapeFM decisions, not claims copied from Bake Off.

## Local source snapshots

The source files below were read directly and were unchanged relative to their repository revisions at the time of review. AMP had unrelated pending seasonal-period work in the ShapeFM checkout.

| Repository and file | Observed revision | SHA256 of inspected file |
| --- | --- | --- |
| Previous project `src/r/utils.R` | `839b5b6dc4334b5f088adce610f6d35935cbbca1` | `5b0e29c472d41db3db140108272a838c213bd091b0b3649b2c30559826cdacc0` |
| ShapeFM `src/python/util/transformations.py` | `94a3f09607f55bdf16e987aebc9be1e4b19f706b` | `eb95b92b5da44486b3b73d144e58ab2497c27506893434494237ec624aa30c47` |

Source references: [previous R utilities](https://github.com/rafmontano/m4-tsc-fmts-2026/blob/839b5b6dc4334b5f088adce610f6d35935cbbca1/src/r/utils.R) and [ShapeFM Python utility](https://github.com/rafmontano/shape_fm_poc/blob/94a3f09607f55bdf16e987aebc9be1e4b19f706b/src/python/util/transformations.py). These links identify locally observed commits; remote accessibility was not separately tested for this record.

Relevant observations in the previous project:

- `standardise_vec` and the pair helpers use R sample SD. The pair helpers fit parameters on history only.
- For constant history, both pair helpers zero supplied future values even when the future changes.
- `04_transformations.R` uses min–max then standardisation on rolling windows; `10a_build_real_eval_parallel.R` and `util_metrics.R` use standardisation only.
- `compute_label_vector` uses strict future > last-history comparison. The old Mantis bridge consumes R-prepared windows; the mismatch above does not prove all previous cross-language processing was inconsistent.
- `infer_frequency` maps Daily to 7 while `period_to_freq` maps it to 1. These mappings must not be migrated into the transformation utility.
- The old context-window grid is Yearly 8/16/32, Quarterly 16/32/64, Monthly/Weekly/Daily 32/64/128, and Hourly 128/256/512 for small/default/large, with full history also supported. These are legacy configuration choices, not universal frequency rules.

At review time, the current Python utility fitted population SD, retained inverse parameters, and forced all inverse forecasts to the historical constant for a constant context. Its callers included normal Gate 3/4 execution and AMP's in-progress historical seasonal-period folds; all applicable callers must be reviewed after that task finishes.

## Mathematical equivalence and its limits

For a nonconstant finite x, let a=min(x), b=max(x) and u=(x−a)/(b−a). With the same SD convention:

```text
mean(u) = (mean(x) - a) / (b - a)
sd(u)   = sd(x) / (b - a)
(u - mean(u)) / sd(u) = (x - mean(x)) / sd(x)
```

This removes the reason for a preceding min–max step. It does not make sample and population SD identical. For n > 1:

```text
z_sample = z_population * sqrt((n - 1) / n)
```

Constants are outside that algebra because b−a and SD are zero. The approved fallback makes the stored map invertible for additional values. Differences in stored precision and arithmetic order can also prevent bitwise equality.

## Read only probes actually executed

The following observations were produced by sourcing the existing old R file and importing the existing ShapeFM Python utility, not by implementing the new design. Runtime: R 4.6.1 and Python 3.13.5 on the Mac. Python bytecode writing and R startup/workspace loading were disabled. These are inspection runtimes, not a proposal to change project environments.

| Probe | Observed result |
| --- | --- |
| Old R min–max then standardise, x=(10,20,30), future=(40) | history=(-1,0,1), future=(2) |
| Old R standardise only, same inputs | history=(-1,0,1), future=(2) |
| Old R min–max then standardise, x=(5,5,5), future=(7) | history=(0,0,0), future=(0) |
| Direction label after that constant-history scaling | 0 |
| Direction label from untouched x=(5,5,5), future=(7) | 1 |
| Existing Python combined recipe, x=(10,20,30) | (-1.224744871391589, 0, 1.224744871391589) |
| Existing Python inverse of that transformed history | (10.000000000000002, 20, 30) |
| Existing Python combined recipe, x=(5,5,5) | (0,0,0) |
| Existing Python inverse of (-100,0,100), fitted on x=(5,5,5) | (5,5,5) |

To reproduce the historical R observations in a session using that source revision:

```r
source("/Users/monta/Documents/Projects/m4_tsc_fmts_2026/src/r/utils.R")
scale_pair_minmax_std(c(10, 20, 30), c(40))
scale_pair_std(c(10, 20, 30), c(40))
flat <- scale_pair_minmax_std(c(5, 5, 5), c(7))
flat
compute_label_vector(flat$x_std, flat$xx_std)
compute_label_vector(c(5, 5, 5), c(7))
```

To reproduce the Python observations at the recorded source revision, with `src/python` on the import path:

```python
from util.transformations import transform, inverse
method = "minmax_then_standardize"
normal = transform([10, 20, 30], method)
normal.values
inverse(normal.values, method, normal.parameters)
flat = transform([5, 5, 5], method)
flat.values
inverse([-100, 0, 100], method, flat.parameters)
```

Run such historical probes in a separate checkout/session if later changes replace these interfaces; do not reset an active project or modify an experiment database to reproduce them.

## Requirements recorded before implementation

The preimplementation review required dated, reproducible evidence for the new
implementation: changed revision and environments, commands actually run,
shared R/Python fixtures, state interoperability, declared tolerances, fold
leakage tests, source immutability, old-recipe compatibility,
persistence/resume, bounded parallel consistency and manual QA. Failures and
skipped checks also had to be retained. The later dated sections report the
completed evidence and remaining limits.

Following the researcher's 30 September 2026 execution update, heavy-test evidence must include both Mac and Ubuntu until advised otherwise: readiness/code/environment checks, host and worker IDs, completed task counts per host, task timestamps demonstrating overlapping execution, wall time, failures/retries and comparison with a small sequential reference. Connected workers alone are insufficient. Record an unavailable host as an outstanding distributed check, not a successful Mac-only substitute. This is a requirement for future runs; the historical Mac-only probes above remain unchanged and do not establish Ubuntu readiness or two-machine acceptance.

Record the Ubuntu code synchronisation performed before testing: method,
verified source/target checkout identities, revisions and relevant submodule
revisions, source fingerprints for relevant uncommitted changes, and
confirmation that test workers loaded the updated code. Revalidate after code
changes. Synchronisation must preserve work on both machines and exclude
platform-specific environments, secrets and existing databases/results.

## Prerequisite closure reviewed on 1 October 2026

AMP reported the execution safeguards complete and published at commit
`9579fc9`. Mac, Ubuntu and `origin/main` were clean at that commit, with the
same 61-file runtime-source manifest (`e28154d3...a365`). The focused two-host
evidence used 8 Mac CPU and 15 Ubuntu CPU workers; all eight Mac workers
completed useful overlapping work, and a real comparison overlapped two ETS
fits on Mac with AutoARIMA on Ubuntu. The three fresh-fold outputs matched the
sequential references with maximum difference 0 at tolerance `1e-10`.

The reported safeguards also passed 81 broader Python tests, 51 focused safety
tests and all nine R forecast-method tests. The accepted 800-forecast database
remained unchanged and an isolated restart skipped completed work. This closes
the operational prerequisite for beginning ID 010; it is not implementation or
acceptance evidence for the standardisation contract itself.

The prerequisite report also noted that `renv` considers the lock not fully
synchronised, although all required pinned R packages loaded. ID 010 must not
turn that observation into an unapproved dependency upgrade or wholesale lock
regeneration.

Use at most the first 100 official M4 Daily series for gate-local acceptance and report nonconstant/constant counts and expected discrepancies. Do not label the small historical probes above as cross-language acceptance of the new code, a full pipeline test or evidence of forecast improvement.

## ID 010 implementation and gate-local acceptance — 1 October 2026

Implementation started from commit `9579fc9` after confirming Mac, Ubuntu and
`origin/main` at that revision with stopped workers. The six approved pending
documentation edits listed in the implementation instructions were retained.
No dependency was added, no lockfile was regenerated, and accepted experiment
databases were not modified.

The new `standardise_sample_v1` implementation is local to both R and Python.
It uses separate fit/apply/inverse operations, exact six-key portable state,
sample SD (n−1), and an effective scale of 1 for exact constants and singletons.
The ordered interface contains only `identity` and the new recipe. The legacy
Python `minmax_then_standardize` path remains unchanged for v1–v3 retrieval and
resume. `standardise_vec` and `scale_pair_std` reuse the same R fit/apply
implementation. Direction labels retain strict future > final-history
semantics, including zero for ties.

Focused commands executed on Mac included:

```sh
PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest \
  tests.test_transformations tests.test_configuration tests.test_experiment_execution
Rscript src/r/tests/test_transformations.R
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database .amp/in/id010_standardisation_acceptance.duckdb \
  --configuration config/experiments/poc2_m4_daily_100_standardised.json \
  --processes 1-3
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database .amp/in/id010_standardisation_acceptance.duckdb --processes 1-3
```

The initial focused Python standardisation set passed 30 tests; the broader
configuration, execution and tuning selection passed 73 tests. After final
documentation and source synchronisation, the exact Python command shown above
passed 43 tests on each host, and the R contract suite passed on each host.
Python was 3.12.14, R was 4.6.1 and jsonlite was 2.0.0. Expected synthetic Dask
failure logs in the broader suite were recovery assertions, not test failures.
The final top-level Python suite passed 155 tests. An unscoped recursive
discovery also loaded the GIFT-Eval integration module in the main environment
and failed because NumPy is intentionally absent there; rerunning that module
with its documented locked GIFT-Eval environment passed both tests. No package
was installed or lock changed to hide this environment boundary.
Manual QA succeeded under the normal repository R startup by sourcing
`src/r/qa/inspect_standardisation.R`; `Rscript --vanilla` could not load the
project DuckDB package and was not used to change dependencies.

The fresh acceptance database contains 100 selected series and 57,235 raw
observations, 200 completed preprocessing rows, and 400 completed
transformations (200 identity and 200 standardised). All 200 fitted histories
were nonconstant. Every standardisation state had exactly the approved keys.
The largest transform/inverse round-trip discrepancy was
`1.8189894035458565e-12`. A resume request skipped Processes 01–03 and every
scientific task attempt remained exactly 1.

Across the 100 robust histories, the maximum R/Python centre difference was
`4.9112713895738125e-11`, the maximum scale difference was
`1.8189894035458565e-12`, and the maximum transformed-value difference was
`1.865174681370263e-14`. New R output matched the prior direct nonconstant
sample-standardisation formula exactly. The observed historical Python
population/sample relation differed from its algebraic expectation by at most
`8.881784197001252e-16`. These are double-precision arithmetic-order effects,
not materially different recipes.

Ubuntu initially received 16 implementation paths and then the complete final
22-path source/documentation set through checksum `rsync`, without `--delete`
and excluding environments, databases and results. Mac and Ubuntu file
manifests were compared after each transfer.
Small isolated process-pool checks used 8 Mac and 15 Ubuntu CPU workers; each
completed 64 deterministic tasks and produced result SHA-256
`73f7cfbc7f7b4bcd1d247aae4a56276f5f251eb8e909dce712edb590ba82d49d`.
This proves both configured host capacities for the bounded numerical work; it
is not presented as a heavy distributed forecast acceptance. A first Mac
stdin-based multiprocessing harness failed because macOS spawn cannot reload
`<stdin>`; the file-based rerun passed and its temporary file was removed.

Preservation checks before and after acceptance retained SHA-256
`cbb5a8604dcfd7dbc8a5c4c6d930825d74711c6721061e13bfafcee884c2dbc7`
for `results/poc2_m4_daily_100_period_tuning.duckdb` and
`a2b99cf97be2bc833aff60874627dcedd16ced13399dfaf3318f673ade84b4fe`
for `data/shapefm.duckdb`. The new context-length utility and v4 setting are
tested but not consumed by current forecast adapters. Gate 4 model refits,
full evaluation, accuracy claims, full-dataset acceptance and GPU execution
remain outside this gate-local migration.

## ID 010 review corrections — 1 October 2026

A focused Process 04 integration test now creates a temporary configuration-v4
database, persists one `standardise_sample_v1` state, and supplies an asymmetric
stub AutoARIMA response on the transformed scale. The coordinator test verifies
that the stored state—not a refit—restores mean `(0, 1)` to `(20, 30)`, median
`(-0.5, 1.5)` to `(15, 35)`, and each of nine distinct quantile vectors to its
independently specified original-scale expectation. It also verifies original
scale, transformation lineage and probabilistic capability. No forecast model
was fitted.

The committed v4 `execution.final_acceptance` snapshot was corrected to the
acceptance actually run: sequential Processes 1–3, one Mac CPU worker, and its
2 GiB system-memory floor. It no longer inherits the historical Objective
1 Mac/GPU topology. This snapshot is not a heavy execution profile; heavy work
still requires the approved named 8-Mac/15-Ubuntu CPU profile.

After checksum synchronisation, the affected suite passed 44 Python tests and
the R standardisation suite independently on both Mac and Ubuntu. The accepted
Gate 1–3 database and both pre-existing accepted databases remained unchanged.
This review test closes the missing inversion-path coverage; it does not expand
the Gate 1–3 acceptance into a real forecasting, distributed, full-pipeline, or
accuracy acceptance.
