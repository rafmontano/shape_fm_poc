# AMP implementation instructions: POC2 ID 037 SMYL Oracle

## Authority and objective

Implement the approved dormant ID 037 increment from
[`poc2-id037-smyl-oracle.md`](poc2-id037-smyl-oracle.md). Read that decision,
`AGENTS.md`, `docs/code-standards.md` and
`docs/poc2-forecast-adjustment-reference.md` completely before editing.

The objective is one small, functional import of the historical SMYL Oracle
calculation with focused parity evidence. Do not redesign the pipeline or add
infrastructure around it.

Preserve the current uncommitted documentation candidate. Work locally on the
Mac and leave the complete result uncommitted and unstaged for Chief Developer
and Chief Architect review.

## Authorised paths

Implementation may add only:

- `src/r/util/forecast_adjustments.R`;
- `src/r/tests/test_forecast_adjustments.R`.

The already approved documentation paths may remain changed:

- `AGENTS.md`;
- `docs/poc2-id037-smyl-oracle.md`;
- `docs/poc2-forecast-adjustment-reference.md`;
- `docs/amp-poc2-id037-smyl-oracle-instructions.md`.

Stop and report before changing any other production, test, configuration,
schema, dependency, lock, entry-point or documentation file.

## Required implementation

Create one shared R utility containing the cohesive forecast-adjustment
calculations. Do not create an ID-specific runner or a sensitivity folder.

Provide one public function:

```r
calculate_smyl_oracle(actual, smyl_forecast)
```

The function must:

1. Accept finite, numeric, non-empty vectors of equal length.
2. Use exactly `seq(0.500, 1.500, by = 0.001)` and verify its 1,001 values.
3. For each multiplier, calculate exactly the historical full-horizon objective:

   ```r
   mean(
     200 * abs(actual - multiplier * smyl_forecast) /
       (abs(actual) + abs(multiplier * smyl_forecast)),
     na.rm = TRUE
   )
   ```

4. Select the first exact minimum, matching `which.min()`.
5. Count minima within `sqrt(.Machine$double.eps)` of the selected objective.
6. Return an ordinary named list with exactly:

   - `method_id = "smyl_scalar_smape_oracle_v1"`;
   - `multiplier`;
   - `objective_smape`;
   - `near_tie_count`;
   - `adjusted_mean`.

7. Preserve the historical omission of zero-over-zero terms. If a candidate
   objective or the complete grid cannot provide a finite comparison, fail with
   a clear error rather than selecting an ambiguous value.
8. Write no file, database row or global state and load no package.

The fixed method ID and multiplier grid are documented code constants, not new
experiment settings. Keep any objective helper internal to this utility. Do not
create a general duplicate evaluation API.

Use the approved bounded functional exception: this stateless mathematical
kernel does not need a class. Add the repository-standard file header and concise
Purpose, Inputs and Outputs documentation for every function and code constant.

## Do not import or change

Do not import, source or reproduce `src/r/sensitivity/00_sensitivity_common.R`.
Do not import its:

- period, subset, worker or Chronos settings;
- M4 rank constants or lambda grids;
- path and frequency globals;
- RDS read/write helpers;
- packages, launch loops or mutable series objects.

Also do not:

- source the new utility from a production entry point;
- add it to Processes 04, 05 or 06;
- add an experiment configuration field;
- change DuckDB tables or stored results;
- place `smyl_oracle` in the ordinary forecast pool;
- use it as a training feature or combination input;
- fit or download SMYL or any other model;
- install or update dependencies;
- modify dependency lockfiles;
- contact Ubuntu, start Prefect/Dask, run a scientific experiment, commit, push,
  tag, amend, reset or manipulate stashes.

## Focused tests

Add a dependency-free R test script following the existing source/test style.
It must be runnable from the repository root with:

```text
Rscript --vanilla src/r/tests/test_forecast_adjustments.R
```

Cover at least:

1. Frozen asymmetric fixtures independently obtained from the historical ID 037
   calculation, checking every output field and the full adjusted vector.
2. Exact first-minimum selection when more than one grid value has the same
   objective.
3. Near-tie counting using the historical tolerance.
4. Selection at multiplier `0.500`, multiplier `1.500`, and an interior value.
5. Mixed valid and zero-over-zero horizon terms, preserving historical omission.
6. The all-zero/all-unavailable objective, which must fail clearly.
7. Empty, non-numeric, dimensional, unequal-length and non-finite inputs.
8. Stable method identity and exact result-field names.
9. Loading the utility adds no package namespace.

Do not source the complete historical script because its top-level runner has
filesystem side effects. A test-only isolated legacy reference function or
frozen values may be used for read-only parity evidence. Do not use the new
function itself to generate its expected values.

## Validation

Run only proportionate local checks:

1. Parse the new R utility and test file.
2. Run the focused test with `Rscript --vanilla`.
3. Run the existing relevant R forecast-method regression suite and ID 033
   dormant-utility suite without changing their source.
4. Confirm no active production source references
   `forecast_adjustments.R`, `calculate_smyl_oracle` or
   `smyl_scalar_smape_oracle_v1`.
5. Confirm all experiment configurations, database schema files and dependency
   locks are byte-identical to the starting revision.
6. Run whitespace validation over tracked and new files.

Do not expand this into a complete two-machine or pipeline test: dormancy is part
of the accepted contract.

## Final report and stop point

Report concisely:

- exact changed paths;
- public input/output contract;
- historical parity evidence and exact fixtures covered;
- focused and existing regression results;
- dependency-free and dormant-isolation evidence;
- unchanged configuration, schema and lockfile evidence;
- remaining boundary: Process 06/Table 1 activation with IDs 038 and 041 is not
  implemented or approved by this increment.

Leave all work uncommitted and unstaged. Stop for Chief Developer and Chief
Architect review.

## Accepted candidate and closure authorisation

The Chief Architect reviewed the actual implementation and tests, independently
reran the focused ID 037, ID 033 and forecast-method suites, and recommended
approval. The researcher accepted the candidate on 7 October 2026 and authorised
normal publication and source synchronisation. This section supersedes only the
earlier no-commit/no-Ubuntu stop point; all scientific and activation boundaries
above remain authoritative.

Perform one pragmatic closure operation:

1. Verify Mac `main` still starts at
   `72cab5b504643081389ad9748fa1dcdaa2bb4673` and the worktree contains exactly
   these six approved paths:

   - `AGENTS.md`;
   - `docs/poc2-forecast-adjustment-reference.md`;
   - `docs/poc2-id037-smyl-oracle.md`;
   - `docs/amp-poc2-id037-smyl-oracle-instructions.md`;
   - `src/r/util/forecast_adjustments.R`;
   - `src/r/tests/test_forecast_adjustments.R`.

2. Confirm Ubuntu is clean on `main` at the same published starting revision and
   record its existing stash fingerprint without modifying any stash. Stop on a
   revision, cleanliness or path-set mismatch.
3. Rerun on Mac only the focused dependency-free test, parse checks and
   whitespace check. Reuse the already accepted broader regression evidence; do
   not rerun an experiment or start Prefect/Dask.
4. Stage exactly the six approved paths, inspect the complete staged diff, and
   create one commit with subject:

   ```text
   feat: add dormant SMYL Oracle diagnostic
   ```

5. Refresh `origin/main`; proceed only if publication is a normal fast-forward.
   Push `main` without force and without a tag.
6. On clean Ubuntu, fetch and fast-forward `main` from `origin/main`. Do not
   install or synchronize dependencies.
7. Run only this dependency-free verification on Ubuntu:

   ```text
   Rscript --vanilla src/r/tests/test_forecast_adjustments.R
   ```

8. Confirm Mac `main` = Ubuntu `main` = `origin/main`, both worktrees are clean,
   Ubuntu's pre-existing stashes are unchanged, and no generated artifact was
   committed.

Report the commit, exact six committed paths, Mac and Ubuntu focused-test result,
three-way revision equality, cleanliness and unchanged stashes. Record ID 037 as
closed only as a dormant capability. Process 06 activation, diagnostic storage,
Table 1 execution and IDs 038/041 remain open.
