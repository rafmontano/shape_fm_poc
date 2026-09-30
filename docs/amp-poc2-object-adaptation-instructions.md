# AMP Code instructions for POC2 object adaptation

Implement the approved decision in [POC2 object adaptation and seasonal
period](poc2-object-adaptation.md). Read the [research vision](research-vision.md)
first. Keep this work limited to POC2. Separately review vision alignment and
report potential issues without correcting them.

## Inspect and preserve

Inspect the latest files, Git status, configuration, schema and tests before
editing. Work on the Mac; Ubuntu availability is not required. Preserve existing
changes, including the uncommitted `src/r/qa/` work. Do not remove scripts,
rename public entry points or undertake a broad refactor. List potentially
obsolete scripts for later user decisions.

The latest QA requirement is intentional: sourcing
`src/r/qa/get_m4_daily_series.R` must leave `selected_m4_series` with exactly
`st`, `x`, `n`, `type`, `h`, `period`, `xx`, `pt_ff`, `up_ff`, `low_ff`.
DuckDB supplies x and xx; M4comp2018 supplies metadata, original time bases and
submission matrices. Preserve identity/value checks and read-only database
access. Do not substitute package observations for DuckDB observations or turn
this QA object into the production request format. Check disk contents before
assuming an RStudio tab is current.

## Implement the minimum change

1. Reuse stored dataset frequency and existing experiment configuration. Locate
   the current authoritative fields before adding any setting. Make the
   explicit preprocessing/R-forecast period override optional and document its
   meaning in the existing configuration mechanism.
2. Resolve the period centrally: explicit override when present, otherwise
   `gluonts.time_feature.get_seasonality(stored_dataset_frequency)` in the pinned
   GIFT-Eval environment. Do not copy its mapping into R, add M4-specific lookup
   rules, or install benchmark dependencies into the lean coordinator solely
   for this call. Reuse the existing benchmark boundary and resolve outside
   per-series/per-model loops where practical.
3. Reuse the existing original/resolved experiment configuration storage. Add no
   duplicate seasonality columns or per-forecast metadata solely for this rule.
   Use existing experiment/dataset links and environment provenance. Retain
   historical schema fields for compatibility; no destructive schema cleanup.
4. Reuse or minimally consolidate the shared R input utility so the nine R
   methods receive their required `ts` input consistently. Keep conversion out
   of individual forecasting methods. Reuse objects within an existing compatible
   worker call where practical; do not redesign scheduling, combine gates or
   add persistent workers merely to eliminate a boundary conversion.
5. Reuse the existing Chronos-2 input/output handling. Do not pass R objects or
   unnecessary seasonality arguments to Chronos. Keep the current canonical
   mean/probabilistic result contract, method registry and visible seasonal-naive
   fallback. No generic provider-registry framework or fable implementation.
6. Use the resolved period for the requested R preprocessing and forecasting
   operations. Preserve standard/robust behaviour and raw values. Keep FFORMA's
   method-specific seasonal tests in their current role.
7. Keep benchmark evaluation seasonality separate from the preprocessing/model
   override. Check the pinned official GIFT-Eval path and use its benchmark
   scoring convention for new experiments. Report any resulting difference
   from previous period-7 scores; do not describe earlier scores as equivalent.
8. For new default experiments remove the mandatory M4 Daily period-7 assumption.
   In the pinned environment the daily default is 1. Provide a documented
   explicit override of 7 for legacy-cleaning comparisons. Preserve existing
   databases and stored scientific configurations, including historical value 7.
   Use configuration versioning only as necessary to distinguish old semantics;
   changed scientific settings require a new experiment, not rewritten results.
9. Add human-readable comments and update affected configuration, preprocessing
   and architecture documentation. Clearly distinguish the approved target from
   functionality not yet implemented. Do not claim complete support for every
   GIFT-Eval dataset simply because frequency resolution is generic.

## Focused validation

- Verify the default against the actual pinned resolver for daily and several
  other supported frequencies; verify an explicit override takes precedence.
  Include invalid overrides and unsupported-input handling.
- Verify the R object contains unchanged input values and the resolved frequency;
  its construction is shared rather than independently redefined by each model.
- Test standard mode preserves finite observations and robust mode matches the
  original `tsclean` expression at the SAME frequency. Use the agreed 100-series
  subset for the period-7 legacy comparison. Do not expect period 1 and period 7
  to give the same cleaned values.
- Retain nine-method FFORMA mean comparisons using matched input values,
  frequency, settings and seeds. Preserve fallback and quantile-contract tests.
- Verify a preprocessing/model override does not silently alter official
  evaluation seasonality. Keep missing-label masking tests.
- Verify old experiment configurations remain interpretable without changing
  stored results, and new default/override configurations can be created.
- Confirm official Smyl/FFORMA retrieval remains unchanged.
- Preserve the QA script and, where dependencies are available, verify sourcing
  it leaves the ten-component object for D1/D100, with DuckDB x/xx and package
  metadata. Keep its source frequency separate from the production policy.
- Run affected tests in their proper existing environments. Do not run the full
  4,227-series forecast pipeline or require Ubuntu/model downloads for this work.

## Vision alignment review only

After the scoped implementation, inspect the repository against the research
vision. Provide one concise table with: file/script, potential issue, vision
principle affected, impact, and suggested future action. Cite relevant functions
or lines and distinguish confirmed mismatches from questions needing research
decisions. Mark future planned capabilities as deferred, not automatically bugs.

Review object conversion, model-list management, storage ownership, duplicated
configuration, frequency assumptions, feature/meta-learner extensibility,
combination/adjustment, forecast retrieval and QA tooling. Identify scripts that
may be redundant or superseded, with their callers and why they might be removed.
Do not fix these wider issues or delete any scripts. If a wider issue blocks the
approved implementation, explain the dependency before expanding scope.

## Handoff

Report changed files, configuration usage, tests actually run, unresolved
limitations and the separate alignment table. Include a short manual QA example
for inspecting the resolved period and the production R object. Do not update
the spreadsheet. Do not include unrelated QA edits in this implementation
commit or claim they are published merely because this work is published.
