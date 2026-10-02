# POC2 Standardisation Decision

Approved: 30 September 2026; implementation authorised: 1 October 2026.
Scope: Objective 2, migration list ID 010, Gate 3.
Status: Implemented; focused Gate 1–3 acceptance completed 1 October 2026.

The prerequisite [execution safeguard follow-up](amp-poc2-execution-safeguards-instructions.md)
was completed, validated and published as commit `9579fc9` on 30 September
2026. The researcher reviewed that result and authorised ID 010 to start on
1 October 2026. The [execution policy](execution-policy.md) governs any heavy
testing.

## Decision

Use the versioned recipe `standardise_sample_v1` for direct per-series or
per-window standardisation, with explicit constant-input handling and
consistent local implementations in R and Python. Do not add min–max scaling
or the redundant min–max followed by standardisation recipe to the new POC2
workflow.

This is a Bake Off inspired approach adapted to ShapeFM, not an exact historical Bake Off reproduction. The aim is reliable, reproducible preparation, not a study of whether transformations improve forecast accuracy. See the [evidence record](poc2-standardisation-evidence.md) and [AMP implementation instructions](amp-poc2-standardisation-instructions.md).

## Mathematical contract

Fit parameters only on the observed history or context window x. Future validation/test observations never participate in fitting.

For a finite nonconstant window with n observations:

```text
centre = sum(x[i]) / n
scale  = sqrt(sum((x[i] - centre)^2) / (n - 1))
forward(y) = (y - centre) / scale
inverse(z) = centre + scale * z
```

Use sample standard deviation, with denominator n − 1, in both languages. This preserves the previous R convention. The reviewed TSML normalisers and current ShapeFM Python utility use n, so they are not numerically identical to this decision. Removing min–max and changing the Python denominator are separate changes.

For nonconstant inputs, min–max followed by standardisation equals direct standardisation algebraically when both use the same denominator convention. Small floating-point differences remain possible.

For an exactly constant history x = (c, ..., c), including a single finite observation:

```text
centre = c
effective scale = 1
constant = true
forward(y) = y - c
inverse(z) = c + z
```

History (5, 5, 5) becomes (0, 0, 0); a future value 7 becomes 2 and inverse(2) returns 7. A transformed forecast 1.5 returns 6.5. The fallback divisor is not an estimated standard deviation. Never force future observations or forecasts to the historical constant.

Gate 2 remains responsible for cleaning missing values. Transformation inputs must be nonempty and finite. Report invalid inputs or numerical failures rather than silently returning zeros. Do not introduce a near-constant threshold: small real variations remain nonconstant. Utility support for short inputs does not imply that every forecasting model accepts them.

## Consistency between languages

Provide one specification and two small local implementations, executed where the data already resides. Transferring data between languages solely to scale it is unnecessary.

Both implementations share:

- Registered names and versions, formulas, ordered-step semantics and fitting boundaries.
- Double-precision calculations and the explicit sample-SD and constant policies.
- Portable fitted parameters: centre, effective scale, observation count and constant flag.
- Separate fit, apply and inverse operations. Apply and inverse never refit.
- Validation rules, numerical test fixtures and declared comparison tolerances.

Maintain the agreed small ordered-list interface with `identity` and
`standardise_sample_v1`. Apply forward steps in order and inverse steps in
reverse order. Do not build a formula interpreter or universal translation
framework. Existing no-transformation execution remains available where
already selected; it is not a new cleaning mode.

Human-readable source headers and function comments must explain the fitting boundary, mathematical rule, constant safeguard and inverse.

## Pipeline ownership and compatibility

The [research vision](research-vision.md) assigns preparation to the outer ShapeFM system. Gate 3 owns the selected transformation and fitted state; forecast-model functions remain unaware of transformation-specific branches. Existing adapters construct native R or Python objects.

Select preparation by consumer or experiment, not one compulsory representation for every model. Do not apply a step twice if the data was already transformed upstream. Document model-internal normalisation separately. Mantis input-length resizing is not amplitude standardisation.

For forecasts produced on the standardised scale, invert the mean and any supplied median or quantiles using the same fitted affine map. Do not fabricate missing probabilistic outputs. Labels and embeddings require no inverse.

Reuse existing storage, task identity, restart and bounded parallel execution. Python remains the sole DuckDB writer. Preserve imported observations and existing results; do not create another canonical dataset. Version the new recipe so its outputs cannot be confused with existing population-SD or constant-collapse outputs.

The exclusion of min–max applies to new POC2 recipes. It does not authorise changing the meaning of old stored recipe IDs or breaking historical retrieval/resume. Retain only the existing compatibility needed for those identities; do not create another selectable legacy experiment mode. A scientifically changed experiment uses a fresh isolated database, not overwritten results.

The formula is independent of sampling frequency. Frequency grouping and the [seasonal-period tuning decision](poc2-seasonal-period-tuning.md) remain separate. In particular, each historical tuning fold must fit this transformation on its own permitted training input.

## Selected functions

These decisions concern migration, not deletion of source from the previous project. Required behaviour may be consolidated instead of copied under every old name.

| Previous function | Decision | Treatment |
| --- | --- | --- |
| `infer_frequency` | Not needed | Use the separately approved central seasonality solution. |
| `get_m4_horizon` | Not needed | Use the dataset or benchmark horizon. |
| `get_window_size_from_h` | Needed with update | Preserve needed legacy context lengths through explicit window configuration, outside transformations. |
| `freq_tag` | Not needed | Legacy filename shorthand; use existing dataset identities. |
| `window_tag` | Not needed | Legacy filename shorthand; use configured window identifiers. |
| `period_to_freq` | Not needed | Do not migrate the duplicate hardcoded frequency mapping. |
| `minmax_vec` | Not needed | Excluded from the revised new POC2 transformation scope. |
| `standardise_vec` | Needed with update | Implement fitted standardisation in both languages. |
| `scale_pair_minmax_std` | Not needed | Replace the required new workflow with direct standardisation. |
| `scale_pair_std` | Needed with update | Fit on history and reuse parameters through the common apply operation. |
| `compute_c` | Not needed | Legacy threshold labels are outside the selected direction path. |
| `label_from_z_int` | Not needed | Legacy alternative label conversion. |
| `compute_z` | Not needed | Legacy aggregate direction definitions. |
| `compute_z_generic` | Not needed | Selector for those legacy definitions. |
| `compute_z_all` | Not needed | Batch wrapper for those legacy definitions. |
| `compute_label_vector` | Needed as is | Preserve future value > last history value as 1, otherwise 0; keep in label preparation. |

`standardise_vec` and `scale_pair_std` represent one reusable capability per language, not independent scaling implementations. The min–max decisions supersede the earlier proposal to retain those functions. Constant-policy corrections must preserve real direction changes rather than reproduce zeroed future labels.

## Acceptance and exclusions

Require shared tests of nonconstant, negative, constant, single-observation, nearly constant and invalid inputs; fitted-state equivalence; additional values outside the historical range; inverse recovery; and no future leakage. Compare to the old R recipe on identical nonconstant inputs within a declared tolerance. Report expected differences from denominator choice, rounding and corrected constants.

Use focused tests and at most the existing first 100 official M4 Daily series. Validate affected fold preparation, persistence, resume and sequential/parallel consistency without a full pipeline, full-dataset run or transformation-accuracy sweep. Account for existing stored numeric precision when comparing with original R data.

Operational update authorised on 30 September 2026: until the researcher advises otherwise, computationally heavy tests should use both Mac and Ubuntu through existing bounded distributed execution. Ubuntu is reported on; readiness must still be checked. Lightweight checks may remain local. Require actual completed tasks on both hosts, overlapping execution, and agreement with a small sequential reference, not just worker connectivity. Preserve the Mac coordinator/sole-writer arrangement and existing resource limits. If a host is unavailable, report the two-machine check as outstanding rather than silently accepting a Mac-only heavy run. This supersedes earlier Mac-only guidance without expanding scientific scope or requiring GPU work for CPU transformations.

Ubuntu must first be synchronised to the intended Mac code baseline after the preceding task finishes, then synchronised again with ID 010 changes before distributed tests. Preserve local work on both machines and verify the exact tested source, including relevant uncommitted files; equal Git HEAD values alone do not establish equal working trees. Do not copy platform-specific environments or overwrite data/results as part of code synchronisation. Unresolved synchronisation blocks distributed acceptance.

No forecast-pool expansion, new feature/meta-learner training pipeline, seasonality-policy change, Box–Cox implementation, general vision clean-up or unrelated script removal is authorised here. Preserve existing manual QA scripts.

## Approval and future reference

The researcher approved the reviewed ID 010 decision and repository
documentation on 30 September 2026, reviewed the completed execution
safeguards, and authorised implementation on 1 October 2026. The approved
design is recorded here; the [evidence record](poc2-standardisation-evidence.md)
separates verified historical observations from future acceptance
requirements.

This decision refines the previously deferred transformation composition discussion: standardisation is the only initial transformation step; the small compositional boundary remains. The [code standards](code-standards.md) still govern configuration authority, comments and naming. The explicit R/Python mathematical counterparts are intentional, not competing orchestration implementations.

Implementation started from the clean, synchronised `9579fc9` baseline. The
focused Gate 1–3 acceptance and its limits are recorded in the
[evidence record](poc2-standardisation-evidence.md). This status does not certify
a full forecasting/evaluation pipeline, forecast improvement, a commit, or
GitHub publication.

For future exploration only, see
[transformation objects and fable research notes](research-notes-transformations.md).
The note preserves implementation options and sources; it does not amend this
decision or authorise work in the current migration.
