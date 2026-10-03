# POC2 centralised directional labels decision

Status: ID 013 approved by the researcher on 3 October 2026. Implementation is
authorised as a bounded extension of the approved window-preparation workflow.
Technical review and independent researcher QA remain required before commit or
publication.

## Scientific contract

ShapeFM has one authoritative numerical implementation per language:
`directional_labels()` in `src/python/util/shared_labels.py` and
`src/r/util/labels.R`. The persisted definition is `directional_strict_v1`,
version 1:

```text
label = 1 when value > reference; otherwise 0
```

Equality is class 0. Comparison uses ordinary double-precision `>` with no
tolerance, so representable near-ties remain distinct. A future vector receives
one finite reference. A future matrix receives exactly one finite reference per
row. A one-element vector is a valid horizon. Shape and order are preserved;
incompatible shapes fail rather than relying on R recycling or NumPy
broadcasting. Different horizons are processed as separate bounded batches.

Missing future observations remain explicitly unavailable (`NA_integer_` in R,
`nan` during Python calculation, and `NULL` in the persisted DuckDB list). They
never become class 0. Positive or negative infinity in a future is invalid.
References must always be present and finite. An all-missing future is valid
because it truthfully produces an all-unavailable label vector.

The caller supplies the reference. The calculation does not select history,
clean or transform data, access storage, interpret probabilities, or handle
model failures. The historical R `compute_label_vector(x, xx)` name remains only
as a delegating compatibility adapter; it validates history, selects its final
value, and calls the same authoritative calculation.

## Connected window policy

The approved window workflow remains responsible for its existing W/H/stride,
input-only robust cleaning, per-window standardisation, S1 membership, bounded
execution and parent/child lineage. ID 013 does not change those settings.

For each prepared window:

1. The R cleaning boundary processes only input positions and returns the final
   cleaned input value on the original scale.
2. The Mac coordinator reads the window's untouched future positions from the
   canonical parent in the same bounded chunk used for persistence.
3. Python calculates labels against that explicit reference and atomically
   stores the prepared window and label record.

No future enters cleaning or transformation fitting. No target is imputed. The
workflow does not transform and invert future values merely to compare them.
The child stores one compact nullable integer vector per existing `window_id`,
its reference value, definition identity and reference policy. It does not copy
window values, future arrays or split membership. Actual labels are reusable by
later models.

Accepted label rows are insert-once and checked on retry. Resume skips complete
windows and labels. The exact reference and its provenance must agree exactly;
strict labels never use an approximate equality check.

Correction approved on 3 October 2026: the earlier automatic inverse-derived
compatibility policy is superseded. Schema-v2 child databases remain readable,
but their labels are unavailable because they do not store the original prepared
reference. Read-only retrieval and resume must not reconstruct that reference
from transformed values. Rows produced by the superseded implementation remain
unchanged and are also reported unavailable when their provenance identifies an
inverse-derived reference. A saved reference qualifies only when its provenance
establishes that it is the original final cleaned input returned by the worker.

Unavailable-reference windows retain their transformed input, untouched future,
membership and lineage. Their retrieval returns no label vector or reference and
one explicit reason; workflow summaries report that label preparation is not
complete. This state is distinct from an exact-reference label vector containing
individual unavailable targets. Regenerating accepted preparation data requires
explicit researcher approval. Existing windows, memberships, hashes,
fingerprints and inverse-derived rows are never rewritten automatically.

## Boundaries and legacy differences

This definition is not a predicted classifier class, a probability threshold,
MDA/MDV/MDPV/PT, an aggregate or ternary label, forecast padding, or failure
handling. Mantis training, prediction and feature generation remain outside ID
013. Legacy copies migrate only with their owning scripts.

The previous R adapter rejected any missing future and selected the reference
from a supplied history; the central contract preserves missing futures and
requires an explicit reference. Some historical pipelines also transformed
constant histories/futures in ways that could erase real direction. The current
window path uses the corrected affine constant policy and original-scale cleaned
reference. These known differences mean this decision does **not** claim
historical numerical equivalence without a verified identical input/reference
artifact.

Robust cleaning itself is not guaranteed bit-identical across supported hosts.
The bounded `[9999, 1, ..., 63]`, period-1 fixture produces a final value of 58
on the ARM macOS host and 57 on x86 Ubuntu despite matching R 4.6.1, forecast
8.24.0 and jsonlite 2.0.0. The difference originates in platform-level floating
results from base R's native `stats::supsmu`, which changes the residual quartile
threshold and therefore `forecast::tsoutliers` membership. ID 013 does not alter
that approved cleaning algorithm: it guarantees that each label stores and uses
the exact value actually returned by cleaning, never an inverse reconstruction.

## Ownership

Prefect/Dask continue to own orchestration and bounded input preparation. The
label calculation is a small vectorised numerical function and deliberately has
no artificial class. Python on the Mac remains the sole DuckDB writer. The label
identity and storage schema are code constants persisted with each preparation;
changing their scientific meaning requires a new version and reviewed migration.
