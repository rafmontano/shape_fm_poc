# Transformation objects and fable research notes

Recorded: 2 October 2026. Status: research only, retained for a future decision.
The researcher requested information gathering, not implementation. This note
does not approve a new library, object system, formula interface, transformation
or release commitment, and does not change AMP's current assignment.

The question is whether fable's separation of model specifications, fitted
objects and transformations could simplify future ShapeFM development. Revisit
it when an actual modelling or transformation need arises, under the
[code standards and precedence rules](code-standards.md#purpose-and-precedence).
Scientific correctness and simplicity take precedence over OOP style.

## Observed design

fable supplies forecasting models; fabletools supplies the common modelling
framework. Its extension interface separates model-specific training and
forecasting from shared transformation handling. Additional models can implement
the same interface without rewriting that handling.
[Model extension guide](https://fabletools.tidyverts.org/articles/extension_models.html).

| Object | Responsibility and implementation |
| --- | --- |
| Specification | Captured expression/formula and evaluation environment, rather than an immediately calculated response vector. |
| Model definition | R6 object holding the specification, training function, checks and model-specific formula terms; tagged `mdl_defn`. |
| Transformation | Callable function with S3 class `transformation` and an inverse function attached. |
| Fitted model | S3 `mdl_ts` object retaining the underlying fit, model definition, response data and transformations. |

These are complementary object systems, not a requirement to use one system
everywhere. The implementation is visible in
[definitions.R](https://github.com/tidyverts/fabletools/blob/main/R/definitions.R),
[transform.R](https://github.com/tidyverts/fabletools/blob/main/R/transform.R) and
[mdl_ts.R](https://github.com/tidyverts/fabletools/blob/main/R/mdl_ts.R).

For a specification such as `ARIMA(log(y + 1))`, the framework identifies the
response and nested operations, constructs the supported inverse, trains on
transformed observations and retains the transformation for forecasting.
The parser caches supported scalar arguments in the transformation environment
and checks combined forward/inverse operations against the training response.
This is not automatic inversion of arbitrary functions. Custom transformations
can explicitly pair forward and inverse functions through `new_transformation()`.
See the [transformation guide](https://fable.tidyverts.org/articles/transformations.html)
and [formula parser](https://github.com/tidyverts/fabletools/blob/main/R/parse.R).

The current forecasting path back-transforms distributions before deriving
point summaries. For nonlinear transformations, inversing a transformed-scale
mean generally does not produce the original-scale mean. Our positive affine
standardisation does not have that particular complication.
[Forecast implementation](https://github.com/tidyverts/fabletools/blob/main/R/forecast.R).

## Possible future options

These are alternatives to investigate, not selected designs or AMP instructions.

| Option | Potential benefit | Main consideration |
| --- | --- | --- |
| Retain the portable contract and use small native fitted-transformation objects | Reuse the same fit/apply/inverse meaning in R and Python. | ShapeFM still maintains its small implementation and parity tests. |
| Use fabletools inside an R provider adapter | Reuse established model extension, formula and transformation facilities. | Requires compatible native data objects, dependency review and scientific equivalence checks; does not solve Python handling. |
| Offer an R formula interface over an agreed portable specification | Readable nested expressions for researchers. | Needs explicit supported operations and translation rules; avoid a custom general-purpose interpreter without demonstrated need. |

## Questions before a future decision

- What concrete need is not met by the existing fit/apply/inverse interface?
- Can proven library operations simplify it without changing the selected
  forecast algorithms, fallback behaviour or accepted results?
- How will history-only fitted parameters, inverse order, domains, constant
  inputs and forecast means/quantiles remain consistent across R and Python?
- Can native formula/function objects stay inside their language boundary,
  while registered names, versions and fitted parameters remain portable?

The [approved standardisation decision](poc2-standardisation.md) remains
authoritative, including sample-SD and constant-input rules, stored fitted state
and the exclusion of a formula interpreter from current scope. Avoid double
transformation between Gate 3 and a future model-internal transformation.
Inspect current needs and dependencies, propose a bounded change, and obtain
researcher approval before implementation.

Evidence boundary: this was a documentation/source review, not a runtime or
compatibility test. Upstream links reference evolving documentation and `main`
branches, not pinned releases. Recheck and pin the selected versions if this
option is revisited; no dependency or implementation change was made by this note.
