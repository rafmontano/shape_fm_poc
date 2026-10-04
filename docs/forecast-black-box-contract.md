# Forecast model black box contract

## Status and purpose

Approved as an architecture decision on 4 October 2026. The same-day M4
activation addendum extends the common result envelope to point-only base
forecasts and the official M4 Comb. The common contract and v9 execution graph
are implemented; distributed acceptance evidence remains a separate checkpoint.

This contract makes every forecast provider behave as the same black box at the
ShapeFM boundary, regardless of its programming language, package, model or
worker machine. Native R and Python objects may be used inside an implementation,
but they must not escape the provider boundary.

The contract is independent of any evaluation library. GIFT-Eval is a downstream
consumer of validated ShapeFM forecasts, not the authority that defines or
completes a model's output. The official point-only M4 Comb is included through
the approved addendum below. Probabilistic combination mathematics remains a
separate future decision.

## Architecture decision

Every active base forecast model must:

1. receive one versioned, language-neutral request with the same field names and
   meanings;
2. convert that request to any native object needed internally, such as an R
   `ts`, R forecast object, NumPy array or Torch tensor;
3. perform only its forecasting responsibility;
4. return one versioned, language-neutral result with the same field names,
   shapes and meanings; and
5. leave orchestration, database access, inverse transformation, combination and
   evaluation to their owning ShapeFM processes.

JSON is the transport format at process, language and machine boundaries. Native
objects remain inside the model implementation. The coordinator validates the
request and result contracts and remains the only DuckDB writer.

```text
Central experiment configuration and DuckDB
                    |
                    v
      Standard ShapeFM forecast request
                    |
        +-----------+-----------+
        |                       |
        v                       v
  Python provider          R provider
  native objects           native objects
        |                       |
        +-----------+-----------+
                    |
                    v
       Standard ShapeFM forecast result
                    |
                    v
       Validate, restore scale and store
                    |
                    v
          GIFT-Eval adapter and metrics
```

## Standard forecast request

Every provider receives the following logical fields. Transport implementations
may batch several requests in one message, but each request retains this complete
identity and scientific content.

| Field | Requirement and meaning |
| --- | --- |
| `contract_version` | Required version of this request and result contract. |
| `experiment_id` | Required experiment identity from DuckDB. |
| `task_id` | Required task identity used for scheduling, retry and result matching. |
| `forecast_instance_id` | Required identity linking the context, horizon and later actual observations. |
| `variant_id` | Required preprocessing and transformation variant identity. |
| `dataset_id` | Required dataset identity. |
| `series_id` | Required time-series identity. |
| `model_id` | Required stable ShapeFM candidate name. |
| `required_capability` | Required `probabilistic` or `mean_only` output profile. The provider must not silently downgrade it. |
| `context` | Required finite one-dimensional numeric training history prepared by Processes 02 and 03. It never contains future actual observations. |
| `horizon` | Required positive number of future steps. |
| `frequency` | Required stored dataset frequency. |
| `seasonal_period` | Required positive period resolved centrally for the model; the provider must not independently reinterpret it. |
| `input_scale` | Required scale identifier for the supplied context. |
| `quantile_levels` | Required ordered probabilities for `probabilistic`; required null for `mean_only`. |
| `seed` | Required centrally configured reproducibility seed. |
| `model_settings` | Required mapping of the approved model-specific settings; it may be empty but not implicit. |

The central configuration and DuckDB remain the authority for these values.
Workers must not substitute local scientific defaults when an authoritative
value is present.

## Standard successful result

Every active base model returns the same fields. Point-only values are represented
by explicit null probabilistic fields, not by a different R or Python structure.
Provider-specific details belong inside `provenance`, not as different top-level
contracts.

| Field | Requirement and meaning |
| --- | --- |
| `contract_version` | Must equal the request contract version. |
| `status` | Must be `success`. |
| `experiment_id` | Must echo the request identity. |
| `task_id` | Must echo the request identity. |
| `forecast_instance_id` | Must echo the request identity. |
| `variant_id` | Must echo the request identity. |
| `requested_model_id` | Stable candidate requested by ShapeFM. |
| `executed_model_id` | Model that actually produced the result. It normally equals `requested_model_id`; any difference must be explicit. |
| `forecast_capability` | Must equal the requested `probabilistic` or `mean_only` capability. |
| `output_scale` | Must identify the scale of every returned numeric forecast and normally equal `input_scale`. |
| `horizon` | Must equal the request horizon. |
| `mean` | Required finite vector with exactly one value per horizon step. |
| `median` | Required finite horizon vector for `probabilistic`; required null for `mean_only`. |
| `quantile_levels` | Must exactly echo the ordered requested levels for `probabilistic`; required null for `mean_only`. |
| `quantiles` | Required finite `quantile levels x horizon` matrix for `probabilistic`; required null for `mean_only`. Quantiles must be nondecreasing at every horizon step. |
| `fallback_used` | Required Boolean. A substitution must never be silent. |
| `fallback_reason` | Required null when no fallback occurred and a concise reason when it did. |
| `runtime_seconds` | Required non-negative provider execution time. |
| `provenance` | Required mapping containing implementation language, provider/library and version, model revision, effective settings, seed and relevant numerical assumptions. |

For `probabilistic`, the median must equal the q0.5 row. For `mean_only`, all
three probabilistic fields must be null. No response may include actual future
observations. The provider result is not permitted to write to DuckDB directly.

| Capability | `mean` | `median` | `quantile_levels` | `quantiles` |
| --- | --- | --- | --- | --- |
| `probabilistic` | Required | Required | Required | Required |
| `mean_only` | Required | Null | Null | Null |

## Evaluation output profile

For probabilistic forecasts, the requested quantile profile is centrally
configured from the metrics ShapeFM has approved. The provider must produce every
requested level as part of its own forecast calculation or a documented model
adapter based on the fitted forecast distribution, intervals or simulations. A
downstream evaluator must not invent, interpolate or extrapolate missing model
outputs.

For the current GIFT-Eval metric set, implementation must validate a common
profile containing:

- `mean`;
- q0.5 as the median;
- q0.1 through q0.9 for the weighted quantile loss; and
- q0.025 and q0.975 for the 95 percent interval used by MSIS.

The exact ordered list becomes part of the experiment configuration and stored
forecast result. Extra levels must not silently change which levels a metric
uses.

Chronos-2 estimates quantile levels independently and can return crossings. Its
ShapeFM provider applies the same per-horizon monotone rearrangement already used
for probabilistic combinations, sets the median from the rearranged q0.5 row,
leaves the model mean unchanged, and records `identity` or
`sorted_per_horizon` in result provenance. This provider-boundary operation
fulfils the noncrossing result contract; Process 06 does not repair forecasts.

## Evaluation boundary

The evaluator receives the complete stored forecast. Its adapter may only map
ShapeFM names and array shapes to the evaluator's interface. It may not refit a
model, change the mean, derive a median, create missing quantiles or repair an
invalid forecast.

Training context, held-out actual observations, forecast start, dataset
frequency, seasonality, domain and variate count remain authoritative in the
forecast-instance, dataset and experiment records. They are joined during
Process 06 and are not duplicated as model-generated forecast values.

The current GIFT-Eval bridge uses evaluation objects and metric functions from
its pinned dependency stack. That dependency is an internal implementation
detail of Process 06; ShapeFM has not selected it as a forecasting model and it
does not define the forecast-provider contract.

## Official M4 combination addendum

The official M4 Comb is a point-only benchmark, not a probabilistic ensemble:

```text
m4_comb = (ses + holt + damped) / 3
```

Process 04 calculates and stores SES, Holt and Damped independently through the
same `mean_only` black-box result envelope. Process 05 runs only after all three
successful component forecasts exist, reads their stored means, applies the
fixed one-third weights, and stores a fourth `mean_only` forecast. It records the
three component forecast IDs, names and weights in `forecast_components` and
never refits a model.

The R package `forecastHybrid` is not used. Its `hybridModel()` supports a
different component set, refits its components from the source series and does
not implement the official SES/Holt/Damped benchmark. Its combined intervals
are heuristic and do not resolve the quantile mathematics. Adding it would
create a different candidate, not M4 Comb.

Process 06 evaluates M4 Comb and its point-only components through a separately
labelled mean-based profile. It may calculate MSE, MAE, MASE, MAPE, sMAPE, RMSE,
NRMSE and ND from the stored mean. It must not calculate median, quantile or
interval metrics, must not fabricate uncertainty outputs, and must not label the
result as a complete probabilistic GIFT-Eval submission.

## Common error result

R and Python providers must also return the same error structure:

| Field | Requirement and meaning |
| --- | --- |
| `contract_version` | Request contract version. |
| `status` | Must be `error`. |
| `experiment_id` | Echoed request identity. |
| `task_id` | Echoed request identity. |
| `forecast_instance_id` | Echoed request identity. |
| `variant_id` | Echoed request identity. |
| `requested_model_id` | Requested candidate. |
| `error_type` | Stable machine-readable category. |
| `error_message` | Concise human-readable cause. |
| `provenance` | Available provider and environment evidence. |

Failed results contain no partial mean, median or quantile arrays. Retry and
failure persistence remain orchestration responsibilities.

## Validation and acceptance

Implementation is complete only when:

1. R and Python receive the same request structure and return the same result
   structure;
2. contract tests send equivalent fixtures through both language boundaries;
3. identities, capability, horizon, scale, quantile levels and array dimensions
   are exact;
4. all supplied numeric outputs are finite; probabilistic q0.5 equals the median
   and quantiles do not cross; mean-only probabilistic fields are null;
5. no evaluator supplies a missing model output;
6. the coordinator restores scale and stores the validated result through the
   existing single-writer path; and
7. M4 Comb waits for and links exactly SES, Holt and Damped, uses one-third
   weights and produces no probabilistic fields; and
8. the existing end-to-end forecast and capability-appropriate evaluation
   workflow remains functional.

Probabilistic combination mathematics remains deferred. M4 Comb must not be used
to weaken or reinterpret the probabilistic base-model contract.
