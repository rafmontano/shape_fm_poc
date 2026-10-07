# AMP instructions: POC2 IDs 038 and 041

## Authority

Implement the approved architecture in
[POC2 IDs 038 and 041: lambda selection and table reproduction](poc2-id038-041-table-reproduction.md).
Read that decision in full before changing code. Also follow the research vision,
architecture, code standards, workflow decision, execution policy, experiment
configuration contract, forecast-adjustment reference and accepted ID 037
decision.

Treat the existing approved documentation candidate as authoritative user work.
Preserve it. This is one integrated, pragmatic increment—not separate ID 038 and
ID 041 implementations and not a wholesale import of the legacy sensitivity
directory.

## Required outcome

Add one frequency-neutral Process 06 path that:

1. consumes canonical actuals, stored base means and stored Mantis predictions;
2. evaluates the exact 525-pair historical grid for SMYL and Chronos-2;
3. persists the 1,050 aggregate Daily candidates through the coordinator;
4. selects one pair per base model with the approved deterministic ordering;
5. retains only the selected adjusted point forecasts as retrievable diagnostics;
6. activates the accepted SMYL-Oracle utility in the same evaluation path;
7. produces Table 1 Daily and Table 2 Panels A–C for Daily only; and
8. exposes the approved model-neutral frequency/model/horizon directional
   evaluation result and descriptive mean-rank mechanism for the applicable
   POC2 model set; and
9. leaves the same code ready for POC3 frequency expansion, complete `All`
   aggregation and Figure 5 reporting.

Do not change the accepted mathematics or identities of existing forecasts,
Mantis, DTW, model storage, GIFT-Eval, combinations or directional labels.
Do not implement POC3 or POC4, import missing historical classifiers, or add the
legacy best-window/last-horizon selection in this increment.

## Starting audit

Before editing:

- report branch, HEAD and concise status;
- identify any pre-existing changes and preserve them;
- map current Process 06 orchestration, storage and schema ownership;
- map the accepted ID 037 R utility and current SMYL, FFORMA, Chronos-2, Naive2
  and Mantis storage contracts;
- map current per-horizon Mantis, DTW and point-forecast evaluation records and
  identify the smallest compatible model-neutral result boundary;
- verify the exact legacy ID 038 formulas and ID 041 selection/table logic; and
- report any missing required input before inventing a substitute.

Do not contact Ubuntu, install dependencies, run a heavy experiment, commit,
push or synchronize during the implementation checkpoint.

## Scientific implementation

Extend the existing cohesive R adjustment utility when practical; do not create
one file per historical spreadsheet ID. Preserve exactly:

```text
direction(base[H], last_observed) = 1 only when base[H] > last_observed
gamma = 1 when base and Mantis directions agree
gamma = lambda_up on disagreement when Mantis predicts 1
gamma = lambda_down on disagreement when Mantis predicts 0
adjusted = gamma * complete base mean vector
```

Use the exact inclusive grids:

```text
lambda_up   1.000..1.120 by 0.005
lambda_down 1.000..0.900 by -0.005
```

Implement the versioned historical table profile separately from GIFT-Eval:
full-horizon sMAPE, full-horizon MASE, terminal DA and Naive2-relative OWA. Daily
MASE lag is 1. Reuse an existing calculation only when its semantics are proven
identical; otherwise give the historical profile one explicit implementation
rather than duplicating formulas across the surface and table builders.

Reject missing, mismatched, non-finite or wrong-horizon scientific inputs. Do not
silently fall back to another model, direction, metric profile or lambda pair.

## Configuration

Create a fresh immutable configuration version after inspecting the current
highest version. Do not renumber or reinterpret versions 1–11. The new contract
must centrally identify:

- Daily as the configured POC2 frequency;
- the adjustment method and exact grid/version;
- SMYL and Chronos-2 as adjusted base models;
- Mantis as the directional input;
- Naive2 as the OWA reference;
- the historical metric profile;
- Table 1, Daily-only Table 2 and approved Figure 2 outputs;
- the applicable configured model set for directional reports; and
- operational batch size and maximum in-flight work through the existing
  execution-profile mechanism.

Provide a bounded 100-series configuration for implementation validation. Keep
the selection and frequency fields general enough that the later complete Daily
run and POC3 expansion require configuration changes, not source changes. Store
the original and resolved values and include every scientific choice in the
scientific fingerprint.

## Process 06 orchestration and parallelism

Use the existing Process 06 wrapper and Prefect workflow. Add process-owned
helpers under `src/python/util/` using the approved `p06_NN_name.py` convention
only when a distinct responsibility exists. Do not add another researcher entry
point or parallel framework.

This is a parallel sensitivity workflow. Prefect owns the Process 06 sequence
and dependency barriers. Dask distributes independent bounded base-model/lambda
batches over eligible CPU workers. Avoid one distributed task per
series/lambda pair, one durable task per pair, or one indivisible task containing
the complete 1,050-candidate surface.

Each worker evaluates its assigned batch using single-threaded or vectorised
scientific calculations. Do not start a nested R future plan, Python process
pool, another Dask scheduler or another orchestration path. Do not request GPU
resources: Chronos-2 forecasts are stored inputs and must not be rerun. Reuse
bounded read-only inputs without serialising a complete dataset separately for
every candidate.

Retain one sequential mode and one scientific calculation shared by sequential,
local-Dask and distributed-Dask execution. All modes must produce identical
candidate identities, metrics, selections and table/report cells. Batch size,
maximum in-flight work and worker topology are operational settings only.

The coordinator alone writes DuckDB; workers must not receive a writable
database connection. It commits validated completion-order responses and
records worker/batch evidence separately from scientific content.

Task and result scientific identities must derive from experiment, frequency,
base model, adjustment/profile/grid definitions and input fingerprints—not host,
address, worker count, elapsed time or completion order.

## Persistence

Use the smallest schema extension that cleanly represents:

- all aggregate model/lambda metric rows;
- which row was selected and why;
- the selected per-series adjusted point means;
- model-neutral per-frequency, per-model and per-horizon directional evaluation
  rows containing correct count, evaluation count, accuracy and scientific
  input/result fingerprints;
- Table 1 and Table 2 Daily cells or an equally direct durable/queryable source;
- input/result fingerprints, valid counts and restart state; and
- separate operational provenance.

Prefer extending existing evaluation/storage objects when their meaning fits.
Do not overload `official_evaluations` with a different metric definition and do
not store ex-post diagnostics as ordinary deployable forecasts. Schema creation
must be idempotent and old databases/configurations must remain readable.

## Selection and reports

Select each adjusted model by ascending:

1. OWA;
2. `abs(lambda_up-1) + abs(lambda_down-1)`;
3. `abs(lambda_up-1)`;
4. `abs(lambda_down-1)`; and
5. stable lambda values.

Build Table 1 and Table 2 from one selected-results object. POC2 Table 2 contains
only Daily. Do not emit `All` from one frequency. Implement the complete-frequency
guard and the historical weighted-component/recalculated-OWA `All` calculation,
then prove it with synthetic multi-frequency tests without executing POC3 data.

Use one common directional-evaluation contract. Direct directional models use
their stored binary predictions. For point forecasts, compare `forecast[h]` and
`actual[h]` with the same last observed value. Table 1 and Table 2 must select
terminal DA from this result rather than recalculate it in a report-specific
path.

Add the model-neutral horizon-profile and descriptive mean-rank calculation
needed by the approved Figure 2 path. It must use the configured applicable model
set, higher accuracy as better, average ranks for ties, and a fingerprinted
frequency/model/horizon matrix. Report incomplete models explicitly rather than
silently including or dropping them. Treat CD-style grouping as descriptive for
Daily horizons, not as independent-sample significance evidence. Do not import
the legacy plotting runner wholesale.

The researcher-facing status/results surfaces must make the diagnostic identity,
selected pair, table scope and ex-post limitation visible without opening DuckDB
manually.

## Validation

Run proportionate local validation only for this checkpoint:

1. exact grid cardinality and endpoints;
2. frozen adjustment parity with the legacy function;
3. historical sMAPE, MASE, DA and OWA fixtures, including Daily MASE lag 1;
4. all deterministic tie levels;
5. cross-host/operational-provenance invariance of scientific results;
6. sequential versus local-Dask parity using the same scientific calculation;
7. bounded batching, no nested parallelism and CPU-only sensitivity routing;
8. coordinator-only persistence and idempotent schema/restart selection;
9. complete, unique model/frequency/horizon directional results and deterministic
   descriptive ranks, including ties and explicit incomplete-model handling;
10. Table 1/Table 2 Daily reconciliation;
11. no `All` result with Daily alone;
12. synthetic complete-frequency `All` aggregation and Figure 5-shaped result
    retrieval without executing POC3 data;
13. accepted ID 037, forecast, Mantis, DTW, configuration and Process 06
    regressions; and
14. compilation/parsing, documentation links and whitespace checks.

Do not claim historical numerical reproduction from synthetic or 100-series
tests. After the implementation candidate is reviewed, the next separately
approved execution is a two-machine 100-series run. That acceptance must prove
Mac and Ubuntu CPU contribution to sensitivity batches, all 1,050 aggregate
candidates without duplicate durable rows, scientific parity with sequential or
local reference results, resource safety, sole-writer persistence and a restart
that schedules no completed candidates. The complete M4 Daily run, Figure 2
evidence and final POC2 closure follow only after that bounded result is accepted.

## Report and stop

Return one concise report containing:

- exact changed paths and responsibility map;
- resolved configuration and schema additions;
- grid, batch, task and result cardinalities;
- sequential/local-Dask parity and the planned two-machine contribution proof;
- selection and Table 1/Table 2 reconciliation evidence;
- directional-result and descriptive-rank evidence;
- restart and scientific-fingerprint evidence;
- tests run and exact outcomes;
- explicit confirmation that GIFT-Eval and accepted existing mathematics did
  not change; and
- remaining steps before the 100-series and complete Daily executions.

Leave the candidate uncommitted and unstaged for Chief Developer and Chief
Architect review. Stop rather than expanding scope if a required source input or
existing contract cannot support the approved design.
