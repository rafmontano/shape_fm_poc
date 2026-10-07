# POC2 IDs 038 and 041: lambda selection and table reproduction

## Status

The researcher approved this architecture and its parallel-execution,
POC3-transition and roadmap amendments on 7 October 2026. Implementation,
execution evidence and researcher acceptance remain open. The POC3 and POC4
roadmap remains planning direction rather than authority to begin either POC.

This decision combines the historical ID 038 lambda-sensitivity calculation
with ID 041 selection and table construction. They are one scientific path:
ID 038 creates the candidate surface, and ID 041 selects from that same surface
and presents the results. ShapeFM must not implement competing calculations for
the two spreadsheet IDs.

## Research scope

POC2 proves that the ShapeFM architecture can reproduce the relevant M4 Daily
research results. Its scientific exit criteria are:

1. reproduce Table 1 for the complete M4 Daily dataset;
2. reproduce the separately approved Figure 2 scope; and
3. execute the Table 2 construction for Daily only, proving that the same
   frequency-neutral implementation can expand in POC3.

The POC2 Table 2 output is explicitly a **Daily-only validation**, not the
complete published Table 2. It contains the Daily column for:

- Panel A: terminal-horizon directional accuracy;
- Panel B: full-horizon OWA; and
- Panel C: OWA improvement relative to SMYL.

POC3 will run the same implementation over all approved M4 frequencies and
reproduce the complete Table 2, including its cross-frequency `All` result.
POC3 should expand configuration and data scope, not introduce another table
calculation or sensitivity architecture.

## Inputs and ownership

Processes 01–05 must first provide the canonical series context, official future
actuals, stored Naive2, FFORMA, SMYL and Chronos-2 point means, Mantis directional
predictions, and the accepted ID 037 SMYL-Oracle diagnostic. Process 06 owns the
historical evaluation, lambda search, deterministic selection and research-table
outputs because the work consumes realised future values.

No forecast model is fitted or rerun inside the lambda search. The original
forecasts remain independently stored and unchanged.

## Historical Mantis adjustment

For one base forecast `b[1:H]`, let `r` be the last observed history value and
`m` the Mantis binary prediction at the final horizon. The base final direction
is strict:

```text
d = 1 when b[H] > r, otherwise 0

gamma = 1            when d == m
gamma = lambda_up    when d != m and m == 1
gamma = lambda_down  when d != m and m == 0

adjusted[h] = gamma * b[h], for h = 1,...,H
```

This is the historical implementation that generated the prior results: one
final-horizon decision scales the complete point-forecast vector. It is not an
independent adjustment at every horizon.

The calculation is an ex-post research diagnostic. It must not be exposed as a
deployable forecast-time decision or silently replace the unadjusted forecast.

## Lambda grid

ShapeFM will reproduce the complete historical grid instead of hardcoding a
previously selected pair:

```text
lambda_up   = 1.000, 1.005, ..., 1.120  (25 values)
lambda_down = 1.000, 0.995, ..., 0.900  (21 values)
```

There are 525 pairs per base model. POC2 evaluates all 525 pairs for SMYL and
all 525 for Chronos-2: 1,050 aggregate Daily candidates. The grid definition,
version and fingerprint are scientific configuration and must be stored with
the experiment. The historical Daily pair near `(1.015, 1.000)` is reference
evidence, not a configured or hardcoded answer.

## Historical evaluation profile

The table-reproduction path uses a separate versioned profile, provisionally
identified as `m4_paper_tables_v1`:

- sMAPE over the complete forecast horizon;
- MASE over the complete forecast horizon;
- terminal-horizon directional accuracy; and
- OWA relative to Naive2.

Daily MASE uses the historical M4 lag of `1`. This profile does not replace,
modify or reinterpret ordinary GIFT-Eval evaluation. Both may coexist as
separately identified Process 06 results.

## Distributed execution

The sensitivity calculation is a parallel workflow. Prefect owns the Process 06
sequence and its dependency barriers. Dask distributes independent, bounded
model/grid batches over eligible CPU workers on every machine enabled by the
approved execution profile. POC2 must be able to use both the Mac and Ubuntu;
POC3 uses the same mechanism when the frequency scope expands.

One scientific candidate is one base model and one lambda pair evaluated over
the configured frequency membership. The durable result remains one aggregate
candidate row, but the operational scheduling unit is a bounded batch containing
several independent candidates. Do not create one distributed task for every
series/lambda pair, and do not send all 1,050 candidates as one indivisible task.

Each Dask worker processes its assigned batch with single-threaded or vectorised
scientific calculations. It must not start another R parallel plan, Python
multiprocessing pool or nested Dask scheduler. This avoids CPU oversubscription
and repeated copies of the complete scientific input. GPU resources are not
required for the lambda-grid arithmetic; the stored Chronos-2 forecasts are
inputs and Chronos-2 is not rerun.

The same calculation must retain a sequential execution mode for focused tests,
debugging and parity evidence. Sequential, local-Dask and distributed-Dask modes
must produce the same candidate identities, metrics, selected lambda pairs and
table cells. Parallelism changes elapsed time only.

Batch size, maximum in-flight work, worker count, host, address and completion
order are operational controls. They must not enter scientific identities or
change results. Their effective values and worker contribution remain separate
execution evidence.

Workers receive bounded read-only scientific inputs and return aggregate metric
evidence. The selected coordinator remains the sole DuckDB writer. Do not create
one durable forecast collection for every grid pair or millions of persistent
series/pair task rows merely to demonstrate parallelism.

For a two-machine acceptance, evidence must show that eligible CPU workers on
both machines contributed sensitivity batches, every expected candidate was
completed exactly once, and a restart selected no already-valid work. A run may
use a different safe worker count without changing scientific identity, but its
actual topology must be recorded.

## Persistence and restart

DuckDB must retain enough information to reproduce and audit the decision:

- experiment, frequency, base-model and adjustment-method identities;
- exact lambda pair and grid fingerprint;
- sMAPE, MASE, DA and OWA plus valid counts;
- input, forecast, direction and result fingerprints;
- completion/failure state and separate operational evidence;
- the selected pair and deterministic selection reason; and
- the selected adjusted point means or an equally direct retrievable diagnostic
  representation linked to their source forecasts.

Store aggregate results for all 1,050 Daily candidates. Recalculate and retain
only the two selected adjusted point-forecast collections. Do not invent
probabilistic quantiles: the historical method and table calculation are
point-forecast diagnostics.

Restart skips valid completed work. A changed dataset, actual, source forecast,
Mantis prediction, historical metric profile or lambda grid invalidates only
the dependent result. A changed execution topology does not alter scientific
identity.

## Deterministic selection

Select SMYL–Mantis and Chronos-2–Mantis independently. Order candidates by:

1. lowest full-horizon OWA;
2. smallest total distance from `(1,1)`;
3. smallest upward departure;
4. smallest downward departure; and
5. lambda values as the final stable ordering.

The selected pair must be calculated from the stored surface, never hardcoded.

## Table construction

One selected-results dataset supplies both outputs.

Table 1 contains the approved Daily methods and historical metrics, including
the unadjusted baselines, selected SMYL–Mantis and Chronos-2–Mantis diagnostics,
and SMYL-Oracle. Naive2 remains available as the OWA denominator and audit row.

During POC2, Table 2 contains only the `Daily` column. It must reconcile with
Table 1:

- Panel A Daily equals Table 1 DA for shared methods;
- Panel B Daily equals Table 1 OWA;
- Panel C Daily equals Table 1 OWA improvement relative to SMYL; and
- both outputs reference the same stored selected pairs and metrics.

Do not calculate or display an `All` column from Daily alone. The complete
Table 2 aggregation is enabled only when the complete approved M4 frequency set
is present. It weights frequency sMAPE, MASE and DA by series count, then
recalculates OWA from aggregated sMAPE and MASE relative to aggregated Naive2;
it does not average frequency OWA values.

## Configuration evolution

The first implementation uses a new immutable configuration version and fresh
database. It selects Daily only and identifies the historical adjustment grid,
metric profile, participating base forecasts and table outputs explicitly.
Existing configurations and databases retain their original meaning.

The implementation must be frequency-neutral. POC3 expands the configured
frequencies to Hourly, Daily, Weekly, Monthly, Quarterly and Yearly and permits
the complete `All` calculation. No frequency-specific source-code branch or
second Table 2 builder is permitted.

## Transition readiness for POC3

The Process 06 result boundary should expose one model-neutral directional
evaluation record for each configured frequency, model and horizon. At minimum
the record identifies the experiment, frequency, model, horizon, correct count,
evaluation count, directional accuracy and scientific input/result fingerprints.

Direct directional models such as Mantis and DTW use their stored binary
predictions. Point-forecast models derive both directions from the same last
observed value: the forecast direction compares `forecast[h]` with that origin,
and the actual direction compares `actual[h]` with it. Table 1 and Table 2 select
their terminal-horizon directional accuracy from this common result rather than
recalculating it in a table-specific path.

The same normalized result supports horizon profiles, distributions and a
descriptive mean-rank report without changing model execution. The mean-rank
report must accept a configured model set, use higher directional accuracy as
better, use average ranks for ties, fingerprint its frequency/model/horizon
matrix and report incomplete models explicitly. Because Daily horizons evaluate
the same series, any CD-style grouping is descriptive and must not be presented
as an independent-sample significance conclusion.

POC2 validates this result boundary and reporting mechanism only for its approved
Daily and applicable-model scope. It does not import missing historical
classifiers merely to recreate every label in the published rank diagram.
POC3 expands frequency and applicable-model configuration over the same boundary.
The historical best-window, last-horizon selection is a separate ex-post design
and remains excluded unless the researcher approves it explicitly.

## Exclusions

Do not import the historical sensitivity folder wholesale. Exclude:

- mutable global variables, RDS path construction and legacy launchers;
- duplicated model fitting, metric functions and parallel orchestration;
- Appendix B heatmaps, contours, case studies and robustness reports;
- complete all-frequency scientific execution during POC2;
- storage of every adjusted vector from every grid pair; and
- claims that ex-post parameter selection is held-out training or deployable
  forecast-time behaviour.

## POC2 definition of done for this increment

The increment is technically ready for final POC2 acceptance when:

1. focused tests prove the grid, adjustment, historical metrics, OWA and tie
   ordering against frozen legacy fixtures;
2. a bounded end-to-end Daily run proves Process 06 orchestration, distributed
   Mac and Ubuntu CPU contribution, sole-writer persistence and restart;
3. the complete M4 Daily run reproduces Table 1 and the Daily portions of all
   three Table 2 panels within separately reviewed numerical evidence;
4. Table 1 and Table 2 Daily values reconcile exactly where they overlap;
5. the agreed Figure 2 result is reproduced through its approved path;
6. results, selected parameters, source identities and fingerprints remain
   queryable from the experiment after restart; and
7. synthetic multi-frequency tests prove that POC3 can expand configuration and
   calculate `All` without an architectural or table-builder change; and
8. sequential, local-Dask and two-machine distributed sensitivity produce
   identical scientific results, while all 1,050 candidates complete exactly
   once and restart schedules none of them again.

This decision approves implementation. Numerical reproduction and POC2 closure
still require Chief Architect review and researcher acceptance of the evidence.

## Integrated selective reporting checkpoint: IDs 058/062

The researcher subsequently authorised selective CD and horizon reporting over
this same stored Process 06 foundation, plus a read-only `export` action. This
does not authorise a two-machine run, publication, or scientific closure.
DuckDB remains authoritative; the mandatory `results/<experiment>/` folder
contains derived tables, explicit figure inputs, PDF/PNG renderings, evidence
and a hashed manifest. Persistent experiment output outside top-level
`results/` is prohibited.
The [central reporting contract](experiment-configuration.md#selective-figure-2-and-read-only-export-ids-058062)
defines the exact outputs and model scopes.

The horizon figure contains Mantis, Chronos-2, 1-NN DTW and SMYL. One
authoritative CD matrix contains Mantis, 1-NN DTW, FFORMA, Chronos-2 and SMYL;
it is not the historical ten-model diagram. That matrix is rendered into two
standalone files, a research version and a paper version. Both contain the same
scientific content in POC2 and must never be combined as panels.
No excluded classifier, Naive2, adjusted forecast or Oracle is silently included.
Locked `scmamp` uses historical alpha/reverse/cex/PDF settings, but dependent
Daily horizons make bars/grouping descriptive rather than independent-sample
statistical evidence. IDs 058/062 introduce no metric/rank implementation,
model execution, RDS discovery, orchestration or ID 061 selection. POC3 can reuse
the same frequency-neutral stored-result and renderer contract later.

The [existing checkpoint evidence](poc2-id038-041-implementation-evidence.md)
records local tests and inspected synthetic renderings, not historical Figure 2
numerical reproduction. Review precedes the single consolidated Mac/Ubuntu
100-series acceptance; all earlier scientific closure conditions remain open.
The researcher clarified before acceptance that no plot or CSV may be written
outside `results/`, and default R output such as root-level `Rplots.pdf` must be
prevented rather than ignored.
