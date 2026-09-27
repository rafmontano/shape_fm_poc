# ShapeFM architecture

## Research architecture

![ShapeFM research architecture](images/shapefm_research_architecture.png)

The research view has seven layers: Research Interface, Experiment Management,
Evaluation & Benchmarking, Forecast Decision, Modelling & Representation,
Time-Series Engineering, and Research Data. Across those layers, the Experiment
Grid is the experiment component, ShapeFM Decision is the scientific function,
Benchmark Evaluation is the evaluation service, and Research Feedback closes
the iteration loop.

The persistent research objects are a Time-Series Record (data object), an
Experiment (research object), an Evidence Report (information object), and the
Research Schema (data structure). They move through one six-process cycle:

```text
01 Import → 02 Clean → 03 Transform → 04 Forecast → 05 Combine → 06 Evaluate
     ▲                                                                    │
     └──────────────────────────── Iterate ────────────────────────────────┘
```

This structure supports reproducible, traceable, scalable research while
avoiding full reruns, data sprawl, and manual orchestration.

## Technical layers

![ShapeFM technical layers](images/shapefm_technical_layers.png)

The technical view has three layers:

1. **DuckDB data layer.** One database stores canonical series, evaluation
   boundaries, scientific results, task/attempt state, and provenance.
2. **Python/R application layer.** Python coordinates and is the sole writable
   database owner. R receives ordinary JSON jobs for specialised `tsclean` and
   AutoARIMA computation and returns ordinary JSON results.
3. **Dask/concurrent.futures execution layer.** Bounded local or distributed
   queues execute serializable work. Workers never open writable DuckDB.

## Identity, transactions, and restart

Dataset identity is derived from pinned source revisions and material import
configuration. Experiment, variant, task, forecast, and evaluation identities
remain independent of worker placement and concurrency. Each successful result
and task completion is committed atomically by the coordinator. Failed or
interrupted work remains retryable; completed work is skipped on restart.

The Dask scheduler is transient and non-authoritative. The Mac hosts the
coordinator and scheduler. CPU work can run on Mac and Ubuntu. Chronos work
requires `CHRONOS_GPU_SLOT=1`; 15 logical Ubuntu workers may share GPU device 0,
but reports separately record `physical_gpu_count: 1` and
`logical_gpu_worker_processes: 15`.

## Schema evolution

Schema versions preserve prior rows while adding experiment identity, task and
attempt state, scientific forecasts, and official evaluation provenance.
Arrays remain in DuckDB list columns. Execution addresses, transient model
weights, logs, and telemetry are not authoritative database content.
