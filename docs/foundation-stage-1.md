# Foundation Stage 1 walkthrough

1. Gate 00 verifies the pinned GIFT-Eval submodule/environment and acquires the
   immutable M4 Daily source.
2. The Stage 1 smoke workflow creates or migrates `data/shapefm.duckdb`.
3. The coordinator derives dataset and logical run identities independent of
   `max_series` and `workers`, then records a distinct invocation.
4. The adapter memory-maps the Arrow stream and yields bounded groups of series.
5. Each worker converts one source series into a canonical result and generic
   evaluation window. A worker never opens writable DuckDB.
6. The coordinator commits each result and task completion transactionally.
7. R and Python retrieve the same canonical object from DuckDB.

Run the smoke import and inspect it:

```sh
Rscript workflows/stage_01_import/01_smoke_m4_daily.R
Rscript workflows/stage_01_import/03_inspect_series.R
```

A repeat discovers the same dataset/run and skips completed tasks. The full
import extends that same run and starts at series 10. Every call remains visible
in `run_invocations`. For local parallel execution, pass `workers = 2L` to
`shapefm_import_m4_daily`; scientific output is identical. The persistent POC
database uses the normal sequential smoke path.
