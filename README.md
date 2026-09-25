# ShapeFM proof of concept

Foundation Stage 1 is frozen at tag `v0.1-foundation`. POC 1 adds an
end-to-end, restartable research grid while keeping official GIFT-Eval as the
enclosing benchmark and sole evaluation authority.

## Start POC 1 in RStudio

```r
source("R/shapefm_import.R")
source("R/shapefm_database.R")
source("R/shapefm_poc1.R")

plan <- shapefm_plan_poc1(scope = "smoke")
shapefm_run_poc1(plan, workers = 1L)

db <- shapefm_open()
forecast <- shapefm_get_forecast(db, plan, series_id = "0")
results <- shapefm_get_official_results(db, plan)
print(results) # concise MASE, sMAPE, CRPS, and RMSE comparison
status <- shapefm_experiment_status(db, plan)
shapefm_close(db)
```

The smoke plan means the first ten **official forecast instances**, not the
first ten arbitrary rows. It produces 4 cleaning/transformation branches and
three candidates per branch (AutoARIMA, Chronos-2, and their equal-weight
combination), for 120 candidate forecasts. See [POC 1](docs/poc1.md).

Foundation Stage 1 imports pinned GIFT-Eval M4 Daily source data into one
authoritative DuckDB database: `data/shapefm.duckdb`. The original source under
`data/source/gift_eval/` remains immutable.

## Start here in RStudio

1. Open `shape_fm_poc.Rproj`.
2. If this is a new machine, source
   `workflows/gate_00_setup/01_prepare_gift_eval.R` once.
3. Source `workflows/stage_01_import/01_smoke_m4_daily.R`. This creates or
   migrates the database and imports ten series sequentially.
4. Source `workflows/stage_01_import/03_inspect_series.R`. It prints import
   status and retrieves series `0` as an ordinary R object.

The smoke workflow is restart-safe. Running it again skips its ten completed
tasks without duplicating series or windows. The later full import has the same
dataset and logical run identity, skips those ten tasks, and extends the database
with the remaining 4,217 series. Every call has its own invocation record.

## Researcher interface

R:

```r
source("R/shapefm_import.R")
source("R/shapefm_database.R")

db <- shapefm_open()
series <- shapefm_get_series(db, dataset = "m4_daily", series_id = "0")
status <- shapefm_stage_status(db, stage = "import", dataset = "m4_daily")
shapefm_close(db)
```

Python:

```python
from shapefm import ShapeFMDatabase

with ShapeFMDatabase.open() as db:
    series = db.get_series(dataset="m4_daily", series_id="0")
    status = db.stage_status(stage="import", dataset="m4_daily")
```

Researchers use these objects rather than source-file or SQL details.

## Stage 1 commands

```sh
# Create or migrate the database
.tools/uv/uv run --locked shapefm-import migrate

# Import the ten-series smoke sample (normal sequential path)
.tools/uv/uv run --locked shapefm-import import --max-series 10 --workers 1

# Inspect status and retrieve one series
.tools/uv/uv run --locked shapefm-import status --dataset m4_daily
.tools/uv/uv run --locked shapefm-import get --dataset m4_daily --series-id 0

# Available later; do not run as part of the smoke gate
Rscript workflows/stage_01_import/02_import_m4_daily.R
```

Set `workers` above one only for local parallel computation. Workers receive
ordinary task objects and never open writable DuckDB connections; the single
coordinator commits each result and task completion in one transaction.

## Scope

Foundation Stage 1 remains the canonical ingestion layer. POC 1 demonstrates
identity/`tsclean` preprocessing, reversible transformations, AutoARIMA,
Chronos-2, equal-weight combination, and official GIFT-Eval evaluation. It
deliberately excludes Mantis, MOMENT, fine-tuning, learned selection, NAS,
multi-machine execution, and automatic leaderboard submission.

See [architecture](docs/architecture.md), [data contract](docs/data-contract.md),
[environment setup](docs/environment.md), and the
[Stage 1 walkthrough](docs/foundation-stage-1.md).
