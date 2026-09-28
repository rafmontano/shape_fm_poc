# Local and two-machine execution

All commands below use existing environments without synchronization:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py --help
```

There are no researcher-facing console scripts, shell launchers, R launchers,
or workflow scripts. `src/python/00_main.py` owns planning, read-only status and
results, configured execution, and the 100-series acceptance workflow.

## Planning and inspection

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py plan \
  --configuration config/experiments/poc2_m4_daily_100.json
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database DATABASE --configuration config/experiments/poc2_m4_daily_100.json \
  --processes 1-3
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database DATABASE --processes 4-6
.tools/uv/uv run --locked --no-sync python src/python/00_main.py status --database DATABASE
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results --database DATABASE
```

`status` and `results` open existing databases read-only. `plan` uses a
temporary database. The first `run` requires a complete JSON document and
creates its database atomically. Later runs reject JSON and reconstruct the
validated configuration from DuckDB. Process prerequisites are ordered,
completed tasks are skipped, and each request appends an execution event.

## Two-machine acceptance

From the clean Mac checkout at the exact revision already present on Ubuntu:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py test \
  --database results/poc2_final_acceptance.duckdb \
  --report results/poc2_final_acceptance_report.json
```

Run the identical command a second time against the same database and report.
The first call executes import through official evaluation; the second proves
that committed scientific work is skipped and fingerprints are preserved.

The POC2 Import target contains exactly two Dask workers:

- Mac: coordinator, scheduler, sole DuckDB writer, and one CPU worker;
- Ubuntu: one Chronos worker advertising `CHRONOS_GPU_SLOT=1` on one physical
  RTX 5090.

The scheduler is not counted as a worker. The configured worker ceilings are
2 GiB for the Mac CPU worker and 4 GiB for the Ubuntu GPU worker. Runtime gates
preserve at least 3 GiB Mac host, 16 GiB Ubuntu host, and 4 GiB GPU-memory
headroom and reject swap growth, spilling, worker replacement, failed tasks,
or retries.

The acceptance harness verifies clean/revision-identical worktrees, the pinned
GIFT-Eval submodule, existing locked environments, source data, final module
imports, ports, and stale processes before starting. It always shuts down the
experimental scheduler and workers.

Generated databases, reports, logs, telemetry, caches, and model files remain
in ignored locations. Never use `data/shapefm.duckdb` for acceptance.
