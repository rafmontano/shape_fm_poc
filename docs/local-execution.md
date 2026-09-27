# Local and two-machine execution

All commands below use existing environments without synchronization:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py --help
```

There are no researcher-facing console scripts, shell launchers, R launchers,
or workflow scripts. `src/python/00_main.py` owns planning, read-only status and
results, and the 100-series acceptance workflow.

## Planning and inspection

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py plan --series-limit 100
.tools/uv/uv run --locked --no-sync python src/python/00_main.py status --database DATABASE
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results --database DATABASE
```

`status` and `results` open existing databases read-only. `plan` uses an
isolated planning database and refuses the authoritative database.

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

The target cluster contains 35 Dask workers:

- Mac: coordinator, scheduler, sole DuckDB writer, and 5 CPU workers;
- Ubuntu: 15 CPU workers;
- Ubuntu: 15 logical Chronos workers advertising `CHRONOS_GPU_SLOT=1`, all
  sharing one physical RTX 5090 through `CUDA_VISIBLE_DEVICES=0`.

At least 15 Chronos batches may be in flight. Scientific batches are not pinned
to transient Dask addresses; acceptance instead requires all 15 logical GPU
workers to contribute. The configured Ubuntu worker ceilings are 30 GiB for
CPU workers (15 × 2 GiB) plus 60 GiB for logical GPU workers (15 × 4 GiB), or
90 GiB combined. Runtime gates preserve at least 16 GiB host headroom and 4 GiB
GPU-memory headroom and reject swap growth, spilling, worker replacement,
failed tasks, or retries.

The acceptance harness verifies clean/revision-identical worktrees, the pinned
GIFT-Eval submodule, existing locked environments, source data, final module
imports, ports, and stale processes before starting. It always shuts down the
experimental scheduler and workers.

Generated databases, reports, logs, telemetry, caches, and model files remain
in ignored locations. Never use `data/shapefm.duckdb` for acceptance.
