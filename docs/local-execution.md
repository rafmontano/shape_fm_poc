# Local and two-machine execution

The [approved execution policy](execution-policy.md) governs heavy testing.
The seasonal recovery and focused
[eight-worker safeguard follow-up](amp-poc2-execution-safeguards-instructions.md)
are complete. Smaller historical profiles below are not permission for a reduced
heavy run. If Ubuntu is unavailable, report that and request direction before
heavy local testing.

Heavy v3 Gate 4 tuning must select the `poc2_seasonal_recovery` execution
profile. The CLI and coordinator both reject an omitted or drifted profile. A
local run requires the separate `--local-heavy-exception` approval reference;
the option records approval but does not create it.

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
  --configuration config/experiments/poc2_m4_daily_100_resolved_period.json
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database DATABASE --configuration config/experiments/poc2_m4_daily_100_resolved_period.json \
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

## Historical Objective 1 acceptance

This section preserves the specific two-worker Objective 1 test. It is not the
current heavy-test configuration or a requirement to rerun all gates for each
change. Seasonal recovery is complete. Its safeguard follow-up uses a small
isolated check after synchronisation, not a rerun of the completed experiment.

From the clean Mac checkout at the exact revision already present on Ubuntu:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py test \
  --database results/poc2_final_acceptance.duckdb \
  --report results/poc2_final_acceptance_report.json
```

Run the identical command a second time against the same database and report.
The first call executes import through official evaluation; the second proves
that committed scientific work is skipped and fingerprints are preserved.

The historical POC2 Import test target contains exactly two Dask workers:

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
