# POC 1.2 local and two-machine execution

Execution profiles are committed in `config/execution_profiles.json`. They
control local scheduling only and do not change scientific experiment or task
identity. Every gate invocation records the requested profile, fully resolved
values, manual overrides, and detected hardware. The coordinator is always the
only DuckDB writer.

| profile | cleaning | transform | AutoARIMA | Chronos processes / batch | combination | evaluation | CPU/GPU overlap |
|---|---:|---:|---:|---:|---:|---:|---|
| `sequential_safe` | 1 | 1 | 1 | 1 / 1 | 1 | 1 | no |
| `mac_m1pro_10core_16gb` | 2 | 4 | 2 | 1 / 8 | 4 | 1 | no |
| `ubuntu_3950x_16core_128gb_rtx5090` | 8 | 16 | 12 | 1 / 16 | 16 | 1 | yes |
| `two_machine_dask` | 6 | 6 | 6 | 1 / 16 | 6 | 1 | yes |

The Mac profile serialises heavy AutoARIMA and Chronos work because CPU and GPU
share 16 GB unified memory. The Ubuntu profile may overlap bounded CPU
AutoARIMA with one persistent CUDA worker while reserving CPU capacity for the
coordinator and input preparation. Manual overrides are available on
`shapefm-poc1 run --help`; database writers remain fixed at one and MPS/CUDA
profiles reject more than one Chronos process.

Chronos inputs are grouped into power-of-two context-length ranges and then
split at the configured batch bound. One subprocess loads the pinned model once
and accepts identified batches. After each successful batch, the coordinator
transactionally stores its results before submitting another. An accelerator
out-of-memory response restarts the subprocess and halves only the failed batch.
Previously committed tasks remain complete; batch size one is the retry floor.

## Mac validation and calibration

```sh
.tools/uv/uv sync --project environments/chronos-2 --locked
.tools/uv/uv run --locked shapefm-poc1 validate-hardware \
  --profile mac_m1pro_10core_16gb --chronos-smoke
.tools/uv/uv run --locked shapefm-poc1 calibrate \
  --profile mac_m1pro_10core_16gb \
  --output docs/calibration/mac_m1pro_calibration.json
```

Calibration reads representative short, median, and long M4 Daily contexts but
writes measurements only to a temporary isolated database. It tests CPU worker
counts 1, 2, and 4 and Chronos batch sizes 1, 2, 4, 8, and 16. The report records
throughput, load/inference time, memory and swap state, retries, effective batch
sizes, failures, and a recommendation. System memory is sampled throughout each
candidate and checked before and after it; accelerator memory is checked for
each Chronos candidate. Unsafe candidates are marked with a rejection reason,
excluded from recommendations, and stop that resource sweep from increasing.
Child RSS is labelled as observed per-process memory rather than an aggregate
concurrent peak. Calibration never rewrites the committed profile.

Chronos calibration always creates a separate batch-size-1 forecast for each
representative context before testing the configured candidate grid. Candidate
equivalence is measured against those references even when batch size 1 is not
itself a candidate, as on Ubuntu.

## Ubuntu RTX 5090 validation

Restore the locked environment on the Ubuntu host, then require the exact CUDA
profile and run a real pinned-model forecast:

```sh
.tools/uv/uv sync --project environments/chronos-2 --locked

environments/chronos-2/.venv/bin/python -c \
  'import torch; assert torch.cuda.is_available(); print(torch.__version__, torch.version.cuda, torch.cuda.get_device_name(0))'

.tools/uv/uv run --locked shapefm-poc1 validate-hardware \
  --profile ubuntu_3950x_16core_128gb_rtx5090 --chronos-smoke
```

The second command must identify `NVIDIA GeForce RTX 5090`; the profile
validator rejects another name or unavailable CUDA. Only after these commands
pass on that machine should Ubuntu calibration run:

```sh
.tools/uv/uv run --locked shapefm-poc1 calibrate \
  --profile ubuntu_3950x_16core_128gb_rtx5090 \
  --output docs/calibration/ubuntu_3950x_rtx5090_calibration.json
```

Hardware provenance records the detected CPU model for auditability, but CPU
wording is not a strict profile requirement. CUDA availability, the exact RTX
5090 name, memory thresholds, and worker constraints remain strict.

The Ubuntu workflow has been validated on the 16-core Ryzen 3950X, 128 GB RAM,
and RTX 5090 host represented by this profile. Its generated calibration report
remains outside Git with the other machine-local results. For a future published
benchmark, Ubuntu CUDA is the provisional canonical model-execution backend;
Mac MPS remains for development and smaller validation. Compare sequential and
profiled results on the same backend with strict absolute and relative
tolerances of `1e-5`. Cross-backend MPS and CUDA results are not assumed
bit-for-bit identical.

To reconstruct the locked environments, reuse a completed import, and resume
Ubuntu validation, calibration, and the smoke POC from an existing checkout:

```sh
./scripts/bootstrap_ubuntu.sh
```

The script does not clone the repository or require GitHub CLI authentication.
By default it reuses `~/ShapeFM-results/shapefm.duckdb` and writes reports and
the append-only bootstrap log beside that database.

## Mac-coordinated Dask execution

The normal researcher interface starts the Mac scheduler, two bounded Mac CPU
workers, four Ubuntu CPU workers, and one dedicated Ubuntu RTX 5090 worker. It
validates all workers, runs Gates 2–6, prints the task/result report, and shuts
the cluster down:

```sh
scripts/run_two_machine.sh --scope smoke
scripts/run_two_machine.sh --scope smoke --resume
```

No Ubuntu command is required. Troubleshooting operations are separate:

```sh
scripts/run_two_machine.sh start
scripts/run_two_machine.sh status
scripts/run_two_machine.sh stop
```

While running, the dashboard is at <http://127.0.0.1:8787/status>. CPU workers
advertise `CPU=1`; the dedicated GPU worker advertises only `GPU=1`. Chronos-2
requests `GPU=1`, while cleaning, transformations, AutoARIMA, and combination
request `CPU=1`. Stage 6 always executes through the official adapter on the
Mac. Runtime files and logs are under ignored `data/dask/`.

The bounded combined-load calibration is run from the Mac with:

```sh
scripts/run_two_machine.sh calibrate-dask
```

It tests the baseline, moderate, recommended-candidate, and (only after a safe
recommended candidate) aggressive topologies in order. Each candidate uses 256
deterministically selected short/medium/long M4 Daily contexts, both cleaning
methods, both transformations, and concurrent AutoARIMA and Chronos-2 queues.
The canonical database is opened read-only; measurements are written to a
temporary DuckDB that is removed after the candidate. Host CPU, available
memory, swap, Dask spilling and worker identity are sampled throughout, along
with RTX 5090 utilisation/free VRAM, retries, failures, effective Chronos batch
sizes, throughput, and scientific equivalence to the baseline. The complete
report is `docs/calibration/two_machine_dask_calibration.json`. The sweep stops
after an unsafe candidate, and a larger safe profile is selected only when it
improves throughput by at least 5%. Environment variables
`SHAPEFM_MAC_CPU_WORKERS`, `SHAPEFM_UBUNTU_CPU_WORKERS`,
`SHAPEFM_CHRONOS_BATCH_SIZE`, and `SHAPEFM_DASK_MAX_IN_FLIGHT` remain available
for explicit troubleshooting overrides.

The September 2026 combined-load calibration selected the baseline topology:
2 Mac CPU workers, 4 Ubuntu CPU workers, Chronos batch 16, and 12 maximum
in-flight CPU batches. It completed 3,584 calibration computations in 622.60 s
(5.76 tasks/s), with no retry, failure, worker restart, swap, or Dask spill;
minimum available memory was 5.14 GiB on the Mac and 116.63 GiB on Ubuntu, and
minimum free RTX 5090 memory was 28.23 GiB. The moderate topology reached 8.76
tasks/s and remained resource-safe, but was rejected because 28 of 1,024
AutoARIMA forecasts changed beyond `atol=rtol=1e-5` when work placement changed
between arm64 macOS and x86_64 Linux. Chronos remained within tolerance. The
sweep therefore stopped before the larger candidates, as required. This POC
keeps cross-platform CPU scheduling as an explicit limitation rather than
weakening the scientific tolerance.

This POC uses unencrypted Dask TCP on the trusted private LAN. Do not expose
ports 8786 or worker ports to an untrusted network; production deployment would
require Dask TLS and network access controls.

`plan --scope m4_daily` now materialises all 4,227 forecast instances and
109,914 deterministic tasks. Use `--dry-run` to inspect counts without writes:

```sh
.tools/uv/uv run --locked shapefm-poc1 plan --scope m4_daily --dry-run
```

After smoke and restart validation is accepted, the exact later full run is:

```sh
caffeinate -dimsu scripts/run_two_machine.sh --scope m4_daily --resume
```

Do not run that command during POC validation; it performs the full experiment.
