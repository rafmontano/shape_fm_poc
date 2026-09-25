# POC 1.1 local execution

Execution profiles are committed in `config/execution_profiles.json`. They
control local scheduling only and do not change scientific experiment or task
identity. Every gate invocation records the requested profile, fully resolved
values, manual overrides, and detected hardware. The coordinator is always the
only DuckDB writer.

| profile | cleaning | transform | AutoARIMA | Chronos processes / batch | combination | evaluation | CPU/GPU overlap |
|---|---:|---:|---:|---:|---:|---:|---|
| `sequential_safe` | 1 | 1 | 1 | 1 / 1 | 1 | 1 | no |
| `mac_m1pro_10core_16gb` | 2 | 4 | 2 | 1 / 8 | 4 | 1 | no |
| `ubuntu_5950x_16core_128gb_rtx5090` | 8 | 16 | 12 | 1 / 16 | 16 | 1 | yes |

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

## Ubuntu RTX 5090 validation (not yet executed)

Restore the locked environment on the Ubuntu host, then require the exact CUDA
profile and run a real pinned-model forecast:

```sh
.tools/uv/uv sync --project environments/chronos-2 --locked

environments/chronos-2/.venv/bin/python -c \
  'import torch; assert torch.cuda.is_available(); print(torch.__version__, torch.version.cuda, torch.cuda.get_device_name(0))'

.tools/uv/uv run --locked shapefm-poc1 validate-hardware \
  --profile ubuntu_5950x_16core_128gb_rtx5090 --chronos-smoke
```

The second command must identify `NVIDIA GeForce RTX 5090`; the profile
validator rejects another name or unavailable CUDA. Only after these commands
pass on that machine should Ubuntu calibration run:

```sh
.tools/uv/uv run --locked shapefm-poc1 calibrate \
  --profile ubuntu_5950x_16core_128gb_rtx5090 \
  --output docs/calibration/ubuntu_5950x_rtx5090_calibration.json
```

Ubuntu/CUDA has not been validated by the local Mac work. For a future published
benchmark, Ubuntu CUDA is the provisional canonical model-execution backend;
Mac MPS remains for development and smaller validation. Compare sequential and
profiled results on the same backend with strict absolute and relative
tolerances of `1e-5`. Cross-backend MPS and CUDA results are not assumed
bit-for-bit identical.
