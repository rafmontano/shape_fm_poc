# Environment installation and verification

The repository has one installation interface for the complete approved
dependency superset:

```sh
scripts/setup.sh
```

Run it from the repository root on macOS Apple silicon or Ubuntu x86-64. It
installs every environment; selective component installation is deliberately
unsupported. Environments are always built locally and must not be copied
between operating systems or machines.

## Prerequisites

Install these system prerequisites before running setup:

- Git with HTTPS access, including Git submodule support;
- a bootstrap Python 3.10 or newer interpreter for the setup verifier;
- R 4.6.1 and the operating-system build libraries required by the locked R
  packages; and
- on NVIDIA Linux hosts, a compatible system NVIDIA driver.

The script installs and pins uv 0.12.18 under `.tools/uv` and uses uv-managed
project-local Python interpreters. It does not use Conda. The committed renv
activation and `renv.lock` provide project-local R isolation. Setup never
installs, replaces, or removes system R, system Python, Git, GPU drivers, global
caches, shell startup settings, or user directories.

## Install and verify

These two installation commands are equivalent:

```sh
scripts/setup.sh
scripts/setup.sh install all
```

Normal installation is non-destructive and idempotent. It creates missing
project directories, initializes pinned submodules, restores `renv.lock`,
synchronizes all six `uv.lock` files, acquires pinned data and model assets,
and runs the complete verification. Existing environments, data, models,
results, and valid caches are retained and reused; no working folder is moved,
renamed, or removed.

Run a complete read-only audit without installing or downloading anything:

```sh
scripts/setup.sh verify
```

Verification checks repository structure, submodule revisions, the R lock and
installed packages, M4 data objects, all Python locks and critical imports,
model and dataset identities, and TensorFlow and PyTorch device operations. It
returns nonzero for missing or mismatched required components. A supported
CPU-only host passes dependency validation while reporting unavailable
accelerator workloads as warnings.

## Full rebuild without deletion

The only rebuild command is:

```sh
scripts/setup.sh rebuild all --confirm-delete
```

The confirmation flag retains its approved interface name, but rebuild never
deletes folders. It verifies the repository root and prints the complete move
plan before changing anything. One local date is used for every archive, with
the form `original-folder-name_DDMMYY`. Managed locations include the root and
five specialist virtual environments, `renv/library`, `data`, `models`,
`results`, and project-local asset, uv, Python, and renv caches.

Every source and destination is constrained to the repository. If any dated
destination already exists, the whole rebuild stops before its first move; it
never overwrites or merges an earlier archive. A failed move stops immediately
and names the affected folder. After all moves succeed, setup recreates the
required structure and performs a complete installation.

The active setup-owned folders and every `_DDMMYY` archive form are ignored by
repository-relative Git rules. Coverage includes root and specialist `.venv`
folders, `renv/library`, `data`, `models`, `results`, `.cache`, and the local
uv, interpreter, package, and renv caches under `.tools`. Incomplete `.part`
downloads, Hugging Face incomplete files, and uv temporary interpreter files
are inside those ignored cache roots; renv staging and other transient private
state remain ignored under `renv`. The rules do not ignore environment manifests,
lockfiles, source, configuration, documentation, tests, or unrelated similarly
named paths.

## Python environment inventory

Each environment has a readable `pyproject.toml`, committed `uv.lock`, local
`.venv`, and independent import/version checks.

| Environment | Location | Purpose |
| --- | --- | --- |
| Core pipeline | repository root | DuckDB, Arrow, Dask, Hugging Face acquisition, and shared execution |
| GIFT-Eval | `environments/gift-eval` | pinned official GIFT-Eval submodule and evaluation dependencies |
| Chronos-2 | `environments/chronos-2` | Chronos 2.2.2 and its compatible PyTorch stack |
| Mantis | `environments/mantis` | Mantis 1.1.0 and its separate compatible PyTorch stack |
| Conventional classifiers | `environments/classifiers` | sktime 0.40.1, scikit-learn 1.7.2, Rotation Forest, ROCKET, and distance classifiers |
| TensorFlow classifiers | `environments/tensorflow` | InceptionTime and an isolated platform-specific TensorFlow stack |

TensorFlow is never combined with PyTorch. Mantis and Chronos-2 remain separate
because their foundation-model dependency ranges can evolve independently.

## R dependency superset

`renv.lock` is the authoritative definition and contains the union of the
current ShapeFM packages and the previous project's analysis, classification,
parallelism, plotting, and reporting packages, including all transitive
dependencies. Setup runs `renv::restore()` and never takes an uncontrolled
snapshot.

`M4comp2018` 0.2.0 is pinned to its established immutable GitHub release
tarball. Verification loads the package and checks that both `M4` and
`submission_info` are available. `scmamp` 0.3.2 is pinned to Git commit
`3cf4d8b9759769cdf20771afa0efc33a5265c7f9`. DBI, DuckDB, forecast,
tsfeatures, dtw, xgboost, caret, tidyverse component packages, parallel tools,
graphics packages, reticulate, cachem, and memoise are also locked in the
project-local library.

## GPU and platform behavior

On Apple silicon, the TensorFlow environment uses TensorFlow 2.18.1 with
TensorFlow Metal 1.2.0; PyTorch uses MPS when available. On Ubuntu x86-64 with
an NVIDIA driver and GPU, the Linux locks provide the CUDA-enabled TensorFlow
and PyTorch dependencies inside their respective environments. CPU-only Linux
installation remains supported for compatible operations.

Verification independently reports TensorFlow version, CUDA build status,
visible GPU or Metal devices, selected device, and a small tensor operation. It
also reports PyTorch version, CUDA runtime, CUDA or MPS availability, device
name, selected device, and a small tensor operation for each PyTorch
environment. `nvidia-smi` is diagnostic only.

On Linux, NVIDIA library and executable directories are discovered from the
environment being checked and applied only to that child process. Setup never
writes a global `LD_LIBRARY_PATH`, changes shell configuration, or exposes
TensorFlow's NVIDIA packages to PyTorch (or vice versa).

## Data, models, timeouts, and retries

The GIFT-Eval code and M4 Daily data identities remain pinned in
`config/dependencies/gift_eval.json`. Chronos-2 and Mantis repository revisions
and required-file SHA-256 checksums are pinned in
`config/dependencies/models.json`. Valid local snapshots are reused.

Network operations use a 7,200-second default timeout and at least three
attempts with a delay. Override these values for slow networks with
`SHAPEFM_DOWNLOAD_TIMEOUT`, `SHAPEFM_DOWNLOAD_ATTEMPTS` (minimum 3), and
`SHAPEFM_DOWNLOAD_RETRY_DELAY`. Hugging Face timeout variables are scoped to
the acquisition process. Ordinary files download through `.part` names and
become final only after validation; Hugging Face's incomplete-file cache and
the committed checksums provide the same publication boundary for model
snapshots. Exhausted retries produce a final corrective error.

## Reports and recovery

A successful installation writes a timestamped report to
`results/installation-report_YYYYMMDDTHHMMSSZ.txt`. Because read-only
verification cannot create files, `scripts/setup.sh verify` prints the same
report to standard output. Reports show date, platform, CPU architecture,
accelerator and driver status, R, Python, uv and renv versions, every
environment and critical package version, submodule and asset identities,
device checks, warnings, and failures. They never include credentials, tokens,
or sensitive environment variables.

After an interrupted normal installation, correct the reported prerequisite or
network error and run `scripts/setup.sh` again. Existing valid downloads and
installed packages are reused; temporary or incomplete downloads are not
treated as valid. After an interrupted rebuild move, do not merge folders or
delete archives. Resolve the named filesystem problem, preserve the dated
folders, and choose a later rebuild date if the collision safeguard reports an
existing destination.
