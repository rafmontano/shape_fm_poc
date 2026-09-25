#!/usr/bin/env bash

# Reconstruct and run the existing ShapeFM POC 1 Ubuntu checkout.
# Safe to rerun: locked environment restores, acquisition, import, planning,
# and experiment execution all resume or verify previously completed work.

set -Eeuo pipefail

readonly BASE_COMMIT="c229060eeb5874a2d2393bfe78ab825b73abe10d"
readonly EXPECTED_SUBMODULE_COMMIT="4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a"
readonly UV_VERSION="0.12.18"
readonly PYTHON_VERSION="3.12.14"
readonly R_VERSION="4.6.1"
readonly RENV_VERSION="1.2.4"
readonly PROFILE="ubuntu_3950x_16core_128gb_rtx5090"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${SHAPEFM_OUTPUT_DIR:-$HOME/ShapeFM-results}"
DATABASE="${SHAPEFM_DATABASE:-$OUTPUT_DIR/shapefm.duckdb}"
IMPORT_WORKERS="${SHAPEFM_IMPORT_WORKERS:-8}"
LOG_FILE="$OUTPUT_DIR/bootstrap_ubuntu.log"

mkdir -p "$OUTPUT_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

info() {
  printf '\n==> %s\n' "$*"
}

die() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

on_error() {
  local status=$?
  printf '\nERROR: bootstrap stopped at line %s (exit code %s).\n' "$1" "$status" >&2
  printf 'Fix the reported problem and rerun this script; completed work will be reused.\n' >&2
  exit "$status"
}
trap 'on_error $LINENO' ERR

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command is unavailable: $1"
}

run_json() {
  local destination=$1
  shift
  local temporary="${destination}.tmp"
  rm -f "$temporary"
  "$@" | tee "$temporary"
  .tools/uv/uv run --locked python -m json.tool "$temporary" >/dev/null
  mv "$temporary" "$destination"
}

verify_checkout() {
  info "Verifying the existing checkout"
  cd "$ROOT"
  [[ -d .git ]] || die "$ROOT is not a Git checkout."
  git merge-base --is-ancestor "$BASE_COMMIT" HEAD || \
    die "The current checkout is not based on immutable v0.3 commit $BASE_COMMIT."

  git submodule update --init --recursive
  local submodule_commit
  submodule_commit="$(git -C external/gift-eval rev-parse HEAD)"
  [[ "$submodule_commit" == "$EXPECTED_SUBMODULE_COMMIT" ]] || \
    die "GIFT-Eval is at $submodule_commit, expected $EXPECTED_SUBMODULE_COMMIT."
  printf 'Checkout:  %s\n' "$(git rev-parse HEAD)"
  printf 'Submodule: %s\n' "$submodule_commit"
}

verify_host() {
  info "Verifying the Ubuntu host"
  [[ "$(uname -s)" == "Linux" ]] || die "This bootstrap requires Linux."
  [[ "$(uname -m)" == "x86_64" ]] || die "This bootstrap requires Linux x86_64."
  require_command curl
  require_command git
  require_command Rscript
  require_command nvidia-smi

  local actual_r
  actual_r="$(Rscript --vanilla -e 'cat(as.character(getRversion()))')"
  [[ "$actual_r" == "$R_VERSION" ]] || \
    die "R $R_VERSION is required; Rscript reports $actual_r."
  Rscript --vanilla -e \
    "stopifnot(requireNamespace('renv', quietly=TRUE), as.character(packageVersion('renv')) == '$RENV_VERSION')"

  nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader
  lscpu | sed -n '/Model name:/p;/^CPU(s):/p;/Core(s) per socket:/p'
  free -h
}

restore_environments() {
  info "Restoring locked Python environments"
  cd "$ROOT"
  mkdir -p .tools/uv
  if [[ ! -x .tools/uv/uv ]]; then
    curl -LsSf "https://astral.sh/uv/$UV_VERSION/install.sh" | \
      env UV_INSTALL_DIR="$ROOT/.tools/uv" UV_NO_MODIFY_PATH=1 sh
  fi
  [[ "$(.tools/uv/uv --version)" == "uv $UV_VERSION"* ]] || \
    die "Repository-local uv is not version $UV_VERSION."

  export UV_PYTHON_INSTALL_DIR="$ROOT/.tools/python"
  export UV_CACHE_DIR="$ROOT/.tools/cache"
  export UV_PYTHON_PREFERENCE="only-managed"
  export RENV_PATHS_CACHE="$ROOT/.tools/renv-cache"

  .tools/uv/uv python install "$PYTHON_VERSION"
  .tools/uv/uv sync --locked
  .tools/uv/uv sync --project environments/gift-eval --locked
  .tools/uv/uv sync --project environments/chronos-2 --locked

  info "Restoring the locked R environment"
  Rscript -e 'renv::restore(prompt = FALSE)'
  Rscript -e 'stopifnot(renv::status()$synchronized)'

  info "Verifying reconstructed environments"
  .tools/uv/uv run --locked python -c \
    'import duckdb, pyarrow; print("DuckDB", duckdb.__version__, "PyArrow", pyarrow.__version__)'
  environments/gift-eval/.venv/bin/python -c \
    'import gift_eval; print("GIFT-Eval", gift_eval.__file__)'
  environments/chronos-2/.venv/bin/python -c \
    'import chronos, torch; print("Chronos", chronos.__file__); print("PyTorch", torch.__version__, "CUDA", torch.version.cuda)'
}

prepare_data() {
  info "Acquiring and verifying pinned M4 Daily"
  cd "$ROOT"
  Rscript workflows/gate_00_setup/01_prepare_gift_eval.R

  if [[ -f "$DATABASE" ]]; then
    run_json "$OUTPUT_DIR/m4_daily_import_status.json" \
      .tools/uv/uv run --locked shapefm-import status \
        --database "$DATABASE" --dataset m4_daily
    if .tools/uv/uv run --locked python -c \
      'import json, sys
status = json.load(open(sys.argv[1]))
assert status["series_count"] == 4227
assert status["task_counts"] == {"completed": 4227}' \
      "$OUTPUT_DIR/m4_daily_import_status.json" 2>/dev/null; then
      info "All 4,227 M4 Daily imports are already complete; skipping import"
      return
    fi
  fi

  info "Importing incomplete M4 Daily work into $DATABASE"
  .tools/uv/uv run --locked shapefm-import migrate --database "$DATABASE"
  .tools/uv/uv run --locked shapefm-import import \
    --database "$DATABASE" \
    --workers "$IMPORT_WORKERS"
  run_json "$OUTPUT_DIR/m4_daily_import_status.json" \
    .tools/uv/uv run --locked shapefm-import status \
      --database "$DATABASE" --dataset m4_daily
}

validate_and_run() {
  cd "$ROOT"

  info "Planning the restartable smoke experiment"
  run_json "$OUTPUT_DIR/ubuntu_smoke_plan.json" \
    .tools/uv/uv run --locked shapefm-poc1 \
      --database "$DATABASE" plan --scope smoke

  info "Checking PyTorch CUDA support"
  environments/chronos-2/.venv/bin/python -c \
    'import torch
assert torch.cuda.is_available(), "CUDA is unavailable"
name = torch.cuda.get_device_name(0)
assert "NVIDIA GeForce RTX 5090" in name, name
print(torch.__version__, torch.version.cuda, name)'

  info "Validating the strict Ubuntu profile with a real Chronos forecast"
  run_json "$OUTPUT_DIR/ubuntu_hardware_validation.json" \
    .tools/uv/uv run --locked shapefm-poc1 \
      --database "$DATABASE" validate-hardware \
      --profile "$PROFILE" --chronos-smoke

  if [[ -s "$OUTPUT_DIR/ubuntu_calibration.json" ]] && \
      .tools/uv/uv run --locked python -c \
        'import json, sys
report=json.load(open(sys.argv[1]))
assert report["profile"]["name"] == sys.argv[2]
chronos=[row for row in report["measurements"] if row["kind"] == "chronos_2"]
assert [row["batch_size"] for row in chronos] == [8, 16, 32, 64]
assert all("strictly_equivalent_to_batch_one" in row for row in chronos)
assert report["isolated_database_measurement_count"] > 0' \
        "$OUTPUT_DIR/ubuntu_calibration.json" "$PROFILE" 2>/dev/null; then
    info "Reusing completed calibration at $OUTPUT_DIR/ubuntu_calibration.json"
  else
    info "Calibrating the Ubuntu execution profile"
    rm -f "$OUTPUT_DIR/ubuntu_calibration.json"
    .tools/uv/uv run --locked shapefm-poc1 \
      --database "$DATABASE" calibrate \
      --profile "$PROFILE" \
      --output "$OUTPUT_DIR/ubuntu_calibration.json"
  fi

  info "Running the smoke POC with the Ubuntu profile"
  .tools/uv/uv run --locked shapefm-poc1 \
    --database "$DATABASE" run --profile "$PROFILE"

  info "Saving final POC status and official GIFT-Eval results"
  run_json "$OUTPUT_DIR/ubuntu_status.json" \
    .tools/uv/uv run --locked shapefm-poc1 \
      --database "$DATABASE" status
  run_json "$OUTPUT_DIR/ubuntu_results.json" \
    .tools/uv/uv run --locked shapefm-poc1 \
      --database "$DATABASE" results
}

main() {
  verify_checkout
  verify_host
  restore_environments
  prepare_data
  validate_and_run
  info "Bootstrap completed successfully"
  printf 'Database: %s\nResults:  %s\nLog:      %s\n' \
    "$DATABASE" "$OUTPUT_DIR" "$LOG_FILE"
}

main "$@"
