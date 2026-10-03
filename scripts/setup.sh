#!/usr/bin/env bash
# ==============================================================================
# setup.sh
#
# Purpose: Install, verify, or safely rebuild the complete ShapeFM dependency superset.
# Inputs: One supported operation, committed R/Python locks, dependency manifests, and optional timeout settings.
# Outputs: Project-local environments/assets and a readable installation report; verify writes nothing.
# Run from: scripts/setup.sh [install all|verify|rebuild all --confirm-delete|--help]
# ==============================================================================

set -euo pipefail

# Repository and installation constants --------------------------------------

SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "$SCRIPT_DIRECTORY/.." && pwd)"
UV_VERSION="0.12.18"
DOWNLOAD_TIMEOUT="${SHAPEFM_DOWNLOAD_TIMEOUT:-7200}"
DOWNLOAD_ATTEMPTS="${SHAPEFM_DOWNLOAD_ATTEMPTS:-3}"
DOWNLOAD_RETRY_DELAY="${SHAPEFM_DOWNLOAD_RETRY_DELAY:-5}"
UV="$REPOSITORY_ROOT/.tools/uv/uv"

PYTHON_PROJECTS=(
  "."
  "environments/gift-eval"
  "environments/chronos-2"
  "environments/mantis"
  "environments/classifiers"
  "environments/tensorflow"
)

usage() {
  cat <<'EOF'
Usage:
  scripts/setup.sh
  scripts/setup.sh install all
  scripts/setup.sh verify
  scripts/setup.sh rebuild all --confirm-delete
  scripts/setup.sh --help

The default is the non-destructive complete installation (`install all`).
Selective component installation is intentionally unsupported.

`rebuild all --confirm-delete` never deletes folders. It first displays and
moves every existing project-managed folder to a dated `_DDMMYY` sibling,
then recreates and installs the complete environment.
EOF
}

fail() {
  printf 'setup error: %s\n' "$*" >&2
  exit 1
}

# Argument parsing ------------------------------------------------------------

parse_arguments() {
  OPERATION="install"
  TARGET="all"
  CONFIRM_ARCHIVE="false"

  if [[ $# -eq 0 ]]; then
    return
  fi
  if [[ $# -eq 1 && "$1" == "--help" ]]; then
    OPERATION="help"
    return
  fi
  if [[ $# -eq 1 && "$1" == "verify" ]]; then
    OPERATION="verify"
    TARGET=""
    return
  fi
  if [[ $# -eq 2 && "$1" == "install" && "$2" == "all" ]]; then
    return
  fi
  if [[ $# -eq 3 && "$1" == "rebuild" && "$2" == "all" && "$3" == "--confirm-delete" ]]; then
    OPERATION="rebuild"
    CONFIRM_ARCHIVE="true"
    return
  fi
  if [[ "${1:-}" == "rebuild" && "${2:-}" == "all" ]]; then
    fail "full rebuild requires exactly: scripts/setup.sh rebuild all --confirm-delete"
  fi
  fail "unsupported or selective operation; use scripts/setup.sh --help"
}

# Safety and prerequisites ----------------------------------------------------

validate_repository_root() {
  [[ -e "$REPOSITORY_ROOT/.git" ]] || fail "repository root has no .git marker: $REPOSITORY_ROOT"
  [[ -f "$REPOSITORY_ROOT/shape_fm_poc.Rproj" ]] || fail "repository root marker is missing: shape_fm_poc.Rproj"
  [[ -f "$REPOSITORY_ROOT/pyproject.toml" ]] || fail "repository root marker is missing: pyproject.toml"
  [[ -f "$REPOSITORY_ROOT/renv.lock" ]] || fail "repository root marker is missing: renv.lock"
  [[ -f "$REPOSITORY_ROOT/scripts/setup_support.py" ]] || fail "setup support module is missing"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required but was not found on PATH"
}

validate_settings() {
  require_command python3
  python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' || \
    fail "bootstrap python3 must be version 3.10 or newer"
  [[ "$DOWNLOAD_TIMEOUT" =~ ^[0-9]+$ ]] || fail "SHAPEFM_DOWNLOAD_TIMEOUT must be an integer"
  [[ "$DOWNLOAD_ATTEMPTS" =~ ^[0-9]+$ ]] || fail "SHAPEFM_DOWNLOAD_ATTEMPTS must be an integer"
  (( DOWNLOAD_ATTEMPTS >= 3 )) || fail "download attempts must be at least three"
}

# Directory preparation -------------------------------------------------------

create_required_directories() {
  printf '[setup] Creating missing project directories without replacing existing content.\n'
  mkdir -p \
    "$REPOSITORY_ROOT/data" \
    "$REPOSITORY_ROOT/models" \
    "$REPOSITORY_ROOT/results" \
    "$REPOSITORY_ROOT/.cache/downloads" \
    "$REPOSITORY_ROOT/.cache/huggingface" \
    "$REPOSITORY_ROOT/.tools/cache" \
    "$REPOSITORY_ROOT/.tools/python" \
    "$REPOSITORY_ROOT/.tools/renv-cache"
}

# Tool and dependency installation -------------------------------------------

ensure_uv() {
  if [[ -x "$UV" ]]; then
    local installed_version
    installed_version="$($UV --version | awk '{print $2}')"
    [[ "$installed_version" == "$UV_VERSION" ]] || fail "project uv is $installed_version; expected $UV_VERSION"
    printf '[setup] Reusing project-local uv %s.\n' "$UV_VERSION"
    return
  fi

  require_command python3
  local installer="$REPOSITORY_ROOT/.cache/downloads/uv-installer-$UV_VERSION.sh"
  python3 -B "$REPOSITORY_ROOT/scripts/setup_support.py" download \
    --url "https://astral.sh/uv/$UV_VERSION/install.sh" \
    --destination "$installer" \
    --attempts "$DOWNLOAD_ATTEMPTS" \
    --timeout "$DOWNLOAD_TIMEOUT" \
    --delay "$DOWNLOAD_RETRY_DELAY" \
    --contains "UV_INSTALL_DIR"
  printf '[setup] Installing uv %s inside .tools/uv.\n' "$UV_VERSION"
  env UV_INSTALL_DIR="$REPOSITORY_ROOT/.tools/uv" UV_NO_MODIFY_PATH=1 sh "$installer"
  [[ -x "$UV" ]] || fail "uv installation did not create $UV"
}

initialize_submodules() {
  printf '[setup] Initializing pinned Git submodules.\n'
  git -C "$REPOSITORY_ROOT" submodule update --init --recursive
}

restore_r_environment() {
  require_command Rscript
  printf '[setup] Restoring the complete R dependency superset from renv.lock.\n'
  env \
    SHAPEFM_DOWNLOAD_TIMEOUT="$DOWNLOAD_TIMEOUT" \
    RENV_PATHS_CACHE="$REPOSITORY_ROOT/.tools/renv-cache" \
    Rscript "$REPOSITORY_ROOT/scripts/setup_r.R" restore
}

sync_python_environments() {
  export UV_PYTHON_INSTALL_DIR="$REPOSITORY_ROOT/.tools/python"
  export UV_CACHE_DIR="$REPOSITORY_ROOT/.tools/cache"
  export UV_PYTHON_PREFERENCE="only-managed"
  for project in "${PYTHON_PROJECTS[@]}"; do
    retry_command "Synchronize locked Python environment: $project" \
      "$UV" sync --project "$REPOSITORY_ROOT/$project" --locked
  done
}

# Recoverable large-asset operations -----------------------------------------

retry_command() {
  local label="$1"
  shift
  local attempt
  for ((attempt = 1; attempt <= DOWNLOAD_ATTEMPTS; attempt++)); do
    printf '[setup] %s (attempt %d/%d).\n' "$label" "$attempt" "$DOWNLOAD_ATTEMPTS"
    if "$@"; then
      return
    fi
    if (( attempt < DOWNLOAD_ATTEMPTS )); then
      sleep "$DOWNLOAD_RETRY_DELAY"
    fi
  done
  fail "$label failed after $DOWNLOAD_ATTEMPTS attempts; check network access and rerun setup"
}

download_model() {
  local environment="$1"
  local model_key="$2"
  local python="$REPOSITORY_ROOT/$environment/.venv/bin/python"
  retry_command "Acquire pinned $model_key model assets" \
    env \
      HF_HOME="$REPOSITORY_ROOT/.cache/huggingface" \
      HF_HUB_DOWNLOAD_TIMEOUT="$DOWNLOAD_TIMEOUT" \
      HF_HUB_ETAG_TIMEOUT="$DOWNLOAD_TIMEOUT" \
      "$python" -B "$REPOSITORY_ROOT/scripts/setup_support.py" acquire-model \
      --root "$REPOSITORY_ROOT" \
      --manifest "$REPOSITORY_ROOT/config/dependencies/models.json" \
      --model "$model_key"
}

acquire_assets() {
  printf '[setup] Acquiring or reusing pinned datasets and model snapshots.\n'
  retry_command "Acquire pinned GIFT-Eval M4 Daily data" \
    env \
      HF_HOME="$REPOSITORY_ROOT/.cache/huggingface" \
      HF_HUB_DOWNLOAD_TIMEOUT="$DOWNLOAD_TIMEOUT" \
      HF_HUB_ETAG_TIMEOUT="$DOWNLOAD_TIMEOUT" \
      "$UV" run --project "$REPOSITORY_ROOT" --locked --no-sync \
      python -B -m util.p01_04_gift_eval_acquisition m4_daily
  download_model "environments/chronos-2" "chronos_2"
  download_model "environments/mantis" "mantis"
}

# Verification and reports ----------------------------------------------------

verify_installation() {
  require_command python3
  python3 -B "$REPOSITORY_ROOT/scripts/setup_support.py" verify --root "$REPOSITORY_ROOT"
}

write_installation_report() {
  local report="$REPOSITORY_ROOT/results/installation-report_$(date -u +%Y%m%dT%H%M%SZ).txt"
  printf '[setup] Validating the complete installation.\n'
  verify_installation | tee "$report"
  printf '[setup] Installation report: %s\n' "$report"
}

# Full rebuild archival -------------------------------------------------------

archive_existing_installation() {
  [[ "$CONFIRM_ARCHIVE" == "true" ]] || fail "rebuild requires --confirm-delete"
  local archive_date
  archive_date="$(date +%d%m%y)"
  printf '[setup] Preflighting the complete archive plan for date %s.\n' "$archive_date"
  # The helper collision-checks the complete plan before its first move. The
  # misleading historical confirmation flag authorizes moves only, never deletion.
  python3 -B "$REPOSITORY_ROOT/scripts/setup_support.py" archive \
    --root "$REPOSITORY_ROOT" --date "$archive_date" --execute
}

install_all() {
  create_required_directories
  ensure_uv
  require_command git
  retry_command "Initialize pinned Git submodules" initialize_submodules
  retry_command "Restore locked R environment" restore_r_environment
  sync_python_environments
  acquire_assets
  write_installation_report
}

main() {
  parse_arguments "$@"
  if [[ "$OPERATION" == "help" ]]; then
    usage
    return
  fi
  validate_repository_root
  validate_settings
  cd "$REPOSITORY_ROOT"

  if [[ "$OPERATION" == "verify" ]]; then
    verify_installation
    return
  fi
  if [[ "$OPERATION" == "rebuild" ]]; then
    archive_existing_installation
  fi
  install_all
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
