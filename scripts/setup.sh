#!/usr/bin/env bash
# ==============================================================================
# setup.sh
#
# Purpose: Restore the locked ShapeFM environment or check readiness offline.
# Inputs: One supported operation, committed R/Python locks, dependency manifests, and optional timeout settings.
# Outputs: Project-local environments/assets and concise readiness; verify writes nothing.
# Run from: scripts/setup.sh [verify|--help]
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

usage() {
  cat <<'EOF'
Usage:
  scripts/setup.sh
  scripts/setup.sh verify
  scripts/setup.sh --help

No arguments restores locked dependencies and acquires pinned assets.
verify checks readiness offline without changing files.
EOF
}

fail() {
  printf 'setup error: %s\n' "$*" >&2
  exit 1
}

# Argument parsing ------------------------------------------------------------

parse_arguments() {
  OPERATION="install"

  if [[ $# -eq 0 ]]; then
    return
  fi
  if [[ $# -eq 1 && "$1" == "--help" ]]; then
    OPERATION="help"
    return
  fi
  if [[ $# -eq 1 && "$1" == "verify" ]]; then
    OPERATION="verify"
    return
  fi
  fail "unsupported operation; use scripts/setup.sh --help"
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
  [[ "$DOWNLOAD_RETRY_DELAY" =~ ^[0-9]+$ ]] || fail "SHAPEFM_DOWNLOAD_RETRY_DELAY must be an integer"
  (( DOWNLOAD_TIMEOUT > 0 )) || fail "download timeout must be positive"
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
  local staging
  staging="$(mktemp -d "$REPOSITORY_ROOT/.tools/uv-stage.XXXXXX")"
  if ! env UV_INSTALL_DIR="$staging" UV_NO_MODIFY_PATH=1 sh "$installer"; then
    rm -rf "$staging"
    fail "uv bootstrap failed"
  fi
  [[ "$("$staging/uv" --version | awk '{print $2}')" == "$UV_VERSION" ]] || fail "unexpected bootstrap uv version"
  mkdir -p "$(dirname "$UV")"
  mv "$staging/uv" "$UV"
  rm -rf "$staging"
  [[ -x "$UV" ]] || fail "uv installation did not create $UV"
}

initialize_submodules() {
  printf '[setup] Initializing pinned Git submodules.\n'
  git -C "$REPOSITORY_ROOT" submodule update --init --recursive -- external/gift-eval
  python3 -B "$REPOSITORY_ROOT/scripts/setup_support.py" check-source --root "$REPOSITORY_ROOT"
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
  local projects
  projects="$(python3 -B "$REPOSITORY_ROOT/scripts/setup_support.py" projects --root "$REPOSITORY_ROOT")"
  while IFS= read -r project; do
    retry_command "Synchronize locked Python environment: $project" \
      "$UV" sync --project "$REPOSITORY_ROOT/$project" --locked
  done <<< "$projects"
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
      python -B -m p01_import.acquisition m4_daily
  download_model "environments/chronos-2" "chronos_2"
  download_model "environments/mantis" "mantis"
}

# Readiness ------------------------------------------------------------------

verify_installation() {
  require_command python3
  python3 -B "$REPOSITORY_ROOT/scripts/setup_support.py" verify --root "$REPOSITORY_ROOT"
}

install_all() {
  export HF_HOME="$REPOSITORY_ROOT/.cache/huggingface"
  export HF_HUB_CACHE="$HF_HOME"
  create_required_directories
  ensure_uv
  require_command git
  retry_command "Initialize pinned Git submodules" initialize_submodules
  sync_python_environments
  retry_command "Restore locked R environment" restore_r_environment
  acquire_assets
  verify_installation
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
  install_all
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
