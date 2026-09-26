#!/usr/bin/env bash

# Start, inspect, stop, or run the Mac-coordinated two-machine ShapeFM cluster.

set -Eeuo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly UBUNTU_HOST="${SHAPEFM_UBUNTU_HOST:-rafmontano@WSUbuntu1.local}"
readonly UBUNTU_ROOT="${SHAPEFM_UBUNTU_ROOT:-/home/rafmontano/Documents/PhD/2026/projects/shape_fm_poc}"
readonly MAC_HOST="${SHAPEFM_MAC_HOST:-RMMacbookPro.local}"
readonly MAC_BIND_HOST="${SHAPEFM_MAC_BIND_HOST:-$(ipconfig getifaddr en0 2>/dev/null || true)}"
readonly SCHEDULER_ADDRESS="${SHAPEFM_DASK_ADDRESS:-tcp://127.0.0.1:8786}"
readonly WORKER_SCHEDULER_ADDRESS="${SHAPEFM_DASK_WORKER_ADDRESS:-tcp://$MAC_HOST:8786}"
readonly DASHBOARD_ADDRESS="http://127.0.0.1:8787/status"
readonly MAC_CPU_WORKERS="${SHAPEFM_MAC_CPU_WORKERS:-2}"
readonly UBUNTU_CPU_WORKERS="${SHAPEFM_UBUNTU_CPU_WORKERS:-4}"
readonly CHRONOS_BATCH_SIZE="${SHAPEFM_CHRONOS_BATCH_SIZE:-16}"
readonly MAX_IN_FLIGHT="${SHAPEFM_DASK_MAX_IN_FLIGHT:-12}"
readonly EXPECTED_WORKERS="$((MAC_CPU_WORKERS + UBUNTU_CPU_WORKERS + 1))"
readonly RUNTIME_DIR="$ROOT/data/dask"
readonly DATABASE="${SHAPEFM_DATABASE:-$ROOT/data/shapefm.duckdb}"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_two_machine.sh [--scope smoke|m4_daily] [--resume]
  scripts/run_two_machine.sh start
  scripts/run_two_machine.sh status
  scripts/run_two_machine.sh stop
  scripts/run_two_machine.sh calibrate-dask

The run form starts the cluster, plans idempotently, executes Gates 2-6, prints
the execution report, and stops the cluster. The dashboard is available at
http://127.0.0.1:8787/status while the cluster is running.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

ssh_ubuntu() {
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$UBUNTU_HOST" "$@"
}

verify_and_restore() {
  cd "$ROOT"
  local commit remote_commit ubuntu_commit
  [[ -z "$(git status --porcelain --untracked-files=all)" ]] || \
    die "Mac worktree must be clean before starting distributed workers."
  [[ "$(git branch --show-current)" == "poc1-dask-distributed" ]] || \
    die "Mac must be on poc1-dask-distributed."
  commit="$(git rev-parse HEAD)"
  remote_commit="$(git ls-remote origin refs/heads/poc1-dask-distributed | awk '{print $1}')"
  [[ "$remote_commit" == "$commit" ]] || \
    die "GitHub poc1-dask-distributed does not match Mac HEAD $commit."

  ssh_ubuntu "set -eu
    cd '$UBUNTU_ROOT'
    test -z \"\$(git status --porcelain --untracked-files=all)\"
    git fetch origin poc1-dask-distributed
    git switch poc1-dask-distributed
    git merge --ff-only origin/poc1-dask-distributed
    test -z \"\$(git status --porcelain --untracked-files=all)\""
  ubuntu_commit="$(ssh_ubuntu "cd '$UBUNTU_ROOT' && git rev-parse HEAD")"
  [[ "$ubuntu_commit" == "$commit" ]] || \
    die "Ubuntu is at $ubuntu_commit; expected $commit."

  [[ "$(git -C external/gift-eval rev-parse HEAD)" == \
    "4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a" ]] || \
    die "Mac GIFT-Eval revision is wrong."
  [[ "$(ssh_ubuntu "git -C '$UBUNTU_ROOT/external/gift-eval' rev-parse HEAD")" == \
    "4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a" ]] || \
    die "Ubuntu GIFT-Eval revision is wrong."

  .tools/uv/uv sync --locked
  ssh_ubuntu "cd '$UBUNTU_ROOT' && .tools/uv/uv sync --locked"
  printf 'Verified commit %s on Mac, Ubuntu, and GitHub.\n' "$commit"
}

start_local_process() {
  local pid_file=$1 log_file=$2
  shift 2
  if [[ -s "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    die "Process already running with PID $(cat "$pid_file")."
  fi
  nohup "$@" >"$log_file" 2>&1 </dev/null &
  echo "$!" >"$pid_file"
}

start_cluster() {
  trap stop_cluster ERR
  [[ -n "$MAC_BIND_HOST" ]] || die "Set SHAPEFM_MAC_BIND_HOST to the Mac LAN IPv4 address."
  verify_and_restore
  mkdir -p "$RUNTIME_DIR"
  export DASK_DISTRIBUTED__SCHEDULER__NO_WORKERS_TIMEOUT="120s"
  start_local_process "$RUNTIME_DIR/scheduler.pid" "$RUNTIME_DIR/scheduler.log" \
    "$ROOT/.tools/uv/uv" run --locked dask scheduler \
      --host 0.0.0.0 --port 8786 --dashboard-address 127.0.0.1:8787
  sleep 2
  start_local_process "$RUNTIME_DIR/mac-cpu.pid" "$RUNTIME_DIR/mac-cpu.log" \
    "$ROOT/.tools/uv/uv" run --locked dask worker "$WORKER_SCHEDULER_ADDRESS" \
      --nworkers "$MAC_CPU_WORKERS" --nthreads 1 --name mac-cpu \
      --host "$MAC_BIND_HOST" --resources CPU=1 --memory-limit 5GiB --no-dashboard

  ssh_ubuntu "set -eu
    cd '$UBUNTU_ROOT'
    mkdir -p data/dask
    nohup .tools/uv/uv run --locked dask worker '$WORKER_SCHEDULER_ADDRESS' \
      --nworkers '$UBUNTU_CPU_WORKERS' --nthreads 1 --name ubuntu-cpu \
      --resources CPU=1 --memory-limit 20GiB --no-dashboard \
      >data/dask/ubuntu-cpu.log 2>&1 </dev/null &
    echo \$! >data/dask/ubuntu-cpu.pid
    nohup env CUDA_VISIBLE_DEVICES=0 .tools/uv/uv run --locked dask worker '$WORKER_SCHEDULER_ADDRESS' \
      --nworkers 1 --nthreads 1 --name ubuntu-gpu \
      --resources GPU=1 --memory-limit 24GiB --no-dashboard \
      >data/dask/ubuntu-gpu.log 2>&1 </dev/null &
    echo \$! >data/dask/ubuntu-gpu.pid"

  .tools/uv/uv run --locked shapefm-cluster preflight \
    --address "$SCHEDULER_ADDRESS" --timeout 180 \
    --expected-workers "$EXPECTED_WORKERS" --require-gpu
  trap - ERR
  printf 'Dask dashboard: %s\n' "$DASHBOARD_ADDRESS"
}

cluster_status() {
  cd "$ROOT"
  .tools/uv/uv run --locked shapefm-cluster status \
    --address "$SCHEDULER_ADDRESS" --timeout 10
  printf 'Dask dashboard: %s\n' "$DASHBOARD_ADDRESS"
}

stop_cluster() {
  cd "$ROOT"
  .tools/uv/uv run --locked shapefm-cluster shutdown \
    --address "$SCHEDULER_ADDRESS" --timeout 10 2>/dev/null || true
  for pid_file in "$RUNTIME_DIR/mac-cpu.pid" "$RUNTIME_DIR/scheduler.pid"; do
    if [[ -s "$pid_file" ]]; then
      pid="$(cat "$pid_file")"
      kill "$pid" 2>/dev/null || true
      rm -f "$pid_file"
    fi
  done
  ssh_ubuntu "cd '$UBUNTU_ROOT'; for file in data/dask/ubuntu-cpu.pid data/dask/ubuntu-gpu.pid; do if test -s \"\$file\"; then kill \"\$(cat \"\$file\")\" 2>/dev/null || true; rm -f \"\$file\"; fi; done" || true
  printf 'Dask cluster stopped.\n'
}

print_report() {
  cd "$ROOT"
  .tools/uv/uv run --locked shapefm-poc1 --database "$DATABASE" status
  .tools/uv/uv run --locked shapefm-poc1 --database "$DATABASE" results
  .tools/uv/uv run --locked python - "$DATABASE" <<'PY'
import collections, hashlib, json, sys
import duckdb

connection = duckdb.connect(sys.argv[1], read_only=True)
experiment = connection.execute(
    "SELECT experiment_id FROM experiments ORDER BY updated_at DESC LIMIT 1"
).fetchone()[0]
attempts = connection.execute(
    """SELECT a.status, a.runtime_seconds, CAST(a.resource_usage AS VARCHAR), a.error
       FROM experiment_task_attempts a JOIN experiment_tasks t USING (task_id)
       WHERE t.experiment_id=?""",
    [experiment],
).fetchall()
hosts = collections.Counter()
failures = []
runtime = 0.0
for status, seconds, encoded, error in attempts:
    metadata = json.loads(encoded or "{}")
    if status == "completed":
        hosts[metadata.get("hostname", "Mac coordinator/local")] += 1
        runtime += seconds or 0.0
    elif error:
        failures.append(error)
fingerprints = [
    row[0]
    for row in connection.execute(
        "SELECT content_hash FROM forecasts WHERE experiment_id=? ORDER BY forecast_id",
        [experiment],
    ).fetchall()
]
report = {
    "experiment_id": experiment,
    "task_contribution_by_host": dict(sorted(hosts.items())),
    "attempt_runtime_seconds": runtime,
    "failure_count": len(failures),
    "failures": failures,
    "forecast_count": len(fingerprints),
    "scientific_fingerprint": hashlib.sha256("\n".join(fingerprints).encode()).hexdigest(),
}
print(json.dumps(report, sort_keys=True))
PY
}

run_experiment() {
  local scope=$1 resume=$2
  start_cluster
  trap stop_cluster EXIT INT TERM
  cd "$ROOT"
  if [[ "$resume" != true ]]; then
    .tools/uv/uv run --locked shapefm-poc1 --database "$DATABASE" \
      plan --scope "$scope"
  else
    # Planning is deterministic and idempotent, and safely fills missing rows.
    .tools/uv/uv run --locked shapefm-poc1 --database "$DATABASE" \
      plan --scope "$scope"
  fi
  .tools/uv/uv run --locked shapefm-poc1 --database "$DATABASE" run \
    --profile two_machine_dask --execution dask \
    --chronos-batch-size "$CHRONOS_BATCH_SIZE" \
    --dask-address "$SCHEDULER_ADDRESS" \
    --dask-timeout 180 --dask-expected-workers "$EXPECTED_WORKERS" \
    --dask-max-in-flight "$MAX_IN_FLIGHT" --dask-retries 2
  print_report
}

calibrate_dask() {
  local scratch="$ROOT/.amp/in/dask-calibration"
  local report="$ROOT/docs/calibration/two_machine_dask_calibration.json"
  mkdir -p "$scratch" "$(dirname "$report")"
  rm -f "$scratch"/*.json
  local names=(baseline moderate recommended_candidate aggressive_candidate)
  local mac_workers=(2 2 4 4)
  local ubuntu_workers=(4 8 12 16)
  local chronos_batches=(16 32 64 64)
  local in_flight=(12 16 24 32)
  local baseline_file="$scratch/baseline.json"
  local completed=()
  for index in 0 1 2 3; do
    local name=${names[$index]}
    local mac=${mac_workers[$index]}
    local ubuntu=${ubuntu_workers[$index]}
    local chronos=${chronos_batches[$index]}
    local maximum=${in_flight[$index]}
    local expected=$((mac + ubuntu + 1))
    local candidate="$scratch/$name.json"
    printf 'Calibrating %s: Mac=%s Ubuntu=%s Chronos=%s in-flight=%s\n' \
      "$name" "$mac" "$ubuntu" "$chronos" "$maximum"
    SHAPEFM_MAC_CPU_WORKERS="$mac" SHAPEFM_UBUNTU_CPU_WORKERS="$ubuntu" \
      "$0" start
    local baseline_arguments=()
    if [[ "$index" -gt 0 ]]; then
      baseline_arguments=(--baseline "$baseline_file")
    fi
    local command_status=0
    .tools/uv/uv run --locked shapefm-poc1 --database "$DATABASE" \
      calibrate-dask --address "$SCHEDULER_ADDRESS" \
      --expected-workers "$expected" --profile-name "$name" \
      --mac-cpu-workers "$mac" --ubuntu-cpu-workers "$ubuntu" \
      --chronos-batch-size "$chronos" --max-in-flight "$maximum" \
      --output "$candidate" "${baseline_arguments[@]}" || command_status=$?
    SHAPEFM_MAC_CPU_WORKERS="$mac" SHAPEFM_UBUNTU_CPU_WORKERS="$ubuntu" \
      "$0" stop
    [[ "$command_status" -eq 0 ]] || return "$command_status"
    completed+=("$candidate")
    if [[ "$(.tools/uv/uv run --locked python -c \
      'import json,sys; print(str(json.load(open(sys.argv[1]))["safe"]).lower())' \
      "$candidate")" != true ]]; then
      printf 'Stopping calibration after unsafe profile %s.\n' "$name"
      break
    fi
  done
  .tools/uv/uv run --locked python - "$report" "${completed[@]}" <<'PY'
import datetime
import json
import pathlib
import sys

output = pathlib.Path(sys.argv[1])
candidates = []
for path in sys.argv[2:]:
    value = json.loads(pathlib.Path(path).read_text())
    value.pop("_scientific_outputs", None)
    candidates.append(value)
safe = [candidate for candidate in candidates if candidate["safe"]]
if not safe:
    selected = None
    rationale = "No candidate satisfied every safety and scientific-equivalence gate."
else:
    selected = safe[0]
    decisions = []
    for candidate in safe[1:]:
        previous = selected["total_throughput_tasks_per_second"]
        current = candidate["total_throughput_tasks_per_second"]
        improvement = (current / previous - 1.0) if previous else 0.0
        if current > previous and improvement >= 0.05:
            decisions.append(
                f"{candidate['profile_name']} replaced {selected['profile_name']} "
                f"with {improvement:.1%} higher throughput"
            )
            selected = candidate
        else:
            decisions.append(
                f"{candidate['profile_name']} was not selected: throughput improvement "
                f"over {selected['profile_name']} was {improvement:.1%}"
            )
    rationale = "; ".join(decisions) or "The baseline was the only safe candidate tested."
report = {
    "generated_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
    "method": "256 deterministic M4 Daily contexts; all cleaning/transformation variants; concurrent AutoARIMA and Chronos-2",
    "safety_thresholds": {
        "mac_minimum_available_memory_gib": 3,
        "ubuntu_minimum_available_memory_gib": 16,
        "rtx5090_minimum_available_vram_gib": 4,
        "persistent_dask_spilling_allowed": False,
        "worker_restarts_allowed": False,
        "scientific_rtol": 1e-5,
        "scientific_atol": 1e-5,
        "minimum_throughput_improvement_for_larger_profile": 0.05,
    },
    "candidates": candidates,
    "recommendation": {
        "selected_profile": selected["profile_name"] if selected else None,
        "settings": selected["settings"] if selected else None,
        "rationale": rationale,
    },
}
output.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["recommendation"], indent=2))
PY
}

main() {
  cd "$ROOT"
  case "${1:-}" in
    start) start_cluster; return ;;
    status) cluster_status; return ;;
    stop) stop_cluster; return ;;
    calibrate-dask) calibrate_dask; return ;;
    -h|--help) usage; return ;;
  esac
  local scope=smoke resume=false
  while (($#)); do
    case "$1" in
      --scope) scope=${2:?missing scope}; shift 2 ;;
      --resume) resume=true; shift ;;
      *) usage; die "Unknown option: $1" ;;
    esac
  done
  [[ "$scope" == smoke || "$scope" == m4_daily ]] || die "Scope must be smoke or m4_daily."
  run_experiment "$scope" "$resume"
}

main "$@"
