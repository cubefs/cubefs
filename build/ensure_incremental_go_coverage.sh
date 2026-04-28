#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  bash build/ensure_incremental_go_coverage.sh [options]

Options:
  --base <commit>         Compare <commit> to the current working tree
  --threshold <percent>   Minimum acceptable incremental coverage (default: 80)
  --coverprofile <path>   Coverprofile path relative to repo root or absolute path (default: coverage.txt)
  --cover-cmd "<cmd>"     Coverage generation command run from repo root (default: bash build/build.sh testcover)
  --log-file <path>       Log file for the coverage generation command (default: /tmp/testcover.log)
  --skip-cover            Reuse the existing coverprofile instead of regenerating it
  -h, --help              Show this help text
EOF
}

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checker="${script_dir}/check_incremental_go_coverage.py"

base=""
threshold="80"
coverprofile="coverage.txt"
cover_cmd="bash build/build.sh testcover"
log_file="/tmp/testcover.log"
skip_cover="0"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base)
            base="${2:-}"
            shift 2
            ;;
        --threshold)
            threshold="${2:-}"
            shift 2
            ;;
        --coverprofile)
            coverprofile="${2:-}"
            shift 2
            ;;
        --cover-cmd)
            cover_cmd="${2:-}"
            shift 2
            ;;
        --log-file)
            log_file="${2:-}"
            shift 2
            ;;
        --skip-cover)
            skip_cover="1"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ! -f "${checker}" ]]; then
    echo "Coverage checker not found: ${checker}" >&2
    exit 2
fi

if [[ "${skip_cover}" != "1" ]]; then
    mkdir -p "$(dirname "${log_file}")"
    echo "Generating Go coverprofile from repository root: ${repo_root}"
    echo "Coverage command: ${cover_cmd}"
    echo "Coverage log: ${log_file}"
    if ! (
        cd "${repo_root}"
        eval "${cover_cmd}"
    ) >"${log_file}" 2>&1; then
        echo "Coverage command failed. See ${log_file}" >&2
        exit 1
    fi
fi

args=(
    python3
    "${checker}"
    --repo "${repo_root}"
    --coverprofile "${coverprofile}"
    --threshold "${threshold}"
)

if [[ -n "${base}" ]]; then
    args+=(--base "${base}")
fi

echo "Checking incremental Go coverage..."
PYTHONDONTWRITEBYTECODE=1 "${args[@]}"
