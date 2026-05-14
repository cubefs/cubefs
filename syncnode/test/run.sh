#!/usr/bin/env bash
# Main runner. Dispatches to a tier of test scripts and aggregates
# pass/fail. Each test script is a standalone executable that exits 0
# on pass; we run them in sub-shells so a failure in one doesn't
# poison the next.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
# Defer env / tool checks until we know we need them — lets `run.sh -h`
# and `run.sh cleanup` work without a full env.
SKIP_AUTOCHECK=1
# shellcheck source=lib/common.sh
source "$ROOT/lib/common.sh"

usage() {
  cat <<EOF
Usage: $0 <tier> [test-name-glob]

Tiers:
  smoke        Health + register + 1 rule. <2 min. Run on every deploy.
  functional   Full single-node feature coverage. ~30 min.
  integration  Multi-node P1 + security + perf baseline. ~1 hour.
  chaos        Long-running stability. 6+ hours. Manual review.
  all          smoke + functional + integration.
  cleanup      Wipe every it-* rule + local fixture residue.

Examples:
  $0 smoke
  $0 functional 20_sync          # only run tests whose name matches *20_sync*
  RUN_VERBOSE=1 $0 integration 41_failover
  $0 all

Env vars: see README.md or env.example.sh.
EOF
}

_run_dir() {
  local dir="$1" glob="${2:-}"
  if [ ! -d "$ROOT/$dir" ]; then
    log_err "no such tier: $dir"; return 1
  fi
  local passed=0 failed=0 skipped=0
  local failed_names=()
  shopt -s nullglob
  for f in "$ROOT/$dir"/*.sh; do
    local name; name=$(basename "$f" .sh)
    if [ -n "$glob" ] && [[ "$name" != *"$glob"* ]]; then
      skipped=$((skipped+1))
      continue
    fi
    if [ ! -x "$f" ]; then chmod +x "$f"; fi
    log_info "▶ $dir/$name"
    local rc=0
    if [ "${TEST_TIMEOUT:-0}" -gt 0 ]; then
      timeout "$TEST_TIMEOUT" bash "$f" || rc=$?
    else
      bash "$f" || rc=$?
    fi
    if [ "$rc" -eq 0 ]; then
      passed=$((passed+1))
    else
      failed=$((failed+1))
      failed_names+=("$dir/$name (exit=$rc)")
    fi
  done
  shopt -u nullglob
  echo
  log_info "── $dir summary ────────────────────────────────────"
  log_info "passed:  $passed"
  log_info "failed:  $failed"
  log_info "skipped: $skipped (glob='$glob')"
  if [ "$failed" -gt 0 ]; then
    log_err "failed tests:"
    for n in "${failed_names[@]}"; do log_err "  - $n"; done
    return 1
  fi
  log_ok "$dir tier complete"
}

main() {
  local tier="${1:-}"
  local glob="${2:-}"
  case "$tier" in
    smoke|functional|integration|chaos|all)
      # Now we DO need env + tools. Subordinate scripts will re-check
      # them when sourced (without SKIP_AUTOCHECK).
      unset SKIP_AUTOCHECK
      require_env_base
      require_tools
      case "$tier" in
        all)
          _run_dir smoke "$glob" && \
          _run_dir functional "$glob" && \
          _run_dir integration "$glob" ;;
        *)
          _run_dir "$tier" "$glob" ;;
      esac
      ;;
    cleanup)
      unset SKIP_AUTOCHECK
      require_env_base
      require_tools
      # shellcheck source=lib/cleanup.sh
      source "$ROOT/lib/cleanup.sh"
      cleanup_all ;;
    -h|--help|"")
      usage; exit 0 ;;
    *)
      usage; exit 2 ;;
  esac
}

main "$@"
