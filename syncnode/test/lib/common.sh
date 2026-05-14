# shellcheck shell=bash
# Common helpers sourced by every test script. Sets strict mode + paths,
# loads sibling lib files, and exposes a small assertion DSL.

set -euo pipefail

# Locate the suite root so tests can run from any cwd.
_TEST_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TEST_ROOT="$(dirname "$_TEST_LIB_DIR")"
export TEST_LIB_DIR="$_TEST_LIB_DIR"
export FIXTURES_DIR="$TEST_ROOT/fixtures"

# Subordinate libs.
# shellcheck source=log.sh
source "$_TEST_LIB_DIR/log.sh"
# shellcheck source=http.sh
source "$_TEST_LIB_DIR/http.sh"
# shellcheck source=master.sh
source "$_TEST_LIB_DIR/master.sh"
# shellcheck source=assert.sh
source "$_TEST_LIB_DIR/assert.sh"
# shellcheck source=fixtures.sh
source "$_TEST_LIB_DIR/fixtures.sh"

# Verify required env vars exist. Tests in the deep tiers may require
# extras; they call require_env_for "<feature>" themselves.
require_env_base() {
  local missing=()
  for v in SYNCNODE_HOST SYNCNODE_HTTP_PORT MASTER_HTTP TEST_DATA_DIR ALLOWED_ROOT; do
    if [ -z "${!v:-}" ]; then missing+=("$v"); fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    log_err "missing env vars: ${missing[*]}"
    log_err "source env.sh first (copy from env.example.sh)"
    exit 2
  fi
  sn_mkdir "$TEST_DATA_DIR"
}

require_env_for() {
  local feature="$1"
  case "$feature" in
    s3)
      for v in S3_ENDPOINT S3_BUCKET AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY; do
        if [ -z "${!v:-}" ]; then
          log_err "feature '$feature' needs env var $v"; exit 2
        fi
      done ;;
    master)
      [ -n "${MASTER_HTTP:-}" ] || { log_err "feature '$feature' needs MASTER_HTTP"; exit 2; } ;;
  esac
}

require_tools() {
  local missing=()
  for t in curl jq dd bc; do
    if ! command -v "$t" >/dev/null 2>&1; then missing+=("$t"); fi
  done
  # md5 vs md5sum: tolerate either; pick the right one for the OS.
  if command -v md5sum >/dev/null 2>&1; then
    export MD5_BIN="md5sum"
  elif command -v md5 >/dev/null 2>&1; then
    # macOS md5 -q prints the hash bare.
    export MD5_BIN="md5 -q"
  else
    missing+=("md5sum or md5")
  fi
  if [ "${#missing[@]}" -gt 0 ]; then
    log_err "missing tools: ${missing[*]}"
    exit 2
  fi
}

# unique_id <prefix> — used for test-scoped rule + task IDs so concurrent
# runs / re-runs don't collide. Format: it-<prefix>-<pid>-<unix-ns>.
unique_id() {
  echo "it-$1-$$-$(date +%s%N | tail -c 7)"
}

# ---------------------------------------------------------------------------
# Remote file-op helpers.
#
# When SYNCNODE_POD is non-empty, file operations are routed through
# kubectl exec on that pod. This is necessary when the test runner
# (laptop) and the syncnode pod do not share a filesystem (e.g. a
# remote k3d cluster). Set SYNCNODE_POD="auto" to auto-discover the
# pod; otherwise set it to the exact pod name.
# ---------------------------------------------------------------------------

# _sn_pod — resolve the pod name, caching in _SN_POD_CACHE.
_SN_POD_CACHE=""
_sn_pod() {
  if [ -z "$_SN_POD_CACHE" ]; then
    if [ "${SYNCNODE_POD:-}" = "auto" ]; then
      _SN_POD_CACHE=$(kubectl get pod \
        -n "${SYNCNODE_NAMESPACE:-storage-cfs}" \
        -l app=cubefs-syncnode \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
      [ -n "$_SN_POD_CACHE" ] || { log_err "could not find syncnode pod"; exit 2; }
    else
      _SN_POD_CACHE="${SYNCNODE_POD}"
    fi
  fi
  echo "$_SN_POD_CACHE"
}

# _sn_exec <cmd...> — run a command inside the syncnode pod (or locally).
_sn_exec() {
  if [ -n "${SYNCNODE_POD:-}" ]; then
    kubectl exec "$(_sn_pod)" \
      -n "${SYNCNODE_NAMESPACE:-storage-cfs}" -- "$@"
  else
    "$@"
  fi
}

# sn_mkdir <path> — create directory (locally or in pod).
sn_mkdir() { _sn_exec mkdir -p "$@"; }

# sn_rm <flags...> <path...> — remove files/dirs (locally or in pod).
sn_rm() { _sn_exec rm "$@"; }

# sn_write_line <content> <path> — write a single line to a file.
sn_write_line() {
  local content="$1" path="$2"
  if [ -n "${SYNCNODE_POD:-}" ]; then
    kubectl exec "$(_sn_pod)" \
      -n "${SYNCNODE_NAMESPACE:-storage-cfs}" \
      -- sh -c "printf '%s\n' \"\$1\" > \"\$2\"" -- "$content" "$path"
  else
    printf '%s\n' "$content" > "$path"
  fi
}

# random_payload <path> <megabytes> — generate a random file.
# When SYNCNODE_POD is set, the file is created inside the pod.
random_payload() {
  local out="$1" sz="$2"
  _sn_exec dd if=/dev/urandom of="$out" bs=1M count="$sz" status=none
}

# md5_of <path> — MD5 of a file (locally or in pod).
md5_of() {
  if [ -n "${SYNCNODE_POD:-}" ]; then
    kubectl exec "$(_sn_pod)" \
      -n "${SYNCNODE_NAMESPACE:-storage-cfs}" \
      -- md5sum "$1" | awk '{print $1}'
  else
    $MD5_BIN < "$1" | awk '{print $1}'
  fi
}

# wait_for <pred-fn> <timeout-sec> <description>
# Polls pred-fn (must echo 'yes' / 'no') until it says yes or timeout
# fires. Returns 0 on success, 1 on timeout.
wait_for() {
  local pred="$1" timeout="$2" desc="$3"
  local start=$SECONDS
  while [ $((SECONDS - start)) -lt "$timeout" ]; do
    if [ "$($pred)" = "yes" ]; then
      log_ok "waited $((SECONDS - start))s for: $desc"
      return 0
    fi
    sleep 1
  done
  log_err "timeout (${timeout}s) waiting for: $desc"
  return 1
}

# trap_cleanup <fn> — register a cleanup func to run on EXIT. Combine
# multiple cleanups via the list-of-fns pattern.
declare -a CLEANUP_FNS=()
trap_cleanup() {
  CLEANUP_FNS+=("$1")
}
_run_cleanups() {
  local rc=$?
  # `set -u` makes ${arr[@]} on an empty array a fatal error in
  # older bash; guard with length check.
  if [ "${#CLEANUP_FNS[@]}" -gt 0 ]; then
    for fn in "${CLEANUP_FNS[@]}"; do
      "$fn" || log_warn "cleanup $fn failed (continuing)"
    done
  fi
  return $rc
}
trap _run_cleanups EXIT

# test_header / test_pass / test_fail — print banners. Each test script
# starts with test_header and ends with test_pass on success.
test_header() {
  log_info "──────────────────────────────────────────────────"
  log_info "  TEST: $1"
  log_info "──────────────────────────────────────────────────"
}
test_pass() { log_ok "PASS — $1"; }
test_fail() { log_err "FAIL — $1"; exit 1; }

# Default initialisation — every sourced script that drives real
# requests calls these. run.sh defers them (so `run.sh -h` works
# without env). Tests that source this file get the checks via the
# init() call right below — controlled by the SKIP_AUTOCHECK guard
# so run.sh can opt out.
if [ "${SKIP_AUTOCHECK:-0}" != "1" ]; then
  require_env_base
  require_tools
fi
