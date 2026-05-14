# shellcheck shell=bash
# Master sync API helpers — P2-7 cutover.
#
# Wraps the master /syncRule/* + /syncTask/* + /syncNode/* endpoints
# so test scripts don't have to know URL paths. Every helper goes
# through the existing master_get / master_post in lib/http.sh, which
# already handles auth + retry + envelope decoding.
#
# Convention: each function prints the response body on stdout AND
# exports HTTP_STATUS + HTTP_BODY for the caller (same shape as the
# syncnode helpers in http.sh).

# ─────────────────── rules ───────────────────

# master_rule_list [state] → JSON envelope; data is []SyncRule.
master_rule_list() {
  local q="/syncRule/list"
  if [ -n "${1:-}" ]; then q="$q?state=$1"; fi
  master_get "$q"
}

# master_rule_get <id>
master_rule_get() { master_get "/syncRule/get?id=$1"; }

# master_rule_create <json-body>
master_rule_create() { master_post "/syncRule/create" "$1"; }

# master_rule_update <json-body>
master_rule_update() { master_post "/syncRule/update" "$1"; }

# master_rule_delete <id>
master_rule_delete() { master_post "/syncRule/delete?id=$1" ""; }

# master_rule_pause <id>
master_rule_pause() { master_post "/syncRule/pause?id=$1" ""; }

# master_rule_resume <id>
master_rule_resume() { master_post "/syncRule/resume?id=$1" ""; }

# master_rule_trigger <id> → synchronous fire, returns {ruleID, taskID}.
master_rule_trigger() { master_post "/syncRule/trigger?id=$1" ""; }

# ─────────────────── tasks ───────────────────

# master_task_list [status] [ruleID] [owner]
master_task_list() {
  local q="/syncTask/list"
  local sep="?"
  for kv in "status=$1" "ruleID=$2" "owner=$3"; do
    local v="${kv#*=}"
    if [ -n "$v" ]; then q="${q}${sep}${kv}"; sep="&"; fi
  done
  master_get "$q"
}

# master_task_get <taskID>
master_task_get() { master_get "/syncTask/get?id=$1"; }

# master_task_cancel <taskID>
master_task_cancel() { master_post "/syncTask/cancel?id=$1" ""; }

# master_task_retry <taskID>
master_task_retry() { master_post "/syncTask/retry?id=$1" ""; }

# master_task_export [since=RFC3339] → NDJSON stream (not enveloped).
master_task_export() {
  local q="/syncTask/export"
  if [ -n "${1:-}" ]; then q="$q?since=$1"; fi
  master_get "$q"
}

# ─────────────────── nodes ───────────────────

# master_node_list
master_node_list() { master_get "/syncNode/list"; }

# master_node_decommission <addr> [force=true|false]
master_node_decommission() {
  local addr="$1" force="${2:-false}"
  master_post "/syncNode/decommission?addr=${addr}&force=${force}" ""
}

# master_node_drain <addr>
master_node_drain() { master_post "/syncNode/drain?addr=$1" ""; }

# master_node_restore <addr>
master_node_restore() { master_post "/syncNode/restore?addr=$1" ""; }

# master_node_tasks <addr> [status]
master_node_tasks() {
  local q="/syncNode/tasks?addr=$1"
  if [ -n "${2:-}" ]; then q="${q}&status=$2"; fi
  master_get "$q"
}

# ─────────────────── helpers ───────────────────

# wait_for_task_terminal <taskID> [timeout_seconds=120] [poll_seconds=2]
#   Polls master_task_get until status is succeeded/failed/cancelled
#   or the timeout fires. Exports TASK_RECORD with the final body.
wait_for_task_terminal() {
  local id="$1" timeout="${2:-120}" poll="${3:-2}"
  local deadline status body
  deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    body=$(master_task_get "$id")
    status=$(echo "$body" | jq -r '.data.status // "missing"')
    case "$status" in
      succeeded|failed|cancelled)
        export TASK_RECORD="$body"
        return 0 ;;
    esac
    sleep "$poll"
  done
  log_err "wait_for_task_terminal timed out after ${timeout}s; last status=$status"
  return 1
}

# wait_for_rule_visible <ruleID> [timeout_seconds=10] [poll_seconds=1]
#   Polls master_rule_get until the rule appears (post raft commit).
wait_for_rule_visible() {
  local id="$1" timeout="${2:-10}" poll="${3:-1}"
  local deadline body code
  deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    body=$(master_rule_get "$id" 2>/dev/null || true)
    code=$(echo "$body" | jq -r '.code // 999')
    if [ "$code" = "0" ]; then return 0; fi
    sleep "$poll"
  done
  log_err "wait_for_rule_visible timed out after ${timeout}s; last body=$body"
  return 1
}
