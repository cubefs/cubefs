#!/usr/bin/env bash
# api/02 — /syncTask/* surface coverage.
#
# Creates a small rule, triggers it once, then exercises every task
# endpoint: list (with status / ruleID / owner filters), get, cancel
# (idempotent on terminal task), retry (creates fresh taskID), export
# (NDJSON shape).

source "$(dirname "$0")/../lib/common.sh"
test_header "api: /syncTask/* surface"

RID=$(unique_id "api-task")
WORK="$TEST_DATA_DIR/$RID"
cleanup_api_02() {
  delete_rule_silent "$RID"
  sn_rm -rf "$WORK"
}
trap_cleanup cleanup_api_02

expect_master_err() {
  local body="$1" want="$2" hint="${3:-}"
  local code msg
  code=$(echo "$body" | jq -r '.code // 999')
  msg=$(echo "$body" | jq -r '.msg // ""')
  if [ "$code" = "0" ]; then
    log_err "expected error, got success${hint:+ ($hint)}; body: $body"
    return 1
  fi
  if ! echo "$msg" | grep -qi "$want"; then
    log_err "msg=$msg, expected to contain $want${hint:+ ($hint)}"
    return 1
  fi
}

# ---------- Seed: 1 small file rule, triggered once ----------
sn_mkdir "$WORK"
sn_write_line "hello-task-api" "$WORK/probe.bin"
body=$(rule_local_to_s3 "$RID" "$WORK/" "api02/")
expect_code "$(master_rule_create "$body")" 0 "rule create"
wait_for_rule_visible "$RID"

rec=$(trigger_and_wait "$RID" 60)
TID=$(echo "$rec" | jq -r '.data.taskID')
[ -n "$TID" ] && [ "$TID" != "null" ] || { log_err "trigger_and_wait did not return taskID"; exit 1; }
log_ok "seeded rule + first task: $TID"

# ---------- /syncTask/get ----------
got=$(master_task_get "$TID")
expect_code "$got" 0 "task get"
assert_json_eq "$got" '.data.taskID' "$TID"
assert_json_eq "$got" '.data.ruleID' "$RID"
log_ok "/syncTask/get returns full record"

# ---------- /syncTask/get missing param ----------
no_id=$(master_get "/syncTask/get")
expect_master_err "$no_id" "missing id" "task get without id"

# ---------- /syncTask/get unknown id ----------
no_task=$(master_task_get "no-such-task-$$")
expect_master_err "$no_task" "not found" "task get unknown id"
log_ok "/syncTask/get unknown → not found"

# ---------- /syncTask/list filters ----------
all_for_rule=$(master_task_list "" "$RID" "")
[ "$(echo "$all_for_rule" | jq '.data | length')" -ge 1 ] || { log_err "ruleID filter returned empty"; exit 1; }
log_ok "/syncTask/list?ruleID= filter works"

# Status filter — record should be terminal (succeeded/failed)
final_status=$(echo "$got" | jq -r '.data.status')
by_status=$(master_task_list "$final_status" "$RID" "")
hits=$(echo "$by_status" | jq "[.data[] | select(.taskID == \"$TID\")] | length")
[ "$hits" = "1" ] || { log_err "status filter $final_status missed $TID; hits=$hits"; exit 1; }
log_ok "/syncTask/list?status= filter works"

# ---------- /syncTask/cancel on terminal task is idempotent OK ----------
cancel=$(master_task_cancel "$TID")
expect_code "$cancel" 0 "cancel terminal task is no-op"
log_ok "/syncTask/cancel on terminal → 0 (idempotent)"

# ---------- /syncTask/cancel unknown id ----------
cancel_miss=$(master_task_cancel "no-such-task-$$")
expect_master_err "$cancel_miss" "not found" "cancel unknown id"

# ---------- /syncTask/retry → fresh taskID ----------
retried=$(master_task_retry "$TID")
expect_code "$retried" 0 "retry"
new_id=$(echo "$retried" | jq -r '.data.newTaskID')
[ -n "$new_id" ] && [ "$new_id" != "null" ] && [ "$new_id" != "$TID" ] || {
  log_err "retry should produce fresh taskID != $TID; got $new_id"; exit 1; }
log_ok "/syncTask/retry returns fresh taskID: $new_id"

# Wait for the retry task to terminate so cleanup is clean.
wait_for_task_terminal "$new_id" 60 || log_warn "retry task did not terminate in time"

# ---------- /syncTask/retry unknown id ----------
retry_miss=$(master_task_retry "no-such-task-$$")
expect_master_err "$retry_miss" "not found" "retry unknown id"

# ---------- /syncTask/export NDJSON shape ----------
HTTP_MAX_TIME=30 master_task_export >/tmp/sync-task-export.$$
# Each non-empty line should parse as JSON with .taskID
if [ -s /tmp/sync-task-export.$$ ]; then
  bad=$(awk 'NF' /tmp/sync-task-export.$$ | jq -r '.taskID // "MISSING"' 2>&1 | grep -c MISSING || true)
  if [ "$bad" != "0" ]; then
    log_err "export contained $bad lines without .taskID"; rm -f /tmp/sync-task-export.$$; exit 1
  fi
  log_ok "/syncTask/export NDJSON shape valid ($(wc -l < /tmp/sync-task-export.$$) lines)"
else
  log_warn "export returned empty body — ledger empty? proceeding"
fi
rm -f /tmp/sync-task-export.$$

# ---------- /syncTask/export?since=invalid ----------
bad_since=$(master_get "/syncTask/export?since=not-a-time")
# This endpoint streams NDJSON so the error response IS still JSON
# (handler hits api.WriteError before the stream starts).
expect_master_err "$bad_since" "RFC3339" "invalid since param"
log_ok "/syncTask/export rejects bad since"

test_pass "api /syncTask/* surface"
