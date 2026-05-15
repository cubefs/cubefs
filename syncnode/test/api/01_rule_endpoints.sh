#!/usr/bin/env bash
# api/01 — /syncRule/* surface, exhaustive endpoint coverage.
#
# This test focuses on HTTP behaviour: shape, error codes, idempotency,
# state filter on /list, and trigger error paths (paused rule, missing
# id, non-existent rule). It does NOT run any task — pure API surface.

source "$(dirname "$0")/../lib/common.sh"
test_header "api: /syncRule/* surface"

RID=$(unique_id "api-rule")
cleanup_api_01() { delete_rule_silent "$RID"; }
trap_cleanup cleanup_api_01

# expect_master_err <body> <substring> [hint]
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

# ---------- /syncRule/list (empty filter) ----------
list=$(master_rule_list)
expect_code "$list" 0 "list (no filter)"
[ "$(echo "$list" | jq '.data | type')" = '"array"' ] || { log_err "list data must be array"; exit 1; }
log_ok "/syncRule/list returns array envelope"

# ---------- /syncRule/get on unknown id ----------
miss=$(master_rule_get "no-such-rule-$$")
expect_master_err "$miss" "not found" "get unknown id"
log_ok "/syncRule/get unknown → not found"

# ---------- /syncRule/get missing param ----------
no_param=$(master_get "/syncRule/get")
expect_master_err "$no_param" "missing id" "get without id"
log_ok "/syncRule/get without id → param error"

# ---------- /syncRule/create happy path ----------
body=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "api01/")
created=$(master_rule_create "$body")
expect_code "$created" 0 "create happy path"
assert_json_eq "$created" '.data.config.id' "$RID"
assert_json_eq "$created" '.data.state' "active"
log_ok "/syncRule/create happy path"

wait_for_rule_visible "$RID"

# ---------- /syncRule/list?state=active includes our rule ----------
active=$(master_rule_list "active")
hits=$(echo "$active" | jq "[.data[]?.config.id // empty | select(. == \"$RID\")] | length")
[ "$hits" = "1" ] || { log_err "list?state=active should include $RID; hits=$hits"; exit 1; }
log_ok "/syncRule/list?state=active filter works"

# ---------- /syncRule/list?state=paused excludes our active rule ----------
paused=$(master_rule_list "paused")
hits=$(echo "$paused" | jq "[.data[]?.config.id // empty | select(. == \"$RID\")] | length")
[ "$hits" = "0" ] || { log_err "list?state=paused should NOT include active $RID; hits=$hits"; exit 1; }
log_ok "/syncRule/list?state=paused excludes active rules"

# ---------- /syncRule/pause → state flips, list filter sees it ----------
master_rule_pause "$RID" >/dev/null
sleep 0.2
paused=$(master_rule_list "paused")
hits=$(echo "$paused" | jq "[.data[]?.config.id // empty | select(. == \"$RID\")] | length")
[ "$hits" = "1" ] || { log_err "after pause, list?state=paused should include $RID"; exit 1; }
log_ok "/syncRule/pause + state filter consistent"

# ---------- /syncRule/trigger on PAUSED rule → error ----------
trigp=$(master_rule_trigger "$RID")
expect_master_err "$trigp" "state" "trigger paused rule"
log_ok "/syncRule/trigger refuses paused rule"

# ---------- /syncRule/resume + /syncRule/trigger happy path ----------
master_rule_resume "$RID" >/dev/null
trig=$(master_rule_trigger "$RID")
expect_code "$trig" 0 "trigger active rule"
[ "$(echo "$trig" | jq -r '.data.taskID')" != "null" ] || { log_err "trigger response missing taskID"; exit 1; }
log_ok "/syncRule/trigger emits taskID"

# ---------- /syncRule/trigger missing id ----------
trig_noid=$(master_post "/syncRule/trigger" "")
expect_master_err "$trig_noid" "missing id" "trigger without id"

# ---------- /syncRule/trigger unknown id ----------
trig_ghost=$(master_rule_trigger "no-such-rule-$$")
expect_master_err "$trig_ghost" "not found" "trigger unknown id"

# ---------- /syncRule/update unknown id ----------
fake=$(rule_local_to_s3 "ghost-$$" "$ALLOWED_ROOT/" "x/")
upd_miss=$(master_rule_update "$fake")
expect_master_err "$upd_miss" "not found" "update unknown id"
log_ok "/syncRule/update unknown → not found"

# ---------- /syncRule/delete + double delete ----------
del=$(master_rule_delete "$RID")
expect_code "$del" 0 "delete happy"
del2=$(master_rule_delete "$RID")
expect_master_err "$del2" "not found" "double delete idempotent error"
log_ok "/syncRule/delete double-call returns not-found"

test_pass "api /syncRule/* surface"
