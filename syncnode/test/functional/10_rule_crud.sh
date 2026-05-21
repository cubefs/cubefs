#!/usr/bin/env bash
# Functional 10 — Rule CRUD exhaustive against MASTER (P2-7 cutover).
# Pause/Resume state machine, Update with body, Delete-twice
# idempotency, query-param validation, conflict detection on duplicate.
#
# Error-code note: master returns proto.HTTPReply codes:
#   2 (ParamError) for shape / missing-field / invalid-shardingStrategy
#   1 (InternalError) wraps "rule not found", "rule already exists"
#       with the message in .msg. Tests assert on .msg substring so a
#       future code remap doesn't break here.

source "$(dirname "$0")/../lib/common.sh"
test_header "rule CRUD exhaustive (master)"

RID=$(unique_id "rcrud")
cleanup_func_10() { delete_rule_silent "$RID"; }
trap_cleanup cleanup_func_10

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

# Empty list (filter to it-*; other tenants may share the cluster)
list=$(master_rule_list)
expect_code "$list" 0
# (no assertion on count — coexistence is fine)

# Create with missing id → master ParamError + "missing required field: id"
bad=$(master_rule_create '{"type":"sync"}')
expect_master_err "$bad" "missing required field: id" "missing id rejection"

# Create with empty type → master ParamError + "missing required field: type"
bad2=$(master_rule_create "{\"id\":\"$RID\"}")
expect_master_err "$bad2" "missing required field: type" "missing type rejection"

# Create valid
body=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "f10/")
resp=$(master_rule_create "$body")
expect_code "$resp" 0
assert_json_eq "$resp" '.data.config.id' "$RID"
assert_json_eq "$resp" '.data.state' "active"

# Wait until the rule shows up via /get (raft commit + cache update is
# usually <100ms but tests under heavy CI load can lag).
wait_for_rule_visible "$RID"

# Duplicate Create → master returns "rule already exists"
dup=$(master_rule_create "$body")
expect_master_err "$dup" "already exists" "duplicate id rejection"

# Pause → state=paused
pause=$(master_rule_pause "$RID")
assert_json_eq "$pause" '.data.state' "paused"
# Re-pause → still paused (idempotent)
pause2=$(master_rule_pause "$RID")
assert_json_eq "$pause2" '.data.state' "paused"

# Resume
resume=$(master_rule_resume "$RID")
assert_json_eq "$resume" '.data.state' "active"

# Update body (change prefix)
updated=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "f10-v2/")
upd=$(master_rule_update "$updated")
expect_code "$upd" 0
assert_json_eq "$upd" '.data.config.dst.prefix' "f10-v2/"

# Update unknown id → "rule not found"
fake=$(rule_local_to_s3 "ghost-$$" "$ALLOWED_ROOT/" "x/")
notfound=$(master_rule_update "$fake")
expect_master_err "$notfound" "not found" "update unknown id"

# Delete missing id query param → ParamError
qp=$(master_post "/syncRule/delete" "")
expect_master_err "$qp" "missing id" "delete no id"

# Delete valid
del=$(master_rule_delete "$RID")
expect_code "$del" 0
# Delete twice → "rule not found"
del2=$(master_rule_delete "$RID")
expect_master_err "$del2" "not found" "delete twice"

test_pass "rule CRUD exhaustive (master)"
