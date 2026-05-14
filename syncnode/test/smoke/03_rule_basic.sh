#!/usr/bin/env bash
# Smoke 03 — Rule CRUD roundtrip through MASTER (P2-7 cutover).
# Creates a rule, gets it back, pauses/resumes, deletes it. Exercises
# the master /syncRule/* surface — the syncnode no longer hosts a rule
# admin API.

source "$(dirname "$0")/../lib/common.sh"
test_header "rule CRUD roundtrip (master)"

RID=$(unique_id "smoke")
cleanup_smoke_03() { delete_rule_silent "$RID"; }
trap_cleanup cleanup_smoke_03

# Create
body=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "smoke/$RID/")
resp=$(master_rule_create "$body")
expect_code "$resp" 0 "syncRule/create"
assert_json_eq "$resp" '.data.config.id' "$RID"
assert_json_eq "$resp" '.data.state' "active"
log_ok "rule created: $RID"

# Get
got=$(master_rule_get "$RID")
expect_code "$got" 0 "syncRule/get"
assert_json_eq "$got" '.data.config.type' "sync"

# Pause → State flips to paused
pause=$(master_rule_pause "$RID")
expect_code "$pause" 0 "syncRule/pause"
assert_json_eq "$pause" '.data.state' "paused"

# Resume → State back to active
resume=$(master_rule_resume "$RID")
expect_code "$resume" 0 "syncRule/resume"
assert_json_eq "$resume" '.data.state' "active"

# Delete
del=$(master_rule_delete "$RID")
expect_code "$del" 0 "syncRule/delete"

# Get on deleted ID → master returns inline 500 with the not-found
# string (ErrCodeInternalError + "sync rule not found"). We assert on
# the rendered message instead of the code so a future code remap
# doesn't break the smoke test.
miss=$(master_rule_get "$RID")
if echo "$miss" | jq -e '.code != 0' >/dev/null; then
  msg=$(echo "$miss" | jq -r '.msg // ""')
  case "$msg" in
    *"not found"*) log_ok "post-delete get returns not-found: $msg" ;;
    *) log_err "post-delete get returned unexpected msg: $msg"; exit 1 ;;
  esac
else
  log_err "post-delete get unexpectedly succeeded"
  exit 1
fi

test_pass "rule CRUD"
