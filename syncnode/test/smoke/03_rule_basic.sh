#!/usr/bin/env bash
# Smoke 03 — Rule CRUD roundtrip. Creates a rule, gets it back, deletes
# it. Doesn't trigger any task; just exercises the admin API + store.

source "$(dirname "$0")/../lib/common.sh"
test_header "rule CRUD roundtrip"

RID=$(unique_id "smoke")
cleanup_smoke_03() { delete_rule_silent "$RID"; }
trap_cleanup cleanup_smoke_03

# Create
body=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "smoke/$RID/")
resp=$(syncnode_post /admin/sync/rule/create "$body")
expect_code "$resp" 0 "rule/create"
assert_json_eq "$resp" '.data.config.id' "$RID"
assert_json_eq "$resp" '.data.state' "active"
log_ok "rule created: $RID"

# Get
got=$(syncnode_get "/admin/sync/rule/get?id=$RID")
expect_code "$got" 0 "rule/get"
assert_json_eq "$got" '.data.config.type' "sync"

# Pause → State flips to paused
pause=$(syncnode_post "/admin/sync/rule/pause?id=$RID")
expect_code "$pause" 0 "rule/pause"
assert_json_eq "$pause" '.data.state' "paused"

# Resume → State back to active
resume=$(syncnode_post "/admin/sync/rule/resume?id=$RID")
expect_code "$resume" 0 "rule/resume"
assert_json_eq "$resume" '.data.state' "active"

# Delete
del=$(syncnode_post "/admin/sync/rule/delete?id=$RID")
expect_code "$del" 0 "rule/delete"

# Get on deleted ID → 404 + code=2004
miss=$(syncnode_get "/admin/sync/rule/get?id=$RID")
expect_code "$miss" 2004 "expected NotFound after delete"

test_pass "rule CRUD"
