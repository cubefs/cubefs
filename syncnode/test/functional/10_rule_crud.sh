#!/usr/bin/env bash
# Functional 10 — Rule CRUD exhaustive. Pause/Resume state machine,
# Update with body, Delete-twice idempotency, query-param validation.

source "$(dirname "$0")/../lib/common.sh"
test_header "rule CRUD exhaustive"

RID=$(unique_id "rcrud")
cleanup_func_10() { delete_rule_silent "$RID"; }
trap_cleanup cleanup_func_10

# Empty list (filter to it-*; other tenants may share the cluster)
list=$(syncnode_get /admin/sync/rule/list)
expect_code "$list" 0
# (no assertion on count — coexistence is fine)

# Create with missing id → 400 + CodeMissingField (2002)
bad=$(syncnode_post /admin/sync/rule/create '{"type":"sync"}')
expect_code "$bad" 2002 "missing id rejection"

# Create with empty type → 400 + CodeMissingField (2002)
bad2=$(syncnode_post /admin/sync/rule/create "{\"id\":\"$RID\"}")
expect_code "$bad2" 2002 "missing type rejection"

# Create valid
body=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "f10/")
resp=$(syncnode_post /admin/sync/rule/create "$body")
expect_code "$resp" 0

# Duplicate Create → 409 + CodeConflict (2005)
dup=$(syncnode_post /admin/sync/rule/create "$body")
expect_code "$dup" 2005 "duplicate id rejection"

# Pause → state=paused
pause=$(syncnode_post "/admin/sync/rule/pause?id=$RID")
assert_json_eq "$pause" '.data.state' "paused"
# Re-pause → still paused (idempotent)
pause2=$(syncnode_post "/admin/sync/rule/pause?id=$RID")
assert_json_eq "$pause2" '.data.state' "paused"

# Resume
resume=$(syncnode_post "/admin/sync/rule/resume?id=$RID")
assert_json_eq "$resume" '.data.state' "active"

# Update body (change prefix)
updated=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "f10-v2/")
upd=$(syncnode_post /admin/sync/rule/update "$updated")
expect_code "$upd" 0
assert_json_eq "$upd" '.data.config.dst.prefix' "f10-v2/"

# Update unknown id → 404 + CodeNotFound (2004)
fake=$(rule_local_to_s3 "ghost-$$" "$ALLOWED_ROOT/" "x/")
notfound=$(syncnode_post /admin/sync/rule/update "$fake")
expect_code "$notfound" 2004

# Delete missing id query param → 400 + CodeMissingField (2002)
qp=$(syncnode_post "/admin/sync/rule/delete")
expect_code "$qp" 2002

# Delete valid
del=$(syncnode_post "/admin/sync/rule/delete?id=$RID")
expect_code "$del" 0
# Delete twice → 404
del2=$(syncnode_post "/admin/sync/rule/delete?id=$RID")
expect_code "$del2" 2004

test_pass "rule CRUD exhaustive"
