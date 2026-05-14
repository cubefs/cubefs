#!/usr/bin/env bash
# Smoke 01 — syncnode is up, replies to /version + /stat, BoltDB healthy.
# Should finish in under 5 seconds.

source "$(dirname "$0")/../lib/common.sh"
test_header "syncnode health"

# /admin/syncnode/version
body=$(syncnode_get /admin/syncnode/version)
expect_code "$body" 0 "/version code"
assert_json_eq "$body" '.data.role' "sync"
assert_json_ne "$body" '.data.version' ""

# /admin/syncnode/stat
stat=$(syncnode_get /admin/syncnode/stat)
expect_code "$stat" 0 "/stat code"
assert_json_eq "$stat" '.data.boltdbHealthy' "true"
assert_json_eq "$stat" '.data.role' "sync"
# uptime is a float; just confirm > 0
assert_json_gte "$stat" '.data.uptimeSeconds' 0

# reloadFailuresTotal should exist (introduced in Phase F-3). Stat may
# report 0 or higher; we just want the key to be there.
echo "$stat" | jq -e '.data.reloadFailuresTotal != null' >/dev/null \
  || test_fail "stat missing reloadFailuresTotal"

test_pass "syncnode health"
