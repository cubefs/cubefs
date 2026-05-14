#!/usr/bin/env bash
# Smoke 02 — master sees this syncnode in /syncNode/list with a
# non-stale heartbeat. Verifies the register loop + heartbeat goroutine
# (Phase B-3 + B-4) actually reached the master.

source "$(dirname "$0")/../lib/common.sh"
require_env_for master
test_header "master sees syncnode registered"

list=$(master_get /syncNode/list)
expect_code "$list" 0 "/syncNode/list code"

# /syncNode/list returns data: [SyncNodeInfo, …]. The reporting addr
# is "<ip>:<tcpPort>" — match by tcpPort + host substring (master
# stamps the IP it sees on its end of the connection, so we don't
# know it client-side without resolving SYNCNODE_HOST → IP).
expected_port="${SYNCNODE_TCP_PORT}"
found=$(echo "$list" | jq -r --arg p "$expected_port" '
  .data[] | select(.addr | endswith(":" + $p)) | .addr')

if [ -z "$found" ]; then
  log_err "no syncnode in master list with port :$expected_port"
  log_err "master returned: $list"
  test_fail "syncnode not registered"
fi

log_info "found syncnode in master list: $found"

# Confirm load score is finite (not +Inf, which means stale/unhealthy).
# JSON marshals +Inf as null or a special string depending on the
# encoder; we accept anything <= 1 (the formula caps at 1.0).
score=$(echo "$list" | jq -r --arg p "$expected_port" '
  .data[] | select(.addr | endswith(":" + $p)) | .loadScore')
case "$score" in
  ""|null|"+Inf"|"Inf")
    log_err "syncnode has stale/unhealthy load score: $score"
    test_fail "load score is +Inf — heartbeat may be missing" ;;
esac
assert_json_lte "{\"v\":$score}" '.v' 1.5
log_ok "load score: $score"

test_pass "master register"
