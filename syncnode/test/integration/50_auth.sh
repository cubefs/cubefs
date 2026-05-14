#!/usr/bin/env bash
# Integration 50 — SEC1+SEC4 auth. Verifies that when an admin token
# is configured, requests without / with wrong tokens return 401.
#
# This test ONLY runs if MASTER_TOKEN or SYNCNODE_TOKEN is non-empty;
# otherwise auth is off by design.

source "$(dirname "$0")/../lib/common.sh"
test_header "shared-token auth"

ran_any=0

if [ -n "${SYNCNODE_TOKEN:-}" ]; then
  ran_any=1
  log_info "testing syncnode AuthMiddleware (SEC4)"

  # Bare GET — must 401
  s=$(curl -sS -o /dev/null -w '%{http_code}' \
    "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}/admin/syncnode/version")
  assert_eq "401" "$s" "no-token GET"

  # Wrong token — must 401
  s=$(curl -sS -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer this-is-wrong" \
    "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}/admin/syncnode/version")
  assert_eq "401" "$s" "wrong-token GET"

  # X-Sync-Token variant accepted
  s=$(curl -sS -o /dev/null -w '%{http_code}' \
    -H "X-Sync-Token: $SYNCNODE_TOKEN" \
    "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}/admin/syncnode/version")
  assert_eq "200" "$s" "X-Sync-Token accepted"

  # Bearer accepted (smoke covers it; assert here too for completeness)
  s=$(curl -sS -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $SYNCNODE_TOKEN" \
    "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}/admin/syncnode/version")
  assert_eq "200" "$s" "Bearer accepted"
  log_ok "syncnode-side auth OK"
fi

if [ -n "${MASTER_TOKEN:-}" ]; then
  ran_any=1
  log_info "testing master /syncNode/* auth (SEC1)"

  # Master /syncNode/list — bare → 401
  s=$(curl -sS -o /dev/null -w '%{http_code}' "${MASTER_HTTP%/}/syncNode/list")
  assert_eq "401" "$s" "master list bare"

  # Correct token → 200
  s=$(curl -sS -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $MASTER_TOKEN" \
    "${MASTER_HTTP%/}/syncNode/list")
  assert_eq "200" "$s" "master list authorised"
  log_ok "master-side auth OK"
fi

if [ "$ran_any" = "0" ]; then
  log_warn "neither SYNCNODE_TOKEN nor MASTER_TOKEN is set — auth is OFF by design; nothing to test"
  exit 0
fi

test_pass "auth"
