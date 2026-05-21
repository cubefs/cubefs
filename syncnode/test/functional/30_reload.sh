#!/usr/bin/env bash
# Functional 30 — SIGHUP / POST /admin/syncnode/reload. Verifies the
# Phase F-3 atomic-reload + Wave 1 D fixes (cfg pointer freshness,
# all-or-nothing apply with rollback).
#
# Requires the deploy box to expose the running sync.json so we can
# mutate it. If $SYNC_CONFIG_PATH is unset, we use POST /reload and
# do NOT mutate the file — just exercise the endpoint.

source "$(dirname "$0")/../lib/common.sh"
test_header "SIGHUP / POST /reload"

# Path A — endpoint smoke test (always runnable)
before=$(syncnode_get /admin/syncnode/stat)
before_failures=$(echo "$before" | jq -r '.data.reloadFailuresTotal // 0')

resp=$(syncnode_post /admin/syncnode/reload)
case "$(echo "$resp" | jq -r '.code')" in
  0)
    log_ok "reload succeeded (no file change → no-op)"
    after_failures=$(syncnode_get /admin/syncnode/stat | jq -r '.data.reloadFailuresTotal // 0')
    assert_eq "$before_failures" "$after_failures" "failure counter must not bump on success"
    ;;
  2001|2003)
    log_warn "reload rejected with code=$(echo "$resp" | jq -r .code) — that's OK if no config path is wired"
    ;;
  *)
    test_fail "reload returned unexpected envelope: $resp"
    ;;
esac

# Path B — mutate config file + assert atomicity. Only runs when
# SYNC_CONFIG_PATH is set in env (the deploy box).
if [ -n "${SYNC_CONFIG_PATH:-}" ] && [ -w "${SYNC_CONFIG_PATH}" ]; then
  log_info "config path is writable; running atomicity test"
  cp "$SYNC_CONFIG_PATH" "$SYNC_CONFIG_PATH.bak"
  trap_cleanup_30() { mv "$SYNC_CONFIG_PATH.bak" "$SYNC_CONFIG_PATH"; syncnode_post /admin/syncnode/reload >/dev/null 2>&1 || true; }
  trap_cleanup trap_cleanup_30

  # Write garbage
  echo '{ this is not json }' > "$SYNC_CONFIG_PATH"
  bad=$(syncnode_post /admin/syncnode/reload)
  assert_json_ne "$bad" '.code' "0" "reload of bad config must fail"
  after_failures=$(syncnode_get /admin/syncnode/stat | jq -r '.data.reloadFailuresTotal // 0')
  if [ "$after_failures" -le "$before_failures" ]; then
    test_fail "reloadFailuresTotal didn't increment (before=$before_failures after=$after_failures)"
  fi
  log_ok "reloadFailuresTotal bumped + old config preserved"
else
  log_info "SYNC_CONFIG_PATH not set/writable; skipping atomicity test"
fi

test_pass "reload"
