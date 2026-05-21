# shellcheck shell=bash
# Wipe-all helper invoked by `./run.sh cleanup`. Removes every rule
# whose ID starts with "it-" (the prefix all tests use via unique_id).
# Idempotent.

# shellcheck source=../lib/common.sh
# (loaded by run.sh before calling into here)

cleanup_all() {
  log_info "scanning for stale it-* rules (master)"
  local list
  list=$(master_rule_list)
  expect_code "$list" 0 "syncRule/list during cleanup"
  local ids
  ids=$(echo "$list" | jq -r '.data[]?.config.id // empty' | grep '^it-' || true)
  if [ -z "$ids" ]; then
    log_ok "nothing to clean up"
    return 0
  fi
  local n=0
  while IFS= read -r id; do
    [ -z "$id" ] && continue
    delete_rule_silent "$id"
    n=$((n+1))
  done <<< "$ids"
  log_ok "deleted $n stale rule(s)"

  # Best-effort cleanup of local fixtures dir
  if [ -n "${TEST_DATA_DIR:-}" ] && [ -d "$TEST_DATA_DIR" ]; then
    rm -rf "$TEST_DATA_DIR"/it-* 2>/dev/null || true
    log_ok "removed fixtures under $TEST_DATA_DIR/it-*"
  fi
}
