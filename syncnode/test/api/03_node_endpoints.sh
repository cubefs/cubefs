#!/usr/bin/env bash
# api/03 — /syncNode/* surface coverage.
#
# Exercises list, drain → tasks (empty after drain) → restore lifecycle
# without permanently affecting the cluster. We pick the first node
# from /syncNode/list as the test target.
#
# IMPORTANT: this test mutates one node's state (drain → restore). If
# the cluster is single-node, drain temporarily removes the only
# candidate; ensure no other tests are running concurrently.

source "$(dirname "$0")/../lib/common.sh"
test_header "api: /syncNode/* surface"

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

# ---------- /syncNode/list shape ----------
list=$(master_node_list)
expect_code "$list" 0 "node list"
n=$(echo "$list" | jq '.data | length')
[ "$n" -ge 1 ] || { log_err "no syncnodes registered"; exit 1; }
log_ok "/syncNode/list returns $n node(s)"

# Pick first node. Prefer the SYNCNODE_HOST:SYNCNODE_TCP_PORT we know
# we can restore to, fallback to whatever list gives us.
my_addr="${SYNCNODE_HOST}:${SYNCNODE_TCP_PORT}"
target=$(echo "$list" | jq -r ".data[] | select(.addr == \"$my_addr\") | .addr")
if [ -z "$target" ]; then
  target=$(echo "$list" | jq -r '.data[0].addr')
  log_warn "self-node $my_addr not in list; using $target"
fi
[ -n "$target" ] && [ "$target" != "null" ] || { log_err "could not pick a target addr"; exit 1; }
log_ok "test target node: $target"

# Trap that always restores the node, even on failure.
api_03_restore() { master_node_restore "$target" >/dev/null 2>&1 || true; }
trap_cleanup api_03_restore

# ---------- /syncNode/tasks (baseline) ----------
tasks_before=$(master_node_tasks "$target")
expect_code "$tasks_before" 0 "node tasks baseline"
log_ok "/syncNode/tasks returns array (baseline)"

# ---------- /syncNode/decommission missing addr ----------
no_addr=$(master_post "/syncNode/decommission" "")
expect_master_err "$no_addr" "missing addr" "decommission no addr"

# ---------- /syncNode/drain missing addr ----------
no_addr2=$(master_post "/syncNode/drain" "")
expect_master_err "$no_addr2" "missing addr" "drain no addr"

# ---------- /syncNode/restore missing addr ----------
no_addr3=$(master_post "/syncNode/restore" "")
expect_master_err "$no_addr3" "missing addr" "restore no addr"

# ---------- /syncNode/decommission unknown addr ----------
ghost=$(master_node_decommission "no.such.host:0" "false")
expect_master_err "$ghost" "not found" "decommission unknown addr"
log_ok "/syncNode/decommission unknown → not found"

# ---------- /syncNode/drain (state → draining) ----------
drain=$(master_node_drain "$target")
expect_code "$drain" 0 "drain"
state=$(echo "$drain" | jq -r '.data.state')
[ "$state" = "draining" ] || { log_err "after drain state=$state, want draining"; exit 1; }
log_ok "/syncNode/drain flips state → draining"

# ---------- After drain, target should be skipped by dispatcher ----------
# We can't easily prove "dispatcher skips" without launching a task;
# proxy: list returns the node with state=draining.
post_list=$(master_node_list)
post_state=$(echo "$post_list" | jq -r ".data[] | select(.addr == \"$target\") | .state")
[ "$post_state" = "draining" ] || { log_err "list post-drain state=$post_state"; exit 1; }
log_ok "/syncNode/list reflects draining state"

# ---------- Re-drain is idempotent ----------
drain2=$(master_node_drain "$target")
expect_code "$drain2" 0 "drain idempotent"

# ---------- /syncNode/restore ----------
restore=$(master_node_restore "$target")
expect_code "$restore" 0 "restore"
state=$(echo "$restore" | jq -r '.data.state')
[ "$state" = "active" ] || { log_err "after restore state=$state, want active"; exit 1; }
log_ok "/syncNode/restore flips state → active"

# ---------- Restore is idempotent ----------
restore2=$(master_node_restore "$target")
expect_code "$restore2" 0 "restore idempotent"

# ---------- /syncNode/tasks?status= filter ----------
tasks_running=$(master_node_tasks "$target" "running")
expect_code "$tasks_running" 0 "node tasks running filter"
[ "$(echo "$tasks_running" | jq '.data | type')" = '"array"' ] || {
  log_err "tasks running data not array"; exit 1; }
log_ok "/syncNode/tasks?status=running returns array"

test_pass "api /syncNode/* surface"
