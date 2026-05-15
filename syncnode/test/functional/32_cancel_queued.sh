#!/usr/bin/env bash
# Functional 32 — Wave 3 Q1 regression guard: a queued task that's
# cancelled while waiting on a concurrency slot MUST be reported as
# cancelled AND must never actually run. Before FIX Q1 the runner's
# Cancel was a silent no-op for queued tasks; the API reported success
# while the task ran later when its slot freed.
#
# Strategy: configure maxConcurrentTasks=1 + maxQueueSize=2 on the
# syncnode (must be done at deploy time — see sync.json), then trigger
# a long-running rule that holds the only slot, trigger a 2nd rule
# (gets queued), cancel the queued one, free the slot by cancelling
# the 1st, and verify the 2nd one never ran.

source "$(dirname "$0")/../lib/common.sh"
require_env_for s3
test_header "cancel queued task (Wave 3 Q1)"

# Confirm capacity config: stat exposes queueLen + runningTasks. We
# expect maxConcurrentTasks=1 in sync.json — if not, skip.
stat=$(syncnode_get /admin/syncnode/stat)
if ! echo "$stat" | jq -e '.data.runningTasks != null' >/dev/null; then
  log_warn "syncnode missing runningTasks in stat; skipping (needs Phase F+)"
  exit 0
fi

# A long-running stub: configure a rule that lists a directory full of
# large files. We need enough work that the task can't finish before we
# trigger + cancel the second.
RID_LONG=$(unique_id "qcancel-long")
RID_Q=$(unique_id "qcancel-queue")
WORK_LONG="$TEST_DATA_DIR/$RID_LONG"
WORK_Q="$TEST_DATA_DIR/$RID_Q"

cleanup_func_32() {
  delete_rule_silent "$RID_LONG"
  delete_rule_silent "$RID_Q"
  sn_rm -rf "$WORK_LONG" "$WORK_Q"
}
trap_cleanup cleanup_func_32

sn_mkdir "$WORK_LONG"
sn_mkdir "$WORK_Q"
# 80 MiB of work in the long rule, 4 MiB in the queued one. With a
# bandwidth limit of 10 MB/s the long rule takes ~8 s — plenty of room
# to trigger + cancel the second.
random_payload "$WORK_LONG/a.bin" 80
random_payload "$WORK_Q/b.bin" 4

# Build rules with a tight per-task bandwidth limit so the first rule
# stalls long enough to schedule the second.
long_body=$(rule_local_to_s3 "$RID_LONG" "$WORK_LONG/" "q-long/")
queue_body=$(rule_local_to_s3 "$RID_Q"    "$WORK_Q/"    "q-q/")

# Inject bandwidthLimitMBps=10 by post-processing JSON (avoid building
# yet another fixture).
long_body=$(echo "$long_body" | jq '.bandwidthLimitMBps = 10')
queue_body=$(echo "$queue_body" | jq '.bandwidthLimitMBps = 10')

expect_code "$(create_rule "$long_body")" 0
expect_code "$(create_rule "$queue_body")" 0

# Trigger the long rule with wait=false so we get a record id back fast.
t1=$(master_rule_trigger "$RID_LONG")
expect_code "$t1" 0 "trigger long"
TID_LONG=$(echo "$t1" | jq -r '.data.taskID')
[ -n "$TID_LONG" ] || test_fail "no taskID for long rule"
log_ok "long task running: $TID_LONG"

# Wait until the long task is actually running (not just queued)
wait_for_running() {
  local s; s=$(syncnode_get "/admin/syncnode/stat")
  local r; r=$(echo "$s" | jq -r '.data.runningTasks // 0')
  [ "$r" -ge 1 ] && echo yes || echo no
}
wait_for wait_for_running 30 "long task to enter running state" || test_fail "long task never ran"

# Now trigger the queued task. With maxConcurrentTasks=1 this either
# queues (if maxQueueSize>0) or fail-fast 503. Both are valid pre-fix;
# we test the queued branch by requiring a queue >= 1. If the deploy
# doesn't support queueing, this test simply checks the immediate
# "ErrQueueFull" rejection.
t2=$(master_rule_trigger "$RID_Q")
code=$(echo "$t2" | jq -r '.code // 999')
TID_Q=$(echo "$t2" | jq -r '.data.taskID // empty')

case "$code" in
  0)
    log_info "queued task admitted: $TID_Q"
    # Cancel TID_Q while it is still queued (TID_LONG holds the only slot).
    c=$(master_task_cancel "$TID_Q")
    expect_code "$c" 0 "cancel queued"
    log_ok "cancel sent for queued task"

    # Wait for TID_Q to reach terminal BEFORE freeing the slot.
    # With TID_LONG still holding the slot, runAfterWait's select has
    # only one ready case (<-taskCtx.Done()), so there is no race between
    # the slot arm and the cancel arm. This ordering is intentional: if we
    # freed the slot first (by cancelling TID_LONG), the admin task manager
    # might deliver cancel-TID_LONG before cancel-TID_Q (map iteration is
    # random), letting TID_Q win the slot with a live context.
    wait_q_terminal() {
      local r; r=$(master_task_get "$TID_Q")
      local s; s=$(echo "$r" | jq -r '.data.status // empty')
      case "$s" in
        succeeded|failed|cancelled) echo yes ;;
        *) echo no ;;
      esac
    }
    wait_for wait_q_terminal 30 "queued task to reach terminal"

    # Slot is still held by TID_LONG. Cancel it now as cleanup.
    master_task_cancel "$TID_LONG" >/dev/null
    # Wait for TID_LONG to actually reach terminal so the concurrency slot is
    # freed before the next test runs. Without this, a test that starts < 10s
    # later (before the next heartbeat) sees RunningTasks=1 with
    # maxConcurrentTasks=1 and gets "no eligible candidate".
    wait_for_task_terminal "$TID_LONG" 30 || log_warn "long task did not terminate in 30s"

    final=$(master_task_get "$TID_Q")
    assert_json_eq "$final" '.data.status' "cancelled"
    assert_json_eq "$final" '.data.progress.filesDone' "0" \
      "queued task must NOT have started running (Wave 3 Q1 regression)"
    ;;
  2007)
    log_info "deploy has queue disabled (ErrQueueFull / 429-like); skipping detailed assertions"
    master_task_cancel "$TID_LONG" >/dev/null
    wait_for_task_terminal "$TID_LONG" 30 || log_warn "long task (code 2007 path) did not terminate in 30s"
    ;;
  *)
    test_fail "unexpected response code $code for queued trigger"
    ;;
esac

test_pass "cancel queued task"
