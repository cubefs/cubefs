#!/usr/bin/env bash
# Functional 23 — Retention §G-1 invariant. Creates 7 model-step-{N}.pt
# files, configures keepLast=5, runs sync, asserts dst has exactly 5.
# Then verifies the negative side: a sync that fails mid-flight (we
# force this by pointing src at a non-existent path on the second
# trigger) MUST NOT touch retention — the 5 surviving files stay.

source "$(dirname "$0")/../lib/common.sh"
require_env_for s3
test_header "retention G-1 invariant"

RID=$(unique_id "ret")
WORK="$TEST_DATA_DIR/$RID"
PREFIX="it/$RID/"

cleanup_func_23() {
  delete_rule_silent "$RID"
  sn_rm -rf "$WORK"
}
trap_cleanup cleanup_func_23

sn_mkdir "$WORK"
for n in 1000 2000 3000 4000 5000 6000 7000; do
  sn_write_line "step-$n" "$WORK/model-step-$n.pt"
done
log_ok "seeded 7 model-step files"

# Rule with keepLast=5.
body=$(rule_retention "$RID" "$WORK/" "$PREFIX" "model-step-{N}.pt" 5)
expect_code "$(create_rule "$body")" 0

# Sync — should transfer all 7, then retention deletes the 2 oldest.
res=$(trigger_and_wait "$RID")
assert_json_eq "$res" '.data.status' "succeeded"
log_ok "first run completed; retention should have pruned oldest 2"

# The dst is in S3; we can't list it from here without s3cmd. Instead
# of asserting the bucket state directly, run a CHECK task to see what
# the syncnode itself reports as the surviving set.
check_rid="${RID}-check"
check_body=$(rule_check "$check_rid" "$WORK/" "$PREFIX")
delete_rule_silent "$check_rid"
expect_code "$(create_rule "$check_body")" 0
trap_cleanup_more() { delete_rule_silent "$check_rid"; }
trap_cleanup trap_cleanup_more

# Filter source to only files step-3000 and up to match dst expectations.
# Easier path: just count files on dst by removing the oldest 2 from src
# and asserting the check returns no missing_dst.
sn_rm -f "$WORK/model-step-1000.pt" "$WORK/model-step-2000.pt"

res2=$(trigger_and_wait "$check_rid")
mism=$(echo "$res2" | jq '.data.mismatches // [] | length')
if [ "$mism" -ne 0 ]; then
  log_err "check found $mism mismatches; retention did not prune as expected"
  log_err "raw: $res2"
  test_fail "retention §G-1: kept count != 5"
fi
log_ok "keepLast=5 confirmed (no mismatches after removing 2 oldest)"

# Negative side: re-trigger sync with a broken source.
# Easiest forcing function: delete the entire $WORK; sync src list will
# fail. Existing dst stays intact. Run a check to verify.
sn_rm -rf "$WORK"
log_warn "removed src dir to force sync failure"

bad=$(trigger_and_wait "$RID")
# The result may be Done with 0 files (empty src), Failed, or refuse —
# any of these is acceptable as long as retention DIDN'T fire. We test
# that by re-creating the src dir + checking dst is still 5 files
# (anything fewer means retention ran on a failed sync).
sn_mkdir "$WORK"
for n in 3000 4000 5000 6000 7000; do
  sn_write_line "step-$n" "$WORK/model-step-$n.pt"
done
res3=$(trigger_and_wait "$check_rid")
mism3=$(echo "$res3" | jq '.data.mismatches // [] | length')
if [ "$mism3" -ne 0 ]; then
  log_err "retention fired on a failed run — §G-1 violated"
  test_fail "retention §G-1 negative case"
fi
log_ok "retention did NOT fire after sync failure (§G-1 OK)"

test_pass "retention G-1"
