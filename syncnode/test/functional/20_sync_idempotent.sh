#!/usr/bin/env bash
# Functional 20 — Sync task end-to-end + idempotency. Generates N files
# locally, triggers a sync to S3 with wait=true, asserts:
#   - filesDone == N, filesFailed == 0
#   - re-trigger → filesSkipped == N (idempotency via Head-then-Put)
#   - mutate one source file → re-trigger → filesDone == 1, skipped == N-1

source "$(dirname "$0")/../lib/common.sh"
require_env_for s3
test_header "sync end-to-end + idempotency"

RID=$(unique_id "sync")
WORK="$TEST_DATA_DIR/$RID"
PREFIX="it/$RID/"
N_FILES=5
FILE_MB=1

cleanup_func_20() {
  delete_rule_silent "$RID"
  sn_rm -rf "$WORK"
}
trap_cleanup cleanup_func_20

# ---- Seed source ----
sn_mkdir "$WORK"
for i in $(seq 1 "$N_FILES"); do
  random_payload "$WORK/f-$i.bin" "$FILE_MB"
done
log_ok "seeded $N_FILES files of ${FILE_MB}MB each at $WORK"

# Compute the original md5 set to verify Sync 3 below
ORIG_MD5_1=$(md5_of "$WORK/f-1.bin")

# ---- Create rule ----
body=$(rule_local_to_s3 "$RID" "$WORK/" "$PREFIX")
expect_code "$(create_rule "$body")" 0

# ---- Sync 1: full transfer ----
res=$(trigger_and_wait "$RID")
expect_code "$res" 0 "trigger #1"
assert_json_eq "$res" '.data.status' "succeeded"
assert_json_eq "$res" '.data.progress.filesDone' "$N_FILES"
assert_json_eq "$res" '.data.progress.filesFailed' "0"
log_ok "sync #1: $N_FILES files transferred"

# ---- Sync 2: idempotent ----
res=$(trigger_and_wait "$RID")
assert_json_eq "$res" '.data.status' "succeeded"
assert_json_eq "$res" '.data.progress.filesSkipped' "$N_FILES" "expected all skipped"
log_ok "sync #2: all skipped (Head matched)"

# ---- Sync 3: mutate one file → exactly 1 re-transfer ----
random_payload "$WORK/f-1.bin" "$FILE_MB"
MUTATED_MD5=$(md5_of "$WORK/f-1.bin")
assert_ne "$ORIG_MD5_1" "$MUTATED_MD5" "expected new content"

res=$(trigger_and_wait "$RID")
assert_json_eq "$res" '.data.status' "succeeded"
assert_json_eq "$res" '.data.progress.filesDone' "1" "expected 1 re-transfer"
assert_json_eq "$res" '.data.progress.filesSkipped' "$((N_FILES - 1))" "rest skipped"
log_ok "sync #3: incremental re-transfer of 1 file"

test_pass "sync end-to-end + idempotency"
