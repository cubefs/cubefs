#!/usr/bin/env bash
# Functional 31 — Task history export via master /syncTask/export.
# After P2-7 the task ledger lives on master (not syncnode). Triggers
# a handful of tasks, waits for them to terminate, then queries the
# master export endpoint and confirms NDJSON output decodes correctly.
#
# Note: the active-task LRU and history LRU both feed the export stream.

source "$(dirname "$0")/../lib/common.sh"
require_env_for s3
test_header "task history export"

RID=$(unique_id "export")
WORK="$TEST_DATA_DIR/$RID"
PREFIX="it/$RID/"

cleanup_func_31() {
  delete_rule_silent "$RID"
  sn_rm -rf "$WORK"
}
trap_cleanup cleanup_func_31

# Seed one small file and run a sync so the ledger is non-empty.
sn_mkdir "$WORK"
sn_write_line "export-test" "$WORK/f.bin"

body=$(rule_local_to_s3 "$RID" "$WORK/" "$PREFIX")
expect_code "$(create_rule "$body")" 0
trigger_and_wait "$RID" >/dev/null || true   # ignore pass/fail — we just want a record

# Query the master export endpoint. Returns NDJSON (not the JSON
# envelope), so use master_get and inspect the raw stream.
out="$(mktemp -t syncnode-export.XXXXXX.jsonl)"
trap "rm -f $out" EXIT

# master_get echoes the body; we need the HTTP status too.
_curl_with_retry -X GET "$(MASTER_BASE)/syncTask/export" > "$out"
export_status="$HTTP_STATUS"

assert_eq "200" "$export_status" "export endpoint HTTP status"

# Empty body is acceptable (active LRU window may have rolled). If
# non-empty, every line must be valid JSON with at least a "taskID".
if [ -s "$out" ]; then
  log_info "$(wc -l < "$out") record(s) in export stream"
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    echo "$line" | jq -e '.taskID != null' >/dev/null \
      || test_fail "invalid NDJSON line (missing taskID): $line"
  done < "$out"
  log_ok "all NDJSON lines valid"
else
  log_info "export stream is empty (acceptable — active LRU may be empty)"
fi

# Invalid ?since= must return a JSON envelope error (code != 0).
# Master uses HTTP 200 + JSON error body for param validation errors.
bad_body=$(master_get "/syncTask/export?since=not-a-time")
bad_code=$(echo "$bad_body" | jq -r '.code // 0')
[ "$bad_code" -ne 0 ] \
  || test_fail "invalid ?since= should return non-zero code, got code=0 body=$bad_body"
log_ok "invalid ?since= rejected with code=$bad_code"

test_pass "task history export"
