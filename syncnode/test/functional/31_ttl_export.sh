#!/usr/bin/env bash
# Functional 31 — TTL Runner + history export. Triggers a handful of
# tasks, waits for them to terminate, then queries the export endpoint
# and confirms the JSONL output decodes line by line.
#
# Note: the move-to-history transition is driven by Phase F-4's
# TTLRunner whose ActiveAge defaults to 24h. In a production build
# operators can't easily shorten this; we just exercise the export
# endpoint and confirm it streams.

source "$(dirname "$0")/../lib/common.sh"
test_header "task history export"

# /admin/sync/task/export is a streaming endpoint with
# Content-Type: application/x-ndjson — NOT the JSON envelope. Use
# direct curl so we get the body bytes.
auth=()
[ -n "${SYNCNODE_TOKEN:-}" ] && auth=(-H "Authorization: Bearer $SYNCNODE_TOKEN")

# Time-stamped temp file for the dump
out="$(mktemp -t syncnode-export.XXXXXX.jsonl)"
trap "rm -f $out" EXIT

status=$(curl -sS -o "$out" -w '%{http_code}' ${auth[@]+"${auth[@]}"} \
  "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}/admin/sync/task/export")

assert_eq "200" "$status" "export endpoint HTTP status"

# Empty body is acceptable (history may genuinely be empty). If non-
# empty, every line must be valid JSON with at least a "taskId" field.
if [ -s "$out" ]; then
  log_info "$(wc -l < "$out") history record(s) in dump"
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    echo "$line" | jq -e '.taskId != null' >/dev/null \
      || test_fail "invalid JSONL line: $line"
  done < "$out"
  log_ok "all JSONL lines valid"
else
  log_info "history is empty (acceptable)"
fi

# Bonus: ?since=<RFC3339> filter syntax check — invalid format must 400.
bad_since=$(curl -sS -o /dev/null -w '%{http_code}' ${auth[@]+"${auth[@]}"} \
  "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}/admin/sync/task/export?since=not-a-time")
assert_eq "400" "$bad_since" "invalid ?since= rejection"

test_pass "history export"
