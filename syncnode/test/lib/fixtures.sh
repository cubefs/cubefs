# shellcheck shell=bash
# Rule and task JSON fixture builders. Each function emits JSON on
# stdout; tests pipe into syncnode_post.

# rule_local_to_s3 <id> <src-path> <prefix>
# Minimal sync rule going from local POSIX → S3 bucket (uses
# S3_ENDPOINT + S3_BUCKET from env). No schedule — for manual trigger.
rule_local_to_s3() {
  local id="$1" srcPath="$2" prefix="$3"
  cat <<EOF
{
  "id": "$id",
  "type": "sync",
  "src": { "kind": "local", "path": "$srcPath" },
  "dst": { "kind": "s3",
           "bucket": "$S3_BUCKET",
           "prefix": "$prefix",
           "endpoint": "$S3_ENDPOINT",
           "region": "${S3_REGION:-us-east-1}",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}",
           "insecureSkipTLS": ${S3_INSECURE_TLS:-false},
           "usePathStyle": ${S3_PATH_STYLE:-false} },
  "afterCopy": "keep",
  "downloadStrategy": "temp_rename",
  "onMismatch": "alert",
  "filter": { "include": ["*"] },
  "bandwidthLimitMBps": 0,
  "parallelism": 1
}
EOF
}

# rule_s3_to_local <id> <prefix> <dst-path>  — load direction
rule_s3_to_local() {
  local id="$1" prefix="$2" dstPath="$3"
  cat <<EOF
{
  "id": "$id",
  "type": "load",
  "src": { "kind": "s3",
           "bucket": "$S3_BUCKET",
           "prefix": "$prefix",
           "endpoint": "$S3_ENDPOINT",
           "region": "${S3_REGION:-us-east-1}",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}",
           "insecureSkipTLS": ${S3_INSECURE_TLS:-false},
           "usePathStyle": ${S3_PATH_STYLE:-false} },
  "dst": { "kind": "local", "path": "$dstPath" },
  "afterCopy": "keep",
  "downloadStrategy": "temp_rename",
  "onMismatch": "alert"
}
EOF
}

# rule_check <id> <src-path> <prefix> [auto_fix|alert|ignore]
rule_check() {
  local id="$1" srcPath="$2" prefix="$3" policy="${4:-alert}"
  cat <<EOF
{
  "id": "$id",
  "type": "check",
  "src": { "kind": "local", "path": "$srcPath" },
  "dst": { "kind": "s3",
           "bucket": "$S3_BUCKET",
           "prefix": "$prefix",
           "endpoint": "$S3_ENDPOINT",
           "region": "${S3_REGION:-us-east-1}",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}",
           "insecureSkipTLS": ${S3_INSECURE_TLS:-false},
           "usePathStyle": ${S3_PATH_STYLE:-false} },
  "afterCopy": "keep",
  "downloadStrategy": "temp_rename",
  "onMismatch": "$policy",
  "sampleStrategy": "full",
  "sampleRate": 1.0
}
EOF
}

# rule_retention <id> <src-path> <prefix> <pattern> <keepLast>
rule_retention() {
  local id="$1" srcPath="$2" prefix="$3" pattern="$4" keep="$5"
  cat <<EOF
{
  "id": "$id",
  "type": "sync",
  "src": { "kind": "local", "path": "$srcPath" },
  "dst": { "kind": "s3",
           "bucket": "$S3_BUCKET",
           "prefix": "$prefix",
           "endpoint": "$S3_ENDPOINT",
           "region": "${S3_REGION:-us-east-1}",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}",
           "insecureSkipTLS": ${S3_INSECURE_TLS:-false},
           "usePathStyle": ${S3_PATH_STYLE:-false} },
  "afterCopy": "keep",
  "downloadStrategy": "temp_rename",
  "onMismatch": "alert",
  "retention": { "pattern": "$pattern", "keepLast": $keep }
}
EOF
}

# Convenience: create a rule via the HTTP API, asserting success. Echoes
# the rule body so callers can grab fields if needed. P2-7: routes to
# master /syncRule/create (the rule store moved off syncnode).
create_rule() {
  local body="$1"
  local resp
  resp=$(master_rule_create "$body")
  expect_code "$resp" 0 "syncRule/create"
  echo "$resp"
}

delete_rule_silent() {
  local id="$1"
  master_rule_delete "$id" >/dev/null 2>&1 || true
}

# trigger_and_wait <ruleID> [timeout-sec]
#   P2-7: triggers via master /syncRule/trigger (synchronous fire) then
#   polls /syncTask/get for the terminal status. The master returns the
#   new taskID immediately; we poll until succeeded / failed / cancelled
#   or the timeout fires. Echoes the final Record JSON.
trigger_and_wait() {
  local id="$1" t="${2:-${WAIT_TIMEOUT_SEC:-120}}"
  local fired taskID
  fired=$(master_rule_trigger "$id")
  if ! echo "$fired" | jq -e '.code == 0' >/dev/null; then
    echo "$fired"
    return 1
  fi
  taskID=$(echo "$fired" | jq -r '.data.taskID')
  if [ -z "$taskID" ] || [ "$taskID" = "null" ]; then
    log_err "trigger_and_wait: master /syncRule/trigger returned no taskID: $fired"
    return 1
  fi
  if ! wait_for_task_terminal "$taskID" "$t"; then
    return 1
  fi
  echo "$TASK_RECORD"
}
