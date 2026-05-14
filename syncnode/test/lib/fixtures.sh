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
           "region": "us-east-1",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}" },
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
           "region": "us-east-1",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}" },
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
           "region": "us-east-1",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}" },
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
           "region": "us-east-1",
           "accessKeyEnv": "${S3_AK:-AWS_ACCESS_KEY_ID}",
           "secretKeyEnv": "${S3_SK:-AWS_SECRET_ACCESS_KEY}" },
  "afterCopy": "keep",
  "downloadStrategy": "temp_rename",
  "onMismatch": "alert",
  "retention": { "pattern": "$pattern", "keepLast": $keep }
}
EOF
}

# Convenience: create a rule via the HTTP API, asserting success. Echoes
# the rule body so callers can grab fields if needed.
create_rule() {
  local body="$1"
  local resp
  resp=$(syncnode_post /admin/sync/rule/create "$body")
  expect_code "$resp" 0 "rule/create"
  echo "$resp"
}

delete_rule_silent() {
  local id="$1"
  syncnode_post "/admin/sync/rule/delete?id=$id" >/dev/null 2>&1 || true
}

# trigger_and_wait <ruleID> [timeout-sec]
#   Hits /admin/sync/task/trigger?wait=true and waits for terminal.
#   Echoes the final Record JSON. timeout defaults to $WAIT_TIMEOUT_SEC.
trigger_and_wait() {
  local id="$1" t="${2:-${WAIT_TIMEOUT_SEC:-120}}"
  HTTP_MAX_TIME="$t" syncnode_post "/admin/sync/task/trigger?ruleID=$id&wait=true"
}
