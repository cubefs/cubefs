# shellcheck shell=bash
# Required environment for syncnode integration tests. Copy this file to
# env.sh, fill in real values, then `source env.sh` before running
# `./run.sh`.

# ---- syncnode under test ----
export SYNCNODE_HOST="127.0.0.1"
export SYNCNODE_HTTP_PORT="17911"
export SYNCNODE_TCP_PORT="17910"
# adminToken from sync.json. Empty disables auth (matches dev / pre-fix
# behaviour) — production should be set.
export SYNCNODE_TOKEN=""

# ---- master cluster ----
# Comma-separated multi-master URLs accepted by curl as space-separated
# in shell expansion is fine — tests fall back to the first reachable.
export MASTER_HTTP="http://127.0.0.1:17010"
# syncAdminToken from master.json. Often equal to SYNCNODE_TOKEN if you
# share infra. Empty disables auth.
export MASTER_TOKEN=""

# ---- S3-compatible backend used by sync/load tests ----
# Endpoint MUST start with http:// or https://.
export S3_ENDPOINT="http://127.0.0.1:9000"
export S3_BUCKET="syncnode-it"
# These are the ENV-VAR NAMES the syncnode reads creds from. Match the
# `s3Defaults.accessKeyEnv` / `secretKeyEnv` in sync.json — the test
# scripts pass these names through so rules pick up the right creds.
export S3_AK="AWS_ACCESS_KEY_ID"
export S3_SK="AWS_SECRET_ACCESS_KEY"
# Actual creds (for s3cmd to verify content). Loaded into the names
# above by run.sh so subprocesses see them.
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

# ---- working set ----
# Tests create fixtures under this dir. MUST be under one of the
# allowedRoots configured in sync.json's posix.allowedRoots.
export TEST_DATA_DIR="/tmp/syncnode-it"
export ALLOWED_ROOT="/tmp"

# ---- tunables ----
# Verbose curl + colored log
export RUN_VERBOSE="${RUN_VERBOSE:-0}"
# Per-test timeout in seconds (0 = no limit)
export TEST_TIMEOUT="${TEST_TIMEOUT:-300}"
# How long the wait=true endpoints can block before the runner gives up
export WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-120}"
