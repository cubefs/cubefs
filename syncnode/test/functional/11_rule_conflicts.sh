#!/usr/bin/env bash
# Functional 11 — E-4 conflict detection. Covers all three classes:
# duplicate-pair (1014) / prefix-overlap (1015) / cycle-sync (1016).
# Server-side startup conflict validation isn't tested here (needs a
# config restart); we exercise the create-time path only.

source "$(dirname "$0")/../lib/common.sh"
test_header "rule conflict detection"

base=$(unique_id "conf")
R1="${base}-a"
R2="${base}-b"
R3="${base}-c"

cleanup_func_11() {
  delete_rule_silent "$R1"
  delete_rule_silent "$R2"
  delete_rule_silent "$R3"
}
trap_cleanup cleanup_func_11

# ---- Duplicate pair (code 1014): same src + same dst ----
r1=$(cat <<EOF
{ "id": "$R1", "type": "sync",
  "src": { "kind": "local", "path": "$ALLOWED_ROOT/dup/" },
  "dst": { "kind": "s3", "bucket": "$S3_BUCKET", "prefix": "conf-dup/" },
  "afterCopy": "keep", "downloadStrategy": "temp_rename", "onMismatch": "alert" }
EOF
)
expect_code "$(syncnode_post /admin/sync/rule/create "$r1")" 0

r2=$(cat <<EOF
{ "id": "$R2", "type": "sync",
  "src": { "kind": "local", "path": "$ALLOWED_ROOT/dup/" },
  "dst": { "kind": "s3", "bucket": "$S3_BUCKET", "prefix": "conf-dup/" },
  "afterCopy": "keep", "downloadStrategy": "temp_rename", "onMismatch": "alert" }
EOF
)
dup=$(syncnode_post /admin/sync/rule/create "$r2")
expect_code "$dup" 1014 "duplicate src+dst rejection"
log_ok "code 1014 fired for duplicate pair"

# ---- Prefix overlap (1015): same backend pair, paths overlap ----
r3=$(cat <<EOF
{ "id": "$R3", "type": "sync",
  "src": { "kind": "local", "path": "$ALLOWED_ROOT/dup/sub/" },
  "dst": { "kind": "s3", "bucket": "$S3_BUCKET", "prefix": "conf-dup/inner/" },
  "afterCopy": "keep", "downloadStrategy": "temp_rename", "onMismatch": "alert" }
EOF
)
ov=$(syncnode_post /admin/sync/rule/create "$r3")
expect_code "$ov" 1015 "prefix overlap rejection"
log_ok "code 1015 fired for prefix overlap"

# ---- Cycle sync (1016): A: local→s3 + B: s3→local same path ----
delete_rule_silent "$R1"      # clean parent
R4="${base}-fwd"
R5="${base}-rev"
trap_cleanup() { :; }
cleanup_func_11_cycle() { delete_rule_silent "$R4"; delete_rule_silent "$R5"; }
trap_cleanup cleanup_func_11_cycle

fwd=$(cat <<EOF
{ "id": "$R4", "type": "sync",
  "src": { "kind": "local", "path": "$ALLOWED_ROOT/cyc/" },
  "dst": { "kind": "s3", "bucket": "$S3_BUCKET", "prefix": "cyc/" },
  "afterCopy": "keep", "downloadStrategy": "temp_rename", "onMismatch": "alert" }
EOF
)
expect_code "$(syncnode_post /admin/sync/rule/create "$fwd")" 0

rev=$(cat <<EOF
{ "id": "$R5", "type": "sync",
  "src": { "kind": "s3", "bucket": "$S3_BUCKET", "prefix": "cyc/" },
  "dst": { "kind": "local", "path": "$ALLOWED_ROOT/cyc/" },
  "afterCopy": "keep", "downloadStrategy": "temp_rename", "onMismatch": "alert" }
EOF
)
cyc=$(syncnode_post /admin/sync/rule/create "$rev")
expect_code "$cyc" 1016 "cycle sync rejection"
log_ok "code 1016 fired for cycle sync"

test_pass "rule conflict detection"
