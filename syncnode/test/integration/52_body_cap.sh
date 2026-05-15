#!/usr/bin/env bash
# Integration 52 — SEC3 body cap. POST a 2 MB body to
# /syncNode/dispatch; expect rejection (the cap is 1 MB).
#
# CubeFS master always replies HTTP 200; errors are signalled via the
# JSON envelope's "code" field.  A non-zero code means the body was
# rejected before any business logic ran.

source "$(dirname "$0")/../lib/common.sh"
require_env_for master
test_header "master /syncNode/dispatch body cap"

big=$(mktemp -t syncnode-big.XXXXXX)
trap "rm -f $big" EXIT
# 2 MB of zeros — malformed JSON. The point is MaxBytesReader fires
# during io.ReadAll, before the JSON decode attempt.
dd if=/dev/zero of="$big" bs=1M count=2 status=none

auth=()
[ -n "${MASTER_TOKEN:-}" ] && auth=(-H "Authorization: Bearer $MASTER_TOKEN")

resp=$(curl -sS \
  ${auth[@]+"${auth[@]}"} \
  -X POST \
  -H 'Content-Type: application/json' \
  --data-binary @"$big" \
  "${MASTER_HTTP%/}/syncNode/dispatch")

code=$(echo "$resp" | jq -r '.code // 0')
msg=$(echo  "$resp" | jq -r '.msg  // ""')

if [ "$code" = "0" ]; then
  test_fail "expected non-zero code for oversized body, got code=0; resp=$resp"
fi
log_ok "oversized body rejected (code=$code msg=$msg)"

test_pass "body cap"
