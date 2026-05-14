#!/usr/bin/env bash
# Integration 52 — SEC3 body cap. POST a 2 MB body to
# /syncNode/dispatch; expect rejection (the cap is 1 MB).

source "$(dirname "$0")/../lib/common.sh"
require_env_for master
test_header "master /syncNode/dispatch body cap"

big=$(mktemp -t syncnode-big.XXXXXX)
trap "rm -f $big" EXIT
# 2 MB of valid-ish JSON-ish bytes. The body is malformed JSON; the
# point is the MaxBytesReader rejection happens BEFORE the JSON decode.
dd if=/dev/zero of="$big" bs=1M count=2 status=none

auth=()
[ -n "${MASTER_TOKEN:-}" ] && auth=(-H "Authorization: Bearer $MASTER_TOKEN")

status=$(curl -sS -o /dev/null -w '%{http_code}' \
  "${auth[@]}" \
  -X POST \
  -H 'Content-Type: application/json' \
  --data-binary @"$big" \
  "${MASTER_HTTP%/}/syncNode/dispatch")

case "$status" in
  4*) log_ok "oversized body rejected with HTTP $status" ;;
  *)  test_fail "expected 4xx for oversized body, got $status" ;;
esac

test_pass "body cap"
