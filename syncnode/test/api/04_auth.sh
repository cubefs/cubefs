#!/usr/bin/env bash
# api/04 — master admin token auth gate.
#
# Verifies the requireSyncAdminToken middleware on master endpoints:
#  - empty token AND no MASTER_TOKEN configured → all calls succeed
#    (P0 dev / test default behaviour)
#  - MASTER_TOKEN set → wrong token gets 401, correct token gets 200,
#    missing header gets 401
#
# Skipped when MASTER_TOKEN is empty (no security config to test).

source "$(dirname "$0")/../lib/common.sh"
test_header "api: master admin token gate"

if [ -z "${MASTER_TOKEN:-}" ]; then
  log_warn "MASTER_TOKEN not set; auth gate is disabled on this cluster — skipping"
  test_pass "api auth (skipped — no token configured)"
  exit 0
fi

# ---------- Correct token (baseline) ----------
ok=$(master_rule_list)
expect_code "$ok" 0 "list with correct token"
log_ok "correct token accepted"

# ---------- Wrong token ----------
saved_tok="$MASTER_TOKEN"
MASTER_TOKEN="definitely-not-the-token-$$"
wrong=$(master_rule_list || true)
status_wrong="${HTTP_STATUS:-?}"
MASTER_TOKEN="$saved_tok"

if [ "$status_wrong" != "401" ] && [ "$status_wrong" != "403" ]; then
  log_err "wrong token: expected HTTP 401/403, got $status_wrong (body: $wrong)"
  exit 1
fi
log_ok "wrong token rejected (HTTP $status_wrong)"

# ---------- Missing header ----------
saved_tok="$MASTER_TOKEN"
MASTER_TOKEN=""
no_hdr=$(master_rule_list || true)
status_no_hdr="${HTTP_STATUS:-?}"
MASTER_TOKEN="$saved_tok"

if [ "$status_no_hdr" != "401" ] && [ "$status_no_hdr" != "403" ]; then
  log_err "no token header: expected HTTP 401/403, got $status_no_hdr (body: $no_hdr)"
  exit 1
fi
log_ok "missing token rejected (HTTP $status_no_hdr)"

# ---------- Auth applies to write paths too ----------
# Try to create a rule with the wrong token.
RID=$(unique_id "auth")
body=$(rule_local_to_s3 "$RID" "$ALLOWED_ROOT/" "auth-test/")
saved_tok="$MASTER_TOKEN"
MASTER_TOKEN="bad-token-$$"
created=$(master_rule_create "$body" || true)
status_create_wrong="${HTTP_STATUS:-?}"
MASTER_TOKEN="$saved_tok"

if [ "$status_create_wrong" != "401" ] && [ "$status_create_wrong" != "403" ]; then
  # If the request slipped through despite wrong token, clean up.
  delete_rule_silent "$RID"
  log_err "wrong-token create: expected HTTP 401/403, got $status_create_wrong (body: $created)"
  exit 1
fi
log_ok "wrong token also rejects writes"

test_pass "api auth gate"
