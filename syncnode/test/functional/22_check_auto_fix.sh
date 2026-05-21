#!/usr/bin/env bash
# Functional 22 — Check task with onMismatch=auto_fix. Seeds a sync,
# corrupts one S3 object (size mismatch), runs check with auto_fix,
# verifies the object was rewritten back to match.
#
# TODO: requires s3cmd (or aws-cli) to mutate the dst-side object.

source "$(dirname "$0")/../lib/common.sh"
require_env_for s3
test_header "check + auto_fix"

log_warn "22_check_auto_fix: implementation TODO (needs s3cmd)"
exit 0
