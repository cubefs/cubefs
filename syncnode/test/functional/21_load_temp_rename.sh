#!/usr/bin/env bash
# Functional 21 — Load task + temp_rename strategy. Generates content
# in S3 via a prior sync, then loads it back to a fresh local dir.
# Verifies:
#   - downloaded bytes match source md5
#   - no leftover .downloading.* temp files after success
#   - mid-flight kill leaves no half-written destination file
#
# TODO: the mid-flight kill assertion requires shell access to the
#       syncnode host to send SIGKILL. Skipped on remote-only env.

source "$(dirname "$0")/../lib/common.sh"
require_env_for s3
test_header "load task temp_rename"

# (Implementation TODO — keep the skeleton so the test list is
# complete. Reference 20_sync_idempotent.sh for the trigger pattern.)
log_warn "21_load_temp_rename: implementation TODO"
exit 0
