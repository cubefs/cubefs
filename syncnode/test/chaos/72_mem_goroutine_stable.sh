#!/usr/bin/env bash
# 72_mem_goroutine_stable — TODO implementation.
# See README.md "What gets created on your cluster" for the AC.
#
# Outline:
#   1. Set up fixtures (rule + payload).
#   2. Drive the scenario (trigger / kill / etc.).
#   3. Assert observables via syncnode_get + assert_json_*.
#   4. Cleanup via trap_cleanup.

source "$(dirname "$0")/../lib/common.sh"
test_header "72_mem_goroutine_stable"

log_warn "72_mem_goroutine_stable: skeleton — implementation TODO"
exit 0
