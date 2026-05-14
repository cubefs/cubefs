#!/usr/bin/env bash
# 71_network_partition — TODO implementation.
# See README.md "What gets created on your cluster" for the AC.
#
# Outline:
#   1. Set up fixtures (rule + payload).
#   2. Drive the scenario (trigger / kill / etc.).
#   3. Assert observables via syncnode_get + assert_json_*.
#   4. Cleanup via trap_cleanup.

source "$(dirname "$0")/../lib/common.sh"
test_header "71_network_partition"

log_warn "71_network_partition: skeleton — implementation TODO"
exit 0
