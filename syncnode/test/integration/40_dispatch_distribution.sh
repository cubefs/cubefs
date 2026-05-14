#!/usr/bin/env bash
# Integration 40 — P1-1/P1-2 dispatcher load distribution. Triggers N
# tasks via the master dispatch endpoint and verifies they're spread
# across the syncnode fleet within the spec tolerance (std-dev ≤ 30%
# of mean).
#
# Pre-req: ≥ 3 syncnodes registered + a rule that exists on every node
# (deployed via sync.json). The rule ID is read from $DISPATCH_RULE_ID
# (default "smoke-noop").

source "$(dirname "$0")/../lib/common.sh"
require_env_for master
test_header "P1-2 dispatch load distribution"

# Sanity: at least 3 active syncnodes
list=$(master_get /syncNode/list)
n_active=$(echo "$list" | jq '[.data[] | select(.loadScore != null and .loadScore < 1.5)] | length')
if [ "$n_active" -lt 3 ]; then
  log_warn "only $n_active active syncnodes; spec requires ≥ 3 — skipping"
  exit 0
fi
log_info "$n_active active syncnodes in the fleet"

RULE_ID="${DISPATCH_RULE_ID:-smoke-noop}"
N=10

# Dispatch N tasks via master. Each task gets a unique id so the
# dispatcher's tie-break doesn't always pick the same node.
dispatched=()
for i in $(seq 1 "$N"); do
  TID="$(unique_id dist)-$i"
  body=$(cat <<EOF
{
  "id": "$TID",
  "opcode": 121,
  "Request": { "taskId": "$TID", "ruleId": "$RULE_ID" }
}
EOF
)
  resp=$(master_post /syncNode/dispatch "$body")
  if [ "$(echo "$resp" | jq -r '.code')" != "0" ]; then
    log_warn "dispatch $i failed: $resp"
    continue
  fi
  dispatched+=("$(echo "$resp" | jq -r '.data.node // empty')")
done

if [ "${#dispatched[@]}" -lt "$N" ]; then
  log_warn "${#dispatched[@]}/$N dispatches succeeded — accuracy of stddev test reduced"
fi

# Build a histogram
declare -A hist
for n in "${dispatched[@]}"; do
  [ -z "$n" ] && continue
  hist[$n]=$(( ${hist[$n]:-0} + 1 ))
done

log_info "owner histogram:"
total=0
for n in "${!hist[@]}"; do
  log_info "  $n: ${hist[$n]}"
  total=$((total + hist[$n]))
done

# Compute std-dev / mean and assert ≤ 30%
buckets=${#hist[@]}
if [ "$buckets" -lt 2 ]; then
  test_fail "all tasks landed on a single node — load balancing broken"
fi
mean=$(echo "scale=4; $total / $buckets" | bc)
var=0
for n in "${!hist[@]}"; do
  d=$(echo "${hist[$n]} - $mean" | bc -l)
  var=$(echo "$var + $d*$d" | bc -l)
done
var=$(echo "scale=4; $var / $buckets" | bc -l)
stddev=$(echo "scale=4; sqrt($var)" | bc -l)
ratio=$(echo "scale=4; $stddev / $mean" | bc -l)

log_info "mean=$mean stddev=$stddev stddev/mean=$ratio"
if (( $(echo "$ratio > 0.30" | bc -l) )); then
  test_fail "load distribution stddev/mean = $ratio > 0.30 (spec §P1-2)"
fi

test_pass "dispatch distribution"
