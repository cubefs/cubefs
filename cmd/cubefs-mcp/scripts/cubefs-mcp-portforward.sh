#!/usr/bin/env bash
# cubefs-mcp wrapper (port-forward orchestrator):
#   - selects any live cubefs-master pod (DaemonSet has no service exposed)
#   - port-forwards $PORT -> 127.0.0.1:$PORT
#   - exports CUBEFS_MASTER_ADDR, then exec's the real cubefs-mcp binary on stdio
#   - traps EXIT so the port-forward dies together with mcp
#
# Install (local user):
#   mkdir -p ~/.local/bin ~/.local/libexec
#   install -m 0755 cubefs-mcp-portforward.sh ~/.local/bin/cubefs-mcp
#   # build the Go binary and put it where the wrapper exec's:
#   go build -o ~/.local/libexec/cubefs-mcp ./cmd/cubefs-mcp
#
# Cluster binding is via env (set by MCP config or the caller):
#   KUBECONFIG          required, points at the target cluster
#   NS                  cubefs namespace        (default: storage-cfs)
#   PORT                master listen port      (default: 17010)
#   MCP_BIN             cubefs-mcp binary path  (default: ~/.local/libexec/cubefs-mcp)
#   LOG                 port-forward log file   (default: /tmp/cubefs-mcp-pf.log)
#   CUBEFS_AUTH_TOKEN   passed through to cubefs-mcp if set
set -euo pipefail

if [[ -z "${KUBECONFIG:-}" ]]; then
  echo "cubefs-mcp: KUBECONFIG is required (set it in MCP config env)" >&2
  exit 1
fi
export KUBECONFIG

NS="${NS:-storage-cfs}"
PORT="${PORT:-17010}"
MCP_BIN="${MCP_BIN:-$HOME/.local/libexec/cubefs-mcp}"
LOG="${LOG:-/tmp/cubefs-mcp-pf.log}"

if [[ ! -x "$MCP_BIN" ]]; then
  echo "cubefs-mcp: binary not found or not executable: $MCP_BIN" >&2
  exit 1
fi

pod=$(kubectl -n "$NS" get pod -o name 2>/dev/null \
  | grep -m1 '^pod/cubefs-master-' || true)
if [[ -z "$pod" ]]; then
  echo "cubefs-mcp: no cubefs-master pod found in ns=$NS (KUBECONFIG=$KUBECONFIG)" >&2
  exit 1
fi

kubectl -n "$NS" port-forward "$pod" "$PORT:$PORT" >"$LOG" 2>&1 &
pf=$!
trap 'kill "$pf" 2>/dev/null || true' EXIT

# Wait up to ~5s for the local port to accept connections.
for _ in $(seq 1 50); do
  if (echo >"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

export CUBEFS_MASTER_ADDR="http://127.0.0.1:$PORT"
exec "$MCP_BIN"
