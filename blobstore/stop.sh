#!/usr/bin/env bash
#
# Stop BlobStore processes started by run.sh / manual nohup from the repo build tree.
# Matches only processes whose command line contains "bin/blobstore/<binary>",
# to avoid killing unrelated processes named "proxy", "access", etc.
#
# Usage: bash blobstore/stop.sh
#

# shellcheck disable=SC2086,SC2046

set -u

kill_by_pattern() {
	local label=$1
	local pattern=$2
	local pids
	pids=$(pgrep -f "${pattern}" 2>/dev/null || true)
	if [[ -z "${pids}" ]]; then
		echo "  (no ${label})"
		return 0
	fi
	echo "  stopping ${label}: $(echo "${pids}" | tr '\n' ' ')"
	kill ${pids} 2>/dev/null || true
}

echo "Stopping BlobStore binaries (cmdline must contain bin/blobstore/<name>)..."

# Ingress / data first, then control plane (best-effort order for SIGTERM).
kill_by_pattern "access" 'bin/blobstore/access'
kill_by_pattern "blobnode" 'bin/blobstore/blobnode'
kill_by_pattern "scheduler" 'bin/blobstore/scheduler'
kill_by_pattern "proxy" 'bin/blobstore/proxy'
kill_by_pattern "clustermgr" 'bin/blobstore/clustermgr'

read -r -p "Stop Consul from build/bin/blobstore? (y/n): " res || true
res=${res:-n}
if [[ "${res}" =~ ^[Yy]$ ]]; then
	kill_by_pattern "consul" 'bin/blobstore/consul'
else
	echo "  (skipped consul)"
fi

read -r -p "Stop Kafka under build/bin/blobstore? (y/n): " res || true
res=${res:-n}
if [[ "${res}" =~ ^[Yy]$ ]]; then
	kill_by_pattern "kafka" 'bin/blobstore/kafka'
else
	echo "  (skipped kafka)"
fi

echo "done."
