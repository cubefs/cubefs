#!/bin/bash
# Entrypoint for the pjd-fstest container.
#
# Inputs (env):
#   MOUNT_DIR     - root of the mounted target volume (default /mnt/target)
#   SUBDIR        - unique subdir under MOUNT_DIR for this run (default pjd-$$)
#   TEST_FILTER   - optional space-separated test paths under PJD_TESTS
#                   (e.g. "rename/00.t chmod"); if empty, run the whole tree.
#
# Output:
#   stdout — full TAP stream from prove; the dashboard backend parses this.
#   exit code — 0 even when individual tests fail (so the K8s Job stays
#               in "Succeeded" state and the backend can still fetch logs).
#               Real Job failures (mount missing, prove missing, etc.) exit 1.
set -e

MOUNT_DIR="${MOUNT_DIR:-/mnt/target}"
SUBDIR="${SUBDIR:-pjd-$$}"
WORK="${MOUNT_DIR}/${SUBDIR}"

if [ ! -d "${MOUNT_DIR}" ]; then
  echo "FATAL: mount dir '${MOUNT_DIR}' does not exist" >&2
  exit 1
fi

mkdir -p "${WORK}"
cd "${WORK}"

# Resolve test paths. PJD_TESTS is set by the Dockerfile.
if [ -n "${TEST_FILTER}" ]; then
  TESTS=""
  for p in ${TEST_FILTER}; do
    TESTS="${TESTS} ${PJD_TESTS}/${p}"
  done
else
  TESTS="${PJD_TESTS}"
fi

# Run prove with -v (verbose) so each test emits an "ok N" or "not ok N"
# TAP line that the dashboard's TAP parser can pick up. --merge folds stderr
# into stdout. --exec ensures bash is used so the test scripts' shebangs
# are respected even on minimal images.
# Test failures must NOT propagate as a non-zero exit (we want the Job to
# Succeed so the dashboard can fetch the TAP output).
prove -r -v --merge --timer --exec 'bash -x' ${TESTS} || true

# Tidy up the working dir; if the test left files behind (some tests do on
# failure) the cleanup is best-effort and never blocks Job success.
cd /tmp
rm -rf "${WORK}" 2>/dev/null || true

exit 0
