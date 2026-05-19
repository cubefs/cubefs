#!/usr/bin/env bash

# shellcheck disable=SC2046
RootPath=$(cd $(dirname "$0") || exit 1; pwd)
# shellcheck source=/dev/null
source "${RootPath}/run_docker.env"

# Detect target platform (default to host architecture)
TARGETPLATFORM=${TARGETPLATFORM:-"linux/$(uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')"}
echo "==> Building Docker images for platform: ${TARGETPLATFORM}"

docker build --platform "${TARGETPLATFORM}" -t "${IMAGE}" -f "${RootPath}/Dockerfile" "${RootPath}"
docker build --platform "${TARGETPLATFORM}" -t "${IMAGELTP}" -f "${RootPath}/Dockerfile-ltp" "${RootPath}"
