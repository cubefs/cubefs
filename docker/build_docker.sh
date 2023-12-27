#!/usr/bin/env bash

# shellcheck disable=SC2046
RootPath=$(cd $(dirname "$0") || exit 1; pwd)
# shellcheck source=/dev/null
source "${RootPath}/run_docker.env"

docker build -t "${IMAGE}" -f "${RootPath}/Dockerfile" "${RootPath}"
docker build -t "${IMAGELTP}" -f "${RootPath}/Dockerfile-ltp" "${RootPath}"
