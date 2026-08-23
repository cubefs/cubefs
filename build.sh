#!/bin/bash
#
# CubeFS build wrapper script
# Supports native and cross-compilation for amd64 and arm64
#

RootPath=$(cd $(dirname $0); pwd)

# Detect host architecture
detect_host_arch() {
    local arch
    arch=$(uname -m)
    case "${arch}" in
        x86_64|amd64)   echo "amd64" ;;
        aarch64|arm64)  echo "arm64" ;;
        *)              echo "unknown" ;;
    esac
}

HOST_ARCH=$(detect_host_arch)

build_linux_x86_64() {
    echo "==> Building CubeFS for linux/amd64"
    make
}

build_linux_arm64_native() {
    echo "==> Building CubeFS for linux/arm64 (native)"
    export PORTABLE=1
    export ARCH=arm64
    export EXTRA_CFLAGS="-fno-strict-aliasing"
    export EXTRA_CXXFLAGS="${EXTRA_CFLAGS}"
    CGO_ENABLED=1 GOOS=linux GOARCH=arm64 make
}

# Cross-compile arm64 from amd64 host using GCC 9 cross-compiler
# Requires: apt-get install -y gcc-9-aarch64-linux-gnu g++-9-aarch64-linux-gnu
build_linux_arm64_gcc9() {
    echo "==> Building CubeFS for linux/arm64 (cross-compile, gcc9)"
    get_rocksdb_compress_dep
    export PORTABLE=1
    export ARCH=arm64
    export CC=aarch64-linux-gnu-gcc-9
    export CXX=aarch64-linux-gnu-g++-9
    export EXTRA_CFLAGS="-Wno-error=deprecated-copy -fno-strict-aliasing -Wclass-memaccess -Wno-error=class-memaccess -Wpessimizing-move -Wno-error=pessimizing-move"
    export EXTRA_CXXFLAGS=$EXTRA_CFLAGS
    CGO_ENABLED=1 GOOS=linux GOARCH=arm64 make
}

# Cross-compile arm64 from amd64 host using GCC 4.9 (for CentOS 7 compat)
# Requires: apt-get install -y gcc-4.9-aarch64-linux-gnu g++-4.9-aarch64-linux-gnu
build_linux_arm64_gcc4() {
    echo "==> Building CubeFS for linux/arm64 (cross-compile, gcc4.9)"
    get_rocksdb_compress_dep
    export PORTABLE=1
    export ARCH=arm64
    export CC=aarch64-linux-gnu-gcc-4.9
    export CXX=aarch64-linux-gnu-g++-4.9
    export EXTRA_CFLAGS="-fno-strict-aliasing"
    export EXTRA_CXXFLAGS=$EXTRA_CFLAGS
    CGO_ENABLED=1 GOOS=linux GOARCH=arm64 make
}

# Download compression library sources for cross-compilation
get_rocksdb_compress_dep() {
    if [ ! -d "${RootPath}/vendor/dep" ]; then
        mkdir -p ${RootPath}/vendor/dep
    fi
    cd ${RootPath}/vendor/dep

    if [ ! -d "${RootPath}/vendor/dep/zlib-1.2.11" ]; then
        wget -q https://zlib.net/fossils/zlib-1.2.11.tar.gz
        tar zxf zlib-1.2.11.tar.gz
    fi

    if [ ! -d "${RootPath}/vendor/dep/bzip2-1.0.6" ]; then
        wget -q https://sourceforge.net/projects/bzip2/files/bzip2-1.0.6.tar.gz
        tar zxf bzip2-1.0.6.tar.gz
    fi

    if [ ! -d "${RootPath}/vendor/dep/zstd-1.4.8" ]; then
        wget -q https://codeload.github.com/facebook/zstd/zip/v1.4.8
        unzip -q v1.4.8
    fi

    if [ ! -d "${RootPath}/vendor/dep/lz4-1.9.3" ]; then
        wget -q https://codeload.github.com/lz4/lz4/tar.gz/v1.9.3
        tar zxf v1.9.3
    fi

    cd ${RootPath}
}

# Normalize CPUTYPE to lowercase
CPUTYPE=$(echo "${CPUTYPE}" | tr 'A-Z' 'a-z')

# If CPUTYPE is not set, auto-detect from host architecture
if [ -z "${CPUTYPE}" ]; then
    case "${HOST_ARCH}" in
        arm64)  CPUTYPE="arm64_native" ;;
        amd64)  CPUTYPE="amd64" ;;
        *)      CPUTYPE="amd64" ;;
    esac
    echo "==> Auto-detected CPUTYPE=${CPUTYPE} (host: $(uname -m))"
fi

echo "==> CPUTYPE=${CPUTYPE}"
case ${CPUTYPE} in
    "arm64_native")
        build_linux_arm64_native
        ;;
    "arm64_gcc9")
        build_linux_arm64_gcc9
        ;;
    "arm64_gcc4"|"arm64")
        build_linux_arm64_gcc4
        ;;
    "amd64"|*)
        build_linux_x86_64
        ;;
esac
