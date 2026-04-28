#!/bin/bash

RootPath=$(cd $(dirname ${BASH_SOURCE[0]})/..; pwd)
BuildPath=${RootPath}/build
BuildOutPath=${BuildPath}/out
BuildBinPath=${BuildPath}/bin
BuildDependsLibPath=${BuildPath}/lib
BuildDependsIncludePath=${BuildPath}/include
VendorPath=${RootPath}/vendor
DependsPath=${RootPath}/depends
use_clang=$(echo ${CC} | grep "clang" | grep -v "grep")
cgo_ldflags="-L${BuildDependsLibPath} -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"
if [ "${use_clang}" != "" ]; then
    cgo_ldflags="-L${BuildDependsLibPath} -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lc++"
fi
cgo_cflags="-I${BuildDependsIncludePath}"
cgo_cxxflags="-I${BuildDependsIncludePath}"
MODFLAGS=""
gomod="on"
if [ "${2:-}" == "on" ] || [ "${2:-}" == "off" ]; then
    gomod="${2}"
fi

if [ "${gomod}" == "off" ]; then
    MODFLAGS="-mod=vendor"
fi

if [ ! -d "${BuildOutPath}" ]; then
    mkdir ${BuildOutPath}
fi

if [ ! -d "${BuildBinPath}" ]; then
    mkdir ${BuildBinPath}
fi

if [ ! -d "${BuildBinPath}/blobstore" ]; then
    mkdir ${BuildBinPath}/blobstore
fi

if [ ! -d "${BuildDependsLibPath}" ]; then
    mkdir ${BuildDependsLibPath}
fi

if [ ! -d "${BuildDependsIncludePath}" ]; then
    mkdir ${BuildDependsIncludePath}
fi

RM=$(find /bin /sbin /usr/bin /usr/local -name "rm" | head -1)
if [[ "-x$RM" == "-x" ]] ; then
    RM=rm
fi

Version=$(git describe --abbrev=0 --tags 2>/dev/null)
BranchName=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
CommitID=$(git rev-parse HEAD 2>/dev/null)
BuildTime=$(date +%Y-%m-%d\ %H:%M)
LDFlags="-X 'github.com/cubefs/cubefs/proto.Version=${Version}' \
    -X 'github.com/cubefs/cubefs/proto.CommitID=${CommitID}' \
    -X 'github.com/cubefs/cubefs/proto.BranchName=${BranchName}' \
    -X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}' \
    -X 'github.com/cubefs/cubefs/blobstore/util/version.version=${BranchName}/${CommitID}' \
    -w -s"

NPROC=$(nproc 2>/dev/null)
if [ -e /sys/fs/cgroup/cpu ] ; then
    NPROC=4
fi
NPROC=${NPROC:-"1"}

case $(uname -s | tr 'A-Z' 'a-z') in
    "linux"|"darwin")
        ;;
    *)
        echo "Current platform $(uname -s) not support";
        exit1;
        ;;
esac

CPUTYPE=${CPUTYPE} | tr 'A-Z' 'a-z'

# Script flow overview:
# 1. Prepare build paths, version flags, and platform-specific defaults.
# 2. Build the native third-party libraries needed by CGO targets.
# 3. Initialize the GOPATH-style workspace used by downstream go commands.
# 4. Run the requested build, unit test, or coverage workflow.
# 5. Optionally clean generated binaries or the local dependency cache.
#
# Build the vendored zlib static library and install its headers if missing.
build_zlib() {
    ZLIB_VER=1.2.13
    if [ -f "${BuildDependsLibPath}/libz.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/zlib-${ZLIB_VER} ]; then
        tar -zxf ${DependsPath}/zlib-${ZLIB_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build zlib..."
    pushd ${BuildOutPath}/zlib-${ZLIB_VER}
    CFLAGS='-fPIC' ./configure --static
    make -j$1
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libz.a ${BuildDependsLibPath}
    cp -f zlib.h zconf.h ${BuildDependsIncludePath}
    popd
}

# Build the vendored bzip2 static library and install its headers if missing.
build_bzip2() {
    BZIP2_VER=1.0.6
    if [ -f "${BuildDependsLibPath}/libbz2.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/bzip2-bzip2-${BZIP2_VER} ]; then
        tar -zxf ${DependsPath}/bzip2-bzip2-${BZIP2_VER}.tar.gz -C ${BuildOutPath}
        if [ "${use_clang}" != "" ]; then
            sed -i '18d' ${BuildOutPath}/bzip2-bzip2-${BZIP2_VER}/Makefile
        fi
    fi

    echo "build bzip2..."
    pushd ${BuildOutPath}/bzip2-bzip2-${BZIP2_VER}
    make -j$1 CFLAGS='-fPIC -O2 -g -D_FILE_OFFSET_BITS=64'
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libbz2.a ${BuildDependsLibPath}
    cp -f bzlib.h bzlib_private.h ${BuildDependsIncludePath}
    popd
}

# Build the vendored lz4 static library and install its headers if missing.
build_lz4() {
    LZ4_VER=1.8.3
    if [ -f "${BuildDependsLibPath}/liblz4.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/lz4-${LZ4_VER} ]; then
        tar -zxf ${DependsPath}/lz4-${LZ4_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build lz4..."
    pushd ${BuildOutPath}/lz4-${LZ4_VER}/lib
    make -j$1 CFLAGS='-fPIC -O2'
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f liblz4.a ${BuildDependsLibPath}
    cp -f lz4frame_static.h lz4.h lz4hc.h lz4frame.h ${BuildDependsIncludePath}
    popd
}

# Build the vendored zstd static library and install its headers if missing.
build_zstd() {
    ZSTD_VER=1.4.0
    if [ -f "${BuildDependsLibPath}/libzstd.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/zstd-${ZSTD_VER} ]; then
        tar -zxf ${DependsPath}/zstd-${ZSTD_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build zstd..."
    pushd ${BuildOutPath}/zstd-${ZSTD_VER}/lib
    make -j$1 CFLAGS='-fPIC -O2'
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libzstd.a ${BuildDependsLibPath}
    cp -f zstd.h common/zstd_errors.h deprecated/zbuff.h dictBuilder/zdict.h ${BuildDependsIncludePath}
    popd
}


# Build the vendored snappy static library and install its headers if missing.
build_snappy() {
    SNAPPY_VER=1.1.7
    if [ -f "${BuildDependsLibPath}/libsnappy.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/snappy-${SNAPPY_VER} ]; then
        tar -zxf ${DependsPath}/snappy-${SNAPPY_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build snappy..."
    mkdir ${BuildOutPath}/snappy-${SNAPPY_VER}/build
    pushd ${BuildOutPath}/snappy-${SNAPPY_VER}/build
    cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DSNAPPY_BUILD_TESTS=OFF .. && make -j$1
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libsnappy.a ${BuildDependsLibPath}
    cp -f ../snappy-c.h ../snappy-sinksource.h ../snappy.h snappy-stubs-public.h ${BuildDependsIncludePath}
    popd
}

# Build the vendored tcmalloc static library and install its headers if missing.
build_tcmalloc() {
    TCMALLOC_VER=2.9.1
    if [ -f "${BuildDependsLibPath}/libtcmalloc.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/gperftools-gperftools-${TCMALLOC_VER} ]; then
        tar -zxf ${DependsPath}/gperftools-gperftools-${TCMALLOC_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build tcmalloc..."
    # mkdir ${BuildOutPath}/gperftools-gperftools-${TCMALLOC_VER}
    pushd ${BuildOutPath}/gperftools-gperftools-${TCMALLOC_VER}
    ./autogen.sh
    CFLAGS='-fPIC' ./configure --enable-frame-pointers
    make -j ${PROCESSOR_NUMS}
    cp -f .libs/libtcmalloc.a ${BuildDependsLibPath}
    cp -rf src/gperftools ${BuildDependsIncludePath}
    popd
}

# Build the vendored rocksdb static library against the prepared compression libraries.
build_rocksdb() {
    ROCKSDB_VER=6.3.6
    if [ -f "${BuildDependsLibPath}/librocksdb.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/rocksdb-${ROCKSDB_VER} ]; then
        tar -zxf ${DependsPath}/rocksdb-${ROCKSDB_VER}.tar.gz -C ${BuildOutPath}
        pushd ${BuildOutPath} > /dev/null
        sed -i '1069s/newf/\&newf/' rocksdb-${ROCKSDB_VER}/db/db_impl/db_impl_compaction_flush.cc
        sed -i '1161s/newf/\&newf/' rocksdb-${ROCKSDB_VER}/db/db_impl/db_impl_compaction_flush.cc
        sed -i '412s/pair/\&pair/' rocksdb-${ROCKSDB_VER}/options/options_parser.cc
        sed -i '63s/std::mutex/mutable std::mutex/' rocksdb-${ROCKSDB_VER}/util/channel.h
        popd
    fi

    echo "build rocksdb..."
    pushd ${BuildOutPath}/rocksdb-${ROCKSDB_VER}
    if [ "${use_clang}" != "" ]; then
        FLAGS="-Wno-error=deprecated-copy -Wno-error=pessimizing-move -Wno-error=shadow -Wno-error=unused-but-set-variable"
    else
        CCMAJOR=`gcc -dumpversion | awk -F. '{print $1}'`
        if [ ${CCMAJOR} -ge 9 ]; then
            FLAGS="-Wno-error=deprecated-copy -Wno-error=pessimizing-move"
        fi
    fi
    FLAGS="${FLAGS} -Wno-unused-variable -Wno-unused-function"
    PORTABLE=1 make -j$1 EXTRA_CXXFLAGS="-fPIC ${FLAGS} -DZLIB -DBZIP2 -DSNAPPY -DLZ4 -DZSTD -I${BuildDependsIncludePath}" static_lib
    if [ $? -ne 0 ]; then
        exit 1
    fi
    make install-static INSTALL_PATH=${BuildPath}
    strip -S -x ${BuildDependsLibPath}/librocksdb.a
    popd
}

# Prepare the GOPATH-style workspace and symlink used by subsequent go commands.
init_gopath() {
    export GO111MODULE=${gomod}
    export GOPATH=$HOME/tmp/cfs/go

    mkdir -p $GOPATH/src/github.com/cubefs
    SrcPath=$GOPATH/src/github.com/cubefs/cubefs
    BlobPath=${SrcPath}/blobstore
    if [ -L "$SrcPath" ]; then
        $RM -f $SrcPath
    fi
    if [  ! -e "$SrcPath" ] ; then
        ln -s $RootPath $SrcPath 2>/dev/null
    fi
}

# Build native dependencies, export CGO flags, and initialize the build workspace.
pre_build() {
    build_zlib $1
    build_bzip2 $1
    build_lz4 $1
    build_zstd $1
    build_snappy $1
    build_tcmalloc $1
    build_rocksdb $1

    export CGO_CFLAGS=${cgo_cflags}
    export CGO_LDFLAGS="${cgo_ldflags}"
    export CGO_CXXFLAGS=${cgo_cxxflags}

    init_gopath
}

# Temporarily switch CGO link flags to the tcmalloc-enabled variant.
build_with_tcmalloc() {
    cgo_ldflags_tcmalloc="-L${BuildDependsLibPath} -ldl -ltcmalloc -lm -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"
    if [ "${use_clang}" != "" ]; then
        cgo_ldflags_tcmalloc="-L${BuildDependsLibPath} -ldl -ltcmalloc -lm -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lc++"
    fi
    export CGO_LDFLAGS="${cgo_ldflags_tcmalloc}"
}

# Run the standard unit test suite and emit a single coverage profile.
run_test() {
    pushd $SrcPath >/dev/null
    export JENKINS_TEST=1
    ulimit -n 65536
    echo -n "${TPATH}"
    go test -cover -v -coverprofile=cover.output $(go list ./... | grep -v depends) | tee cubefs_unittest.output
    ret=$?
    popd >/dev/null
    exit $ret
}

# Keep the intended coverage scope and the execution shards separate so split
# go test runs can be verified without treating any single module as special.
# Append a temporary coverage profile into the merged report while skipping its header.
_cover_merge_profile() {
    local src dst
    src="$1"
    dst="$2"
    sed '1d' "${src}" >> "${dst}" && rm -f "${src}"
}

# Run the optional incremental Go coverage gate against the merged coverprofile.
_run_incremental_coverage_check() {
    local coverprofile checker
    coverprofile="$1"
    checker="${RootPath}/build/check_incremental_go_coverage.py"
    if [ -z "${incremental_coverage_threshold}" ]; then
        return 0
    fi
    if [ ! -f "${checker}" ]; then
        echo "ERROR: incremental coverage checker not found: ${checker}"
        return 1
    fi
    echo "Running incremental Go coverage check (threshold=${incremental_coverage_threshold}${incremental_coverage_base:+, base=${incremental_coverage_base}})"
    if [ -n "${incremental_coverage_base}" ]; then
        PYTHONDONTWRITEBYTECODE=1 python3 "${checker}" \
            --repo "${RootPath}" \
            --coverprofile "${coverprofile}" \
            --threshold "${incremental_coverage_threshold}" \
            --base "${incremental_coverage_base}"
    else
        PYTHONDONTWRITEBYTECODE=1 python3 "${checker}" \
            --repo "${RootPath}" \
            --coverprofile "${coverprofile}" \
            --threshold "${incremental_coverage_threshold}"
    fi
}

# Verify that the union of split coverage shards exactly matches the intended package scope.
_cover_verify_scope_matches_shards() {
    local label expected_fn tmpd expected actual missing extra shard_fn
    label="$1"
    expected_fn="$2"
    shift 2

    tmpd=$(mktemp -d) || return 1
    expected="${tmpd}/expected"
    actual="${tmpd}/actual"
    "${expected_fn}" | sort -u >"${expected}"
    for shard_fn in "$@"; do
        "${shard_fn}"
    done | sort -u >"${actual}"
    missing=$(comm -23 "${expected}" "${actual}")
    extra=$(comm -13 "${expected}" "${actual}")
    rm -rf "${tmpd}"
    if [ -n "${missing}" ] || [ -n "${extra}" ]; then
        echo "ERROR: ${label} package list mismatch (expected coverage scope vs union of go test args)."
        if [ -n "${missing}" ]; then
            echo "Packages in scope but not covered by any go test run:"
            echo "${missing}"
        fi
        if [ -n "${extra}" ]; then
            echo "Packages in go test runs but outside intended scope (adjust ${label} shard definitions):"
            echo "${extra}"
        fi
        return 1
    fi
    return 0
}

# List the full package scope for the combined testcover workflow.
_cover_list_run_test_cover_scope() {
    go list ./... | grep -v depends | grep -v '/blobstore/cmd' | grep -v '/blobstore/common/tcmalloc'
}

# List the primary shard for the combined testcover workflow.
_cover_list_run_test_cover_shard_main() {
    _cover_list_run_test_cover_scope | grep -v /blobstore/shardnode
}

# List an additional shard that is merged back into the combined testcover report.
_cover_list_run_test_cover_shard_extra_1() {
    go list ./... | grep /blobstore/shardnode/catalog/allocator
}

# List an additional shard that is merged back into the combined testcover report.
_cover_list_run_test_cover_shard_extra_2() {
    go list ./... | grep /blobstore/shardnode | grep -v /catalog/allocator
}

# Check that all split shards for testcover still cover the full intended package set.
_cover_verify_run_test_cover_union() {
    _cover_verify_scope_matches_shards \
        "run_test_cover" \
        _cover_list_run_test_cover_scope \
        _cover_list_run_test_cover_shard_main \
        _cover_list_run_test_cover_shard_extra_1 \
        _cover_list_run_test_cover_shard_extra_2
}

# List the full package scope for the cubefs-only coverage workflow.
_cover_list_run_test_cover_cubefs_scope() {
    go list ./... | grep -v /cubefs/depends/ | grep -v /cubefs/blobstore
}

# List the primary shard for the cubefs-only coverage workflow.
_cover_list_run_test_cover_cubefs_shard_main() {
    _cover_list_run_test_cover_cubefs_scope
}

# Check that all split shards for testcovercubefs still cover the full intended package set.
_cover_verify_run_test_cover_cubefs_union() {
    _cover_verify_scope_matches_shards \
        "run_test_cover_cubefs" \
        _cover_list_run_test_cover_cubefs_scope \
        _cover_list_run_test_cover_cubefs_shard_main
}

# List the full package scope for the blobstore-only coverage workflow.
_cover_list_run_test_cover_blobstore_scope() {
    go list ./blobstore/... | grep -v '/blobstore/cmd' | grep -v '/blobstore/common/tcmalloc'
}

# List the primary shard for the blobstore-only coverage workflow.
_cover_list_run_test_cover_blobstore_shard_main() {
    _cover_list_run_test_cover_blobstore_scope | grep -v /blobstore/shardnode
}

# List an additional shard that is merged back into the blobstore coverage report.
_cover_list_run_test_cover_blobstore_shard_extra_1() {
    go list ./blobstore/... | grep /blobstore/shardnode/catalog/allocator
}

# List an additional shard that is merged back into the blobstore coverage report.
_cover_list_run_test_cover_blobstore_shard_extra_2() {
    go list ./blobstore/... | grep /blobstore/shardnode | grep -v /catalog/allocator
}

# Check that all split shards for testcoverblobstore still cover the full intended package set.
_cover_verify_run_test_cover_blobstore_union() {
    _cover_verify_scope_matches_shards \
        "run_test_cover_blobstore" \
        _cover_list_run_test_cover_blobstore_scope \
        _cover_list_run_test_cover_blobstore_shard_main \
        _cover_list_run_test_cover_blobstore_shard_extra_1 \
        _cover_list_run_test_cover_blobstore_shard_extra_2
}

# Run the full split coverage workflow for the repository and merge all shard profiles.
run_test_cover() {
    pushd $SrcPath >/dev/null
    export JENKINS_TEST=1
    # Align local coverage runs with the docker test environment so flashnode
    # disk-path tests do not depend on host mount-point setup.
    export DOCKER_FLASHNODE_TMPFS_OFF=on
    ulimit -n 65536
    echo -n "${TPATH}"

    _cover_verify_run_test_cover_union || exit 1

    go test -trimpath -covermode=count --coverprofile coverage.txt \
        $(_cover_list_run_test_cover_shard_main)
    if [ $? -ne 0 ]; then
        exit 1
    fi

    # Append each separately executed shard back into the merged profile so the
    # final report still reflects the full intended coverage scope.
    go test -trimpath -covermode=count --coverprofile cover.txt \
        $(_cover_list_run_test_cover_shard_extra_1)
    if [ $? -ne 0 ]; then
        exit 1
    fi
    _cover_merge_profile cover.txt coverage.txt

    build_with_tcmalloc
    go test -trimpath -covermode=count --coverprofile cover.txt \
        $(_cover_list_run_test_cover_shard_extra_2)
    if [ $? -ne 0 ]; then
        exit 1
    fi
    _cover_merge_profile cover.txt coverage.txt
    export CGO_LDFLAGS="${cgo_ldflags}"

    _run_incremental_coverage_check coverage.txt || exit 1

    popd >/dev/null
    exit 0
}

# Run the split coverage workflow for cubefs packages and merge all shard profiles.
run_test_cover_cubefs() {
    pushd $SrcPath >/dev/null
    # Align local coverage runs with the docker test environment so flashnode
    # disk-path tests do not depend on host mount-point setup.
    export DOCKER_FLASHNODE_TMPFS_OFF=on
    ulimit -n 65536
    echo -n "${TPATH}"

    _cover_verify_run_test_cover_cubefs_union || exit 1

    go test -trimpath -covermode=count --coverprofile coverage.txt \
        $(_cover_list_run_test_cover_cubefs_shard_main)
    if [ $? -ne 0 ]; then
        exit 1
    fi

    _run_incremental_coverage_check coverage.txt || exit 1

    popd > /dev/null
    exit 0
}

# Run the split coverage workflow for blobstore packages and merge all shard profiles.
run_test_cover_blobstore() {
    pushd $SrcPath >/dev/null
    export JENKINS_TEST=1
    ulimit -n 65536
    echo -n "${TPATH}"

    _cover_verify_run_test_cover_blobstore_union || exit 1

    go test -trimpath -covermode=count --coverprofile coverage.txt \
        $(_cover_list_run_test_cover_blobstore_shard_main)
    if [ $? -ne 0 ]; then
        exit 1
    fi

    go test -trimpath -covermode=count --coverprofile cover.txt \
        $(_cover_list_run_test_cover_blobstore_shard_extra_1)
    if [ $? -ne 0 ]; then
        exit 1
    fi
    _cover_merge_profile cover.txt coverage.txt

    build_with_tcmalloc
    go test -trimpath -covermode=count --coverprofile cover.txt \
        $(_cover_list_run_test_cover_blobstore_shard_extra_2)
    if [ $? -ne 0 ]; then
        exit 1
    fi
    _cover_merge_profile cover.txt coverage.txt
    export CGO_LDFLAGS="${cgo_ldflags}"

    _run_incremental_coverage_check coverage.txt || exit 1

    popd >/dev/null
    exit 0
}

# Build the main CubeFS server binary.
build_server() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-server   "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-server ${SrcPath}/cmd/*.go && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the blobstore clustermgr binary.
build_clustermgr() {
    pushd $SrcPath/blobstore/cmd/clustermgr >/dev/null
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

# Build the blobstore blobnode binary.
build_blobnode() {
    pushd $SrcPath/blobstore/cmd/blobnode >/dev/null
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

# Build the blobstore access binary.
build_access() {
    pushd $SrcPath/blobstore/cmd/access >/dev/null
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

# Build the blobstore scheduler binary.
build_scheduler() {
    pushd $SrcPath/blobstore/cmd/scheduler >/dev/null
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

# Build the blobstore proxy binary.
build_proxy() {
    pushd $SrcPath/blobstore/cmd/proxy >/dev/null
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

# Build the blobstore CLI binary.
build_blobstore_cli() {
    pushd $SrcPath/blobstore/cli/cli >/dev/null
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore/blobstore-cli .
    popd >/dev/null
}

# Build the blobstore shardnode binary with the tcmalloc-enabled link flags.
build_shardnode() {
    pushd $SrcPath/blobstore/cmd/shardnode >/dev/null
    build_with_tcmalloc
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    export CGO_LDFLAGS="${cgo_ldflags}"
    popd >/dev/null
}

# Build the standalone blobstore dial test helper binary.
build_blobstore_dialtest_bin() {
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${BlobPath} -asmflags=all=-trimpath=${BlobPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore/blobstore-dialtest ${SrcPath}/blobstore/testing/dial/main
}

# Build the blobstore benchmark tool.
build_blobstore_bench() {
    pushd $SrcPath > /dev/null
    echo -n "build blobstore bench"
    go build ${MODFLAGS} -gcflags=all=-trimpath="${SrcPath}" -asmflags=all=-trimpath="${SrcPath}" -ldflags="${LDFlags}" -o "${BuildBinPath}/blobstore/blobstore-bench" "${SrcPath}/blobstore/tool/bench"
    popd > /dev/null
}

# Build the full blobstore binary set as a grouped target.
build_blobstore() {
    pushd $SrcPath >/dev/null
    echo -n "build blobstore    "
    build_clustermgr && build_blobnode && build_access && build_scheduler && build_proxy && build_blobstore_cli && build_shardnode && build_blobstore_bench && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the blobstore dial test target and print a success/failure summary.
build_blobstore_dialtest() {
    pushd $SrcPath >/dev/null
    echo -n "build blobstore dialtest"
    build_blobstore_dialtest_bin && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the cfs-client binary.
build_client() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-client   "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-client ${SrcPath}/client/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the cfs-authtool binary.
build_authtool() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-authtool "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-authtool ${SrcPath}/authnode/authtool/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the cfs-cli binary with the CGO dependencies it requires.
build_cli() {
    #cli need gorocksdb too
    pushd $SrcPath >/dev/null
    echo -n "build cfs-cli      "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-cli ${SrcPath}/cli/*.go  && echo "success" || echo "failed"
    #sh cli/build.sh ${BuildBinPath}/cfs-cli && echo "success" || echo "failed"
    popd >/dev/null
}



# Build the cfs-deploy binary with the CGO dependencies it requires.
build_cfs_deploy() {
    #cfs_deploy need gorocksdb too
    pushd $SrcPath >/dev/null
    echo -n "build cfs-deploy      "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-deploy ${SrcPath}/deploy/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the cfs-fsck binary.
build_fsck() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-fsck      "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-fsck ${SrcPath}/tool/fsck/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the cfs-snapshot binary.
build_snapshot() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-snapshot	"
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-snapshot ${SrcPath}/tool/snapshot/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build only the libcfs shared library without packaging the Java artifacts.
build_libsdkpre() {
    case `uname` in
        Linux)
            TargetFile=${1:-${BuildBinPath}/libcfs.so}
            ;;
        *)
            echo "Unsupported platform"
            exit 1
            ;;
    esac
    pushd $SrcPath > /dev/null
    echo -n "build libsdk: libcfs.so"
    CGO_ENABLED=1 go build $MODFLAGS -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -buildmode c-shared -o ${TargetFile} ${SrcPath}/client/libsdk/*.go && echo "success" || echo "failed"
    popd > /dev/null
}

# Build the libcfs shared library and then package the Java SDK artifacts.
build_libsdk() {
    case `uname` in
        Linux)
            TargetFile=${1:-${BuildBinPath}/libcfs.so}
            ;;
        *)
            echo "Unsupported platform"
            exit 0
            ;;
    esac
    pushd $SrcPath >/dev/null
    echo -n "build libsdk: libcfs.so       "
    CGO_ENABLED=1 go build $MODFLAGS -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -buildmode c-shared -o ${TargetFile} ${SrcPath}/client/libsdk/*.go && echo "success" || echo "failed"
    popd >/dev/null

    pushd $SrcPath/java >/dev/null
    echo -n "build java libcubefs        "
    mkdir -p $SrcPath/java/src/main/resources/
    \cp  -rf ${TargetFile}  $SrcPath/java/src/main/resources/
    mvn clean package
    \cp -rf $SrcPath/java/target/*.jar ${BuildBinPath}  && echo "build java libcubefs success" || echo "build java libcubefs failed"
    popd >/dev/null
}

# Build the fdstore helper binary.
build_fdstore() {
    pushd $SrcPath >/dev/null
    echo -n "build fdstore "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/fdstore ${SrcPath}/client/fdstore/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the cfs-bcache binary.
build_bcache(){
    pushd $SrcPath >/dev/null
    echo -n "build cfs-blockcache      "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-bcache ${SrcPath}/client/blockcache/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the remote cache benchmark binary.
build_rctest(){
    pushd $SrcPath >/dev/null
    echo -n "build cfs-remotecache-benchmark      "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-remotecache-benchmark ${SrcPath}/tool/remotecache-benchmark/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Build the remote cache config helper binary.
build_rcconfig(){
    pushd $SrcPath >/dev/null
    echo -n "build cfs-remotecache-config      "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-remotecache-config ${SrcPath}/tool/remotecache-config/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

# Remove generated binaries while keeping the unpacked dependencies.
clean() {
    $RM -rf ${BuildBinPath}
}

# Remove generated binaries together with unpacked dependencies and headers.
dist_clean() {
    $RM -rf ${BuildBinPath}
    $RM -rf ${BuildOutPath}
    $RM -rf ${BuildDependsLibPath}
    $RM -rf ${BuildDependsIncludePath}
}

cmd=${1:-"all"}

if [ "${cmd}" == "dist_clean" ]; then
    dist_clean
    exit 0
elif [ "${cmd}" == "clean" ]; then
    clean
    exit 0
fi

# Return the detected CPU core count for default build parallelism.
get_cpu_cores() {
    cores=`cat /proc/cpuinfo | grep processor | wc -l`
    return $cores
}

threads=0
incremental_coverage_threshold=""
incremental_coverage_base=""
for para in $*
do
    check=`echo $para | grep "^--threads=" | wc -l`
    if test $check -eq 1
    then
        check=`echo "$para" | grep "^--threads=[0-9]*[^0-9]\{1,\}" | wc -l`
        if test $check -eq 0
        then
            threads=`echo "$para" | grep -o "[0-9]\{1,\}"`
        fi
    fi
    check=`echo $para | grep "^--incremental-coverage=" | wc -l`
    if test $check -eq 1
    then
        incremental_coverage_threshold=`echo "$para" | sed 's/^--incremental-coverage=//'`
    fi
    check=`echo $para | grep "^--incremental-base=" | wc -l`
    if test $check -eq 1
    then
        incremental_coverage_base=`echo "$para" | sed 's/^--incremental-base=//'`
    fi
done

if test $threads -eq 0
then
    get_cpu_cores
    threads=`expr $? + 1`
    threads=`expr $threads / 2`
fi

pre_build $threads

case "$cmd" in
    "all")
        build_server
        build_client
        build_cli
        build_libsdk
        build_bcache
        build_rctest
        build_rcconfig
        ;;
    "test")
        run_test
        ;;
    "testcover")
        run_test_cover
        ;;
    "testcovercubefs")
        run_test_cover_cubefs
        ;;
    "testcoverblobstore")
        run_test_cover_blobstore
        ;;
    "server")
        build_server
        ;;
    "blobstore")
        build_blobstore
        ;;
    "blobstoredialtest")
        build_blobstore_dialtest
        ;;
    "client")
        build_client
        ;;
    "authtool")
        build_authtool
        ;;
    "cli")
        build_cli
        ;;
    "deploy")
        build_cfs_deploy
        ;;
    "fsck")
        build_fsck
        ;;
    "snapshot")
        build_snapshot
        ;;
    "libsdkpre")
        build_libsdkpre
        ;;
    "libsdk")
        build_libsdk
        ;;
    "fdstore")
        build_fdstore
        ;;
    "bcache")
        build_bcache
        ;;
    "rctest")
        build_rctest
        ;;
    "rcconfig")
        build_rcconfig
        ;;
    *)
        ;;
esac
