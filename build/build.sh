#!/bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
BuildPath=${RootPath}/build
BuildOutPath=${BuildPath}/out
BuildBinPath=${BuildPath}/bin
VendorPath=${RootPath}/vendor
DependsPath=${RootPath}/depends


RM=$(find /bin /sbin /usr/bin /usr/local -name "rm" | head -1)
if [[ "-x$RM" == "-x" ]] ; then
    RM=rm
fi

Version=$(git describe --abbrev=0 --tags 2>/dev/null)
BranchName=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
CommitID=$(git rev-parse HEAD 2>/dev/null)
BuildTime=$(date +%Y-%m-%d\ %H:%M)
LDFlags="-X github.com/cubefs/cubefs/proto.Version=${Version} \
    -X github.com/cubefs/cubefs/proto.CommitID=${CommitID} \
    -X github.com/cubefs/cubefs/proto.BranchName=${BranchName} \
    -X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}'"
MODFLAGS=""

NPROC=$(nproc 2>/dev/null)
if [ -e /sys/fs/cgroup/cpu ] ; then
    NPROC=4
fi
NPROC=${NPROC:-"1"}

GCC_LIBRARY_PATH="/lib /lib64 /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64"
cgo_cflags=""
cgo_ldflags="-lstdc++ -lm"

case $(uname -s | tr 'A-Z' 'a-z') in
    "linux"|"darwin")
        ;;
    *)
        echo "Current platform $(uname -s) not support";
        exit1;
        ;;
esac

CPUTYPE=${CPUTYPE} | tr 'A-Z' 'a-z'

build_zlib() {
    ZlibSrcPath=${VendorPath}/dep/zlib-1.2.11
    ZlibBuildPath=${BuildOutPath}/zlib
    found=$(find ${ZlibBuildPath} -name libz.a 2>/dev/null | wc -l)
    if [ ${found} -eq 0 ] ; then
        if [ ! -d ${ZlibBuildPath} ] ; then
            mkdir -p ${ZlibBuildPath}
            cp -rf ${ZlibSrcPath}/* ${ZlibBuildPath}
        fi
        echo "build Zlib..."
        pushd ${ZlibBuildPath} >/dev/null
        [ "-$LUA_PATH" != "-" ]  && unset LUA_PATH
        ./configure
        make -j ${NPROC}   && echo "build Zlib success" || {  echo "build Zlib failed" ; exit 1; }
        popd >/dev/null
    fi
    cgo_cflags="${cgo_cflags} -I${ZlibSrcPath}"
    cgo_ldflags="${cgo_ldflags} -L${ZlibBuildPath} -lz"
    LD_LIBRARY_PATH="${ZlibBuildPath}:${LD_LIBRARY_PATH}"
    C_INCLUDE_PATH="${cgo_cflags}:$C_INCLUDE_PATH"
    CPLUS_INCLUDE_PATH="${cgo_cflags}:$CPLUS_INCLUDE_PATH"
}

build_bzip2() {
    Bzip2SrcPath=${VendorPath}/dep/bzip2-1.0.6
    Bzip2BuildPath=${BuildOutPath}/bzip2
    found=$(find ${Bzip2BuildPath} -name libz2.a 2>/dev/null | wc -l)
    if [ ${found} -eq 0 ] ; then
        if [ ! -d ${Bzip2BuildPath} ] ; then
            mkdir -p ${Bzip2BuildPath}
            cp -rf ${Bzip2SrcPath}/* ${Bzip2BuildPath}
        fi
        echo "build Bzip2..."
        pushd ${Bzip2BuildPath} >/dev/null
        [ "-$LUA_PATH" != "-" ]  && unset LUA_PATH

        make -j ${NPROC} bzip2  && echo "build Bzip2 success" || {  echo "build Bzip2 failed" ; exit 1; }
        popd >/dev/null
    fi
    cgo_cflags="${cgo_cflags} -I${Bzip2SrcPath}"
    cgo_ldflags="${cgo_ldflags} -L${Bzip2BuildPath} -lbz2"
    LD_LIBRARY_PATH="${Bzip2BuildPath}:${LD_LIBRARY_PATH}"
    C_INCLUDE_PATH="${cgo_cflags}:$C_INCLUDE_PATH"
    CPLUS_INCLUDE_PATH="${cgo_cflags}:$CPLUS_INCLUDE_PATH"
}

build_lz4() {
    ZstdSrcPath=${VendorPath}/dep/lz4-1.9.3
    ZstdBuildPath=${BuildOutPath}/lz4
    found=$(find ${ZstdBuildPath}/lib -name liblz4.a 2>/dev/null | wc -l)
    if [ ${found} -eq 0 ] ; then
        if [ ! -d ${ZstdBuildPath} ] ; then
            mkdir -p ${ZstdBuildPath}
            cp -rf ${ZstdSrcPath}/* ${ZstdBuildPath}
        fi
        echo "build Zstd..."
        pushd ${ZstdBuildPath} >/dev/null
        [ "-$LUA_PATH" != "-" ]  && unset LUA_PATH

        make -j ${NPROC}   && echo "build lz4 success" || {  echo "build Zstd failed" ; exit 1; }
        popd >/dev/null
    fi
    cgo_cflags="${cgo_cflags} -I${ZstdSrcPath}/programs"
    cgo_ldflags="${cgo_ldflags} -L${ZstdBuildPath}/lib -llz4"
    LD_LIBRARY_PATH="${ZstdBuildPath}/lib:${LD_LIBRARY_PATH}"
    C_INCLUDE_PATH="${cgo_cflags}:$C_INCLUDE_PATH"
    CPLUS_INCLUDE_PATH="${cgo_cflags}:$CPLUS_INCLUDE_PATH"
}

 # dep zlib,bz2,lz4
 build_zstd() {
    ZstdSrcPath=${VendorPath}/dep/zstd-1.4.8
    ZstdBuildPath=${BuildOutPath}/zstd
    found=$(find ${ZstdBuildPath}/lib -name libzstd.a 2>/dev/null | wc -l)
    if [ ${found} -eq 0 ] ; then
        if [ ! -d ${ZstdBuildPath} ] ; then
            mkdir -p ${ZstdBuildPath}
            cp -rf ${ZstdSrcPath}/* ${ZstdBuildPath}
        fi
        echo "build Zstd..."
        pushd ${ZstdBuildPath} >/dev/null
        [ "-$LUA_PATH" != "-" ]  && unset LUA_PATH

        make -j ${NPROC}  allzstd && echo "build Zstd success" || {  echo "build Zstd failed" ; exit 1; }
        popd >/dev/null
    fi
    cgo_cflags="${cgo_cflags} -I${ZstdSrcPath}/programs"
    cgo_ldflags="${cgo_ldflags} -L${ZstdBuildPath}/lib -lzstd"

    LD_LIBRARY_PATH="${ZstdBuildPath}/lib:${LD_LIBRARY_PATH}"
    C_INCLUDE_PATH="${cgo_cflags}:$C_INCLUDE_PATH"
    CPLUS_INCLUDE_PATH="${cgo_cflags}:$CPLUS_INCLUDE_PATH"
}


build_snappy() {
    SnappySrcPath=${DependsPath}/snappy-1.1.7
    SnappyBuildPath=${BuildOutPath}/snappy
    found=$(find ${SnappyBuildPath} -name libsnappy.a 2>/dev/null | wc -l)
    if [ ${found} -eq 0 ] ; then
        mkdir -p ${SnappyBuildPath}
        echo "build snappy..."
        pushd ${SnappyBuildPath} >/dev/null
        cmake ${SnappySrcPath} && make -j ${NPROC}  && echo "build snappy success" || {  echo "build snappy failed"; exit 1; }
        popd >/dev/null
    fi
    cgo_cflags="${cgo_cflags} -I${SnappySrcPath}"
    cgo_ldflags="${cgo_ldflags} -L${SnappyBuildPath} -lsnappy"

    LD_LIBRARY_PATH="${SnappyBuildPath}:${LD_LIBRARY_PATH}"
    C_INCLUDE_PATH="${cgo_cflags}:$C_INCLUDE_PATH"
    CPLUS_INCLUDE_PATH="${cgo_cflags}:$CPLUS_INCLUDE_PATH"
}

build_rocksdb() {
    RocksdbSrcPath=${DependsPath}/rocksdb-5.9.2
    RocksdbBuildPath=${BuildOutPath}/rocksdb
    found=$(find ${RocksdbBuildPath} -name librocksdb.a 2>/dev/null | wc -l)
    if [ ${found} -eq 0 ] ; then
        if [ ! -d ${RocksdbBuildPath} ] ; then
            mkdir -p ${RocksdbBuildPath}
            cp -rf ${RocksdbSrcPath}/* ${RocksdbBuildPath}
        fi

        pushd ${RocksdbBuildPath} >/dev/null

        [ "-$LUA_PATH" != "-" ]  && unset LUA_PATH
        MAJOR=$(echo __GNUC__ | $(which gcc) -E -xc - | tail -n 1)
        if [ ${MAJOR} -ge 12 ] ; then
            CXXFLAGS='-Wno-error=deprecated-copy -Wno-error=class-memaccess -Wno-error=pessimizing-move -Wno-error=range-loop-construct'" ${CXXFLAGS}" \
                make -j ${NPROC} static_lib  && echo "build rocksdb success" || {  echo "build rocksdb failed" ; exit 1; }
        elif [ ${MAJOR} -ge 10 ] ; then
            CXXFLAGS='-Wno-error=deprecated-copy -Wno-error=class-memaccess -Wno-error=pessimizing-move'" ${CXXFLAGS}" \
                make -j ${NPROC} static_lib  && echo "build rocksdb success" || {  echo "build rocksdb failed" ; exit 1; }
        elif [ ${MAJOR} -ge 8 ] ; then
            CXXFLAGS='-Wno-error=class-memaccess'" ${CXXFLAGS}" \
                make -j ${NPROC} static_lib  && echo "build rocksdb success" || {  echo "build rocksdb failed" ; exit 1; }
        else
            make -j ${NPROC} static_lib  && echo "build rocksdb success" || {  echo "build rocksdb failed" ; exit 1; }
        fi
        popd >/dev/null
    fi
    cgo_cflags="${cgo_cflags} -I${RocksdbSrcPath}/include"
    cgo_ldflags="${cgo_ldflags} -L${RocksdbBuildPath} -lrocksdb"

    LD_LIBRARY_PATH="${RocksdbBuildPath}:${LD_LIBRARY_PATH}"
    C_INCLUDE_PATH="${cgo_cflags}:$C_INCLUDE_PATH"
    CPLUS_INCLUDE_PATH="${cgo_cflags}:$CPLUS_INCLUDE_PATH"
}

init_gopath() {
    export GO111MODULE=on
    export GOPATH=$HOME/tmp/cfs/go

    mkdir -p $GOPATH/src/github.com/cubefs
    SrcPath=$GOPATH/src/github.com/cubefs/cubefs
    if [ -L "$SrcPath" ]; then
        $RM -f $SrcPath
    fi
    if [  ! -e "$SrcPath" ] ; then
        ln -s $RootPath $SrcPath 2>/dev/null
    fi
}

pre_build_server() {
    rocksdb_libs=( z bz2 lz4 zstd )
    if [[ "$CPUTYPE" == arm64* ]];
    then
        build_zlib
        build_bzip2
        build_lz4
     #   build_zstd
    else
        for p in ${rocksdb_libs[*]} ; do
            found=$(find /usr -name lib${p}.so 2>/dev/null | wc -l)
            if [ ${found} -gt 0 ] ; then
                cgo_ldflags="${cgo_ldflags} -l${p}"
            fi
        done
    fi

    build_snappy
    build_rocksdb

    export CGO_CFLAGS=${cgo_cflags}
    export CGO_LDFLAGS="${cgo_ldflags}"

    init_gopath
}

pre_build() {
    export CGO_CFLAGS=""
    export CGO_LDFLAGS=""

    init_gopath
}

run_test() {
    pre_build_server
    pushd $SrcPath >/dev/null
    echo -n "${TPATH}"
#    go test $MODFLAGS -ldflags "${LDFlags}" -cover ./master

    go test -cover -v -coverprofile=cover.output $(go list ./... | grep -v depends | grep -v master) | tee cubefs_unittest.output
    ret=$?
    popd >/dev/null
    exit $ret
}

run_test_cover() {
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "${TPATH}"
    go test -trimpath -covermode=count --coverprofile coverage.txt $(go list ./... | grep -v depends)
    ret=$?
    popd >/dev/null
    exit $ret
}

build_server() {
    pre_build_server
    pushd $SrcPath >/dev/null
    echo -n "build cfs-server   "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-server ${SrcPath}/cmd/*.go && echo "success" || echo "failed"
    popd >/dev/null
}

build_client() {
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "build cfs-client   "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client ${SrcPath}/client/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_client2() {
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "build cfs-client2  "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client2 ${SrcPath}/clientv2/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_authtool() {
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "build cfs-authtool "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-authtool ${SrcPath}/authtool/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_cli() {
    #cli need gorocksdb too
    pre_build_server
    pushd $SrcPath >/dev/null
    echo -n "build cfs-cli      "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-cli ${SrcPath}/cli/*.go  && echo "success" || echo "failed"
    #sh cli/build.sh ${BuildBinPath}/cfs-cli && echo "success" || echo "failed"
    popd >/dev/null
}

build_fsck() {
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "build cfs-fsck      "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-fsck ${SrcPath}/fsck/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_libsdk() {
    pre_build_server
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
    go build $MODFLAGS -ldflags "${LDFlags}" -buildmode c-shared -o ${TargetFile} ${SrcPath}/libsdk/*.go && echo "success" || echo "failed"
    popd >/dev/null

    pushd $SrcPath/java >/dev/null
    echo -n "build java libcubefs        "
    mkdir -p $SrcPath/java/src/main/resources/
    \cp  -rf ${TargetFile}  $SrcPath/java/src/main/resources/
    mvn clean package
    \cp -rf $SrcPath/java/target/*.jar ${BuildBinPath}  && echo "build java libcubefs success" || echo "build java libcubefs failed"
    popd >/dev/null
}

build_fdstore() {
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "build fdstore "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/fdstore ${SrcPath}/fdstore/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_preload() {
    pre_build_server
    pushd $SrcPath >/dev/null
    echo -n "build cfs-preload   "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-preload ${SrcPath}/preload/*.go && echo "success" || echo "failed"
}

build_bcache(){
    pre_build
    pushd $SrcPath >/dev/null
    echo -n "build cfs-blockcache      "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-bcache ${SrcPath}/blockcache/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

clean() {
    $RM -rf ${BuildBinPath}
}

dist_clean() {
    $RM -rf ${BuildBinPath}
    $RM -rf ${BuildOutPath}
    $RM -rf ${VendorPath}/dep
}

cmd=${1:-"all"}

case "$cmd" in
    "all")
        build_server
        build_client
        build_cli
        build_libsdk
        build_bcache
        ;;
    "test")
        run_test
        ;;
    "testcover")
        run_test_cover
        ;;
    "server")
        build_server
        ;;
    "client")
        build_client
        ;;
    "client2")
        build_client2
        ;;
    "authtool")
        build_authtool
        ;;
    "cli")
        build_cli
        ;;
    "fsck")
        build_fsck
        ;;
    "libsdk")
        build_libsdk
        ;;
    "fdstore")
        build_fdstore
        ;;
    "preload")
        build_preload
        ;;
    "bcache")
        build_bcache
        ;;
    "clean")
        clean
        ;;
    "dist_clean")
        dist_clean
        ;;
    *)
        ;;
esac
