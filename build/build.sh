#!/bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
BuildPath=${RootPath}/build
BuildOutPath=${BuildPath}/out
BuildBinPath=${BuildPath}/bin
VendorPath=${RootPath}/vendor

[[ $(uname -s) != "Linux" ]] && { echo "ChubaoFS only support Linux os"; exit 1; }

NPROC=$(nproc 2>/dev/null)
NPROC=${NPROC:-"1"}

build_snappy() {
    SnappySrcPath=${VendorPath}/snappy-1.1.7
    SnappyBuildPath=${BuildOutPath}/snappy
    found=$(find ${SnappyBuildPath} -name libsnappy.a 2>/dev/null | wc -l)
    if [[ ${found} -gt 0 ]] ; then
        return
    fi
    mkdir -p ${SnappyBuildPath}
    echo "build snappy..."
    pushd ${SnappyBuildPath} >/dev/null
    cmake ${SnappySrcPath} && make -j ${NPROC}  && echo "build snappy success" || {  echo "build snappy failed"; exit 1; }
    popd >/dev/null
}

build_rocksdb() {
    RocksdbSrcPath=${VendorPath}/rocksdb-5.9.2
    RocksdbBuildPath=${BuildOutPath}/rocksdb
    found=$(find ${RocksdbBuildPath} -name librocksdb.a 2>/dev/null | wc -l)
    if [[ ${found} -gt 0 ]] ; then
        return
    fi
    if [[ ! -d ${RocksdbBuildPath} ]] ; then
        mkdir -p ${RocksdbBuildPath}
        cp -rf ${RocksdbSrcPath}/* ${RocksdbBuildPath}
    fi
    echo "build rocksdb ..."
    pushd ${RocksdbBuildPath} >/dev/null
    [[ "-$LUA_PATH" != "-" ]]  && unset LUA_PATH
    make -j ${NPROC} static_lib  && echo "build rocksdb success" || {  echo "build rocksdb failed" ; exit 1; }
    popd >/dev/null
}

build_snappy
build_rocksdb

BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`
LDFlags="-X main.CommitID=${CommitID} -X main.BranchName=${BranchName} -X 'main.BuildTime=${BuildTime}'"
MODFLAGS=""

cgo_cflags="-I${RocksdbSrcPath}/include -I${SnappySrcPath}"
cgo_ldflags="-L${RocksdbBuildPath} -L${SnappyBuildPath} -lrocksdb -lstdc++ -lm -lsnappy"
rocksdb_libs=( z bz2 lz4 zstd )
for p in ${rocksdb_libs[*]} ; do
    found=$(find /usr -name lib${p}.so 2>/dev/null | wc -l)
    if [[ ${found} -gt 0 ]] ; then
        cgo_ldflags="${cgo_ldflags} -l${p}"
    fi
done

export CGO_CFLAGS=${cgo_cflags}
export CGO_LDFLAGS="${cgo_ldflags}"
export GO111MODULE=off
export GOPATH=/tmp/cfs/go

mkdir -p $GOPATH/src/github.com/chubaofs
SrcPath=$GOPATH/src/github.com/chubaofs/chubaofs
if [[  ! -e "$SrcPath" ]] ; then
    ln -s $RootPath $SrcPath 2>/dev/null
fi

run_test() {
    pushd $SrcPath >/dev/null
    echo "run test "
    go test -ldflags "${LDFlags}" ./...
    popd >/dev/null
}

build_server() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-server "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-server ${SrcPath}/cmd/*.go && echo "success" || echo "failed"
    popd >/dev/null
}

build_client() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-client "
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client ${SrcPath}/client/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

clean() {
    rm -rf ${BuildBinPath}
}

dist_clean() {
    rm -rf ${BuildBinPath}
    rm -rf ${BuildOutPath}
}

cmd=${1:-"all"}

case "$cmd" in
    "all")
        build_server
        build_client
        ;;
    "test")
        run_test
        ;;
    "server")
        build_server
        ;;
    "client")
        build_client
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
