#!/bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
BuildPath=${RootPath}/build
BuildBinPath=${BuildPath}/bin
VendorPath=${RootPath}/vendor

[[ $(uname -s) != "Linux" ]] && { echo "ChubaoFS only support Linux os"; exit 1; }

NPROC=$(nproc 2>/dev/null)
NPROC=${NPROC:-"1"}

build_snappy() {
    echo "build snappy..."
    pushd ${VendorPath}/snappy-1.1.7 >/dev/null
    mkdir -p build
    pushd build >/dev/null
    cmake .. && make -j ${NPROC}  && echo "build snappy success" || {  echo "build snappy failed"; exit 1; }
    popd >/dev/null
    popd >/dev/null
}

build_rocksdb() {
    echo "build rocksdb ..."
    pushd ${VendorPath}/rocksdb-5.9.2 >/dev/null
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

cgo_cflags="-I${VendorPath}/rocksdb-5.9.2/include -I${VendorPath}/snappy-1.1.7"
cgo_ldflags="-L${VendorPath}/rocksdb-5.9.2 -L${VendorPath}/snappy-1.1.7/build -lrocksdb -lstdc++ -lm -lsnappy"
rocksdb_libs=( z bz2 lz4 zstd )
for p in ${rocksdb_libs[*]} ; do
    found=$(find /usr -name lib${p}.so | wc -l)
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
ln -s $RootPath $SrcPath 2>/dev/null
pushd $SrcPath >/dev/null
echo -n "build cfs-server "
go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-server ${SrcPath}/cmd/*.go && echo "success" || echo "failed"
echo -n "build cfs-client "
go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client ${SrcPath}/client/*.go  && echo "success" || echo "failed"
popd >/dev/null

