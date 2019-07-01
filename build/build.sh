#!/bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
echo ${RootPath}
BuildPath=${RootPath}/build
BuildBinPath=${BuildPath}/bin

[[ "no$GOPATH" == "no" ]] && { echo "GOPATH env not set" ; exit 1 ; }
#rm -rf ${BuildBinPath}/*

pushd ${BuildPath}

install_rocksdb() {
  found=$(find /usr -name librocksdb.a)
  if [[ "no$found" == "no" ]] ; then
    echo "rocksdb lib not found, now install..."
    git clone https://github.com/facebook/rocksdb
    pushd rocksdb
    git checkout v5.9.2 -b v5.9.2
    make -j `nproc` static_lib && make install && echo "rocksdb lib install success"
    popd
  fi
}

install_snappy() {
  found=$(find /usr -name libsnappy.a)
  if [[ "no$found" == "no" ]] ; then
    echo "snappy lib not found, now install..."
    git clone https://github.com/google/snappy
    pushd snappy
    mkdir build
    pushd build
    cmake .. && make -j `nproc` && make install && echo "snappy lib install success"
    popd
    popd
  fi
}

install_snappy  || { echo "install snappy lib failed!"; exit 1 ; }
install_rocksdb || { echo "install rocksdb lib failed!"; exit 1 ; }

BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`
LDFlags="-X main.CommitID=${CommitID} -X main.BranchName=${BranchName} -X 'main.BuildTime=${BuildTime}'"
#MODFLAGS="-mod vendor"
MODFLAGS=""

go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-server $RootPath/cmd/*.go
go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client $RootPath/client/*.go
go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client2 $RootPath/clientv2/*.go
