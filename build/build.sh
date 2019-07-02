#!/bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
echo ${RootPath}
BuildPath=${RootPath}/build
BuildBinPath=${BuildPath}/bin
ThirdPartyPath=${RootPath}/third-party

[[ "no$GOPATH" == "no" ]] && { echo "GOPATH env not set" ; exit 1 ; }
#rm -rf ${BuildBinPath}/*

pushd ${BuildPath}

install_rocksdb() {
  found=$(find /usr -name librocksdb.a 2>/dev/null)
  if [[ "no$found" == "no" ]] ; then
    echo "rocksdb lib not found, now install..."
    pushd ${ThirdPartyPath}/rocksdb-5.9.2
    make -j `nproc` static_lib && make install && echo "rocksdb lib install success"
    popd >/dev/null
  fi
}

install_snappy() {
  found=$(find /usr -name libsnappy.a 2>/dev/null)
  if [[ "no$found" == "no" ]] ; then
    echo "snappy lib not found, now install..."
    pushd ${ThirdPartyPath}/snappy-1.1.7
    mkdir -p build
    pushd build
    cmake .. && make -j `nproc` && make install && echo "snappy lib install success"
    popd >/dev/null
    popd >/dev/null
  fi
}

install_snappy  || { echo "install snappy lib failed!"; exit 1 ; }
install_rocksdb || { echo "install rocksdb lib failed!"; exit 1 ; }

BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`
LDFlags="-X main.CommitID=${CommitID} -X main.BranchName=${BranchName} -X 'main.BuildTime=${BuildTime}'"
MODFLAGS=""

go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-server $RootPath/cmd/*.go
go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client $RootPath/client/*.go
go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-client2 $RootPath/clientv2/*.go
