#!/usr/bin/env bash
RootPath=$(cd $(dirname $0)/..; pwd)

Version=`git describe --abbrev=0 --tags 2>/dev/null`
BranchName=`git rev-parse --abbrev-ref HEAD 2>/dev/null`
CommitID=`git rev-parse HEAD 2>/dev/null`
BuildTime=`date +%Y-%m-%d\ %H:%M`

SrcPath=${RootPath}/libsdk
case `uname` in
    Linux)
        TargetFile=${1:-${SrcPath}/libcfs.so}
        ;;
    *)
        echo "Unsupported platform"
        exit 0
        ;;
esac

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1; }

LDFlags="-X github.com/cubefs/cubefs/proto.Version=${Version} \
    -X github.com/cubefs/cubefs/proto.CommitID=${CommitID} \
    -X github.com/cubefs/cubefs/proto.BranchName=${BranchName} \
    -X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}' "

GO111MODULE=off \
go build \
    -ldflags "${LDFlags}" \
    -buildmode c-shared \
    -o $TargetFile \
    ${SrcPath}/*.go
