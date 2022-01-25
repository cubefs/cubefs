#!/usr/bin/env bash
RootPath=$(cd $(dirname $0)/..; pwd)

Version=`git describe --abbrev=0 --tags 2>/dev/null`
BranchName=`git rev-parse --abbrev-ref HEAD 2>/dev/null`
CommitID=`git rev-parse HEAD 2>/dev/null`
BuildTime=`date +%Y-%m-%d\ %H:%M`

SrcPath=${RootPath}/cli
TargetFile=${1:-$RootPath/cli/cfs-cli}

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1; }

LDFlags="-X github.com/cubefs/cubefs/proto.Version=${Version} \
    -X github.com/cubefs/cubefs/proto.CommitID=${CommitID} \
    -X github.com/cubefs/cubefs/proto.BranchName=${BranchName} \
    -X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}' "

GO111MODULE=off \
go build \
    -ldflags "${LDFlags}" \
    -o $TargetFile \
    ${SrcPath}/*.go
