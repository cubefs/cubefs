#!/usr/bin/env bash
Version=`git describe --abbrev=0 --tags`
BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1; }

GO111MODULE=off \
go build -ldflags "\
-X github.com/cubefs/cubefs/proto.Version=${Version} \
-X github.com/cubefs/cubefs/proto.CommitID=${CommitID} \
-X github.com/cubefs/cubefs/proto.BranchName=${BranchName} \
-X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}'" \
-o cfs-server
