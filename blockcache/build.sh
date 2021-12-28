#!/usr/bin/env bash
Version=`git describe --abbrev=0 --tags`
BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1; }

go build -ldflags "\
-X github.com/chubaofs/chubaofs/proto.Version=${Version} \
-X github.com/chubaofs/chubaofs/proto.CommitID=${CommitID} \
-X github.com/chubaofs/chubaofs/proto.BranchName=${BranchName} \
-X 'github.com/chubaofs/chubaofs/proto.BuildTime=${BuildTime}'" \
-o cfs-bcache
