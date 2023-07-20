#!/usr/bin/env bash
BIN_PATH=bin
FRONT_PATH=frontend
BACK_PATH=backend
RootPath=$(pwd)

Version=`git describe --abbrev=0 --tags 2>/dev/null`
BranchName=`git rev-parse --abbrev-ref HEAD 2>/dev/null`
CommitID=`git rev-parse HEAD 2>/dev/null`
BuildTime=`date +%Y-%m-%d\ %H:%M`

#SrcPath=${RootPath}/console
TargetFile=${1:-$RootPath/bin/cfs-gui}
TargetDic=$(dirname $TargetFile)
mkdir -p $TargetDic

LDFlags="-X github.com/cubefs/cubefs/proto.Version=${Version} \
    -X github.com/cubefs/cubefs/proto.CommitID=${CommitID} \
    -X github.com/cubefs/cubefs/proto.BranchName=${BranchName} \
    -X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}' "
cd $BACK_PATH
go build \
    -ldflags "${LDFlags}" \
    -o $TargetFile \
    *.go
if [[ $? -ne 0 ]];then
  echo "go build error"
  exit 13
fi
cd -
cp -rf ./$BACK_PATH/conf/config.yml $TargetDic/config.yml

#compile frontend
cd $FRONT_PATH
npm install
if [[ $? -ne 0 ]];then
  echo "npm install error"
  exit 11
fi
npm run build
if [[ $? -ne 0 ]];then
  echo "npm run build error"
  exit 12
fi
cd -
cp -rf ./$FRONT_PATH/dist $TargetDic/dist