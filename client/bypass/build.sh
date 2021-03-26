#!/usr/bin/env bash

BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`
Debug="0"

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1 ; }

goflag="-s"
while getopts "g" arg; do
  case $arg in
    g)
      Debug="1"
      goflag=""
      gccflag="-g"
      ;;
  esac
done

dir=$(dirname $0)
go build -ldflags "${goflag} -X main.CommitID=${CommitID} -X main.BranchName=${BranchName} -X 'main.BuildTime=${BuildTime}' -X 'main.Debug=${Debug}'" -buildmode=c-shared -o ${dir}/libcfssdk.so ${dir}/*.go
gcc -std=c99 ${gccflag} -fPIC -shared -o ${dir}/libcfsclient.so ${dir}/client.c ${dir}/ini.c -ldl -L${dir} -lcfssdk
