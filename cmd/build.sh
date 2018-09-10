#!/usr/bin/env bash
export GOPATH=/root/work/baud
cd $GOPATH/src/github.com/tiglabs/containerfs/cmd
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
CGO_CFLAGS="-I/usr/local/include" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy " go build -v cmd.go