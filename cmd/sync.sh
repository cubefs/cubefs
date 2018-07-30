#!/usr/bin/env bash
export GOPATH=/root/work/baud
go build cmd.go
sshpass -p ipd@2018.com scp -P 80 cmd root@172.20.189.98:/home/guowl/baud/src/github.com/chubaoio/cbfs/cmd/bdfs
