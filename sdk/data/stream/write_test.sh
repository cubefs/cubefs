#!/usr/bin/env bash
nohup go test -v -run TestExtentClient_Write  -test.timeout=100000m > log/test.log &
