#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./env.sh

#gofmt check
echo "gofmt check"
if [[ "${GOFMT_AND_MODIFY}" != "" ]];then
    echo "Will use this command to format your local code: [find . -name "*.go" | grep -v "/vendor/" | xargs gofmt -s -w]"
    find . -name "*.go" | grep -v "/vendor/" | xargs gofmt -s -w
else
    echo "Will use this command to check your local code gofmt: [find . -name "*.go" | grep -v "/vendor/" | xargs gofmt -s -d]"
    diff=`find . -name "*.go" | grep -v "/vendor/" | xargs gofmt -s -d`
    if [[ -n "${diff}" ]]; then
        echo "Gofmt check failed since of:"
        echo "${diff}"
        echo "Please run this command to fix: [find . -name \"*.go\" | grep -v \"/vendor/\" | xargs gofmt -s -w]"
        exit 1
   fi
fi

# go vet check
echo
echo "go vet check"
echo "Will use this command to check your local code: [go vet -tags="dev embed" ./...]"
go vet -tags="dev embed" `go list ./...`
if [[ $? -ne 0 ]];then
    echo "go vet failed. exit"
    exit 1
fi
