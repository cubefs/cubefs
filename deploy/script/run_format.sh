#!/bin/bash
CurrentPath=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
pushd ${CurrentPath}/../../
find . -type f -name "*.go" | grep -v 'vendor' |grep -v 'depends'| xargs gofmt -l > gofmt_results.txt
cat gofmt_results.txt
if [ "$(cat gofmt_results.txt|wc -l)" -gt 0  ]; then 
    popd
    exit 1; 
fi
popd
