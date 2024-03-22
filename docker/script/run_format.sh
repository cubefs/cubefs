#!/bin/bash
CurrentPath=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
gofmtFile=gofmt_results.diff
pushd ${CurrentPath}/../../
find . -type f -name "*.go" | \
    grep -v 'vendor'|grep -v 'depends'|grep -v ./sdk/graphql/client/ | \
    xargs gofumpt -l -d > ${gofmtFile}
cat ${gofmtFile}
if [ "$(cat ${gofmtFile}|wc -l)" -gt 0  ]; then
    popd
    exit 1;
fi
rm -f ${gofmtFile}
popd

export PATH=$PATH:/go/bin

for subdir in proto blockcache storage cli lcnode flashnode
do
    pushd ${CurrentPath}/../../${subdir}
    go generate ./...
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
    popd
done
