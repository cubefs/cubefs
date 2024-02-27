#!/bin/bash
CurrentPath=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
gofmtFile=gofmt_results.diff
pushd ${CurrentPath}/../../
find . -type f -name "*.go" | \
    grep -v 'vendor'|grep -v 'depends'|grep -v ./sdk/graphql/client/ | \
    xargs gofmt -l -d > ${gofmtFile}
cat ${gofmtFile}
if [ "$(cat ${gofmtFile}|wc -l)" -gt 0  ]; then
    popd
    exit 1;
fi
rm -f ${gofmtFile}
popd
