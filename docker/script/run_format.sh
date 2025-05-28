#!/bin/bash
CurrentPath=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
gofmtFile=gofmt_results.diff
pushd ${CurrentPath}/../../
find . -type f -name "*.go" | \
    grep -v -E '^./vendor/' | \
    grep -v -E '^./depends/' | \
    grep -v -E '^./sdk/graphql/client/' | \
    grep -v -E '\.pb\.go$' | \
    xargs gofumpt -l -d > ${gofmtFile}
cat ${gofmtFile}
if [ "$(cat ${gofmtFile}|wc -l)" -gt 0  ]; then
    popd
    exit 1;
fi
rm -f ${gofmtFile}
popd

export PATH=$PATH:/go/bin

for subdir in proto client/blockcache datanode/storage cli lcnode
do
    pushd ${CurrentPath}/../../${subdir}
    go generate ./...
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
    popd
done

golintFile=golint.diff
pushd ${CurrentPath}/../..
oldldflags="$(go env CGO_LDFLAGS)"
lpath=$(echo -n ${oldldflags} | awk '{print $1}')
go env -w CGO_LDFLAGS="${lpath} -ldl -ltcmalloc -lm -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"
go generate . > ${golintFile}
go env -w CGO_LDFLAGS="${oldldflags}"
cat ${golintFile}
if [[ $? -ne 0 ]]; then
    exit 1
fi
if [ "$(cat ${golintFile}|wc -l)" -gt 0 ]; then
     popd
    exit 1
fi
rm -f ${golintFile}
popd
