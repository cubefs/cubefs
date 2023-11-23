#!/bin/bash
BuildPath=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
cgo_cflags="-I${BuildPath}/include"
cgo_ldflags="-L${BuildPath}/lib -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"
use_clang=$(echo ${CC} | grep "clang" | grep -v "grep")
if [ "${use_clang}" != "" ]; then
    cgo_ldflags="-L${BuildPath}/lib -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lc++"
fi
export CGO_CFLAGS=${cgo_cflags}
export CGO_LDFLAGS=${cgo_ldflags}
