FROM golang:1.18.10

# install requirements for LTP (Linux Test Program) tests
RUN apt-get update && apt-get install -y xz-utils make gcc-10 g++-10 flex bison automake autoconf psmisc cmake
RUN wget --no-check-certificate https://ocs-cn-north1.heytapcs.com/cubefs/github/ci/rocksdb_libs.tar.gz

# install requirements for s3-compatible tests
RUN apt-get install -y sudo python3 python3-pip
RUN pip3 install boto3 unittest2 requests

# install go tools for code format checking
RUN wget --no-check-certificate https://ocs-cn-north1.heytapcs.com/cubefs/github/ci/go_bin.tar.gz
RUN tar -zxf go_bin.tar.gz -C /go/

# prepare env cgo flags
RUN go env -w CGO_CFLAGS="-I/go/src/github.com/cubefs/cubefs/build/include"
RUN go env -w CGO_LDFLAGS="-L/go/src/github.com/cubefs/cubefs/build/lib -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"

# cleanup environment
RUN apt-get install -y jq fuse \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean
