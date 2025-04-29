FROM golang:1.18.10@sha256:50c889275d26f816b5314fc99f55425fa76b18fcaf16af255f5d57f09e1f48da

RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

# install requirements for LTP (Linux Test Program) tests
RUN apt-get update && apt-get install -y xz-utils make gcc-10 g++-10 flex bison automake autoconf psmisc cmake
RUN wget --no-check-certificate https://ocs-cn-north1.heytapcs.com/cubefs/github/ci/go1.18_rocksdb_libs.tar.gz -O rocksdb_libs.tar.gz

# install requirements for s3-compatible tests
RUN apt-get install -y sudo python3 python3-pip
RUN pip3 install boto3==1.34.8 unittest2==1.1.0 requests==2.31.0

# install go tools for code format checking
RUN wget --no-check-certificate https://ocs-cn-north1.heytapcs.com/cubefs/github/ci/go1.18_bin.tar.gz -O go_bin.tar.gz
RUN tar -zxf go_bin.tar.gz -C /go/

# prepare env cgo flags
RUN go env -w CGO_CFLAGS="-I/go/src/github.com/cubefs/cubefs/build/include"
RUN go env -w CGO_CXXFLAGS="-I/go/src/github.com/cubefs/cubefs/build/include"
RUN go env -w CGO_LDFLAGS="-L/go/src/github.com/cubefs/cubefs/build/lib -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"

# cleanup environment
RUN apt-get install -y jq fuse \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean
