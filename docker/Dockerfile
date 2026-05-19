# Multi-architecture CubeFS build image
# Supports: linux/amd64, linux/arm64
# Use: docker buildx build --platform linux/arm64 -t cubefs/cbfs-base:arm64 .
ARG TARGETARCH=amd64
FROM golang:1.18.10

RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

# install requirements for LTP (Linux Test Program) tests
RUN apt-get update && apt-get install -y xz-utils make gcc-10 g++-10 flex bison automake autoconf psmisc cmake

# Build RocksDB and compression libs from source for the target architecture
# This replaces the prebuilt amd64-only tarball download
COPY depends/ /tmp/depends/
COPY build/ /tmp/build/
RUN cd /tmp && mkdir -p build/lib build/include build/out && \
    bash -c 'source /tmp/build/build.sh && build_rocksdb_deps 4' && \
    cp -a /tmp/build/lib/* /go/src/github.com/cubefs/cubefs/build/lib/ 2>/dev/null || true && \
    cp -a /tmp/build/include/* /go/src/github.com/cubefs/cubefs/build/include/ 2>/dev/null || true || \
    (echo "==> Falling back to prebuilt libs (amd64 only)" && \
     wget --no-check-certificate https://ocs-cn-north1.heytapcs.com/cubefs/github/ci/go1.18_rocksdb_libs.tar.gz -O rocksdb_libs.tar.gz && \
     tar -zxf rocksdb_libs.tar.gz -C /go/src/github.com/cubefs/cubefs/ && \
     rm -f rocksdb_libs.tar.gz)

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
