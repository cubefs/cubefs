FROM golang:1.17.13

ENV LTP_VERSION=20210121
ENV LTP_SOURCE=https://github.com/linux-test-project/ltp/archive/${LTP_VERSION}.tar.gz

# install requirements for LTP (Linux Test Program) tests
RUN apt-get update && apt-get install -y xz-utils make gcc-10 g++-10 flex bison automake autoconf psmisc cmake
RUN wget --no-check-certificate https://ocs-cn-north1.heytapcs.com/cubefs/github/ci/rocksdb_libs.tar.gz
RUN  mkdir -p /tmp/ltp /opt/ltp && cd /tmp/ltp \
        && wget --no-check-certificate ${LTP_SOURCE} && tar xf ${LTP_VERSION}.tar.gz && cd ltp-${LTP_VERSION} \
        && make autotools && ./configure \
        && make -j "$(getconf _NPROCESSORS_ONLN)" all && make install \
        && rm -rf /tmp/ltp

# install requirements for s3-compatible tests
RUN apt-get install -y sudo python3 python3-pip
RUN pip3 install boto3 unittest2 requests

# install go tools for code format checking
RUN go env -w GOPROXY="https://goproxy.cn,direct" GOFLAGS="-insecure"
RUN go install mvdan.cc/gofumpt@v0.2.1
RUN go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow@latest
RUN go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.43.0
RUN rm -rf /go/pkg/

# prepare env cgo flags
RUN go env -w CGO_CFLAGS="-I/go/src/github.com/cubefs/cubefs/build/include"
RUN go env -w CGO_LDFLAGS="-L/go/src/github.com/cubefs/cubefs/build/lib -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"

# cleanup environment
RUN apt-get install -y jq fuse \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean
