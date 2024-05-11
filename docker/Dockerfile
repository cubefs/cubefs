FROM golang:1.16.12

# intall rocksdb
RUN     apt-get update && \
        apt-get -y install libz-dev libbz2-dev libsnappy-dev && \
        apt-get -y install librocksdb-dev

ENV LTP_VERSION=20210121
ENV LTP_SOURCE=https://github.com/linux-test-project/ltp/archive/${LTP_VERSION}.tar.gz

# install requirements for LTP (Linux Test Program) tests
RUN apt-get install -y xz-utils make gcc flex bison automake autoconf psmisc
RUN  mkdir -p /tmp/ltp /opt/ltp && cd /tmp/ltp \
        && wget --no-check-certificate ${LTP_SOURCE} && tar xf ${LTP_VERSION}.tar.gz && cd ltp-${LTP_VERSION} \
        && make autotools && ./configure \
        && make -j "$(getconf _NPROCESSORS_ONLN)" all && make install \
        && rm -rf /tmp/ltp

# install requirements for s3-compatible tests
RUN apt-get install -y sudo python3 python3-pip
RUN pip3 install boto3 unittest2 requests

# install requirements for rdma
RUN apt-get update && \
    apt-get install -y libibverbs-dev && \
    apt-get install -y librdmacm-dev && \
    apt-get install -y libibverbs1 ibverbs-utils librdmacm1 libibumad3 ibverbs-providers rdma-core

# cleanup environment
RUN apt-get install -y jq fuse \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean
