FROM golang:1.15

# intall rocksdb
RUN     apt-get update && \
        apt-get -y install libz-dev libbz2-dev libsnappy-dev && \
        apt-get -y install librocksdb-dev

ENV LTP_VERSION=20200515
ENV LTP_SOURCE=https://github.com/linux-test-project/ltp/archive/${LTP_VERSION}.tar.gz

# install ltptest
RUN apt-get install -y xz-utils make gcc flex bison automake autoconf
RUN  mkdir -p /tmp/ltp /opt/ltp && cd /tmp/ltp \
        && wget --no-check-certificate ${LTP_SOURCE} && tar xf ${LTP_VERSION}.tar.gz && cd ltp-${LTP_VERSION} \
        && make autotools && ./configure \
        && make -j "$(getconf _NPROCESSORS_ONLN)" all && make install \
        && rm -rf /tmp/ltp

RUN apt-get install -y jq fuse \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean
