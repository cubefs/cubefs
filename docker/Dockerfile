FROM golang:1.12

# intall rocksdb
RUN echo "deb http://deb.debian.org/debian stretch-backports main" >> /etc/apt/sources.list && \
        apt-get update && \
        apt-get -y install libz-dev libbz2-dev libsnappy-dev && \
        apt-get -y -t stretch-backports install librocksdb-dev

ENV LTP_VERSION=20180926
ENV LTP_SOURCE=https://github.com/linux-test-project/ltp/archive/${LTP_VERSION}.tar.gz

# install ltptest
RUN apt-get install -y xz-utils make gcc flex bison automake autoconf
RUN  mkdir -p /tmp/ltp /opt/ltp && cd /tmp/ltp \
        && wget ${LTP_SOURCE} && tar xf ${LTP_VERSION}.tar.gz && cd ltp-${LTP_VERSION} \
        && make autotools && ./configure \
        && make -j "$(getconf _NPROCESSORS_ONLN)" all && make install \
        && rm -rf /tmp/ltp

RUN apt-get install -y jq fuse \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean
