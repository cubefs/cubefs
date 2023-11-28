FROM debian:11

# install all requrement packages and cleanup
RUN apt-get update && \
    apt-get install -y ca-certificates curl netbase wget gnupg dirmngr git openssh-client procps g++ gcc libc6-dev make pkg-config && \
    apt-get install -y xz-utils make gcc flex bison automake autoconf libtool libsysfs-dev && \
    apt-get install -y libz-dev libbz2-dev libsnappy-dev librocksdb-dev && \
    apt-get install -y sudo python3 python3-pip && \
    apt-get install -y jq fuse && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

ENV LTP_VERSION=20210121
ENV LTP_SOURCE=https://github.com/linux-test-project/ltp/archive/${LTP_VERSION}.tar.gz

# install requirements for LTP (Linux Test Program) tests
RUN  mkdir -p /tmp/ltp /opt/ltp && cd /tmp/ltp \
        && wget --no-check-certificate ${LTP_SOURCE} && tar xf ${LTP_VERSION}.tar.gz && cd ltp-${LTP_VERSION} \
        && make autotools && ./configure \
        && make -j "$(getconf _NPROCESSORS_ONLN)" all && make install \
        && rm -rf /tmp/ltp

# Install golang SDKs to /usr/local/: go1.21.4, go1.17.3-alternative
RUN mkdir -p /usr/local/ && \
    cd /opt > /dev/null && \
    wget "https://go.dev/dl/go1.21.4.linux-amd64.tar.gz" -O go1.21.4.linux-amd64.tar.gz && \
    tar zxf go1.21.4.linux-amd64.tar.gz && \
    rm go1.21.4.linux-amd64.tar.gz && \
    mv go /usr/local/go1.21.4 && \
    wget "http://storage.jd.local/dpgimage/cfs_spark/myy/client_upgrade/go-1.17.3-alternative.tar.gz" -O go-1.17.3-alternative.tar.gz && \
    tar zxf go-1.17.3-alternative.tar.gz && \
    rm go-1.17.3-alternative.tar.gz && \
    mv go-1.17.3-alternative /usr/local/go1.17.3-alternative

# Setup enviroment for golang
ENV GOPATH=/go
ENV PATH=/go/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH" && ln -s /usr/local/go1.21.4 /usr/local/go

# install requirements for s3-compatible tests
RUN pip3 install boto3==1.24.22 unittest2==1.1.0 requests==2.28.1 &&\
    pip3 cache purge
