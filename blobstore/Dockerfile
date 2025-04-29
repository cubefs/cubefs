FROM golang:1.18.10@sha256:50c889275d26f816b5314fc99f55425fa76b18fcaf16af255f5d57f09e1f48da

RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

ENV JAVA_HOME=bin/jdk1.8.0_321
ENV CLASSPATH=$CLASSPATH:$JAVA_HOME/lib
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /apps
VOLUME ["/apps/conf", "/apps/bin"]
COPY  ./build/bin/blobstore/ /apps/bin/
COPY  ./blobstore/cmd/clustermgr/clustermgr1.conf /apps/conf/clustermgr1.conf
COPY  ./blobstore/cmd/clustermgr/clustermgr2.conf /apps/conf/clustermgr2.conf
COPY  ./blobstore/cmd/clustermgr/clustermgr3.conf /apps/conf/clustermgr3.conf
COPY  ./blobstore/cmd/blobnode/blobnode.conf /apps/conf/blobnode.conf
COPY  ./blobstore/cmd/access/access.conf /apps/conf/access.conf
COPY  ./blobstore/cmd/scheduler/scheduler.conf /apps/conf/scheduler.conf
COPY  ./blobstore/cmd/proxy/proxy.conf /apps/conf/proxy.conf
COPY  ./blobstore/cli/cli/cli.conf /apps/conf/blobstore-cli.conf
COPY  ./blobstore/start_docker.sh /apps/start_docker.sh
ENTRYPOINT ["/bin/sh", "-c", "/apps/start_docker.sh"]
