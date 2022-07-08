FROM golang:1.15


ENV JAVA_HOME=jdk1.8.0_321
ENV CLASSPATH=$CLASSPATH:$JAVA_HOME/lib
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /apps
VOLUME ["/apps/conf"]
COPY   ./bin/ /apps/
COPY  ./cmd/clustermgr/clustermgr.conf /apps/conf/clustermgr.conf
COPY  ./cmd/clustermgr/clustermgr1.conf /apps/conf/clustermgr1.conf
COPY  ./cmd/clustermgr/clustermgr2.conf /apps/conf/clustermgr2.conf
COPY  ./cmd/blobnode/blobnode.conf /apps/conf/blobnode.conf
COPY  ./cmd/access/access.conf /apps/conf/access.conf
COPY  ./cmd/scheduler/scheduler.conf /apps/conf/scheduler.conf
COPY  ./cmd/proxy/proxy.conf /apps/conf/proxy.conf
COPY  start_docker.sh /apps/start_docker.sh
ENTRYPOINT ["/bin/sh","-c","/apps/start_docker.sh"]
