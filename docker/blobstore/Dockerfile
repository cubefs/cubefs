FROM golang:1.17.13

RUN mkdir -p /cfs/log /cfs/data /cfs/bin /cfs/conf /cfs/log/auditlog /cfs/data/clustermgr/db /cfs/data/clustermgr/raftwal /cfs/data/proxycache \
        && apt-get update \
        && apt-get install -y jq vim iputils-ping \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get clean 

COPY ./docker/blobstore/*.sh /cfs/bin/
COPY ./build/bin/blobstore/ /cfs/bin
RUN chmod +x /cfs/bin/*