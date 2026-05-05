FROM hub.shiyak-office.com/storage/ubuntu:22.04 AS base
RUN apt-get update && \
    apt-get install -y --no-install-recommends dnsutils xfsprogs jq fuse && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /cfs/bin /cfs/conf /cfs/logs /cfs/data
ENV PATH="/cfs/bin:/cfs/bin/blobstore:$PATH"

FROM base
COPY build/bin/ /cfs/bin/
