# ============================================================================
# Stage 1: build mdtest from the ior project.
# mdtest is shipped as part of hpc/ior (https://github.com/hpc/ior). Pin a
# stable tag so the image is reproducible. Only the compiled mdtest binary
# is copied into the runtime image; the build toolchain is discarded.
# ============================================================================
FROM hub.shiyak-office.com/storage/ubuntu:22.04 AS mdtest-builder
ARG IOR_VERSION=3.3.0
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential autoconf automake libtool \
        libopenmpi-dev openmpi-bin git ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
RUN git clone --depth 1 --branch ${IOR_VERSION} https://github.com/hpc/ior.git
WORKDIR /opt/ior
RUN ./bootstrap && \
    ./configure --prefix=/opt/ior-install && \
    make -j"$(nproc)" && \
    make install

# ============================================================================
# Stage 2: runtime base. Adds openmpi runtime libs + the prebuilt mdtest
# binary on top of the original cubefs runtime deps.
# ============================================================================
FROM hub.shiyak-office.com/storage/ubuntu:22.04 AS base
RUN apt-get update && \
    apt-get install -y --no-install-recommends dnsutils xfsprogs jq fuse \
        libibverbs1 librdmacm1 ibverbs-providers \
        openmpi-bin libopenmpi3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=mdtest-builder /opt/ior-install/bin/mdtest /usr/local/bin/mdtest
COPY --from=mdtest-builder /opt/ior-install/bin/ior    /usr/local/bin/ior
RUN mkdir -p /cfs/bin /cfs/conf /cfs/logs /cfs/data
ENV PATH="/cfs/bin:/cfs/bin/blobstore:$PATH"

FROM base
COPY build/bin/ /cfs/bin/
