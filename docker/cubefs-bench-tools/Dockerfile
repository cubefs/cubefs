# cubefs-bench-tools — IOR / mdtest sidecar 镜像
#
# 与 syncnode 同 Pod 部署，监听 127.0.0.1:18000 接受 syncnode 主容器的 HTTP
# 调度，执行 ior / mdtest 子进程并把结果以 JSON 形式返回。
#
# 强制启用 IOR JSON 输出（-O summaryFormat=JSON），所以必须 IOR >= 3.3。

# ─── Stage 1: build IOR + mdtest ─────────────────────────────────────────────
FROM ubuntu:22.04 AS ior-build

ARG IOR_VERSION=3.3.0
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        automake \
        autoconf \
        libtool \
        pkg-config \
        git \
        wget \
        libopenmpi-dev \
        openmpi-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN wget -O ior.tar.gz https://github.com/hpc/ior/releases/download/${IOR_VERSION}/ior-${IOR_VERSION}.tar.gz \
    && tar xzf ior.tar.gz \
    && cd ior-${IOR_VERSION} \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install

# ─── Stage 2: build Go runner ────────────────────────────────────────────────
# 构建上下文必须为 cubefs 仓库根：runner 放在主模块 cmd/bench-tools-runner
# 下，需要拷 go.mod / go.sum / depends（被 replace 指向）。runner 自身仅依赖
# stdlib，所以拷这几样就够，无需拷整个仓库。
FROM golang:1.24-bookworm AS go-build

WORKDIR /src/cubefs
COPY go.mod go.sum ./
COPY depends ./depends
COPY cmd/bench-tools-runner ./cmd/bench-tools-runner
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS="-mod=mod" go build -trimpath \
        -ldflags='-s -w' \
        -o /out/bench-tools-runner ./cmd/bench-tools-runner

# ─── Stage 3: runtime ────────────────────────────────────────────────────────
FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        openmpi-bin \
        tini \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ior-build /usr/local/bin/ior /usr/local/bin/ior
COPY --from=ior-build /usr/local/bin/mdtest /usr/local/bin/mdtest
COPY --from=go-build /out/bench-tools-runner /usr/local/bin/bench-tools-runner

# 默认监听 127.0.0.1:18000；如需调试可通过 BENCH_TOOLS_LISTEN 覆盖。
ENV BENCH_TOOLS_LISTEN=127.0.0.1:18000

EXPOSE 18000

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/bench-tools-runner"]
