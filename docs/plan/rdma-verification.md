# CubeFS RDMA 优化代码 — Linux 验证 runbook

> 用于验证 `ft_support_rdma` 分支上 P0–P6 的 8 个 commit 在真实 Linux 环境的可编译、可测试、可运行性。
> 设计与进度见 [`rdma-optimization-spec.md`](rdma-optimization-spec.md)；运维部署见 [`rdma-deployment.md`](rdma-deployment.md)。

> ⚠️ macOS 上无法编译 `linux && rdma` build tag（缺 libibverbs / cgo + RDMA 头），所以必须在 Linux 跑这套验证。

---

## 0. 验证目标

按从快到慢分四级：

| 级别 | 目标 | 时间 | 硬件依赖 |
|---|---|---|---|
| **L1** 编译 | `go build -tags rdma ./...` 通过 | 5 分钟 | Linux + libibverbs |
| **L2** 单测 | `go test -tags rdma ./util/rdma/` 通过（含 stub 测试） | 10 分钟 | 同上 |
| **L3** rxe loopback | `TestLoopback` 在软件 RoCE 上端到端跑通 | 30 分钟 | + rdma_rxe 内核模块 |
| **L4** 真硬件性能 | `perf-parallel` 跑 spec 基线 | 数小时 | + Mellanox NIC + RoCEv2/IB |

最低要求：**至少跑完 L3**，证明协议端到端正确。L4 是性能验证，必须时再上真硬件。

---

## 1. 环境准备

### 1.1 OS / 内核

测试通过的版本（其他兼容版本也行）：
- Ubuntu 22.04 LTS 或 RHEL/CentOS 8+
- 内核 5.4+（rdma_rxe 需要 5.0+，建议 5.10+）

```bash
uname -r
# 期望 ≥ 5.4
```

### 1.2 包安装

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    libibverbs-dev \
    librdmacm-dev \
    rdma-core \
    iproute2 \
    perftest \
    tcpdump

# RHEL/CentOS
sudo yum install -y \
    libibverbs-devel \
    librdmacm-devel \
    rdma-core-devel \
    perftest \
    tcpdump
```

**验证**：

```bash
pkg-config --cflags --libs libibverbs librdmacm
# 期望输出非空，含 -libverbs -lrdmacm
```

### 1.3 Go 工具链

```bash
go version
# 期望 ≥ 1.21（cubefs go.mod 要求）
```

### 1.4 软件 RoCE（rxe）— L3 集成测试用

无 RDMA 硬件时，用内核 rdma_rxe 模块在 lo 上模拟：

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev lo

# 验证
rdma link show
# 期望: link rxe0/1 state ACTIVE physical_state LINK_UP netdev lo
ibv_devinfo | head -20
# 期望: hca_id: rxe0, transport: InfiniBand (0)
```

**清理**（测试完）：

```bash
sudo rdma link delete rxe0
```

---

## 2. 拉代码

```bash
cd ~/work
git clone <你的 fork>  # 或 oppo upstream
cd cubefs
git fetch
git checkout ft_support_rdma
git log --oneline -10
# 期望看到 9ba6a47c6..6f6958ea9 这 8 个 commit
```

如果是从 macOS 推过来的本地 commit（**当前未 push**），需要先在 macOS：

```bash
# macOS 端
cd /Users/tao.fang/codes/cubefs
git push origin ft_support_rdma
```

或用 `git bundle` 离线传：

```bash
# macOS 端
git bundle create rdma.bundle ft_support_rdma ^master
scp rdma.bundle linux-host:/tmp/

# Linux 端
git fetch /tmp/rdma.bundle ft_support_rdma:ft_support_rdma
git checkout ft_support_rdma
```

---

## 3. L1 — 编译验证

### 3.1 完整编译（含 rdma tag）

```bash
# 编译 cfs-server (datanode)
make CGO_ENABLED=1 build-server

# 编译 cfs-client
make CGO_ENABLED=1 build-client
```

或直接 go build：

```bash
go build -tags rdma -o /tmp/cfs-server ./cmd/cfs-server
go build -tags rdma -o /tmp/cfs-client ./cmd/cfs-client
```

**期望**：无错误，二进制产物正常。

**常见错误**：

| 错误 | 原因 | 解决 |
|---|---|---|
| `'rdma/rdma_cma.h' file not found` | 缺 librdmacm-dev | `apt install librdmacm-dev` |
| `undefined reference to ibv_*` | 缺 -libverbs | 检查 cgo LDFLAGS |
| `undefined: rdma.SlotHeaderSize` | rdma tag 没传 | 加 `-tags rdma` |

### 3.2 验证非 RDMA 构建（兼容性）

确认非 RDMA 构建仍能编译：

```bash
go build -o /tmp/cfs-client-tcp ./cmd/cfs-client
# 不带 rdma tag，应该走 stub 路径
```

**期望**：编译通过；二进制不含 cgo + libibverbs 依赖。

```bash
ldd /tmp/cfs-client-tcp | grep -E "(libibverbs|librdmacm)"
# 期望：无输出
ldd /tmp/cfs-client | grep -E "(libibverbs|librdmacm)"
# 期望：含 libibverbs.so / librdmacm.so
```

---

## 4. L2 — 单测验证

### 4.1 纯 Go 单测（无需硬件）

```bash
go test -count=1 -race -timeout 120s ./util/rdma/ ./proto/
```

**期望**：

```
ok  	github.com/cubefs/cubefs/util/rdma	2.0s
ok  	github.com/cubefs/cubefs/proto	1.5s
```

**44 个测试** 应全 PASS：8 SlotPool + 9 自适应 poll + 8 credit + 4 wrid + 4 limits + 3 metrics + 4 stub + 4 handshake + 其他。

### 4.2 RDMA tag 单测（仅纯 Go 子集，cgo 不依赖硬件）

```bash
go test -count=1 -race -timeout 120s -short -tags rdma ./util/rdma/
```

`-short` 跳过需要 rxe 的 `TestLoopback`；剩余测试纯 Go 状态机，需 cgo 编译但不需要 rdma 设备。

**期望**：所有非-`testing.Short()` 测试 PASS。

### 4.3 datanode 端单测

```bash
go test -count=1 -race -timeout 60s -tags rdma -run TestIsReadOp ./datanode/
```

**期望**：`TestIsReadOp_Classification` PASS。

---

## 5. L3 — rxe loopback 集成测试

### 5.1 准备 rxe

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev lo
ibv_devinfo                # 确认 rxe0 可见
```

### 5.2 跑 TestLoopback

```bash
# 必须有 rxe 设备 + 有 root 权限或 CAP_NET_RAW
go test -count=1 -timeout 120s -tags rdma -run TestLoopback -v ./util/rdma/
```

`TestLoopback` 端到端覆盖：
- 握手 ConnectInfo/AcceptInfo（含 P0 credit 字段）
- WRITE_WITH_IMM doorbell（P2）
- recv pool refill（P2）
- drainer goroutine（P1）
- WriteSlotZeroCopy 不会触发（这个测试只走 WriteData/ReturnCredit 普通路径）— 单独验证见 5.3

**期望**：

```
=== RUN   TestLoopback
--- PASS: TestLoopback (0.5s)
PASS
ok  	github.com/cubefs/cubefs/util/rdma	0.6s
```

**故障排查**：

| 错误 | 原因 |
|---|---|
| `no devices found` | rxe 没加载或没 link |
| `bind_addr: errno 99` | 用了非本地 IP |
| `ibv_create_qp failed` | qp 数超过 rxe 限制；试试改小 numSlots |
| 测试 hang | drainer 没启动；查 conn 创建路径 |

### 5.3 端到端 read 路径手动测试（rxe）

L3 完整版需要起 datanode + client，比较重。如果时间紧，可以先在 5.2 跑通 `TestLoopback` 即可，然后直接 L4 真硬件验证。

或者写个简化的本机 datanode + client：

```bash
# 终端 1：启动 datanode（rxe 模式）
mkdir -p /tmp/cfs-test/data
cat > /tmp/datanode.json <<EOF
{
  "role": "datanode",
  "listen": "127.0.0.1:17310",
  "logDir": "/tmp/cfs-test/log",
  "logLevel": "info",
  "raftHeartbeat": "127.0.0.1:17312",
  "raftReplica": "127.0.0.1:17313",
  "raftDir": "/tmp/cfs-test/raft",
  "masterAddr": ["127.0.0.1:17010"],
  "rdmaEnable": true,
  "rdmaPort": 17320,
  "rdmaNumSlots": 64,
  "rdmaSlotSize": 135168,
  "rdmaMinPayloadBytes": 4096
}
EOF
sudo /tmp/cfs-server -c /tmp/datanode.json -f
# 期望日志含 "RDMA server on port 17320" 和 "follower RDMA enabled"
```

完整 cluster 测试要起 master 也要 datanode，建议直接跳到 L4 在已有测试环境跑。

### 5.4 清理 rxe

```bash
sudo rdma link delete rxe0
```

---

## 6. L4 — 真硬件性能基线

仅在有 RoCEv2/IB NIC 的机器跑。环境名按你内部约定（spec 说 `test-hb`）。

### 6.1 部署

按 [`rdma-deployment.md`](rdma-deployment.md) 配置 datanode 和 client，关键开关：

```toml
rdmaEnable = true
rdmaNumSlots = 256
rdmaSlotSize = 135168    # 132KB
# P2 默认值即可
rdmaMinPayloadBytes = 4096
```

### 6.2 验证 RDMA 真在用（不是悄悄走 TCP）

**(a) tcpdump 在 datanode 端口** — 期望 RDMA 流量不进 TCP socket：

```bash
# 在 datanode 上
sudo tcpdump -i any -nn 'tcp port 17310' -c 100 &
# 跑客户端写测试
# 期望：tcpdump 几乎无流量（小包除外，<4KB 走 TCP）
```

**(b) Prometheus metrics**：

```bash
# datanode 上
curl -s localhost:9505/metrics | grep cubefs_rdma_
```

**期望看到**：

```
cubefs_rdma_requests_total{role="server",addr="..."}  > 0
cubefs_rdma_active_slots{role="..."}  ≥ 0
cubefs_rdma_poll_spin_total{role="server",phase="busy"}  > 0
cubefs_rdma_latency_seconds_bucket{role="...",le="0.0001"}  > 0    # < 100µs
cubefs_rdma_fallback_total  # 应该接近 0；非零时看 reason label
```

**(c) 验证非 RDMA 构建**：

```bash
go build -o /tmp/cfs-client-tcp ./cmd/cfs-client    # 不带 rdma tag
curl -s localhost:9505/metrics | grep cubefs_rdma_
# 期望：无任何 cubefs_rdma_* 指标
```

### 6.3 spec 性能基线

按 spec 表格跑 `perf-parallel`：

| 指标 | 基准（TCP）| 目标 | 验证命令 |
|------|-----------|------|----------|
| 顺序写吞吐 | 155 MB/s/node | P1 后 ≥ 300 | `perf-parallel --op write --size 128k --threads 16` |
| 顺序读吞吐 | 待测 | P4b 后 ≥ TCP × 1.5 | `perf-parallel --op read --size 128k --threads 16` |
| 写 P99 延迟 | — | 记录基线 | 同上 + `--latency` |
| polling CPU 空载 | — | < 1% | `top -p $(pgrep cfs-server)` 空闲 30s |

**关键观察**：

```bash
# 把 metrics 抓到本地观察
curl -s localhost:9505/metrics | grep cubefs_rdma_credit_stall_total
# 期望接近 0 — 非零说明 numSlots 配小了，credit 经常耗尽

curl -s localhost:9505/metrics | grep -E 'cubefs_rdma_poll_spin_total.*phase="sleep"'
# 满载时应远小于 busy/yield；空载时应主导
```

### 6.4 fallback 行为验证

**小包走 TCP**：

```bash
# datanode 上
sudo tcpdump -i any -nn 'tcp port 17310 and (tcp[((tcp[12:1] & 0xf0) >> 2):4] = 0x12345678)' &
# (用 ProtoMagic 0x12345678 过滤 cubefs 包)

# 写一个 100B 文件
echo "tiny" > /mount/path/tiny.txt

# 期望：tcpdump 看到这个写包；metrics fallback_total{reason="small_payload"} +1
curl -s localhost:9505/metrics | grep 'fallback_total.*small_payload'
```

**RDMA 网络断开 fallback**：

```bash
# 关掉 RDMA NIC
sudo ip link set ib0 down
# 跑写测试 — 应仍能成功（走 TCP）
echo "fallback test" > /mount/path/test.txt
# metrics 应有 fallback_total{reason="write_packet"} 或 "acquire_slot"
sudo ip link set ib0 up
```

---

## 7. 故障排查

### 7.1 编译错误

```
util/rdma/rdma.h:4:10: fatal error: 'rdma/rdma_cma.h' file not found
```
→ `apt install librdmacm-dev`

```
ld: cannot find -libverbs
```
→ `apt install libibverbs-dev`

### 7.2 运行时错误

```
rdma: Listen: SlotSize=131072 too small; need >= 131157
```
→ 升级配置：`rdmaSlotSize = 135168`（spec MinValidSlotSize 之上）。**P0 在保护你**，TCP 路径不变，RDMA 不会启用。

```
rdma: connect to ...: rdma_resolve_addr failed
```
→ NIC 没起来 / 路由错。`rdma link show`、`ip a`、`ibv_devinfo` 排查。

```
rdma drainer (...): WR error op=opSlot status=12
```
→ status 12 = `IBV_WC_RETRY_EXC_ERR`，QP 超时。可能对端 hang 或网络不通。conn 会 force-close，pool 自动重建。

```
panic: rdma: handler invoked net.Conn.Write on RDMA fakeConn ...
```
→ **P4a 的断言触发**。说明 read-style handler 走到了 RDMA 路径。检查 `isReadOp` 是否漏了某个 opcode。把那条 panic 消息和触发包的 opcode 发回来。

### 7.3 性能不达标

| 现象 | 可能原因 |
|---|---|
| 吞吐没升到 300 MB/s | numSlots 太少（默认 256，可调更高至 1024）；多 slot 并发不充分 |
| 延迟高 | `BusySpinCount=200` 太小，调大到 1000；或 NIC tuning |
| CPU 空载 > 1% | drainer 没真正 sleep；查 `cubefs_rdma_poll_spin_total{phase="sleep"}` 是否大于 busy/yield |
| credit_stall 非零 | numSlots 太少（增大）或对端 ReturnCredit 慢 |

---

## 8. 验证完成 checklist

- [ ] L1 编译通过（`-tags rdma`，含 cfs-server/cfs-client）
- [ ] L1 非 rdma 构建编译通过且不含 libibverbs 链接
- [ ] L2 单测全 PASS（`-tags ''` 和 `-tags rdma -short` 各一遍）
- [ ] L3 `TestLoopback` 在 rxe 上 PASS
- [ ] L4 真硬件 datanode 启动 + `cubefs_rdma_*` 指标可见
- [ ] L4 `perf-parallel` 顺序写 ≥ 300 MB/s/node
- [ ] L4 `perf-parallel` 顺序读 ≥ TCP × 1.5
- [ ] L4 空载时 polling CPU < 1%
- [ ] L4 关 NIC 后 fallback TCP 工作正常
- [ ] L4 小包（< 4KB）触发 `fallback_total{reason="small_payload"}`

完成后 push 分支 + 提 PR。
