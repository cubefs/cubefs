# CubeFS RDMA 部署指南

> 本文档面向部署和运维，说明 RDMA 功能的开启方式、配置项含义和故障排查。
> 设计原理见 `rdma-design.md`，实现细节见 `rdma-impl-plan.md` / `rdma-optimization-spec.md`，验证步骤见 `rdma-verification.md`。

---

## 1. 功能概述

CubeFS RDMA 传输层覆盖以下数据路径，**全部支持透明回退到 TCP**：

| 路径 | 传输 | 配置位置 |
|---|---|---|
| Client → DataNode（写） | RDMA Write 替换 TCP send | `cfs-client` mount option |
| Client → DataNode（读） | RDMA Write + 数据回写 | 同上 |
| DataNode 间副本复制 | RDMA Write 替换 TCP send | `cfs-server` 配置 |

**不在 RDMA 路径范围**（继续走 TCP，无需配置）：

- Master ↔ Master / Master ↔ DataNode 心跳与上报
- Client ↔ Master 元数据查询
- Client ↔ MetaNode 元数据操作
- ObjectNode / FlashNode / cfs-cli 控制流

**三层兼容保护，TCP 路径零影响**：

1. **Build Tag**：`make RDMA=0 build` 编译产物完全不含 RDMA 代码
2. **总开关**：`rdmaEnable=false` 时进程行为与未改动前完全一致
3. **运行时降级**：RDMA 任一阶段失败（init / 握手 / WR / 包大小不达标）自动回退 TCP，进程不退出，自动记 Prometheus `cubefs_rdma_fallback_total{reason=...}`

---

## 2. 前置条件

### 硬件（DataNode 和 Client 节点均需）

| 要求 | 说明 |
|---|---|
| RDMA NIC | Mellanox ConnectX-4 / 5 / 6（IB 或 RoCEv2 均支持）|
| 网络 | InfiniBand HDR/EDR 或 RoCEv2 100 GbE |
| 内核驱动 | `mlx5_core` 或 Mellanox OFED ≥ 5.0 |
| MTU | RoCEv2 建议 9000（jumbo），减少 4 KB-128 KB 大包分片 |

### 软件（DataNode 和 Client 节点均需安装）

```bash
# RHEL / CentOS / Rocky
sudo dnf install -y libibverbs-devel librdmacm-devel rdma-core-devel

# Ubuntu / Debian
sudo apt-get install -y libibverbs-dev librdmacm-dev rdma-core
```

**验证设备可见**：

```bash
ibv_devinfo            # 期望至少一个 hca_id 输出
rdma link show         # 期望 state ACTIVE physical_state LINK_UP
```

### 不需要 RDMA 的节点

Master / MetaNode / ObjectNode / FlashNode 只需 TCP 网络。**不要在这些节点装 libibverbs-devel 或开 RDMA 配置**，无意义。

---

## 3. 编译

`make build` 默认带 RDMA tag（`-tags rdma`）+ 链接 libibverbs/librdmacm。

```bash
# 默认带 RDMA：所有 cfs-server / cfs-client 二进制含 RDMA 代码
make build

# 显式禁用 RDMA：纯 TCP 二进制，不依赖 libibverbs/librdmacm
make RDMA=0 build
```

**验证**：

```bash
ldd build/bin/cfs-server | grep -E "libibverbs|librdmacm"
# 带 RDMA 编译：能看到 libibverbs.so.1 / librdmacm.so.1
# RDMA=0 编译：无输出
```

> 是否运行时启用由配置 `rdmaEnable` 决定。带 RDMA 编译 + `rdmaEnable=false` 是合法组合，进程纯 TCP 运行。

---

## 4. 配置文件

### 4.1 Master / MetaNode

**无需任何 RDMA 相关改动**。沿用现有配置。

### 4.2 DataNode（`datanode.json`）

启动：`/cfs/bin/cfs-server -f -c /cfs/conf/datanode.json`

最小开启 RDMA 的配置（其他字段保持原样）：

```json
{
  "role": "datanode",
  "listen": "17310",
  "raftHeartbeat": "17320",
  "raftReplica": "17330",
  "localIP": "192.168.1.10",
  "masterAddr": "192.168.1.1:17010,192.168.1.2:17010,192.168.1.3:17010",
  "logDir": "/cfs/log",
  "dataDir": "/cfs/data",
  "diskPath": "/data1:209715200,/data2:209715200",

  "rdmaEnable": true,
  "rdmaPort": 17315,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 135168,
  "rdmaMinPayloadBytes": 4096,
  "rdmaBusySpinCount": 200,
  "rdmaYieldCount": 1000,
  "rdmaSleepThresholdUs": 50
}
```

### 4.3 Client（`client.json` 或 mount options）

Client 端通过 mount options 配置：

```json
{
  "mountPoint": "/cfs/mnt",
  "volName": "myvol",
  "owner": "myapp",
  "masterAddr": "192.168.1.1:17010,192.168.1.2:17010,192.168.1.3:17010",
  "logDir": "/cfs/log",
  "logLevel": "info",

  "rdmaEnable": true,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 135168,
  "rdmaMinPayloadBytes": 4096,
  "rdmaBusySpinCount": 200,
  "rdmaYieldCount": 1000,
  "rdmaSleepThresholdUs": 50
}
```

### 4.4 配置一致性要求

DataNode 和 Client 之间**必须一致**的字段：

| 字段 | 不一致后果 |
|---|---|
| `rdmaNumSlots` | 握手时 `numSlots` 字段不匹配，连接建立失败（fallback TCP）|
| `rdmaSlotSize` | 握手时 `slotSize` 不匹配，导致大包截断 |

**可以独立配置**的字段（每端自己决定）：
- `rdmaMinPayloadBytes`：每端独立判断小包跳 RDMA
- `rdmaBusySpinCount` / `rdmaYieldCount` / `rdmaSleepThresholdUs`：调度策略，不参与握手
- `rdmaPort`：仅 DataNode 需要

---

## 5. 配置项参考

### DataNode 配置项

| 配置键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `rdmaEnable` | bool | `false` | 总开关。`false` 时所有路径走 TCP |
| `rdmaPort` | int | 0 | DataNode RDMA 监听端口（建议 TCP 端口 +5）|
| `rdmaNumSlots` | int | 256 | 每条连接的 ring buffer slot 数。决定连接内并发上限 |
| `rdmaSlotSize` | int | 135168 (132 KB) | 每个 slot 字节数。最小 = SlotHeader(16) + max PacketHeader(69) + BlockSize(128 KB) = 131157；P0 启动时校验，过小自动 fallback TCP |
| `rdmaMinPayloadBytes` | int | 4096 | 小于此阈值的包跳 RDMA 走 TCP（P6）。0 = 关闭阈值 |
| `rdmaBusySpinCount` | int | 200 | 自适应 poll phase 1 最大忙轮询次数（约 1µs，不让 sub-µs 包付出 yield 代价）|
| `rdmaYieldCount` | int | 1000 | phase 2 最大 Gosched 次数 |
| `rdmaSleepThresholdUs` | int | 50 | phase 2 累计微秒数超过则进 phase 3 sleep（comp_channel 阻塞）|

### Client 配置项

| 配置键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `rdmaEnable` | bool | `false` | 总开关 |
| `rdmaNumSlots` | int | 256 | 同 DataNode，必须与服务端一致 |
| `rdmaSlotSize` | int | 135168 | 同 DataNode，必须与服务端一致 |
| `rdmaMinPayloadBytes` | int | 4096 | 同 DataNode |
| `rdmaBusySpinCount` | int | 200 | 同 DataNode |
| `rdmaYieldCount` | int | 1000 | 同 DataNode |
| `rdmaSleepThresholdUs` | int | 50 | 同 DataNode |

> Client 没有 `rdmaPort` —— Client 是发起方，连接 DataNode 公布的端口。

### 调优建议

| 场景 | 调整方向 |
|---|---|
| 顺序大文件写（4 MB+） | `rdmaSlotSize` 提到 4194304+128，`rdmaNumSlots` 不变。注意握手两端一致 |
| 小文件密集随机写 | `rdmaMinPayloadBytes` 设小（如 1024），让更多包走 RDMA。但若 NIC PF 性能弱，反而保留默认 |
| 高 QPS 场景 | `rdmaNumSlots` 提到 512+；注意 `rdmaSlotSize` × `rdmaNumSlots` × 6（6 个 MR）= 单连接内存，128 KB × 512 × 6 = 384 MB |
| 极低延迟（亚 10µs P99）| `rdmaBusySpinCount` 提到 1000、`rdmaSleepThresholdUs` 提到 200，避免过早进 sleep；代价是空载 CPU 升高 |
| 空载省 CPU | `rdmaSleepThresholdUs` 设小（如 10），尽快 comp_channel sleep |

---

## 6. 启动验证

### 6.1 看 DataNode 日志

```bash
grep -i rdma /cfs/log/datanode/datanode_info.log | head -20
```

**期望看到**（成功）：

```
Start: init RDMA server on port 17315
initConnPool: follower RDMA enabled (numSlots=256 slotSize=135168)
```

**降级警告**（进程仍正常运行，走 TCP）：

```
initConnPool: RDMA init failed, degraded to TCP-only: <err>           # server 初始化失败
initConnPool: RDMA Start failed, degraded to TCP-only: <err>          # server 启动失败（多半端口冲突）
initConnPool: follower RDMA init failed, replication uses TCP: <err>  # 副本复制初始化失败
```

### 6.2 看 Client 日志

```bash
grep -i rdma /cfs/log/client.log | head -10
```

**期望**：

```
NewSuper: RDMA client pool initialized (numSlots=256 slotSize=135168 busy=200 yield=1000 sleep=50us)
```

### 6.3 抓 Prometheus metrics（重要）

DataNode 和 Client 都暴露在标准 metrics 端点（默认 9505 等）：

```bash
curl -s http://192.168.1.10:9505/metrics | grep cubefs_rdma_
```

**期望看到**：

```
cubefs_rdma_requests_total{role="server",addr="..."} > 0
cubefs_rdma_active_slots{role="..."} ≥ 0
cubefs_rdma_poll_spin_total{role="server",phase="busy"} > 0
cubefs_rdma_latency_seconds_bucket{role="...",le="0.0001"} > 0   # 100µs 以内
cubefs_rdma_fallback_total ≈ 0  # 非零看 reason label
```

**fallback reason 标签**（用于诊断）：

| reason | 含义 |
|---|---|
| `acquire_slot` | 连接池满 / 拨号失败 |
| `write_packet` | post WR 失败（QP error）|
| `poll_response` | 响应超时（30s）|
| `return_credit` | credit 回写失败 |
| `reqid_mismatch` | 收到错号响应（含解析错误时 server 主动回 ReqID=0）|
| `crc_mismatch` | 数据 CRC 校验失败 |
| `op_again` | 服务端要求 fallback（包过大）|
| `size_mismatch` | 响应字节数不符 |
| `small_payload` | 包小于 `rdmaMinPayloadBytes`（P6 主动跳）|

---

## 7. 部署场景

### 场景 A：全集群启用（最常见）

所有 DataNode 和 Client 节点都有 RDMA NIC。

```
DataNode×N  (rdmaEnable=true, rdmaPort=17315)
   ↕ RDMA
Client×M    (rdmaEnable=true)
```

期望状态：所有写 / 读流量走 RDMA，TCP 仅承载 master 通信。

### 场景 B：仅副本复制走 RDMA

Client 节点没 RDMA NIC，但 DataNode 集群有内部 RDMA 网络（典型：客户端在传统机架，存储集群在专用机架）。

```
DataNode×N  (rdmaEnable=true)  内部副本走 RDMA
   ↕ TCP
Client×M    (rdmaEnable=false)
```

DataNode 之间副本复制走 RDMA（leader→follower），延迟 -80%；Client 走 TCP 不变。

### 场景 C：渐进灰度

```
DataNode×N    (rdmaEnable=true)  全开
   ↕
Client-RDMA   (rdmaEnable=true, 部分机器)
Client-TCP    (rdmaEnable=false, 其余机器)
```

混合运行，对比 metrics 中两类 client 延迟差异决定全推。

### 场景 D：临时禁用（无需重编译）

修改 `rdmaEnable` 为 `false`，重启进程。TCP 路径完全不受影响。带 RDMA tag 编译的二进制 + `rdmaEnable=false` 完全合法。

---

## 8. 故障排查

### Symptom: DataNode 启动后看不到 RDMA Info 日志

```bash
# 1. 确认配置生效
grep rdmaEnable /cfs/conf/datanode.json
# 期望：true

# 2. 确认编译带了 RDMA
ldd /cfs/bin/cfs-server | grep ibverbs
# 无输出说明用了 RDMA=0 编译，重新 make build
```

### Symptom: `RDMA init failed` Warn

```bash
# 1. RDMA 设备可见性
ibv_devinfo
# 至少一个 hca_id

# 2. 端口占用
ss -tlnp | grep 17315

# 3. SlotSize 校验
grep -i "SlotSize.*too small" /cfs/log/datanode/*.log
# 出现 → 调大 rdmaSlotSize，最小 131157
```

### Symptom: `cubefs_rdma_fallback_total{reason="X"}` 持续增长

| reason | 排查方向 |
|---|---|
| `acquire_slot` | DataNode 端 RDMA 服务挂了 / 端口不通；用 `ibv_devinfo` + `rdma link show` 确认链路 |
| `write_packet` | NIC retry 耗尽（RNR_RETRY_EXC_ERR），网络抖动严重；检查交换机 PFC、丢包率 |
| `poll_response` | 服务端处理超时（>30s）；检查 DataNode 磁盘 IO 性能 |
| `crc_mismatch` | 内存 / 网卡数据损坏；查 NIC 计数器、内存 ECC |
| `op_again` | 客户端单次请求 size 超过 slotSize；调大 `rdmaSlotSize` 或减小客户端 IO 切片 |
| `small_payload` | 正常现象（<4KB 包主动走 TCP）；如果想这部分也走 RDMA，把 `rdmaMinPayloadBytes` 设小 |

### Symptom: Client 端报 `ReqID mismatch`

服务端解析包失败时回了 ReqID=0 让 client 立刻 fallback（M3 fast-fail）。看服务端日志：

```bash
grep "DeserializePacket" /cfs/log/datanode/*.log
# 看具体错误，常见：bad slot magic（slot 被覆盖）/ TotalLen 超 slot
```

### Symptom: 空载 CPU 没降下来

```bash
# 看 poll phase 分布
curl -s :9505/metrics | grep cubefs_rdma_poll_spin_total
# 期望：sleep > yield > busy（空载时）
# 如果 busy 占比高，说明流量持续到达（不是真空载）或 BusySpinCount 设过大
```

### Symptom: 频繁 credit_stall

```bash
curl -s :9505/metrics | grep cubefs_rdma_credit_stall_total
# 持续增长说明发送方比接收方快太多，credit 被打空
```

应对：
- DataNode 磁盘 IO 慢，加磁盘 / 调 `diskQos`
- 客户端并发过高超过 `rdmaNumSlots`，调小并发或加大 numSlots
- Sleep 退避 ~1ms 工作正常，goroutine 不会忙等

---

## 9. 性能参考（旧基准）

以下数据基于 Mellanox ConnectX-5 100GbE RoCEv2，单流顺序写，**P0–P6 实施前的基线**，仅供量级参考。新基线需在你环境实测：

| 场景 | TCP | RDMA（旧实现）| 改善 |
|---|---|---|---|
| 单副本写延迟（4 MB block） | ~800 µs | ~150 µs | ~5× |
| 三副本写总延迟 | ~2.4 ms | ~450 µs | ~5× |
| DataNode CPU（10 GB/s 写） | ~70% | ~20% | −50% |

**P1+P2+P5 lite 后预期改善**（实测前为目标值）：

| 指标 | 旧 RDMA | 新 RDMA 目标 |
|---|---|---|
| 写吞吐 | 1×（基线）| ≥ 2× |
| 读吞吐 | 不支持 | ≥ TCP × 1.5 |
| 空载 CPU | ~30% | < 1% |
| credit_stall 阻塞频率 | 不可观测 | metrics 透明 |

> 实测方法见 `rdma-verification.md` L4 节。

---

## 10. 部署 checklist

按顺序勾完：

- [ ] DataNode 节点装 `libibverbs-devel + librdmacm-devel`
- [ ] Client 节点装同上
- [ ] `ibv_devinfo` 在每个 RDMA 节点能看到设备
- [ ] `rdma link show` state=ACTIVE
- [ ] `make build` 通过；`ldd cfs-server | grep ibverbs` 有输出
- [ ] DataNode 配置加 `rdmaEnable=true` + `rdmaPort` + 上面一组 P0-P6 字段
- [ ] DataNode `rdmaPort` 在防火墙 / SG 放行（IB 不需要，RoCEv2 走 UDP/4791）
- [ ] Client 配置加 `rdmaEnable=true` + 上面一组字段
- [ ] DataNode / Client 的 `rdmaNumSlots` 和 `rdmaSlotSize` 一致
- [ ] 启动 DataNode → 日志看 "Start: init RDMA server"
- [ ] 启动 Client → 日志看 "RDMA client pool initialized"
- [ ] 抓 metrics → `cubefs_rdma_requests_total > 0` 且 `fallback_total` 增长缓慢
- [ ] 跑业务流量 1 小时 → fallback reason 分布看是否合理
- [ ] **Master / MetaNode / ObjectNode / FlashNode 不动**

---

## 11. 回滚

```bash
# 方式 1：配置回滚（推荐，无重启 master/metanode）
# 修改 datanode.json 和 client.json
"rdmaEnable": false
# 重启 DataNode 和 Client 即可

# 方式 2：编译回滚（极端情况）
make RDMA=0 build
# 重新部署二进制
```

回滚后 metrics `cubefs_rdma_*` 全部停止变化（编译回滚）或归零（配置回滚）。
