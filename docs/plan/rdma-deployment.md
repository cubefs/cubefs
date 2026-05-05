# CubeFS RDMA 部署指南

> 本文档面向部署和运维，说明 RDMA 功能的开启方式、配置项含义和故障排查。
> 设计原理见 `rdma-design.md`，实现细节见 `rdma-impl-plan.md`。

---

## 1. 功能概述

CubeFS RDMA 传输层在 Client→DataNode 和 DataNode→DataNode 副本两条路径上引入
**纯 One-sided RDMA Write**，绕过操作系统内核协议栈，显著降低写入延迟和 DataNode CPU 占用。

| 路径 | 传输协议 | 涉及组件 |
|------|---------|---------|
| Client → DataNode（leader） | RDMA Write（替换 TCP send） | `cfs-client`、`cfs-server` |
| DataNode leader → Follower | RDMA Write（替换 TCP send） | `cfs-server`（副本复制） |

**三层保护机制，TCP 路径零影响**：

1. **Build Tag**：`RDMA=0 make build` 编译产物中不含任何 RDMA 代码
2. **函数变量注入**：`rdmaEnable=false` 时函数变量为 nil，热路径代码不走任何 RDMA 分支
3. **运行时降级**：RDMA 初始化失败时输出 Warn 日志并自动回退到 TCP，进程不退出

---

## 2. 前置条件

### 硬件

| 要求 | 说明 |
|------|------|
| RDMA NIC | Mellanox ConnectX-4/5/6（IB 或 RoCEv2 均支持） |
| 网络 | InfiniBand HDR/EDR 或 RoCEv2 100GbE |
| 内核驱动 | `mlx5_core` 或 Mellanox OFED ≥ 5.0 |

### 软件（各 DataNode 和 Client 节点均需安装）

```bash
# Ubuntu 22.04
apt-get install -y libibverbs-dev librdmacm-dev rdma-core

# 验证设备可见
ibv_devinfo
rdma link show
```

### 开发 / CI 环境（无 RDMA 硬件时用软件 RoCE）

```bash
modprobe rdma_rxe
rdma link add rxe0 type rxe netdev eth0
rdma link show
# 期望: link rxe0/1 state ACTIVE physical_state LINK_UP
```

---

## 3. 编译

RDMA 默认编译进二进制（`make build` 即带 RDMA 支持）。
如需排除 RDMA 依赖（例如目标环境没有 libibverbs），使用 `RDMA=0`：

```bash
# 默认：编译带 RDMA 的 cfs-server 和 cfs-client
make build

# 排除 RDMA（纯 TCP，无 libibverbs 依赖）
RDMA=0 make build

# 或仅排除 server
RDMA=0 make server
```

> **注意**：编译带 RDMA 的二进制不等于启用 RDMA。
> 是否实际使用 RDMA 由运行时配置 `rdmaEnable` 决定，默认 `false`。

---

## 4. 配置文件

### 4.1 DataNode（`datanode.json`）

DataNode 启动命令：`/cfs/bin/cfs-server -f -c /cfs/conf/datanode.json`

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

  "_rdma_comment": "RDMA 配置（默认全部关闭，不影响 TCP 路径）",
  "rdmaEnable": false,
  "rdmaPort": 17315,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 131072
}
```

**RDMA 开启示例**（仅改这四行）：

```json
  "rdmaEnable": true,
  "rdmaPort": 17315,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 131072
```

### 4.2 Client（`client.json`）

Client 挂载命令：`/cfs/bin/cfs-client -f -c /cfs/conf/client.json`

```json
{
  "mountPoint": "/cfs/mnt",
  "volName": "myvol",
  "owner": "myapp",
  "masterAddr": "192.168.1.1:17010,192.168.1.2:17010,192.168.1.3:17010",
  "logDir": "/cfs/log",
  "logLevel": "info",

  "_rdma_comment": "RDMA 配置（默认关闭）",
  "rdmaEnable": false,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 131072
}
```

**RDMA 开启示例**：

```json
  "rdmaEnable": true,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 131072
```

---

## 5. 配置项说明

### DataNode 配置项

| 配置键 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `rdmaEnable` | bool | `false` | 是否启用 RDMA。`false` 时进程行为与未改动前完全一致 |
| `rdmaPort` | int | `0` | DataNode RDMA 监听端口。建议设为 TCP 监听端口 +5（如 TCP=17310 则 RDMA=17315）|
| `rdmaNumSlots` | int | `256` | 每条 RDMA 连接的 Ring Buffer slot 数。决定同一连接的最大并发请求数 |
| `rdmaSlotSize` | int | `131072` | 每个 slot 的字节数（字节）。必须 ≥ 最大写入 block 大小，默认 128 KB |

### Client 配置项

| 配置键 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `rdmaEnable` | bool | `false` | 是否启用 RDMA 数据路径。Client 需与 DataNode 均开启才能生效 |
| `rdmaNumSlots` | int | `256` | 每条连接的 slot 数，应与 DataNode 侧配置一致 |
| `rdmaSlotSize` | int | `131072` | 每个 slot 的字节数，应与 DataNode 侧配置一致 |

### 配置要点

- **DataNode 和 Client 的 `rdmaNumSlots`/`rdmaSlotSize` 必须一致**，否则握手时连接建立失败
- `rdmaSlotSize` 设置为最大 I/O block 大小 + 头部开销（约 128 bytes）。4 MB 写入场景设为 `4194432`（4 MB + 128）
- `rdmaPort` 仅 DataNode 需要（监听用）；Client 通过连接 DataNode 的 RDMA 端口发起连接

---

## 6. 日志说明

### 启动日志

| 日志内容 | 级别 | 含义 |
|---------|------|------|
| `Start: init RDMA server on port 17315` | Info | DataNode 开始初始化 RDMA server |
| `initConnPool: follower RDMA enabled (numSlots=256 slotSize=131072)` | Info | DataNode 副本复制路径 RDMA 初始化成功 |
| `NewSuper: RDMA client pool initialized (numSlots=256 slotSize=131072)` | Info | Client 连接池初始化成功 |

### 降级警告（进程正常运行，自动回退到 TCP）

| 日志内容 | 级别 | 含义 | 处理建议 |
|---------|------|------|---------|
| `initConnPool: RDMA init failed, degraded to TCP-only: <err>` | **Warn** | DataNode RDMA server 初始化失败 | 检查 RDMA 驱动和端口占用 |
| `initConnPool: RDMA Start failed, degraded to TCP-only: <err>` | **Warn** | DataNode RDMA server 启动失败 | 检查 `rdmaPort` 是否被占用 |
| `initConnPool: follower RDMA init failed, replication uses TCP: <err>` | **Warn** | 副本复制路径初始化失败，改用 TCP | 通常与 server 初始化失败同时出现 |
| `NewSuper: RDMA init failed, falling back to TCP: <err>` | **Warn** | Client 连接池初始化失败 | 检查 DataNode 是否已开启 RDMA |
| `sendToDataPartition: rdma failed, addr(...) ... fallthrough to TCP` | **Warn** | 单次写请求 RDMA 失败，自动改用 TCP | 检查 RDMA 连接状态；频繁出现时排查网络 |

> **关于日志频率**：单次写请求 RDMA 失败的 Warn 日志在 RDMA 持续不可用时会随每次 I/O 输出。
> 若大量出现，表明 RDMA 已整体不可用，建议临时将 `rdmaEnable` 设为 `false` 并排查。

---

## 7. 典型部署场景

### 场景 A：DataNode 集群全部开启，Client 选择性开启

适用于部分 Client 机器有 RDMA NIC、部分没有的混合环境。

```
DataNode-1 (rdmaEnable=true, rdmaPort=17315)
DataNode-2 (rdmaEnable=true, rdmaPort=17315)
DataNode-3 (rdmaEnable=true, rdmaPort=17315)

Client-A (rdmaEnable=true)   → 使用 RDMA 路径
Client-B (rdmaEnable=false)  → 使用 TCP 路径（与 RDMA 无关，完全独立）
```

### 场景 B：仅启用副本复制 RDMA（DataNode 间）

如果 Client 节点没有 RDMA NIC，仍可仅在 DataNode 间开启 RDMA 以降低副本复制延迟。

```
DataNode（rdmaEnable=true）：RDMA server 接收 Client 写入（但 Client 用 TCP），
同时副本复制走 RDMA（follower RDMA 也一并开启）。
```

> 当前实现中，`rdmaEnable=true` 同时开启接收侧和副本侧。
> 如需拆分控制，可在未来版本中增加独立配置项。

---

## 8. 故障排查

### DataNode 启动后看不到 RDMA 相关 Info 日志

**原因**：`rdmaEnable` 未设为 `true`，或配置文件路径不对。

```bash
# 确认配置已生效
grep rdmaEnable /cfs/conf/datanode.json
```

### RDMA Warn：`RDMA init failed`

常见原因：

```bash
# 1. RDMA 设备不可见
ibv_devinfo
# 期望看到至少一个设备；若无输出，检查驱动

# 2. rdmaPort 被占用
ss -tlnp | grep 17315

# 3. 缺少 libibverbs
ldd /cfs/bin/cfs-server | grep verbs
# 若 "not found"，说明编译时未链接 RDMA 库（RDMA=0 编译）
```

### Client 端 Warn：单包 RDMA 失败频繁

```bash
# 检查 RDMA 链路状态
rdma link show
# 期望: state ACTIVE physical_state LINK_UP

# 检查丢包
perfquery   # IB
ethtool -S <eth_interface> | grep -i drop   # RoCEv2
```

### 临时禁用 RDMA（不重新编译）

修改配置文件中 `rdmaEnable` 为 `false`，重启进程即可。TCP 路径完全不受影响。

---

## 9. 性能参考

以下数据基于 Mellanox ConnectX-5 100GbE RoCEv2，单流顺序写，仅供参考。

| 场景 | TCP | RDMA | 改善 |
|------|-----|------|------|
| 单副本写延迟（4 MB block） | ~800 μs | ~150 μs | ~5× |
| 三副本写总延迟 | ~2.4 ms | ~450 μs | ~5× |
| DataNode CPU（10 GB/s 写） | ~70% | ~20% | −50% |

> 实际效果受网络拓扑、NIC 型号和工作负载影响。
> 建议在目标环境用实际业务流量验收，而非依赖基准数字。
