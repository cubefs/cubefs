# CubeFS RDMA 架构（post-P0.5）

> **状态**：post-P0.5 当前架构 + P1 设计。
> **写作日期**：2026-05-13。
> **替代**：旧的 `rdma-architecture-review.md`（分析对象是 P0 之前的全栈双向 RDMA，已删除）以及本文档自己的 post-P0 版本（DataNode RDMA 保留版，已在 P0.5 下线）。

---

## 1. 当前 RDMA 边界

P0.5 完成后，CubeFS RDMA 范围从"DataNode 间复制 + 客户端"双场景再收敛到"仅 P1 专用 SDK"一个明确场景。

| 路径 | 状态 | 开关 |
|---|---|---|
| **DataNode 间写复制 + 修复流量** | ✗ 删除（P0.5） | — |
| **DataNode RDMA listener** | ✗ 删除（P0.5） | — |
| **客户端 SDK（ObjectNode / cfs-client / cfs-sync 标准 SDK）** | ✗ 删除（P0） | — |
| **Phase A 一侧 read** | ✗ 删除（P0） | — |
| **客户端 scratch / ConnLRU** | ✗ 删除（P0） | — |
| **Native RDMA SDK（P1，cfs-sync 专用）** | ○ 规划 | `import sdk/data/rdma-direct`，调用方主动启用 |
| **元数据 RDMA / GPU-direct** | ○ P2+，未启动 | — |

**为什么 P0.5 也下了 DataNode 间 RDMA**：

在 P0 删除客户端 RDMA 后，DataNode 间复制路径在负载下持续出现 `repl follower rdma: follower ResultCode=244`（OpArgMismatchErr）错误。根因不是单点 bug，而是三层都被 RDMA 的 slot + 并行 dispatch 模型与 CubeFS extent append-offset 严格串行的语义不匹配触发：

1. **Partial-block ArgUnmatch**：SDK 重试时 partial-block CRC 不命中存储 CRC——用 on-disk verify 修了
2. **Follower worker 打散**：`slotIdx % numWorkers` 把同 extent 写散到多 worker 并行 apply——按 `(PartitionID, ExtentID)` hash 改了
3. **Slot 回绕导致 pollLoop 扫描顺序 ≠ 发送顺序**：leader 端 nextRR 回绕后给同 extent 新写分配低位 slot；follower pollLoop 按 slot index 升序扫，先扫到的反而是后发的——还没修

每修一档浮出下一档，说明这是架构层面的不匹配而非个别 bug。要真正根治需要在 leader 上加 per-(P, E) 单 in-flight gate（≈ 80 行 sync 逻辑），或者在 follower 加 reorder buffer。

**评估收益**：CubeFS 典型负载（对象存储、AI training data）是大块顺序写，replication 的物理延迟被 follower 端 disk write（50–500 µs）淹没。RDMA 相对 TCP 节省的 RTT（≈ 100–200 µs）在典型场景边际收益约 10–20%，不足以抵消正确性维护成本。

**结论**：DataNode 间 RDMA 复制全部删除，回归纯 TCP。RDMA 物理优势的真正杠杆点已经规划在 P1 native SDK（cfs-sync 走完全独立路径，绕过 Streamer/ExtentReader 抽象，显式零拷贝、显式 buffer 池）——保留 `util/rdma` 库就够了。

---

## 2. P0.5 删除清单

### 删除的代码

| 文件 | 内容 |
|---|---|
| `datanode/rdma_server.go` | RDMA listener + handleSlot 派发 |
| `datanode/rdma_server_stub.go` | non-rdma build stub |
| `datanode/rdma_server_test.go` | 关联测试 |
| `datanode/repl/rdma_follower.go` | leader → follower RDMA send pipeline |
| `datanode/repl/rdma_follower_stub.go` | stub |
| `datanode/repl/replicate_helper.go` | `PrepareRDMAReplicate` / `WaitForRDMAReplicate` |
| `datanode/dedup_cache.go` | RDMA→TCP fallback 重放专用去重，路径下了无意义 |

### 删除的字段 / 常量

| 位置 | 内容 |
|---|---|
| `datanode/server.go` | 6 个 `ConfigKeyRDMA*` 常量、`enableRDMA` / `rdmaPort` / `rdmaNumSlots` / `rdmaSlotSize` / `rdmaMaxConns` / `rdmaPollCfg` / `rdmaMinPayloadBytes` / `rdmaCtx` / `writeDedup` 字段，对应 parse + listener startup block |
| `datanode/repl/repl_protocol.go` | `followerRDMASend` / `followerRDMACanCarry` 两个 var + `sendRequestToAllFollowers` 内的 RDMA 分支 |
| `datanode/wrap_operator.go` | `dedupEligible` + `writeDedup.Has` 短路 |
| `datanode/wrap_post.go` | `writeDedup.Remember` 调用 |
| `datanode/metric.go` | `MetricPhaseAActiveMR` gauge + `setPhaseAActiveMRMetrics`，`util/rdma` import 移除 |

### 保留

- `util/rdma/*` 整套库代码——P1 native SDK 用
- `datanode/storage/extent.go` `isExactIdempotentReplay`——通用 SDK 重试防御（partial-block CRC verify），不是 RDMA 专用
- `util/rdma/conn.go` 中"调用方需自行序列化 WritePacket"的注释（之前点名 `followerRDMASend`，现在改成通用措辞）

---

## 3. P1 Native RDMA SDK（计划）

### 3.1 目标

为 cfs-sync（未来扩展到 GPU dataloader / 跨集群复制）提供独立的高性能 RDMA 读路径，**绕过标准 SDK 的多层抽象**，发挥 RDMA 的物理优势：

- 单一应用控制 buffer 池 → **真正零拷贝**（pre-registered MR + RDMA Read 直入用户 buffer）
- 显式 extent-level 并发 → 不走 Streamer / ExtentReader 抽象层
- kernel bypass → CPU 解放给业务（GPU 训练、压缩 / 加密等）

### 3.2 与 DataNode 的关系

**不依赖 DataNode RDMA listener**。P0.5 把 DataNode 端 RDMA listener 也删了，所以 P1 SDK 启动时必须**自己拉起 RDMA listener 或者改用 RDMA Read 直读已注册 MR 的模型**。

两种可行模型：

| 模型 | 描述 | 选择 |
|---|---|---|
| **A. SDK 主动连 DataNode RDMA port** | DataNode 重新启动 RDMA listener（仅响应 OpExtentMRLookup + 单边 RDMA Read），不参与复制 | 推荐——服务端职责清晰 |
| **B. 通过 TCP 控制 + GPUDirect/RDMA 外带** | DataNode 用 TCP 返回 rkey + addr，SDK 用 raw RDMA Read | 复杂，依赖 NIC 双方握手 |

P1 实施前先做 PoC-A 决定模型。

### 3.3 API 表面

```go
package rdmadirect

type Client struct { ... }
func NewClient(cfg Config) (*Client, error)
func (c *Client) Close() error

// 文件级读取
func (c *Client) Open(volName string, inode uint64) (*FileReader, error)
func (c *Client) CapableForFile(volName string, inode uint64, size int64) bool

type FileReader struct { ... }
func (r *FileReader) Read(p []byte) (int, error)        // io.Reader 兼容路径（一次 memcpy）
func (r *FileReader) ReadInto() (*MRBuffer, error)      // 零拷贝路径
func (r *FileReader) Close() error

type MRBuffer struct { ... }
func (b *MRBuffer) Data() []byte
func (b *MRBuffer) Release()
```

### 3.4 架构关键决定

| 项 | 决定 | 理由 |
|---|---|---|
| **scope** | P1 仅 read，写留 P1.5 | 写涉及 extent 分配 + 复制 + commit，复杂度 5× |
| **PD scoping** | per-(client process, datanode)，每个 PD 注册同一段物理内存 | 安全（避免 mlx5 LOC_PROT_ERR），256 MB 物理 × N datanode 视图可接受 |
| **buffer pool** | 启动一次 mmap+mlock+reg_mr，运行时 Acquire/Release | 真零拷贝 + 无 GC churn |
| **并发** | 单文件 N 个 extent worker + 单 ordered output channel | 顺序流给 caller，底层 N 路并发拉 |
| **lookup 协议** | 重新设计 `OpExtentMRLookup`（DataNode RDMA listener 配合）| 不复用已删除的复制路径 |
| **fallback** | SDK 内部不 fallback；返回 `ErrNotCapable` 让 caller 决定 | 简化职责，cfs-sync 走标准 SDK 重试 |
| **配置** | Config struct 启动时定（pool size、conns、超时） | 不引入运行时配置 |

### 3.5 SLO

| 指标 | SLO |
|---|---|
| 单 64 GB 文件读吞吐 | ≥ 2.5 GB/s |
| 聚合吞吐（32 并发 × 1 GB 文件） | ≥ NIC 单向 70% |
| `read_seconds p99`（单 4 MiB chunk） | ≤ 5 ms |
| `read_seconds p99.9` | ≤ 50 ms |
| CPU 效率 | ≤ 0.5 核 / (GB/s) |
| 进程内存（pool + conn state） | ≤ 1 GB |
| `fallback rate` 到标准 SDK | < 1% files |
| Server-side `active_mr` 增量 | ≤ 200 / 单 sync 会话 |
| NIC bond RX 利用率（sync 期间） | ≥ 60% 单向 |

### 3.6 Metric Contract

```promql
# 数据量
cubefs_rdma_direct_bytes_total{
    role="client",
    op="read",
    datanode,
    obj_size_bucket="xs"|"s"|"m"|"l"|"xl"
}

# 延迟（histogram）
cubefs_rdma_direct_request_seconds{
    role="client",
    op="read"|"lookup",
    datanode,
    obj_size_bucket,
    le="0.001"|"0.005"|"0.01"|"0.05"|"0.1"|"0.5"|"1"|"5"
}

# Fallback 计数
cubefs_rdma_direct_fallback_total{
    from="rdma_direct",
    to="standard_sdk",
    reason="not_capable"|"pool_exhausted"|"lookup_timeout"|"read_timeout"|"extent_missing"|"datanode_unreachable",
    datanode
}

# Pool 状态
cubefs_rdma_direct_pool_buffers{
    state="free"|"in_use",
    datanode
}

# 在飞 WR
cubefs_rdma_direct_inflight_wrs{datanode}

# 活跃 lease
cubefs_rdma_direct_active_leases{datanode}
```

**告警**：

| 红线 | 告警 |
|---|---|
| `fallback_rate > 5% files in 5min` | warning |
| `fallback_rate > 30%` | critical |
| `request_seconds p99 > 50ms` | warning |
| `pool_buffers{state=free} == 0 sustained 30s` | warning（pool 太小） |

### 3.7 故障演练清单

| 场景 | 注入 | 预期 | 不可接受 |
|---|---|---|---|
| **datanode 中途挂** | mid-read kill datanode | 标 ErrExtentMissing 返回 caller；cfs-sync 跳过该文件 | 进程 hang / 数据损坏 |
| **lease 过期** | 短 TTL + 长读 | 透明 re-lookup | caller 看到错 |
| **pool 耗尽** | 申请 > pool size 文件 | Acquire 阻塞 timeout 后报错 | OOM / 死锁 |
| **NIC link flap** | down/up | 检测后报错给 caller | 静默坏数据 |
| **fallback 后又失败** | force native fail + 标准 SDK 也 fail | 明确 error 返回，不 silent | silent 部分数据 |

### 3.8 实施分解

| 步骤 | 内容 | 工期 |
|---|---|---|
| 0. PoC：DataNode 端只读 RDMA listener 重建（OpExtentMRLookup + RDMA Read，无复制依赖）| 2 天 |
| 1. MR buffer pool + 单测（mock）| 2 天 |
| 2. 单 conn read（单 datanode、单 extent） | 2 天 |
| 3. 多 conn pool + 多 PD MR 注册 | 1 天 |
| 4. extent fan-out + ordered output channel | 2 天 |
| 5. FileReader API（io.Reader + ReadInto） | 1 天 |
| 6. cfs-sync 集成（storage/cfs_linux.go 判断） | 1 天 |
| 7. benchmark + tune | 2-3 天 |
| 8. 故障演练 + 文档 | 1-2 天 |
| **合计** | **14-16 天** ≈ 3 周 |

---

## 4. 阶段 Gate（按集群规模）

P0.5 之后 DataNode 不再依赖 RDMA，按集群规模推进的工作只剩 P1 SDK 和后续元数据/multi-rail 探索。

| 规模 | 必做 | 当前状态 |
|---|---|---|
| **10 节点** | • P0 客户端 RDMA 删除<br>• P0.5 DataNode RDMA 删除<br>• 纯 TCP 复制基线验证 | ✅ P0 done<br>✅ P0.5 done<br>⬜ baseline benchmark |
| **50 节点** | • P1 Native RDMA SDK MVP 跑通<br>• DataNode RDMA listener（只读专用）PoC<br>• cfs-sync 大文件基准跑完 | ⬜ |
| **100 节点** | • P1 SDK 生产化（cfs-sync 默认启用 native 路径）<br>• 全部 P1 SLO 有 baseline 数 | ⬜ |
| **300 节点** | • Multi-rail RDMA PoC（NIC bond × 多 PD） | ⬜ |
| **1000 节点** | • DC QP 评估 / 元数据 RDMA 评估 | ⬜ |

### 4.1 P0.5 后纯 TCP 复制基线（10 节点 gate 前必做）

为了在未来评估 P1 RDMA 收益时有可比 baseline，必须先跑：

| 工作负载 | 指标 | 期望基线 |
|---|---|---|
| fio 4 KB iodepth=1 sync write | replication P99 | ≤ 1 ms |
| fio 128 KB iodepth=32 seq write | 单 DP 吞吐 | ≥ 250 MB/s |
| s3bench 64 MB GET 32 并发 | 聚合吞吐 | ≥ 3.5 GB/s（已知）|
| s3bench 4 KB GET 128 并发 | QPS / P99 | 待测 |

**输出**：`docs/perf/tcp-baseline-2026q2.md`，作为 P1 RDMA 上线时的对照。

### 4.2 小对象基准矩阵（50 节点 gate 前必做）

| 对象大小 | 并发度 | 测什么 |
|---|---|---|
| 128 KiB | 1, 8, 32, 128 | 每秒 GET 数、P99、lookup 占比 |
| 256 KiB | 1, 8, 32, 128 | 同上 |
| 512 KiB | 1, 8, 32, 128 | 同上 |
| 1 MiB | 1, 8, 32, 128 | 同上 |

**对比维度**：P1 native RDMA SDK vs 标准 SDK（TCP）。

**决策准则**：

| 现象 | 决策 |
|---|---|
| 任何 cell 里 lookup > 30% read time | 实施 batch lookup |
| 128 KiB 并发 1 RDMA 不能 ≥ TCP × 1.2 | 加 size threshold，小对象走 TCP |
| 32 并发 RDMA / TCP < 1.5 | RDMA 价值不足，触发架构 review |

**输出**：`docs/perf/small-object-baseline-2026q2.md`。

---

## 5. 修订记录

| 日期 | 修订 |
|---|---|
| 2026-05-12 | 初稿（`rdma-architecture-review.md`），分析全栈双向 RDMA 架构 |
| 2026-05-13 | 加 SLO + Metric Contract + 阶段 Gate（基于全栈架构）|
| 2026-05-13 | **P0 落地后**，旧文档主体内容（客户端 RDMA、Phase A、ConnLRU）失效。删除旧文档，本文档作为 post-P0 现状 + P1 计划的统一来源 |
| 2026-05-13 | **P0.5：DataNode 间 RDMA 复制全部删除**。三档乱序 bug 连续浮现（partial-block / worker 打散 / slot 回绕）证实 RDMA 的 slot 并行模型与 extent append-offset 严格串行语义本质不匹配，维护成本超过 ≈ 10–20% 的 P99 边际收益。util/rdma 库保留供 P1 native SDK 使用。本文档同步更新 |
