# Phase A Connection LRU 设计文档

> **状态**：设计阶段，**未实施**。
> **目的**：把 Phase A 从"每个 datanode 一条 conn"演进到"全局 LRU 管理 active conn"，
> 使其在 1000+ 节点 cluster 下不 OOM 且不抖动。
> **范围**：仅 SDK 客户端 Phase A read 路径。两边路径、写路径不受影响。

---

## 1. 背景与动机

### 1.1 Phase A 当前架构

```
SDK 进程
  ├── rdmaConnPool       (两边路径)
  └── rdmaPhaseAConnPool (Phase A)
        └── 每个 datanode 一个 sub-pool
              └── MaxConns × RDMAConn 实例
                    └── lazy-allocated read scratch
                          = ReadSlotCount × ReadSlotSize
```

Phase A scratch 总占用公式：

```
ScratchTotal = ReadSlotCount × ReadSlotSize × MaxConns × NumDataNodes
```

### 1.2 大集群的 OOM 问题

默认配置 `(ReadSlotCount=64, ReadSlotSize=4 MiB, MaxConns=4)`：

| Cluster 规模 | Scratch 总占用 | 可接受性 |
|---|---|---|
| 3 节点（当前）| ~3 GiB | ✓ |
| 100 节点 | 102 GiB | ✗ |
| 1000 节点 | **1 TiB** | ✗✗ |

**Phase A 设计本身在大 cluster 下不可线性扩展**。Lazy allocation 救不了——
ObjectNode 服务 bucket 流量长跑下来会触及几乎所有 datanode。

### 1.3 业界参照

公开论文里 1000+ 节点 production RDMA 极少，以下几个有具体可借鉴的工程实践：

- **FaRM**（NSDI'14 / SOSP'15, Microsoft Research）：connection pool 限制
  RC QP 数量，shared scratch buffer。90 台 server cluster production。
- **PolarFS**（VLDB'18, 阿里）：RDMA over RoCE，storage / compute pool 之间
  point-to-point，不做 N×N mesh。
- **Pangu**（FAST'24, 阿里云）：大规模 cluster 下用户态网络栈 + connection
  pooling，描述了 scratch lifecycle 管理。
- **NVMe-oF Transport spec**：标准化 per-controller QP 数量上限（16~64）。

这些系统的共同模式：**不做"每 peer 一条 conn"的 N×M mesh**，而是限定
active conn 数 + LRU evict。

---

## 2. 目标与非目标

### 2.1 目标

1. **支持 1000+ 节点 cluster Phase A 不 OOM**：通过总内存预算 + active peer
   数双上限控制总 scratch 占用。
2. **Hot peer 性能不退化**：高频访问的 datanode 维持 active conn，Phase A
   命中率接近现状。
3. **低侵入改造**：复用现有 `RDMAConnPool` 不动其结构；ConnLRU 是一层
   wrapper。
4. **运维可观测**：暴露 metrics（active peers、scratch 占用、eviction 次数、
   thrashing 指标），运维能据此调参。

### 2.2 非目标

1. **不解决 RDMA 协议层问题**（PD scoping、ODP 行为、RC QP 故障模型等）。
2. **不替换底层 `RDMAConnPool`**：保留 hash routing 等机制。
3. **不引入 DC QP**：DC QP 是 1000+ 节点的"终极方案"但工程量极大，本设计
   是中间阶段方案（100-1000 节点 sweet spot）。
4. **不改两边路径**：两边路径每个 packet 只占一个 128 KiB slot，scratch
   开销低，不需要 LRU。
5. **不主动迁移 lease**：被 evict 的 datanode lease cache 一起 invalidate，
   下次访问重新 lookup。不试图把 lease "迁移"到新 conn（PD 不同，物理上不
   可能）。

---

## 3. 现状分析

### 3.1 关键代码结构

```
util/rdma/slot_pool.go
  RDMAConnPool
    pools  map[addr]*singleSlotPool
    singleSlotPool
      conns  []*connSlots          // maxConns 长度
      anyAliveConn() / aliveConnAtIndex(idx)

sdk/data/stream/rdma_client.go
  rdmaPhaseAConnPool *rdma.RDMAConnPool  // Phase A 专用 pool

sdk/data/stream/extent_reader_rdma_read.go
  tryReadViaRDMARead()
    pool.ConnIfReadyForKey(addr, "pid-extId")  // hash routing
    conn.PostRDMAReadAndWait(...)

util/rdma/read_waiter.go
  initReadScratch()  // sync.Once，conn 第一次 read 时 lazy 分配 scratch
```

### 3.2 关键约束

1. **lookup + read 必须同 conn**：rkey 是 PD-scoped，跨 conn 不通用。
   现有 hash routing 保证 `AcquireSlotForKey` 和 `ConnIfReadyForKey` 用同
   一个 key 落到同一 conn。**LRU evict 必须以 conn 为单位，不能 evict
   单个 lease**。
2. **Scratch 是 lazy 分配的**：conn 创建后不立即分配 scratch，第一次
   `PostRDMAReadAndWait` 才触发。LRU 可以提前 evict 还没分配 scratch 的
   conn（开销低）。
3. **WR 完成是异步的**：`PostRDMAReadAndWait` post 后 select 等 CQE，期间
   user 的 dst buffer 不能被搬动。如果在等 CQE 时 conn 被 close，CQE 落到
   死 channel——这是 use-after-free 的核心场景。

---

## 4. 设计方案

### 4.1 整体架构

```
SDK Phase A read 入口
  ↓
ConnLRU.Get(addr, key) → (conn, ok)
  ├─ Hit: atomic update lastUsed, MoveToFront, inFlight++
  └─ Miss:
       ├─ 未达上限: 调底层 pool → 新 conn → 加入 LRU
       └─ 达上限:
            ├─ 找 LRU tail
            ├─ 等其 inFlight=0
            ├─ Close conn + 释放 scratch
            └─ Invalidate lease cache for that addr
                ↓ then dial new
  ↓
PostRDMAReadAndWait(conn, ...)
  ↓
ConnLRU.Release(addr) → inFlight--
```

ConnLRU **不替换** `RDMAConnPool`，而是包一层准入控制 + 生命周期管理。

### 4.2 数据结构

```go
type ConnLRU struct {
    mu   sync.Mutex
    pool *RDMAConnPool

    // 硬上限（任一触发同步 evict）
    maxPeers        int    // 最多多少 datanode 维持 active conn
    maxScratchBytes int64  // 总 scratch 字节预算
    
    // 软上限（后台 sweep）
    idleEvictAfter  time.Duration  // 超过这个时间 idle 被后台 evict
    redialCooldown  time.Duration  // evict 后多久不让重新 dial 同 peer
    
    // LRU 状态
    active     map[string]*lruEntry  // addr → entry
    lruList    *list.List            // front=最近用，back=最久未用
    evictedAt  map[string]time.Time  // 用于 cooldown
    totalScratchBytes int64
    
    stopCh chan struct{}
}

type lruEntry struct {
    addr          string
    conn          *RDMAConn
    listElem      *list.Element

    lastUsedNanos int64   // atomic
    inFlight      int32   // atomic：在用此 conn 的 read 数
    scratchBytes  int64   // 实际分配的 scratch 大小（未分配=0）
    evicting      atomic.Bool  // CAS 防 double-evict
}
```

### 4.3 关键 API

```go
// Get 返回 addr 对应的 conn。
// 命中：increment inFlight, update LRU position, 返回 conn。
// 未命中：根据上限决定是 dial 新的还是 fallback。
// 调用方必须配对 Release(addr)。
func (p *ConnLRU) Get(addr string, key string) (*RDMAConn, bool)

// Release 必须在每次 Get 成功后调用（typical: defer）。
// 减少 inFlight 计数；如果 entry 正在 evicting 且 inFlight=0，
// 真正完成 evict（关闭 conn + 释放 scratch）。
func (p *ConnLRU) Release(addr string)

// Stats 返回快照供 metrics 使用。
func (p *ConnLRU) Stats() ConnLRUStats

// Close 停止后台 sweeper，关闭所有 active conn。
func (p *ConnLRU) Close()
```

### 4.4 Eviction Safety 协议

**核心问题**：Phase A 是异步的。`PostRDMAReadAndWait` 中间会等 CQE，
期间不能 Close conn，否则 user 的 dst buffer 收不到数据。

**解决**：引用计数 + 两阶段 evict：

```
阶段 1 (mark evicting):
  CAS entry.evicting false→true
  - LRU 操作直接看 evicting=true 跳过这个 entry
  - 新的 Get 走 fallback，不再 increment inFlight
  - 已经 inFlight 的 read 继续执行

阶段 2 (drain):
  poll inFlight == 0
  - 已存在 read 完成（Release 调用减 1）
  - 注意：超时也算完成（PostRDMAReadAndWait 会返回）

阶段 3 (commit):
  Close conn (异步，避免阻塞 LRU)
  释放 scratch (atomic.Add totalScratchBytes -= entry.scratchBytes)
  invalidate lease cache for addr
  从 lruList / active map 移除
  记录 evictedAt[addr] = now (启动 cooldown)
```

**风险**：如果某个 read 卡死（WR 永不完成）会无限阻塞 evict。
**缓解**：drain 加最大等待时间（比如 30 秒），超时则强 close。
被强 close 的 read 自然在 timeout 时返回错误，无大碍。

### 4.5 LRU Thrashing 抑制

访问模式接近 evict 边界时，peer 来回被 evict + redial 抖动。
**Cooldown 机制**：

```go
// Get 路径
if t, ok := p.evictedAt[addr]; ok {
    if time.Since(t) < p.redialCooldown {
        return nil, false  // fallback two-sided，不重新 dial
    }
}
```

5 秒 cooldown 让访问模式自然衰减。如果某 peer 真的是 hot，5 秒后还会
被访问 → 重新 dial，但此时其他 peer 自然不再访问这个频段，LRU 平衡到
新稳态。

### 4.6 Lease Cache 同步

被 evict 的 conn 的 lease（rkey/VA）是 PD-scoped，conn 死了 lease 立刻
失效。如果 evict 时不 invalidate lease cache：

```
Time T:   evict conn for "dn1:17350" → close (PD destroyed)
Time T+1: 同进程其他 goroutine 调 cache.Get(dn1, pid, ext) → cache hit
          → 拿到旧 lease (rkey 在死 PD 下)
          → ConnIfReadyForKey 重新 dial 拿到新 conn (新 PD)
          → PostRDMAReadAndWait 用旧 rkey 在新 PD QP 上 post
          → server NIC 在新 PD 找不到 rkey → 5s timeout
```

**对策**：evict 阶段 3 调 `extentMRCache.InvalidateByAddr(addr)` —— 删
该 datanode 所有 lease 条目。下次访问 cache miss → 重新 lookup → 拿到
新 PD 的 rkey。

需要在 `sdk/data/stream/extent_mr_cache.go` 加 `InvalidateByAddr(addr)`
方法（按 addr 前缀清空 cache）。

### 4.7 配置项

```
rdmaPhaseAMaxActivePeers      默认 64    最多多少 datanode 维持 active conn
rdmaPhaseAMaxScratchMiB       默认 4096  scratch 总预算 (MiB)
rdmaPhaseAIdleEvictSeconds    默认 120   超过这个 idle 时间后台 evict
rdmaPhaseARedialCooldownSec   默认 5     evict 后多久内拒绝重新 dial 同 peer
rdmaPhaseADrainTimeoutSec     默认 30    drain inFlight 的最大等待
```

任一为 0 时使用默认。`rdmaPhaseAMaxActivePeers=0` 还有第二个含义：
**禁用 LRU**，回退到当前行为（每 datanode 一条 conn）。这样 < 100 节点
cluster 默认不引入 LRU 复杂度。

### 4.8 Metrics

```
cubefs_phasea_lru_active_peers       gauge     当前活跃 peer 数
cubefs_phasea_lru_max_peers          gauge     配置上限
cubefs_phasea_lru_scratch_bytes      gauge     当前 scratch 总占用
cubefs_phasea_lru_evictions_total    counter   by reason {max_peers, max_scratch, idle, shutdown}
cubefs_phasea_lru_dial_after_evict   counter   同 peer 重新 dial 次数 (thrashing 指标)
cubefs_phasea_lru_inflight_total     gauge     在用 conn 的 read 总数
cubefs_phasea_lru_drain_timeouts     counter   evict 时 drain 超时次数
```

**关键告警**：`dial_after_evict / evictions_total > 0.3` 持续 5 分钟 →
LRU 在 thrash，需要调大上限。

---

## 5. 风险点

### 5.1 Use-after-free（最严重）

**场景**：Phase A read 期间 conn 被 evict 后 close，CQE 落到死 channel
或者 dst buffer 被 NIC 写到回收后的内存。

**Mitigation**：
- `inFlight` 引用计数 + 两阶段 evict（4.4 节）。
- evict 阶段 2 drain 必须等 `inFlight == 0`。
- 最坏情况：read 卡死 → drain timeout → 强 close。强 close 时 user dst
  buffer 不会被新写（NIC QP 已 destroy），buffer 内容是 partial 但 user
  side 会收到 timeout 错误，不会用 partial 数据。

**残余风险**：
- drainer goroutine 在 conn close 后再 deliver CQE：当前代码 `completeRDMARead`
  对已关闭 channel 用 non-blocking send，**已经处理**。✓
- 但是 cgo 层 `ibv_destroy_qp` 跟 user goroutine 读 dst buffer 之间没有
  显式同步。Linux mlx5 driver 保证 destroy_qp 等所有 WR 完成才返回，但
  这是 driver-specific，**需要验证**。

### 5.2 Lease cache 不一致

详见 4.6。**Mitigation**：evict 时同步 `InvalidateByAddr(addr)`。

**残余风险**：cache invalidation 跟 evict 不是原子的（两个锁），可能短
窗口里有 goroutine 拿到旧 lease。**结果**：下次 read 走 fallback
two-sided（最坏情况），不是数据错误。可接受。

### 5.3 LRU Thrashing

详见 4.5。Cooldown 是抑制不是消除。如果应用真的均匀访问 1000+ peer，
任何固定上限的 LRU 都会 thrash。

**Mitigation**：
- Cooldown 5 秒。
- Metrics 报警 thrashing 比例。
- 运维收到报警后**调大上限**（增加内存预算 + max peers）或**关闭 Phase A**
  （`rdmaOneSidedReadDisabled=true`）。

**残余风险**：在均匀访问模式下 LRU 不是好选择。但实际 ObjectNode 工作
负载典型有访问局部性（80/20）—— LRU work 得很好。

### 5.4 Drain timeout 选择

drain 等 30 秒是个魔术数字。**太短**导致正常 read 被强 close；**太长**
导致 evict 被卡住，新 peer 无法接入。

**Mitigation**：
- 30 秒 = read timeout (1 秒) × 30，给最坏情况 retry 缓冲。
- Metric `drain_timeouts` 暴露，运维观察实际值调整。

### 5.5 Cooldown 期间访问该 peer 完全没 RDMA

被 evict 后 5 秒内访问该 peer 走 two-sided（lookup-and-read 都是）。
**对吞吐有影响**，但只发生在 evict 的临界点。

**Mitigation**：cooldown 不是越长越好。5 秒经验值，可调。

### 5.6 ObjectNode 启动冷启动

进程启动后 LRU 是空的，前 N 个请求会触发 dial。每个 dial 是 RDMA cm
握手，几 ms。冷启动期吞吐低。

**Mitigation**：
- 可选：启动时预 dial 第一批 datanode（从 master view 拿到的 hot 列表）。
- 暂时不做：冷启动时间是秒级，对 long-running ObjectNode 进程可忽略。

### 5.7 Metrics 黑洞

如果 ConnLRU 自己出 bug 不报错，运维只看到吞吐下降但不知道原因。

**Mitigation**：
- 所有路径都有 metrics（4.8 节）。
- evict / dial / drain 路径都有 LogWarnf（sampled）。

---

## 6. 改造范围

### 6.1 新增文件

```
util/rdma/conn_lru.go           ConnLRU 核心实现（build-tag-free，可单测）
util/rdma/conn_lru_test.go      LRU 行为单测（mock dial）
docs/plan/rdma-phasea-conn-lru.md  本文档
```

### 6.2 修改现有文件

```
util/rdma/config.go
  RDMAPoolConfig 加 PhaseAMaxPeers / PhaseAMaxScratchMiB /
                    PhaseAIdleEvictSeconds / PhaseARedialCooldownSec
                    /PhaseADrainTimeoutSec 字段

sdk/data/stream/rdma_client.go
  InitRDMAConnPool 检查 cfg.PhaseAMaxPeers
    > 0: 创建 rdmaPhaseAConnLRU 包装 rdmaPhaseAConnPool
    == 0: 保持现状（直接用 pool，无 LRU）

sdk/data/stream/extent_reader_rdma_read.go
  tryReadViaRDMARead:
    if rdmaPhaseAConnLRU != nil:
      conn, ok := rdmaPhaseAConnLRU.Get(poolAddr, poolKey)
      defer rdmaPhaseAConnLRU.Release(poolAddr)
    else:
      conn, ok := rdmaPhaseAConnPool.ConnIfReadyForKey(...)  // 现状

sdk/data/stream/extent_mr_cache.go
  加 InvalidateByAddr(addr) 方法

sdk/data/stream/phase_a_stats.go
  60s stats 行追加 LRU 状态：active / max / scratch / evictions

proto/mount_options.go
client/fuse.go, client/fs/super.go
objectnode/server.go, objectnode/rdma_init.go
tool/cfs-sync/config.go, tool/cfs-sync/rdma_init.go
  透传 5 个新 mount option，同前面 rdmaPhaseA* 系列的 pattern
```

### 6.3 测试改动

```
util/rdma/conn_lru_test.go (新)
  - LRU 顺序：Get 移到 front
  - 容量上限：达上限 evict tail
  - inFlight 引用计数：drain 阻塞直到 Release
  - Cooldown：evict 后 5s 内拒绝 redial
  - 并发：N 个 goroutine 并发 Get/Release，无 race
  - Close idempotent

util/rdma/conn_lru_concurrency_test.go (新，可选)
  - 高并发 stress test，配 -race 跑

sdk/data/stream 集成测试（如果有）
  - mock pool，测 SDK 入口在 LRU 启用/禁用下的行为
```

### 6.4 工作量分解

| 步骤 | 工作量 | 验证方式 |
|---|---|---|
| 1. ConnLRU 核心数据结构 + Get/Release | 1 天 | 单测 |
| 2. inFlight + 两阶段 evict | 0.5 天 | -race 测试 |
| 3. Cooldown / thrashing 抑制 | 0.5 天 | 单测 |
| 4. Lease cache InvalidateByAddr | 1 天 | 单测 + 集成 |
| 5. Metrics + 60s stats 接入 | 0.5 天 | promtool 验证 |
| 6. SDK / mount option 透传 | 1 天 | linux 集成 build |
| 7. 灰度测试 + 调参 | 1-2 天 | 你环境 200/500/1000 节点压测 |
| **合计** | **5-7 天** | |

---

## 7. 对现在系统的影响

### 7.1 默认行为不变

`PhaseAMaxPeers=0` （默认）= LRU 禁用 = 现有行为。**不部署新配置，
完全没有任何变化**。

### 7.2 小集群（< 100 节点）

启用 LRU 也几乎无影响：max=64 在小集群下根本不会触发 evict，LRU
退化成普通 pool wrapper，多了一次 map 查找的开销（纳秒级）。

可选择不启用，保持当前代码路径。

### 7.3 性能影响

**Hot peer 性能**：跟当前相同（命中 LRU map 一次，开销可忽略）。

**Cold peer 性能**：
- 首次访问：dial RDMA conn + lookup MR + 第一次 read。比之前多了一次
  LRU 准入检查（μs 级），其他不变。
- 重 dial：被 evict 后再访问会重新 dial，dial 耗时 1-10 ms。

**Thrashing 状态**：如果配置不当导致 thrash，性能可能**比无 LRU 还差**
（来回 dial + close + cache invalidate）。Metrics 暴露 thrashing 比例
让运维及时发现。

### 7.4 内存影响

ConnLRU 自身数据结构占用：每 entry ~200 字节 × 1000 peer = 200 KB。
跟 scratch 几 GiB 比可忽略。

**关键节省**：scratch 从"N × per-conn"变成"max_active × per-conn"，
1000 节点下从 1 TiB 降到 max=64 × 256 MiB = 16 GiB。**节省 ~62x**。

### 7.5 运维影响

新增 5 个 mount option，运维需要了解：
- 各项默认值的物理意义
- thrashing 报警怎么响应（先调大上限，不行就关 Phase A）
- 内存预算怎么算

**配套需要的文档**：
- 运维手册章节：Phase A LRU 调参指南
- Grafana dashboard 模板：Phase A LRU 健康度看板

### 7.6 向后兼容

完全向后兼容。老配置（不传 PhaseA* mount option）跑新二进制 = 老行为。

### 7.7 测试覆盖率

新代码需要单测覆盖到：
- LRU 行为
- inFlight drain
- Eviction race
- Thrashing cooldown

预期 ~80% 覆盖率。集成测试在用户环境（200-1000 节点）灰度。

---

## 8. 待定决策

实施前需要明确的：

### 8.1 默认值校准

5 个配置项的默认值都是经验估计，需要在用户实际 cluster 上调优：

```
rdmaPhaseAMaxActivePeers      64    ← 经验，看实际 cluster 访问局部性
rdmaPhaseAMaxScratchMiB       4096  ← 经验，看 SDK 进程内存预算
rdmaPhaseAIdleEvictSeconds    120   ← 经验，看访问 burst 模式
rdmaPhaseARedialCooldownSec   5     ← 经验，看 thrashing 频率
rdmaPhaseADrainTimeoutSec     30    ← 经验，看 read timeout 配置
```

**建议**：先按经验值上线，跑 1 周观察 metrics，根据 thrashing 比例和
内存占用调整默认。

### 8.2 LRU vs LFU vs 加权

LRU 是最简单的策略。如果发现访问模式不适合 LRU（比如"少数 peer 几次
访问 + 大量 peer 一次访问"），需要换 LFU 或加权策略（recency × frequency）。

**建议**：先用 LRU，metrics 上看 `dial_after_evict / evictions` 比率，
> 0.3 持续就考虑换策略。

### 8.3 是否同步 invalidate lease cache？

evict 时同步调 `InvalidateByAddr` 跟 evict 不是同一把锁，存在短窗口。
要不要加全局锁让这两步原子？

**建议**：不加。窗口期最坏 fallback two-sided，不是数据错误。加锁会降
LRU 吞吐。

### 8.4 跟 stub 兼容性

`util/rdma/stub.go` 提供非 RDMA build 的 stub。ConnLRU 是 RDMAConn 的
封装，stub 模式下 `*RDMAConn` 是空壳。

**建议**：ConnLRU 跟 RDMAConnPool 一样 tag-free（map + list 操作不需要
cgo）。dial 注入式让单测能跑。

### 8.5 是否 retroactively 给 readonly_pool 一样的处理？

之前删过 ReadOnlyConnPool，理论上 Phase A 是 readonly。但 ConnLRU 用的
是 RDMAConnPool（统一），不再分两类。

**建议**：保持现状，不再引入新的 pool 类型。

---

## 9. 业界参照（公开文献）

| 系统 | 借鉴点 | 出处 |
|---|---|---|
| FaRM | RC QP conn pool 限制 + scratch sharing | NSDI'14 paper |
| FaRM v2 | 90 server cluster production conn 管理 | SOSP'15 paper |
| PolarFS | RDMA over RoCE, point-to-point conn | VLDB'18 paper |
| Pangu | 大规模 cluster 用户态网络栈 + pool | FAST'24 paper |
| NVMe-oF Transport | per-controller QP 上限 (16-64) | nvmexpress.org spec 1.4+ |
| Mellanox DC QP | 1000+ 节点终极方案（本设计不采用） | docs.nvidia.com/networking |

**本设计的定位**：FaRM 思路的 cubefs 实现，扛 100-1000 节点。1000+ 真正
扩展需要 DC QP 重构（更大工程，单独 spike）。

---

## 10. 不在本设计范围内的事

明确**不做**的：

1. **DC QP 改造**：本设计是 100-1000 节点中间方案。DC QP 是 1000+ 节点的
   终极方案，独立的大工程。
2. **Proxy 聚合层**：改 cluster 拓扑，工程量极大。
3. **Server 端 MR 池化**：本设计只管 client 端 scratch；server 端 extent
   MR pool 是另一个独立问题。
4. **Lease migration**：被 evict 的 lease 不试图"迁移"到新 conn，PD 不
   同物理上不可能。
5. **两边路径的 LRU**：两边 packet slot 内存开销小（128 KiB），不需要 LRU。
6. **DC QP 兼容性 fallback**：如果未来上 DC QP，conn LRU 跟它怎么交互是
   未来的事，本设计不预留接口。

---

## 11. 决策检查清单（实施前 review 用）

实施这个设计前，需要回答：

- [ ] cluster 真的计划扩到 100+ 节点？ → 是 → 继续；否 → 不做
- [ ] 当前 SDK 进程内存预算（ObjectNode / cfs-client）确定？ → 用来定
      `MaxScratchMiB` 默认
- [ ] 当前 datanode 访问模式有 80/20 局部性？ → 是 → LRU 适用；否 →
      考虑 LFU 或重新评估
- [ ] 运维有 prometheus + grafana 接入？ → 是 → metrics 有用；否 → 先
      上 metrics 基建
- [ ] 是否计划上 DC QP？ → 是 → 本方案可作为短期方案，跟 DC QP 不冲突；
      否 → 本方案是长期方案
- [ ] mlx5 driver 在 destroy_qp 时是否保证 WR 完成？ → 验证 driver
      文档 + 实测
- [ ] cooldown 期间走 two-sided 是否能接受？ → 是 → 按设计走；否 → 需要
      更复杂的"半 evict"机制

---

## 12. 版本与签字

- v0.1 (2026-05-12): 初稿，未实施
- 作者：Claude Opus 4.7（与 fangtaozc 讨论）

实施前需要 review 的人：
- 工程层：熟悉 cubefs SDK 数据路径的工程师
- 性能层：熟悉 RDMA 调优 / mlx5 driver 行为的工程师
- 运维层：负责 ObjectNode / cfs-client 部署的 SRE

实施触发条件：cluster 规模扩到 ≥ 100 节点之前**完成代码 review 和单测**，
扩到 ≥ 50 节点时开始**灰度上线**。
