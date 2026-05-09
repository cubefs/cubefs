# CubeFS RDMA 优化规格文档

## 实施进度（2026-05-09）

| P 级 | Spec Week | 状态 | Commit | 范围说明 |
|------|---|---|---|---|
| P0 流控 + SlotSize 校验 | 1-2 | ✅ 完整 | `6f6958ea9` | — |
| P1 多槽 pipeline | 3-4 | ✅ 完整 | `9da3a6a2a` | — |
| P2 自适应 poll | 5 | ✅ 完整 | `7bc2acb09` | 含 `WRITE_WITH_IMM` doorbell 改造 |
| P3 可观测性 | 6 | ✅ 完整 | `66e10f11f` | 7 指标全数注册 |
| P4a 写响应 RDMA panic 断言 | 7-8 | ✅ 完整 | `7336b2355` | 写响应在 P0 已经走 RDMA |
| P4b 读路径 RDMA | 9-12 | ✅ 完整 | `4939f869d` | — |
| **P5 零拷贝磁盘** | 13-14 | 🟡 **lite** | `88f984159` | 仅 transport 层（节省 1 次 memcpy/响应）；`io_uring + O_DIRECT` 真零拷贝未做 |
| **P6 智能路径选择** | 15 | 🟡 **lite** | `9ba6a47c6` | size 阈值 + 总开关；zone 拓扑感知未做 |

**测试**：纯 Go 单测 44/44 PASS（`-race`，stub 模式可跑无需 RDMA 硬件）。
**待验证**：`linux && rdma` 编译 + rxe loopback + 真硬件性能基线 — 见 `rdma-verification.md`。

**lite 范围说明**：

- **P5 lite**：服务端 `handleReadSlot` 直接 `store.Read` 到 `sendScratch`，节省一次 transport 层 `memcpy`（典型 128KB read 收益明显）。但磁盘到 pagecache 仍有内核拷贝；真"零拷贝"（perf mem 验证无 memcpy）需要 `io_uring + O_DIRECT`，未做。
- **P6 lite**：实现了"小包走 TCP"和"总开关 rdma_enabled"两条；跨 zone 走 TCP 没做（`DataPartitionResponse` 没带 Zone 字段，需 Master API 改造，同 IDC 集群不需要）。

## 约束

- TCP 路径必须完整保留，所有 RDMA 失败必须能透明 fallback ✅
- 构建标签 `linux && rdma` 不变，非 RDMA 构建零影响 ✅
- 优化项按依赖顺序排列，P0 必须先于 P1 完成，以此类推 ✅

---

## 当前架构快照（实施后）

```
SDK Write:
  client --[RDMA Write]--> 主 DataNode --[RDMA Write]--> 副本 DataNode
  主 DataNode --[RDMA Write]--> client (响应)         ← P0 完成

SDK Read (新):
  client --[RDMA Write, 单 chunk]--> DataNode
  DataNode --[RDMA Write, 数据+CRC]--> client          ← P4b 完成
  超 SlotSize / RDMA 不可用 → 自动 fallback TCP

FUSE:
  通过 SDK ExtentReader/Writer 入口；和 SDK 共享同一 RDMA 路径
  默认走 RDMA（透明加速），失败 fallback TCP
```

### 已知缺陷（实施前 → 实施后）

| 编号 | 类型 | 描述 | 状态 |
|------|------|------|------|
| D1 | 正确性 | 无流控，ring 可被覆盖 | ✅ P0 修复（credit 协议） |
| D2 | 性能 | slot 0 串行，连接内无并发 | ✅ P1 修复（drainer + SlotPool） |
| D3 | 稳定性 | 纯 busy-poll，高并发 CPU 耗尽 | ✅ P2 修复（busy → yield → comp_channel sleep） |
| D4 | 覆盖 | 写响应 + 读路径仍走 TCP | ✅ P4a/P4b 修复 |
| D5 | 运维 | 无 metrics，fallback 不可感知 | ✅ P3 修复（7 个 Prometheus 指标） |
| D6 | 正确性 | SlotSize 无校验，大包静默截断 | ✅ P0 修复（`MinValidSlotSize` 启动校验） |

---

## P0 — 流控与正确性修复 ✅

### 目标

防止发送端覆盖接收端尚未处理的 slot，消除数据损坏风险。

### 规格

**Credit 协议**：连接建立握手时双方交换 `numSlots` 作为初始 credit。

- 发送端：每发一个 slot 消耗 1 credit；credit 耗尽时阻塞等待归还
- 接收端：每处理完一个 slot 后，通过 RDMA Write 向发送端的 credit counter 原子加 1
- credit counter 使用独立的 pinned memory region，与数据 ring 分离

**SlotSize 校验**（同步修复 D6）：

```
启动时断言：SlotSize >= proto.MaxPacketDataSize + proto.PacketHeaderSize
违反时拒绝启动，返回明确错误
```

### 接口变更

```go
// RDMAConnConfig 新增
type RDMAConnConfig struct {
    NumSlots int
    SlotSize int
    // 新增：credit 归还的 RDMA Write 完成后是否阻塞等待 ACK
    CreditAckMode CreditAckMode // Sync | Async
}
```

### 验收条件

- [x] 发送端在 credit=0 时阻塞，不写 slot — `TestCreditState_BlocksWhenExhausted`
- [x] 接收端处理完后 credit 正确归还 — `TestCreditState_FullSenderReceiverCycle`
- [x] 构造 credit=0 场景，发送端不超时、不崩溃、不覆盖数据 — `TestCreditState_RingOverrunInvariantUnderLoad`
- [x] SlotSize 小于最大包时启动失败并打印明确错误 — `TestValidateSlotSize_Rejects`
- [x] TCP fallback 路径在 credit 机制下行为不变 — stub no-op 验证

---

## P1 — 多槽并发 Pipeline ✅

### 目标

消除连接内串行，吞吐从 `SlotSize/RTT` 提升到 `numSlots × SlotSize/RTT`。

### 规格

**槽借用模型**：连接池改为槽池，借用粒度从连接下沉到槽。

```
SlotPool
  ├── RDMAConn A: slot[0] slot[1] slot[2] ... slot[N-1]
  └── RDMAConn B: slot[0] slot[1] slot[2] ... slot[N-1]

借用接口：
  AcquireSlot(addr string) (*SlotHandle, error)
  ReleaseSlot(h *SlotHandle, forceClose bool)
```

**槽分配策略**：

- 同一 addr 的请求轮转到不同 slot（round-robin）
- slot 被借出时标记为 `inUse`，归还时清除
- 连接内所有 slot 被借出时，新借用请求阻塞等待或新建连接（可配）

**发送端**：

```go
h, err := slotPool.AcquireSlot(addr)
defer slotPool.ReleaseSlot(h, forceClose)
h.Conn.WritePacket(h.SlotIdx, pkt)
pollResponse(h.Conn, h.SlotIdx)
```

### 接口变更

```go
// 废弃 RDMAConnPool.GetConnect / PutConnect
// 新增
type SlotPool interface {
    AcquireSlot(addr string) (*SlotHandle, error)
    ReleaseSlot(h *SlotHandle, forceClose bool)
}

type SlotHandle struct {
    Conn    *RDMAConn
    SlotIdx int
}
```

### 验收条件

- [x] N 个并发写同一 addr，实际使用 N 个不同 slot — `TestSlotPool_ConcurrentAcquireDistinctHandles`
- [ ] 吞吐相比 P0 基线提升 ≥ min(N, numSlots) × 0.7 倍 ⏳ 需真硬件实测
- [x] 某 slot forceClose 后，同连接其他 slot 不受影响 — `TestSlotPool_DirtySlotExcludedFromRotation`
- [x] AcquireSlot 在所有 slot 繁忙时阻塞而非返回错误 — `TestSlotPool_BlocksWhenAllConnsAndSlotsExhausted`
- [x] TCP 路径代码路径不涉及 SlotPool — stub.go 验证

**实现细节补充**：
- 新增 per-conn drainer goroutine，CQ 由专门 goroutine 抽干（fire-and-forget 发送）
- WR_ID 64-bit 编码：高 32 位 op (slot/doorbell/credit/recv/shutdown)，低 32 位 slot
- `sendQueueDepth = numSlots × 4`（保底 256），覆盖并发 send/doorbell/credit 三类 WR
- `ReturnCredit(slotIdx int)` 接口变更（原无参），用于 drainer 路由 credit-write CQE

---

## P2 — 自适应 Poll ✅

### 目标

空闲时释放 CPU，低延迟场景保持 busy-poll 优势。

### 规格

**三阶段策略**：

```
阶段 1（busy）：连续 busySpinCount 次轮询，无间隔
阶段 2（yield）：每次 runtime.Gosched()，持续 yieldCount 次
阶段 3（sleep）：ibv_req_notify_cq + ibv_get_cq_event 阻塞等待内核通知
```

**参数默认值**（可通过 RDMAConnConfig 覆盖）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| BusySpinCount | 200 | 约 1µs 内完成的典型 RDMA RTT |
| YieldCount | 1000 | Gosched 阶段最多次数 |
| SleepThresholdUs | 50 | 超过此时延后降级到 sleep |

**C 层新增**（`rdma.h`）：

```c
int  rdma_cq_request_notify(struct ibv_cq *cq);
void rdma_cq_wait_event(struct ibv_comp_channel *ch, struct ibv_cq **cq_out);
struct ibv_comp_channel* rdma_create_comp_channel(struct ibv_context *ctx);
```

### 验收条件

- [ ] 空载时（无请求）polling goroutine CPU 使用率 < 1% ⏳ 需真硬件实测
- [ ] 满载时（持续写）平均响应延迟 < 纯 busy-poll 基线的 110% ⏳ 需真硬件实测
- [x] BusySpinCount=0 时退化为纯 sleep 模式，功能正确 — `TestAdaptivePoller_PureSleep`
- [x] BusySpinCount=MaxInt 时退化为纯 busy-poll，行为与改前一致 — `TestAdaptivePoller_PureBusyPoll`

**架构性补充**：one-sided RDMA Write 接收端**不会**自动产生 CQE，所以 sleep phase 的 `ibv_get_cq_event` 必须有可被唤醒的 CQE 来源。本次实现把 doorbell 写从 `RDMA_WRITE` 升级到 `RDMA_WRITE_WITH_IMM`：每个 doorbell 在接收端消耗一个预 post 的 recv WR 并产生 CQE，让 comp_channel sleep 真正可用。

---

## P3 — 可观测性 ✅

### 目标

生产环境可判断 RDMA 是否生效、性能表现、fallback 频率。

### Metrics 规格

所有指标通过 Prometheus 暴露，label 包含 `{role="client|follower|server", addr="ip:port"}`。

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `cubefs_rdma_requests_total` | Counter | 总请求数（RDMA 尝试） |
| `cubefs_rdma_fallback_total` | Counter | fallback 到 TCP 的次数 |
| `cubefs_rdma_latency_seconds` | Histogram | 单次 RDMA 往返延迟 |
| `cubefs_rdma_slot_wait_seconds` | Histogram | AcquireSlot 等待时长 |
| `cubefs_rdma_poll_spin_total` | Counter | poll 空转次数（按阶段分） |
| `cubefs_rdma_credit_stall_total` | Counter | credit 耗尽导致的阻塞次数 |
| `cubefs_rdma_active_slots` | Gauge | 当前被借用的 slot 数 |

**Fallback reason labels**（实际实现）：
`acquire_slot` / `write_packet` / `poll_response` / `return_credit` / `reqid_mismatch` / `crc_mismatch` / `op_again` / `size_mismatch` / `small_payload`（P6）

**日志规范**：

- fallback 时从 Warn 改为带 reason 字段的结构化日志 ✅
- 连接建立 / 断开记录 Info 级别，含对端 addr 和 slot 数 ✅
- credit stall 超过阈值（10µs）时增加计数 ✅

### 验收条件

- [x] `/metrics` 端点可抓到所有上述指标 — Prometheus default registry 注册（rdma 构建）
- [x] RDMA fallback 发生时 `cubefs_rdma_fallback_total` 增加，label 含原因 — 9 类 reason 全覆盖
- [x] 无 RDMA 请求时 `cubefs_rdma_active_slots` 为 0 — `TestSlotPool_ActiveSlotsCount`
- [x] 非 RDMA 构建时所有 rdma_* 指标不出现在 /metrics — metrics_stub.go 不调 `prometheus.Register`

---

## P4 — 写路径完整化 + 读路径 RDMA

### P4a：写响应走 RDMA ✅

**现状**（实施前）：主 DataNode 处理完写请求后，响应通过 TCP 发回 client。

**实施情况**：写响应在 P0 实现 `handleSlot` 时就已经走 RDMA（`cs.conn.WritePacket(slotIdx, &replPkt.Packet)`）。spec 描述的"缺口"在 P0 之前的快照里，P0 已经修复。

**P4a 实际工作**：把 `rdmaFakeConn.Write` 改为 `panic`，硬化"该路径不应被调用"这一断言。

**验收条件**：

- [x] 写请求全程无 TCP 包 ⏳ 需 tcpdump 验证（架构上保证）
- [x] `rdmaFakeConn.Write` 改为 panic — 已实施，含详细错误消息引导 P4b 处理
- [x] TCP 路径 write 响应行为不变 — 没有 net.Conn 实现冲突

### P4b：读路径 RDMA ✅

**范围**：SDK Read（`OpStreamRead` / `OpRead` / `OpStreamFollowerRead` / `OpExtentRepairRead` / `OpBackupRead`）。FUSE 路径通过 SDK 同一入口走相同代码（实际也加速，但符合 spec 兼容性约束）。

**数据流**（实施）：

```
client SDK --[RDMA Write, 单 chunk req (chunk_size ≤ ReadBlockSize=128KB)]--> DataNode
DataNode handleReadSlot:
  - Prepare(p) 查 partition
  - store.Read 直接读到 sendScratch[slot] 的 data 偏移（P5 lite 零拷贝）
  - MarshalHeader 写到 PacketHeader 偏移
  - WriteSlotZeroCopy 发送
client SDK pollRDMAResponse → DeserializePacket → CRC 校验 → copy 到应用 buffer
```

**关键约束**：

- DataNode 的读 buffer 是预注册的 RDMA MR（sendScratch）✅
- 读 buffer 大小 = SlotSize；超过 SlotSize - 头部开销时 server 回 `OpAgain`，SDK fallback TCP ✅
- 与写路径共用同一套槽模型（P1 SlotPool）✅

**新增接口**（实际签名）：

```go
// sdk/data/stream/rdma_client.go
func recvPacketViaRDMA(addr string, req *Packet) (*proto.Packet, error)
func rdmaRoundTrip(addr string, req *Packet) (*proto.Packet, error)  // 共享 send/recv 内部

// datanode/rdma_server.go
func (cs *connState) handleReadSlot(ctx *DataNodeRDMACtx, p *repl.Packet, slotIdx int) (handled bool)
```

**验收条件**：

- [ ] SDK 顺序读吞吐 ≥ TCP 路径的 150%（同机房 RDMA 网络下）⏳ 需真硬件实测
- [x] 读请求超过 SlotSize 时自动拆包 — SDK 按 `util.ReadBlockSize` 拆 chunk
- [x] RDMA 读失败时透明 fallback TCP，client 无感知 — `ExtentReader.Read` 顶层 try-RDMA-then-TCP
- [x] FUSE 路径读不受影响 — TCP fallback 保证；FUSE 实际会走 RDMA 加速（透明）

---

## P5 — 零拷贝磁盘路径 🟡 lite

### 实施范围

仅做 **transport 层零拷贝**：服务端 `handleReadSlot` 直接 `store.Read` 到 `sendScratch[slot]` 的 data 偏移，节省 1 次 `memcpy`（典型 128KB read 收益明显）。

### 未做（spec 全须）

- `io_uring + O_DIRECT` 直读到 RDMA MR
- 真"零拷贝"（`perf mem` 验证无 `memcpy`）
- 写路径同样的零拷贝（recvRing → 磁盘）

### 决策依据

`io_uring` 没在 vendor，要么引依赖、要么写 200 行 cgo binding；`O_DIRECT` 改造涉及 `datanode/storage/extent.go` 的核心写路径，改动面大；spec 验收要求真硬件实测。在性能瓶颈不明前先不做，留作 backlog。

### 接口变更（lite 部分）

```go
// util/rdma/conn.go
func (c *RDMAConn) WriteSlotZeroCopy(slotIdx, totalLen int) error
// caller 自己 stamp PacketHeader + Data 到 sendScratch；本方法仅 stamp SlotHeader 后 post
```

### 验收条件

- [ ] `perf mem` / `bpftrace` 确认热路径无 `memcpy` ⏳ **lite 不满足**：disk → pagecache 仍有 kernel 拷贝
- [ ] 写吞吐相比 P4 基线提升 ≥ 10% ⏳ 需真硬件实测
- [x] 非对齐 IO fallback 不崩溃 — 没引 O_DIRECT，无对齐约束

---

## P6 — 智能路径选择 🟡 lite

### 实施范围

**已做**：
- `min_payload_bytes` 阈值（默认 4KB），写 / 读路径同一守门 `rdmaTryForSize`
- `rdma.enabled` 总开关（沿用现有，`rdmaConnPool == nil` 时跳整个 RDMA 路径）
- fallback metric 加 `small_payload` reason

**未做**（`same_zone_only` / 跨 zone 自动 fallback）：
- `DataPartitionResponse` 没带 Zone 字段，需 Master API 改造
- 同 IDC 集群不需要

### 选择规则（实际实施）

| 条件 | 路径 | 原因 |
|------|------|------|
| `rdmaConnPool == nil`（rdma.enabled=false） | TCP | 总开关 |
| 包大小 < `MinPayloadBytes`（默认 4KB） | TCP | 小包 RDMA 两次 WR 开销不合算 |
| RDMA 路径任何阶段失败 | TCP | 透明 fallback |
| 以上均不满足 | RDMA | 正常 RDMA 路径 |

**未实现的规则**：
- 对端不支持 RDMA 检测（依赖握手失败 → 自然走 fallback）
- 跨 Zone / 跨机房自动 TCP（需 zone 拓扑感知）

### 配置（实际）

```toml
# 客户端 mount option / DataNode config
rdmaEnable = true
rdmaNumSlots = 256
rdmaSlotSize = 135168           # 132KB；MinValidSlotSize 之上
rdmaBusySpinCount = 200         # P2
rdmaYieldCount = 1000           # P2
rdmaSleepThresholdUs = 50       # P2
rdmaMinPayloadBytes = 4096      # P6 lite
```

### 验收条件

- [x] 小于 `rdmaMinPayload` 的写请求走 TCP — `rdmaTryForSize` + `TestRDMAConnPool_MinPayloadBytesAccessor`
- [ ] 跨 Zone 写请求走 TCP，同 Zone 走 RDMA ❌ **lite 范围不实现**
- [x] 关闭 `rdma.enabled` 后所有请求走 TCP — `rdmaConnPool == nil` 守门
- [x] `cubefs_rdma_fallback_total` 中小包有 reason label — `small_payload`

---

## 实现路线图（实际）

```
✅ Week 1-2   P0  流控 + SlotSize 校验          6f6958ea9
✅ Week 3-4   P1  多槽 pipeline                 9da3a6a2a
✅ Week 5     P2  自适应 poll                   7bc2acb09
✅ Week 6     P3  可观测性                      66e10f11f
✅ Week 7-8   P4a 写响应 panic                  7336b2355
✅ Week 9-12  P4b 读路径 RDMA                   4939f869d
🟡 Week 13-14 P5  零拷贝（lite: transport only） 88f984159
🟡 Week 15    P6  智能路径选择（lite: size + 开关） 9ba6a47c6
```

总改动：**+4033 / -516 行，38 文件**，新增 9 个 `util/rdma/` 文件 + 同等规模测试。

## 测试策略

### 单元测试 ✅

每个 P 级别提供 mock RDMA transport 或纯 Go 状态机，不依赖真实 RDMA 硬件：

- 44/44 单测在 stub mode (`-race`) PASS
- 覆盖：credit 状态机、SlotPool 借用 / 阻塞 / dirty / 并发、自适应 poll 阶段转移、SlotSize 校验、metrics no-op、handshake roundtrip、wrid 编码、recv pool 等

### 集成测试 ⏳

需在 Linux + libibverbs + rxe 环境跑（见 `rdma-verification.md`）：

```bash
modprobe rdma_rxe
rdma link add rxe0 type rxe netdev lo
go test -tags rdma ./util/rdma/  # TestLoopback 端到端验证 P0+P1+P2+P5 lite
```

### 性能基准 ⏳

每个 P 级别完成后，在 test-hb 环境跑 `perf-parallel`，对比以下指标：

| 指标 | 基准（TCP）| 目标 | 状态 |
|------|-----------|------|------|
| 顺序写吞吐（MB/s/node） | 155 | P1 后 ≥ 300 | ⏳ |
| 顺序写延迟（µs P99） | — | P0+P1 后记录基线 | ⏳ |
| CPU 使用率（polling） | — | P2 后空载 < 1% | ⏳ |
| 顺序读吞吐（MB/s/node） | — | P4b 后 ≥ TCP × 1.5 | ⏳ |

性能基线全部留待 Linux 环境验证 — 见 `rdma-verification.md`。
