# CubeFS RDMA 传输层设计方案

> 目标：在 client→datanode 和 datanode→datanode 副本路径引入纯 One-sided RDMA，
> 完整保留 TCP 路径，支持 InfiniBand 和 RoCEv2。
>
> **设计约束：client→datanode / datanode→datanode 全程不使用 Two-sided Send/Recv，
> 包括 header、Arg、Data 在内的所有内容均通过 RDMA Write 传输。**

---

## 1. 现有架构基线

### 1.1 通信路径

```
Client
 ├── sdk/data/stream/stream_conn.go
 │     StreamConnPool (util.ConnectPool) → *net.TCPConn → DataNode(leader)
 │
 └── sdk/data/stream/extent_handler.go
       直接 WriteToConn / ReadFromConnWithVer

DataNode(leader)
 └── datanode/repl/repl_protocol.go
       FollowerTransport.conn (net.Conn) → DataNode(follower)
       选择函数：getSmuxConn (smux) 或 gConnPool (TCP)
```

### 1.2 Packet 结构（wire 格式）

```
[0]      Magic         uint8   = 0xFF
[1]      ExtentType    uint8
[2]      Opcode        uint8
[3]      ResultCode    uint8
[4]      RemainingFollowers uint8
[5:9]    CRC           uint32
[9:13]   Size          uint32   ← Data payload 字节数
[13:17]  ArgLen        uint32
[17:25]  PartitionID   uint64
[25:33]  ExtentID      uint64
[33:41]  ExtentOffset  int64
[41:49]  ReqID         int64
[49:57]  KernelOffset  uint64   ← 基础 header = 57 bytes（最大 69 bytes）

+ Arg[]byte  (ArgLen bytes)
+ Data[]byte (Size bytes)
```

整个 packet（header + Arg + Data）作为一个整体写入 RDMA slot，不拆分传输。

---

## 2. 设计原则

| 原则 | 说明 |
|------|------|
| 纯 One-sided | client↔datanode / datanode↔datanode 全程 RDMA Write，无 Send/Recv |
| 连接建立时一次性协商 | 内存地址（rkey + base_va）通过 rdma_cm private_data 交换，之后无需再协商 |
| Ring Buffer + Doorbell | 请求/响应各用一个 ring buffer，doorbell RDMA Write 通知对端 |
| TCP 零改动 | 现有所有函数签名、连接池、packet 序列化完全不动 |
| Build Tag 隔离 | 默认编译不含 RDMA，`-tags rdma` 开启 |
| IB/RoCEv2 透明 | rdma_cm 屏蔽底层差异，应用层无感知 |

---

## 3. 核心机制：Ring Buffer + Doorbell

### 3.1 内存布局（双向各一套）

每条 RDMA 连接建立时，双方各自注册两段内存并通过 rdma_cm private_data 交换地址：

```
发送方（Client / Leader DN）本地注册：
  req_ring:   NUM_SLOTS × SLOT_SIZE     ← 发送请求的 ring buffer
  resp_ring:  NUM_SLOTS × RESP_SLOT_SIZE ← 接收响应的 ring buffer

接收方（DataNode / Follower DN）本地注册：
  req_ring:   NUM_SLOTS × SLOT_SIZE     ← 接收请求的 ring buffer
  resp_ring:  NUM_SLOTS × RESP_SLOT_SIZE ← 发送响应的 ring buffer
  doorbell:   NUM_SLOTS × 8 bytes       ← 请求到达通知数组
  resp_db:    NUM_SLOTS × 8 bytes       ← 响应完成通知数组
```

### 3.2 Slot 结构（每个请求独占一个 slot）

```
┌─────────────────────────────────────────────────────────┐
│  Slot Header（固定 16 bytes）                            │
│    [0:8]   magic + seq（防止旧数据误读）                  │
│    [8:12]  total_len（header + Arg + Data 总字节数）      │
│    [12:16] reserved                                      │
├─────────────────────────────────────────────────────────┤
│  Packet Header（57~69 bytes，原始 proto.Packet header）  │
├─────────────────────────────────────────────────────────┤
│  Arg（ArgLen bytes）                                     │
├─────────────────────────────────────────────────────────┤
│  Data（Size bytes，最大 SLOT_SIZE - overhead）           │
└─────────────────────────────────────────────────────────┘
```

Doorbell 数组中每个 entry 是 8 字节：`{seq(4) | slot_idx(4)}`，
Client 在 RDMA Write 完整 slot 后，再 RDMA Write 这 8 字节到服务端 doorbell 数组对应位置。

### 3.3 连接建立（一次性协商，无 Send/Recv）

```
rdma_cm private_data（最大 56 bytes）用于双向交换内存信息：

Client → DataNode（连接请求时携带）：
  {client_req_rkey, client_req_base_va,    // Client 的 req ring（DataNode 不需要写这里）
   client_resp_rkey, client_resp_base_va,  // DataNode 写响应到这里
   client_resp_db_rkey, client_resp_db_va, // DataNode 写响应 doorbell 到这里
   num_slots, slot_size}

DataNode → Client（连接接受时携带）：
  {server_req_rkey, server_req_base_va,    // Client 写请求到这里
   server_db_rkey, server_db_va,           // Client 写 doorbell 到这里
   num_slots, slot_size}
```

握手完成后，双方持有对方全部内存信息，后续请求和响应均为纯 RDMA Write。

---

## 4. 协议流

### 4.1 Client → DataNode 写（纯 One-sided）

```
Client                                         DataNode
  │                                                │
  │  slot_idx = ReqID % NUM_SLOTS                 │
  │  组装 slot: SlotHeader + PacketHeader + Arg + Data
  │                                                │
  │──RDMA Write: slot → server_req_base_va         │
  │              + slot_idx * SLOT_SIZE            │  NIC 直传，CPU 不参与
  │                                                │
  │──RDMA Write: {seq, slot_idx}                  │
  │              → server_db_va + slot_idx * 8    │  doorbell，触发 DataNode 处理
  │                                                │
  │  (等待响应 doorbell)                           │  DataNode 轮询 doorbell
  │                                                │  发现 seq 更新 → 读取 slot
  │                                                │  解析 SlotHeader + Packet
  │                                                │  执行写操作（磁盘 / 转发副本）
  │                                                │
  │                                                │──RDMA Write: resp_slot
  │                                                │   → client_resp_base_va
  │                                                │    + slot_idx * RESP_SLOT_SIZE
  │                                                │
  │←─────────────────────────────────────────────│──RDMA Write: {seq, slot_idx}
  │  DataNode 写 resp_doorbell 到 Client           │   → client_resp_db_va
  │                                                │     + slot_idx * 8
  │  Client 轮询 resp_doorbell                     │
  │  发现更新 → 从 resp_ring 读取响应              │
```

### 4.2 Client → DataNode 读（纯 One-sided）

```
Client                                         DataNode
  │  在本地 resp_ring[slot_idx] 预备接收区域       │
  │                                                │
  │──RDMA Write: slot（header + Arg）             │
  │   slot 中附带 client_resp_rkey + resp_va       │  DataNode 需要知道往哪里写数据
  │──RDMA Write: doorbell                          │
  │                                                │
  │                                                │  解析请求，从磁盘读数据到本地 buf
  │                                                │
  │                                                │──RDMA Write: data
  │                                                │   → client_resp_base_va + slot_idx * RESP_SLOT_SIZE
  │                                                │   （数据直接写入 Client 内存）
  │                                                │──RDMA Write: resp_doorbell
  │                                                │
  │  Client 检测到 resp_doorbell 更新              │
  │  数据已在 resp_ring[slot_idx] 中               │
```

读路径中，Client 的 `client_resp_rkey` 和 `client_resp_base_va` 已在连接建立时告知 DataNode，
DataNode 计算目标地址 = `client_resp_base_va + slot_idx * RESP_SLOT_SIZE`，直接 RDMA Write。

### 4.3 DataNode → DataNode 副本写（纯 One-sided）

与 4.1 完全相同，Leader DN 充当 Client，Follower DN 充当 DataNode。
`FollowerTransport` 中不再有 `conn.Write()`，替换为 `rdmaConn.WriteSlot()`。

---

## 5. 核心数据结构

### 5.1 连接协商结构（rdma_cm private_data）

```go
// util/rdma/handshake.go

// ConnectInfo 通过 rdma_cm private_data 在连接建立时传递（≤ 56 bytes）
// 发起方（Client/Leader）发送给接收方
type ConnectInfo struct {
    RespRkey    uint32  // 接收方写响应时使用的 rkey
    RespBaseVA  uint64  // 响应 ring buffer 基址
    RespDbRkey  uint32  // 响应 doorbell 的 rkey
    RespDbVA    uint64  // 响应 doorbell 基址
    NumSlots    uint32  // ring buffer slot 数量
    SlotSize    uint32  // 每个 slot 的字节数
} // 36 bytes

// AcceptInfo 接收方返回给发起方
type AcceptInfo struct {
    ReqRkey   uint32  // 发起方写请求时使用的 rkey
    ReqBaseVA uint64  // 请求 ring buffer 基址
    DbRkey    uint32  // 请求 doorbell 的 rkey
    DbVA      uint64  // 请求 doorbell 基址
    NumSlots  uint32  // 需与 ConnectInfo.NumSlots 一致
    SlotSize  uint32  // 需与 ConnectInfo.SlotSize 一致
} // 36 bytes
```

### 5.2 Slot Header

```go
// util/rdma/slot.go

const SlotHeaderSize = 16

// SlotHeader 放在每个 ring buffer slot 的起始位置
// DataNode 通过读 SlotHeader 判断 slot 是否为新请求（seq 变化）
type SlotHeader struct {
    Magic    uint32  // 固定魔数，防止读到未初始化内存
    Seq      uint32  // 单调递增序号，DataNode 轮询此字段
    TotalLen uint32  // SlotHeader + PacketHeader + Arg + Data 总字节数
    _        uint32  // 对齐保留
}
```

### 5.3 RDMAMem（内存注册单元）

```go
// util/rdma/mem.go

// RDMAMem 封装一块向 NIC 注册的 pinned 内存
// 通过 C.malloc 分配，绕过 Go GC（GC 会移动内存，导致 DMA 地址失效）
type RDMAMem struct {
    buf  unsafe.Pointer         // C.malloc 分配的内存起始地址
    mr   *C.struct_ibv_mr       // ibv_reg_mr 返回的 MR handle
    Lkey uint32                 // 本地访问 key（本端 post_send 时使用）
    Rkey uint32                 // 远端访问 key（告知对端，对端 RDMA Write 时使用）
    VA   uint64                 // 内存虚拟地址（= uintptr(buf)，告知对端）
    Size int
}

func AllocRDMAMem(pd *C.struct_ibv_pd, size int) (*RDMAMem, error)
func (m *RDMAMem) Free()
func (m *RDMAMem) Bytes() []byte  // 返回 Go slice，指向同一块内存
```

### 5.4 RDMAConn（单条连接）

```go
// util/rdma/conn.go

// RDMAConn 封装一条 RDMA RC（Reliable Connected）连接
// 纯 One-sided：所有数据通过 RDMA Write 传输，不使用 Send/Recv
type RDMAConn struct {
    cmID *C.struct_rdma_cm_id
    pd   *C.struct_ibv_pd
    cq   *C.struct_ibv_cq       // 仅用于轮询 RDMA Write completion
    qp   *C.struct_ibv_qp

    // 本端内存（向 NIC 注册）
    reqRing  *RDMAMem  // 发送请求的 ring buffer（Client 端）
    respRing *RDMAMem  // 接收响应的 ring buffer
    respDB   *RDMAMem  // 响应 doorbell 数组

    // 对端内存信息（连接建立时协商）
    remote AcceptInfo  // 对端 req_ring / doorbell 的 rkey + VA

    numSlots  int
    slotSize  int
    nextSeq   [maxSlots]uint32  // 每个 slot 当前的 seq 值（用于 doorbell 对比）

    remoteAddr string
    closed     int32
}

// WriteSlot 将完整 packet 写入对端 req_ring 的指定 slot，并触发 doorbell
// packet 已序列化为 []byte（SlotHeader + PacketHeader + Arg + Data）
func (c *RDMAConn) WriteSlot(slotIdx int, data []byte) error

// PollResp 轮询本端 resp_doorbell，有响应时返回 slot 内容
// 非阻塞，无响应时返回 nil
func (c *RDMAConn) PollResp(slotIdx int) ([]byte, bool)

// Close 关闭连接，释放 QP 和内存
func (c *RDMAConn) Close() error
```

### 5.5 RDMAConnPool（连接池）

```go
// util/rdma/pool.go

// RDMAConnPool 管理到各 DataNode 的 RDMA 连接
// API 与 util.ConnectPool 平行，方便调用侧对称替换
type RDMAConnPool struct {
    mu      sync.RWMutex
    pools   map[string]*singlePool
    cfg     RDMAPoolConfig
}

type RDMAPoolConfig struct {
    Device      string        // RDMA 设备名，如 "mlx5_0"
    Port        int           // RDMA 监听端口
    NumSlots    int           // ring buffer slot 数（默认 256）
    SlotSize    int           // slot 大小（默认 128MB，覆盖最大 extent）
    MaxConns    int           // 每个目标地址的最大连接数
    IdleTimeout time.Duration
}

func NewRDMAConnPool(cfg RDMAPoolConfig) (*RDMAConnPool, error)
func (p *RDMAConnPool) GetConnect(addr string) (*RDMAConn, error)
func (p *RDMAConnPool) PutConnect(c *RDMAConn, forceClose bool)
func (p *RDMAConnPool) Close()
```

### 5.6 DataNode 侧接收上下文（DataNodeRDMACtx）

```go
// datanode/rdma_server.go

// DataNodeRDMACtx 负责监听 RDMA 连接请求，管理每条连接的 ring buffer，
// 并将接收到的 packet 分发给现有的请求处理逻辑
type DataNodeRDMACtx struct {
    cfg      RDMAServerConfig
    listener *C.struct_rdma_cm_id

    // 每条连接各自的 ring buffer 内存（服务端预注册）
    // connID → ConnState
    conns   sync.Map

    // 接入现有处理管道（不改变 DataNode 现有请求处理逻辑）
    handlePacket func(p *proto.Packet) error
}

// ConnState 维护单条 RDMA 连接的服务端状态
type ConnState struct {
    conn     *rdma.RDMAConn
    reqRing  *rdma.RDMAMem  // 接收 Client 写入请求的 ring buffer
    doorbell *rdma.RDMAMem  // Client 写 doorbell 到这里
    respRing *rdma.RDMAMem  // DataNode 写响应到 Client
    respDB   *rdma.RDMAMem  // DataNode 写 resp_doorbell 到 Client

    pollCh chan int          // doorbell 轮询协程通知槽索引
}

type RDMAServerConfig struct {
    Device      string
    Port        int
    NumSlots    int
    SlotSize    int
    RespSlotSize int
}
```

---

## 6. Doorbell 轮询策略

DataNode 需要知道何时有新请求到达（Client 写完 slot + doorbell 后）。
纯 One-sided 下没有 Send/Recv 通知，有三种轮询方案：

| 方案 | 机制 | 延迟 | CPU 消耗 |
|------|------|------|---------|
| 忙轮询 | goroutine 持续检查 doorbell seq | 最低（亚微秒） | 高（一个核） |
| CQ 事件通知 | ibv_req_notify_cq + 阻塞等待 | 中（需内核介入） | 低 |
| 混合（adaptive） | 先忙轮询 N 次，无请求则退化为事件通知 | 接近忙轮询 | 可控 |

**建议：混合方案**，忙轮询阈值可配置（默认 10000 次），与 DPDK/SPDK 实践一致。

```go
// DataNode 每条连接一个 goroutine 轮询
func (cs *ConnState) pollLoop(ctx context.Context) {
    spin := 0
    for {
        for i := 0; i < cs.conn.NumSlots(); i++ {
            if cs.doorbellUpdated(i) {
                cs.pollCh <- i
                spin = 0
            }
        }
        spin++
        if spin > spinThreshold {
            // 退化为 runtime.Gosched()，让出 CPU
            runtime.Gosched()
            spin = 0
        }
        if ctx.Err() != nil {
            return
        }
    }
}
```

---

## 7. 文件变更范围

### 7.1 新增文件（纯新增，不影响现有任何逻辑）

```
util/rdma/
  verbs.go        CGO 绑定：ibv_open_device, ibv_alloc_pd, ibv_create_cq,
                            ibv_create_qp, ibv_reg_mr, ibv_post_send,
                            ibv_poll_cq, rdma_cm_* 系列函数
  handshake.go    ConnectInfo / AcceptInfo 结构及序列化
  mem.go          RDMAMem：C.malloc + ibv_reg_mr
  slot.go         SlotHeader，slot 序列化/反序列化工具
  conn.go         RDMAConn：WriteSlot / PollResp / Close
  pool.go         RDMAConnPool：连接池管理

datanode/
  rdma_server.go  DataNodeRDMACtx：监听、握手、ConnState 管理、pollLoop
```

### 7.2 修改现有文件（改动极小）

| 文件 | 改动内容 | 改动量 |
|------|----------|--------|
| `datanode/server.go` | 仿照 smux initConnPool，增加 RDMA 分支初始化 DataNodeRDMACtx | ~25 行 |
| `sdk/data/stream/stream_conn.go` | `GetConnect` 前增加 pool 选择（TCP 或 RDMA） | ~15 行 |
| `datanode/repl/repl_protocol.go` | `FollowerTransport` 增加 `rdmaConn *rdma.RDMAConn` 字段及发送分支 | ~20 行 |

### 7.3 TCP 路径改动

**零改动**。`WriteToConn`、`ReadFromConnWithVer`、`ConnectPool`、`FollowerTransport.serverWriteToFollower` 保持原样。

---

## 8. Build Tag 隔离

```go
//go:build linux && rdma
// +build linux,rdma
```

所有 `util/rdma/` 和 `datanode/rdma_server.go` 均加此 tag。
`datanode/server.go` 中的 RDMA 初始化代码通过 stub 文件在非 rdma build 下编译为空操作。

```
datanode/rdma_server_stub.go   // !rdma build tag，空实现
util/rdma/stub.go              // !rdma build tag，空实现
```

默认 `make build` 不引入任何 RDMA 依赖，`make build TAGS=rdma` 开启。

---

## 9. 配置项

### Client 配置（`client/fs/super.go` 读取）

```json
{
  "rdmaEnable": false,
  "rdmaDevice": "mlx5_0",
  "rdmaPort": 18510,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 134217728,
  "rdmaMaxConns": 4
}
```

### DataNode 配置（`datanode/server.go` 读取）

```json
{
  "rdmaEnable": false,
  "rdmaDevice": "mlx5_0",
  "rdmaPort": 18510,
  "rdmaNumSlots": 256,
  "rdmaSlotSize": 134217728,
  "rdmaRespSlotSize": 4096,
  "rdmaSpinThreshold": 10000
}
```

`rdmaEnable=false` 时不启动 RDMA listener，退化为纯 TCP 行为，行为与现在完全一致。

---

## 10. IB 与 RoCEv2 兼容

`rdma_cm` 自动处理底层差异，应用层无感知：

| 环境 | rdmaDevice | 配置 rdmaAddr | 底层 |
|------|-----------|--------------|------|
| RoCEv2 100GbE | mlx5_0 | 以太网 IP | RoCE RC QP |
| InfiniBand HDR | mlx5_0 | IPoIB IP | IB RC QP（自动路径解析） |

---

## 11. 实现阶段

### Phase 1：util/rdma 基础库（约 3 周）

- `util/rdma/` 全部文件（verbs.go、mem.go、handshake.go、slot.go、conn.go、pool.go）
- stub 文件
- 单机回环测试（loopback bench：WriteSlot → PollResp 往返延迟）

### Phase 2：DataNode 接收侧（约 2 周）

- `datanode/rdma_server.go`（DataNodeRDMACtx、ConnState、pollLoop）
- `datanode/server.go` 初始化分支
- 与现有 `handlePacket` 逻辑对接（packet 从 slot 中反序列化后复用现有处理路径）

### Phase 3：Client 写/读路径（约 2 周）

- `sdk/data/stream/stream_conn.go` pool 选择
- `RDMAConnPool` 客户端侧连接建立
- 端到端写入 + 读取测试，与 TCP 路径对比

### Phase 4：副本路径（约 1 周）

- `datanode/repl/repl_protocol.go` FollowerTransport RDMA 分支
- 三副本写延迟测试

---

## 12. 预期收益

| 场景 | TCP 100GbE | RDMA（RoCEv2/IB 100G） |
|------|-----------|----------------------|
| 顺序写延迟（4MB block） | ~800μs | ~150μs |
| 三副本写总延迟 | ~2.4ms | ~450μs |
| 顺序读吞吐（单流） | ~7GB/s | ~11GB/s |
| DataNode CPU（高吞吐写） | 60-80% | 15-25% |

---

## 附：关键依赖

| 依赖 | 说明 |
|------|------|
| `libibverbs` | RDMA verbs 用户态库（`libibverbs-dev`） |
| `librdmacm` | RDMA 连接管理（`librdmacm-dev`） |
| RDMA NIC 驱动 | Mellanox OFED 或内核自带 `mlx5_core`/`rxe`（软件 RoCE 用于开发测试） |
| Go 1.17+ | 现有 go.mod 版本满足 |
| CGO | 仅 `rdma` build tag 下开启 |
