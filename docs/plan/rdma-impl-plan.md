# CubeFS RDMA 实现规划与验收标准

> 本文档为 AI 辅助开发和验收设计，配合 `rdma-design.md` 使用。
> 每个 Phase 可独立交付和验收。

---

## 核心约束：原始数据路径不受影响

**这是最高优先级约束，高于所有性能和功能目标。**

### 保护机制

三层防线，缺一不可：

#### 第一层：Build Tag 隔离（默认编译无 RDMA 代码）

```
默认 make build     → 二进制中 RDMA 代码根本不存在
make build TAGS=rdma → 引入 RDMA 代码，但需通过第二层保护
```

所有 `util/rdma/` 和 `datanode/rdma_server.go` 均带 `//go:build linux && rdma` tag。
现有任何文件不得 import `util/rdma` 包——依赖通过接口注入，见第二层。

#### 第二层：函数变量注入（沿用 smux 已有模式）

`repl_protocol.go` 中 `getSmuxConn == nil` → TCP path，这个模式已经存在。
RDMA 完全照搬：新增函数变量，默认 nil，nil 时热路径代码**一行不改**。

```
getSmuxConn == nil  AND  sendViaRDMA == nil  →  gConnPool.GetConnect(addr)（原来的代码）
getSmuxConn != nil                           →  smux path（已有）
sendViaRDMA != nil                           →  RDMA path（新增）
```

`sendViaRDMA` 是函数变量，由 `rdma_server.go`（build tagged）在启动时注入。
不带 rdma tag 编译时，注入代码不存在，变量永远是 nil，永远走 TCP path。

#### 第三层：运行时开关（rdmaEnable=false 的双重保险）

即使带 rdma tag 编译，`rdmaEnable=false`（默认值）时：
- `DataNodeRDMACtx` 不启动
- `sendViaRDMA` 不被注入（保持 nil）
- 热路径执行路径与改动前 100% 相同

### One-sided RDMA 与 net.Conn 的根本差异

**smux 能透明替换 TCP，One-sided RDMA 不能。**

smux 返回 `net.Conn`，`conn.Write(data)` 语义不变，热路径代码零修改。

One-sided RDMA 的调用是 `rdmaConn.WriteSlot(slotIdx, data)`，语义完全不同：
- TCP：write header → write arg → write data（3次独立写，内核负责拼装）
- RDMA：一次 RDMA Write 写完整 slot（SlotHeader+Header+Arg+Data），否则失去零拷贝收益

因此热路径在有 RDMA 连接时必须走不同分支。**这是对现有文件唯一不可避免的改动。**

改动的安全约束：
1. 分支判断只做一次（连接建立时），不在每个 Write 调用里判断
2. 判断条件是函数变量是否为 nil（与 smux 完全一致）
3. nil 时代码路径与今天字节级一致

### 现有文件改动清单（完整）

| 文件 | 改动内容 | 改动量 | TCP path 影响 |
|------|---------|--------|--------------|
| `datanode/server.go` | `initConnPool()` 末尾增加 RDMA init 调用（build tagged 函数） | ~5 行 | 零：rdmaEnable=false 时不执行 |
| `datanode/repl/repl_protocol.go` | 新增 `sendViaRDMA` 函数变量字段（nil 默认）；连接建立处增加 nil 检查 | ~15 行 | 零：nil 时走原有 gConnPool.GetConnect |
| `sdk/data/stream/stream_conn.go` | 新增 `rdmaConnFunc` 函数变量（nil 默认）；GetConnect 处增加 nil 检查 | ~10 行 | 零：nil 时走原有 StreamConnPool.GetConnect |

**以上是全部改动。TCP 相关的所有函数（WriteToConn、ReadFromConnWithVer、ConnectPool、FollowerTransport.conn）签名和实现一行不改。**

### 回归验证门禁（每个 Phase 提交前强制执行）

```bash
# 不带 rdma tag 编译，验证现有功能完全不受影响
go build ./...
go test ./datanode/... ./sdk/... ./proto/... -count=1 -timeout 5m

# 带 rdma tag 编译，rdmaEnable=false 配置，验证行为一致
go test -tags 'linux rdma' ./datanode/... ./sdk/... -count=1 -run 'TestTCP'
```

所有现有测试必须 100% 通过，否则该 Phase 不得合入。

---

## 构建环境前提

```bash
# 开发机需安装（Ubuntu 22.04）
apt-get install -y libibverbs-dev librdmacm-dev rdma-core

# 验证 RDMA 设备可用（软件 RoCE 用于本地测试）
rdma link show
ibv_devices

# 启用软件 RoCE（无硬件时用于开发测试）
modprobe rdma_rxe
rdma link add rxe0 type rxe netdev eth0

# 带 RDMA tag 编译
make build TAGS=rdma
```

---

## Phase 1：`util/rdma` 基础库

### 1.1 文件清单与职责

#### `util/rdma/rdma.h`（C 头文件，CGO include 用）

```c
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
```

#### `util/rdma/verbs.go`

**Build tag**: `//go:build linux && rdma`

必须实现的 CGO 封装函数（每个对应一个 Go wrapper）：

| Go 函数 | C 调用 | 说明 |
|---------|--------|------|
| `openDevice(name string) (*C.struct_ibv_context, error)` | `ibv_open_device` | 打开 RDMA 设备 |
| `allocPD(ctx) (*C.struct_ibv_pd, error)` | `ibv_alloc_pd` | 分配 Protection Domain |
| `createCQ(ctx, size) (*C.struct_ibv_cq, error)` | `ibv_create_cq` | 创建 Completion Queue |
| `createQP(pd, cq, cap) (*C.struct_ibv_qp, error)` | `ibv_create_qp` | 创建 Queue Pair（RC 类型） |
| `regMR(pd, buf, size) (*C.struct_ibv_mr, error)` | `ibv_reg_mr` | 注册内存（IBV_ACCESS_LOCAL_WRITE\|REMOTE_WRITE） |
| `deregMR(mr)` | `ibv_dereg_mr` | 注销内存 |
| `postSendWrite(qp, lkey, lva, rkey, rva, len, wr_id) error` | `ibv_post_send` | 投递 RDMA Write WR |
| `pollCQ(cq, max) ([]C.struct_ibv_wc, error)` | `ibv_poll_cq` | 轮询完成队列 |
| `createIDClient() (*C.struct_rdma_cm_id, error)` | `rdma_create_id` | 创建 client CM ID |
| `resolveAddr(id, dst string, timeout) error` | `rdma_resolve_addr` + `rdma_resolve_route` | 解析地址和路由 |
| `connect(id, info ConnectInfo) error` | `rdma_connect` | 发起连接（携带 private_data） |
| `createIDServer(port) (*C.struct_rdma_cm_id, error)` | `rdma_create_id` + `rdma_bind_addr` + `rdma_listen` | 服务端监听 |
| `getRequest(id) (*C.struct_rdma_cm_id, ConnectInfo, error)` | `rdma_get_request` | 接受连接请求，解析 private_data |
| `accept(id, info AcceptInfo) error` | `rdma_accept` | 接受连接（携带 private_data） |
| `destroyID(id)` | `rdma_destroy_id` | 销毁 CM ID |

**关键实现要求**：
- `postSendWrite` 必须设置 `IBV_SEND_SIGNALED` 以便轮询 completion
- QP 初始化参数：`max_send_wr=256, max_recv_wr=1, max_send_sge=1`（纯 One-sided 不需要 Recv WR）
- `regMR` 权限必须包含 `IBV_ACCESS_REMOTE_WRITE`（允许对端写入本地内存）

#### `util/rdma/handshake.go`

**Build tag**: `//go:build linux && rdma`

```go
// ConnectInfo 发起方通过 rdma_cm private_data 发送给接收方（共 36 bytes）
type ConnectInfo struct {
    RespRkey   uint32  // offset 0
    _pad0      uint32  // offset 4（对齐）
    RespBaseVA uint64  // offset 8
    RespDbRkey uint32  // offset 16
    _pad1      uint32  // offset 20（对齐）
    RespDbVA   uint64  // offset 24
    NumSlots   uint32  // offset 32
    SlotSize   uint32  // offset 36
} // 共 40 bytes，需 <= 56 bytes（rdma_cm 限制）

// AcceptInfo 接收方通过 rdma_cm accept private_data 发送（共 36 bytes）
type AcceptInfo struct {
    ReqRkey   uint32
    _pad0     uint32
    ReqBaseVA uint64
    DbRkey    uint32
    _pad1     uint32
    DbVA      uint64
    NumSlots  uint32
    SlotSize  uint32
} // 共 40 bytes

// MarshalConnectInfo / UnmarshalConnectInfo：小端序字节序列化
func MarshalConnectInfo(ci ConnectInfo) []byte
func UnmarshalConnectInfo(b []byte) (ConnectInfo, error)
func MarshalAcceptInfo(ai AcceptInfo) []byte
func UnmarshalAcceptInfo(b []byte) (AcceptInfo, error)
```

**要求**：序列化必须使用 `binary.LittleEndian`，字节序固定，与 C 端内存布局一致。

#### `util/rdma/mem.go`

**Build tag**: `//go:build linux && rdma`

```go
type RDMAMem struct {
    buf  unsafe.Pointer   // C.malloc 分配（绕过 GC）
    mr   *C.struct_ibv_mr
    Lkey uint32
    Rkey uint32
    VA   uint64           // uintptr(buf)，告知对端
    Size int
}

func AllocRDMAMem(pd *C.struct_ibv_pd, size int) (*RDMAMem, error)
func (m *RDMAMem) Free()
func (m *RDMAMem) Bytes() []byte          // 返回 Go slice，zero-copy
func (m *RDMAMem) SlotBytes(idx, slotSize int) []byte  // 返回第 idx 个 slot 的 slice
```

**关键实现要求**：
- 必须使用 `C.malloc`，不得使用 `make([]byte, ...)`（Go GC 会移动内存，导致 DMA 地址失效）
- `Free()` 必须先 `ibv_dereg_mr`，再 `C.free`，顺序不能反
- `Bytes()` 使用 `unsafe.Slice((*byte)(m.buf), m.Size)` 返回指向同一块内存的 slice

#### `util/rdma/slot.go`

**Build tag**: `//go:build linux && rdma`

```go
const SlotHeaderSize = 16
const DoorbellEntrySize = 8

type SlotHeader struct {
    Magic    uint32  // 固定值 0x52444D41（"RDMA"）
    Seq      uint32  // 单调递增，接收方轮询此字段
    TotalLen uint32  // SlotHeader + PacketData 总字节数
    Reserved uint32  // 保留，写 0
}

// WriteSlotHeader 将 SlotHeader 序列化写入 buf（小端序）
func WriteSlotHeader(buf []byte, seq uint32, totalLen uint32)

// ReadSlotHeader 从 buf 读取并校验 SlotHeader
func ReadSlotHeader(buf []byte) (SlotHeader, error)

// WriteDoorbellEntry 将 {seq, slotIdx} 写入 doorbell 数组的第 idx 个 entry
func WriteDoorbellEntry(buf []byte, idx int, seq uint32, slotIdx uint32)

// ReadDoorbellEntry 读取 doorbell entry
func ReadDoorbellEntry(buf []byte, idx int) (seq uint32, slotIdx uint32)

// SerializePacket 将 proto.Packet 的 header+Arg+Data 序列化到 slot（SlotHeader 之后）
// 返回写入的总字节数（含 SlotHeader）
func SerializePacket(slot []byte, p *proto.Packet) (int, error)

// DeserializePacket 从 slot 反序列化出 proto.Packet
func DeserializePacket(slot []byte) (*proto.Packet, error)
```

**关键实现要求**：
- `Magic` 字段必须校验，不匹配时返回错误（防止读到未初始化内存）
- `SerializePacket` 需支持 proto.Packet 的完整 header（最大 69 bytes）+ Arg + Data
- `DeserializePacket` 需复用现有 `proto.Packet` 结构，不创建新类型

#### `util/rdma/conn.go`

**Build tag**: `//go:build linux && rdma`

```go
const maxSlots = 1024  // 上限，实际运行时使用配置值

type RDMAConn struct {
    cmID *C.struct_rdma_cm_id
    pd   *C.struct_ibv_pd
    cq   *C.struct_ibv_cq
    qp   *C.struct_ibv_qp

    // 本端内存
    reqRing  *RDMAMem  // 本端 req ring（Client 端组装 slot 用；Server 端接收请求用）
    respRing *RDMAMem  // 本端 resp ring（Server 端组装响应用；Client 端接收响应用）
    respDB   *RDMAMem  // 本端 resp doorbell 数组

    // 对端内存信息（连接建立时协商）
    remote AcceptInfo  // 对端 req_ring / doorbell 的 rkey + VA

    numSlots  int
    slotSize  int
    respSlotSize int
    nextSeq   [maxSlots]uint32  // 每个 slot 当前的发送 seq

    remoteAddr string
    closed     int32  // atomic
}

// Dial 建立到 addr 的 RDMA 连接（Client 侧调用）
// 分配本端内存，通过 ConnectInfo 告知对端，解析对端 AcceptInfo
func Dial(addr string, cfg RDMAConnConfig) (*RDMAConn, error)

// Accept 在已监听的 cmID 上接受一条连接（Server 侧调用）
// 分配本端内存，解析对端 ConnectInfo，通过 AcceptInfo 告知对端
func Accept(listenID *C.struct_rdma_cm_id, cfg RDMAConnConfig) (*RDMAConn, ConnectInfo, error)

// WriteSlot 将序列化好的 packet 写入对端 req_ring 的 slotIdx 槽，并触发 doorbell
// data 已包含 SlotHeader（由调用方通过 SerializePacket 填充）
func (c *RDMAConn) WriteSlot(slotIdx int, data []byte) error

// PollSendCompletion 轮询 CQ，等待 WriteSlot 的 RDMA Write 完成
// 非阻塞，无完成时返回 0
func (c *RDMAConn) PollSendCompletion() (int, error)

// WriteRespSlot 将响应写入对端 resp_ring 的 slotIdx 槽（Server 侧调用）
func (c *RDMAConn) WriteRespSlot(slotIdx int, data []byte, clientRespRkey uint32, clientRespVA uint64) error

// PollRespDoorbell 轮询本端 resp_doorbell，检查 slotIdx 是否有新响应
// 非阻塞
func (c *RDMAConn) PollRespDoorbell(slotIdx int, expectedSeq uint32) bool

// RespSlotBytes 返回本端 resp_ring 中 slotIdx 的 slice（零拷贝读取响应）
func (c *RDMAConn) RespSlotBytes(slotIdx int) []byte

// NumSlots 返回 ring buffer slot 数量
func (c *RDMAConn) NumSlots() int

// Close 关闭连接，释放所有资源
func (c *RDMAConn) Close() error

type RDMAConnConfig struct {
    NumSlots     int
    SlotSize     int
    RespSlotSize int
}
```

**关键实现要求**：
- `WriteSlot` 必须是两次 `postSendWrite`：第一次写 slot 数据，第二次写 doorbell entry
- 两次 Write 之间**不需要等待第一次 completion**（RDMA 保证顺序），但 doorbell 的 WR 必须在 slot WR 之后 post
- `closed` 字段用 `atomic.CompareAndSwap` 保证 Close 幂等
- 错误路径必须释放已分配的内存（防止内存泄漏）

#### `util/rdma/pool.go`

**Build tag**: `//go:build linux && rdma`

```go
type RDMAPoolConfig struct {
    Device       string
    Port         int
    NumSlots     int
    SlotSize     int
    RespSlotSize int
    MaxConns     int
    IdleTimeout  time.Duration
}

type RDMAConnPool struct {
    mu    sync.RWMutex
    pools map[string]*singlePool  // key: "host:port"
    cfg   RDMAPoolConfig
}

type singlePool struct {
    mu      sync.Mutex
    idle    []*RDMAConn
    active  int
    maxConn int
}

func NewRDMAConnPool(cfg RDMAPoolConfig) (*RDMAConnPool, error)

// GetConnect 获取到 addr 的连接，无可用连接时新建
func (p *RDMAConnPool) GetConnect(addr string) (*RDMAConn, error)

// PutConnect 归还连接，forceClose=true 时直接关闭
func (p *RDMAConnPool) PutConnect(c *RDMAConn, forceClose bool)

// Close 关闭所有连接
func (p *RDMAConnPool) Close()
```

**关键实现要求**：
- `GetConnect` 超时应与 TCP 的 `ConnectPool.GetConnect` 保持一致（300ms）
- 连接断开检测：`PollSendCompletion` 返回错误时标记连接为 broken，`PutConnect` 时关闭
- 空闲连接超时回收（定时 goroutine）

#### `util/rdma/stub.go`（非 rdma build）

**Build tag**: `//go:build !(linux && rdma)`

```go
// 所有类型和函数的空实现，保证非 rdma build 时编译通过
type RDMAConnPool struct{}
type RDMAConn struct{}
type RDMAPoolConfig struct{}

func NewRDMAConnPool(cfg RDMAPoolConfig) (*RDMAConnPool, error) {
    return nil, errors.New("RDMA not supported in this build")
}
func (p *RDMAConnPool) GetConnect(addr string) (*RDMAConn, error) { return nil, nil }
func (p *RDMAConnPool) PutConnect(c *RDMAConn, forceClose bool)   {}
func (p *RDMAConnPool) Close()                                     {}
```

---

### 1.2 Phase 1 测试要求

#### 单元测试（`util/rdma/*_test.go`）

**handshake_test.go**：
- `TestMarshalConnectInfo`：序列化再反序列化，字段值不变
- `TestMarshalAcceptInfo`：同上
- `TestConnectInfoSize`：序列化后字节数 <= 56（rdma_cm 限制）

**slot_test.go**：
- `TestSlotHeaderRoundtrip`：WriteSlotHeader → ReadSlotHeader，所有字段正确
- `TestSlotHeaderMagicCheck`：magic 错误时 ReadSlotHeader 返回 error
- `TestDoorbellRoundtrip`：WriteDoorbellEntry → ReadDoorbellEntry，值一致
- `TestSerializeDeserializePacket`：构造包含 Arg 和 Data 的 proto.Packet，序列化再反序列化，所有字段一致

**mem_test.go**（需 RDMA 设备，可用 rxe）：
- `TestAllocFree`：分配后 Bytes() 长度正确，Free 后无 panic
- `TestSlotBytes`：SlotBytes(0, slotSize).len == slotSize，SlotBytes(n-1, slotSize) 不越界

#### 集成测试（`util/rdma/conn_test.go`，需 rxe）

**TestLoopback**（核心验收测试）：
```
1. 启动 goroutine A 监听（Accept）
2. goroutine B Dial 连接
3. B 构造 proto.Packet（含 Arg + 4KB Data），SerializePacket → WriteSlot → WriteRespSlot doorbell
4. A 轮询 doorbell 检测到写入，DeserializePacket，验证字段与原始 Packet 一致
5. A 写响应（WriteRespSlot），写 resp_doorbell
6. B 轮询 PollRespDoorbell，读取 RespSlotBytes，验证响应正确
```

#### 性能基准（`util/rdma/bench_test.go`，需 rxe）

```go
func BenchmarkWriteSlot4MB(b *testing.B)   // 目标: < 200μs/op（rxe 软件 RoCE 无此要求，硬件时验收）
func BenchmarkWriteSlot4KB(b *testing.B)   // 目标: < 10μs/op
func BenchmarkRoundTrip(b *testing.B)       // WriteSlot + PollRespDoorbell 往返，目标: < 20μs（rxe）
```

### 1.3 Phase 1 验收标准

- [ ] `go build -tags 'linux rdma' ./util/rdma/...` 编译通过
- [ ] `go build ./util/rdma/...`（无 tag）编译通过（stub）
- [ ] `TestLoopback` 通过（rxe 软件 RoCE）
- [ ] 所有单元测试通过，覆盖率 >= 80%
- [ ] `go vet -tags 'linux rdma' ./util/rdma/...` 无报错
- [ ] `valgrind --tool=memcheck`（或 `go test -race`）无内存错误

---

## Phase 2：DataNode 接收侧

### 2.1 文件清单与职责

#### `datanode/rdma_server.go`

**Build tag**: `//go:build linux && rdma`

```go
type RDMAServerConfig struct {
    Device       string
    Port         int
    NumSlots     int
    SlotSize     int
    RespSlotSize int
    SpinThreshold int  // doorbell 轮询忙等阈值（默认 10000）
}

// ConnState 维护单条 RDMA 连接的服务端状态
type ConnState struct {
    conn      *rdma.RDMAConn
    clientCI  rdma.ConnectInfo  // 客户端连接时传来的内存信息（用于写响应）
    pollCh    chan int           // doorbell 轮询协程通知槽索引
    cancelFn  context.CancelFunc
}

type DataNodeRDMACtx struct {
    cfg          RDMAServerConfig
    listenID     *C.struct_rdma_cm_id
    conns        sync.Map           // connKey → *ConnState
    handlePacket func(*proto.Packet, *ConnState) error  // 注入现有处理逻辑
    wg           sync.WaitGroup
    stopCh       chan struct{}
}

func NewDataNodeRDMACtx(cfg RDMAServerConfig, handlePacket func(*proto.Packet, *ConnState) error) (*DataNodeRDMACtx, error)

// Start 启动监听协程（非阻塞）
func (ctx *DataNodeRDMACtx) Start() error

// Stop 优雅停止，等待所有连接处理完毕
func (ctx *DataNodeRDMACtx) Stop()

// acceptLoop 持续接受新连接，每条连接启动 pollLoop goroutine
func (ctx *DataNodeRDMACtx) acceptLoop()

// pollLoop 每条连接一个 goroutine，轮询 doorbell
// 混合策略：先忙轮询 SpinThreshold 次，无请求则 runtime.Gosched()
func (cs *ConnState) pollLoop(ctx context.Context, rdmaCtx *DataNodeRDMACtx)

// sendResponse 将响应 packet 写回 Client 的 resp_ring
func (cs *ConnState) sendResponse(slotIdx int, seq uint32, resp *proto.Packet) error
```

**handlePacket 接入要求**：
- DataNode 现有的 `handleWritePacket` / `handleReadPacket` 等函数签名不变
- `DataNodeRDMACtx.handlePacket` 从 slot 反序列化出 `*proto.Packet` 后，直接调用现有处理函数
- 响应通过 `ConnState.sendResponse` 写回，而非 `conn.Write()`

#### `datanode/rdma_server_stub.go`

**Build tag**: `//go:build !(linux && rdma)`

```go
type DataNodeRDMACtx struct{}
type RDMAServerConfig struct{}

func NewDataNodeRDMACtx(cfg RDMAServerConfig, handlePacket func(*proto.Packet) error) (*DataNodeRDMACtx, error) {
    return nil, errors.New("RDMA not supported")
}
func (ctx *DataNodeRDMACtx) Start() error { return nil }
func (ctx *DataNodeRDMACtx) Stop()        {}
```

#### `datanode/server.go`（修改，约 25 行）

在 `initConnPool()` 函数中，仿照 smux 分支增加 RDMA 初始化：

```go
// 新增字段（DataNode struct）
rdmaCtx *DataNodeRDMACtx  // nil 表示未启用

// initConnPool() 末尾追加
if s.clusterInfo.RDMAEnable {
    cfg := RDMAServerConfig{
        Device:        s.clusterInfo.RDMADevice,
        Port:          s.clusterInfo.RDMAPort,
        NumSlots:      s.clusterInfo.RDMANumSlots,
        SlotSize:      s.clusterInfo.RDMASlotSize,
        RespSlotSize:  s.clusterInfo.RDMARespSlotSize,
        SpinThreshold: s.clusterInfo.RDMASpinThreshold,
    }
    s.rdmaCtx, err = NewDataNodeRDMACtx(cfg, s.dispatchRDMAPacket)
    if err != nil {
        return err
    }
    if err = s.rdmaCtx.Start(); err != nil {
        return err
    }
}
```

**配置项（DataNode config JSON 新增字段）**：

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

### 2.2 Phase 2 测试要求

#### 集成测试（`datanode/rdma_integration_test.go`，需 rxe）

**TestDataNodeRDMAWritePacket**：
```
1. 启动 DataNodeRDMACtx（mockHandlePacket 记录收到的 packet）
2. 使用 rdma.Dial 建立连接
3. 构造 Write 类型的 proto.Packet（含 ExtentID、Offset、Data）
4. SerializePacket → WriteSlot → 触发 doorbell
5. 等待 mockHandlePacket 被调用，验证 packet 字段与原始一致
6. 验证响应写回 Client 的 resp_ring
```

**TestDataNodeRDMAReadPacket**：类似，验证读请求路径

**TestDataNodeRDMAMultiConn**：
- 同时建立 4 条 RDMA 连接，并发写入，验证所有 packet 被正确处理，无数据混淆

**TestDataNodeRDMAConnDisconnect**：
- 建立连接，写入若干 packet，强制关闭连接，验证 DataNode 侧 goroutine 正常退出，无 goroutine leak

### 2.3 Phase 2 验收标准

- [ ] `datanode/rdma_server.go` 编译通过（带 rdma tag）
- [ ] `datanode/rdma_server_stub.go` 编译通过（不带 tag）
- [ ] `datanode/server.go` 修改不影响现有 TCP 路径（所有现有 datanode 测试通过）
- [ ] `TestDataNodeRDMAWritePacket` 通过，packet 字段 100% 一致
- [ ] `TestDataNodeRDMAMultiConn` 通过，4 条连接并发无数据混淆
- [ ] `TestDataNodeRDMAConnDisconnect` 通过，无 goroutine leak（用 `runtime.NumGoroutine()` 验证）
- [ ] `rdmaEnable=false` 时行为与修改前完全一致

---

## Phase 3：Client 写/读路径

### 3.1 文件修改清单

#### `sdk/data/stream/stream_conn.go`（修改，约 15 行）

```go
// 新增包级变量（rdma build tag 下）
var gRDMAPool *rdma.RDMAConnPool

// initRDMAPool 由 super.go 在启动时调用（rdmaEnable=true 时）
func initRDMAPool(cfg rdma.RDMAPoolConfig) error {
    var err error
    gRDMAPool, err = rdma.NewRDMAConnPool(cfg)
    return err
}

// getDataConn 替换现有 StreamConnPool.GetConnect 的调用点
// 返回值用 interface{ Write; ReadResponse; Close } 或分开处理
func getDataConn(addr string, useRDMA bool) (conn interface{}, isRDMA bool, err error) {
    if useRDMA && gRDMAPool != nil {
        c, err := gRDMAPool.GetConnect(addr)
        return c, true, err
    }
    c, err := StreamConnPool.GetConnect(addr)
    return c, false, err
}
```

**修改原则**：
- 现有 `sendToConn`、`WriteToConn`、`ReadFromConnWithVer` 函数签名**不变**
- RDMA 路径在调用方（`extent_handler.go`）判断，走不同分支，不混入现有函数

#### `sdk/data/stream/extent_handler.go`（修改，约 20 行）

在 `write()` 和 `read()` 方法中，在获取连接后增加 RDMA 分支：

```go
// 写路径（伪代码，保持现有 TCP 分支不变）
if rdmaEnabled {
    rdmaConn, _ := gRDMAPool.GetConnect(addr)
    slotIdx := int(p.ReqID % int64(rdmaConn.NumSlots()))
    data, _ := slot.SerializePacket(make([]byte, rdmaConn.SlotSize()), p)
    rdmaConn.WriteSlot(slotIdx, data)
    // 等待响应
    for !rdmaConn.PollRespDoorbell(slotIdx, expectedSeq) {
        runtime.Gosched()
    }
    resp, _ := slot.DeserializePacket(rdmaConn.RespSlotBytes(slotIdx))
    // 处理响应（与 TCP 路径相同逻辑）
} else {
    // 现有 TCP 路径，零改动
}
```

#### `client/fs/super.go`（修改，约 10 行）

在 `NewSuperBlock()` 中读取 RDMA 配置并初始化连接池：

```go
if cfg.RDMAEnable {
    stream.initRDMAPool(rdma.RDMAPoolConfig{
        Device:   cfg.RDMADevice,
        Port:     cfg.RDMAPort,
        NumSlots: cfg.RDMANumSlots,
        SlotSize: cfg.RDMASlotSize,
        MaxConns: cfg.RDMAMaxConns,
    })
}
```

**Client 配置项（新增）**：
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

### 3.2 Phase 3 测试要求

#### 端到端测试（需 rxe + DataNode Phase 2 完成）

**TestClientWriteRDMA**：
```
1. 启动 DataNodeRDMACtx（mock，直接返回成功响应）
2. 初始化 Client RDMA 连接池
3. 构造写请求 Packet（4MB Data），通过 RDMA 发送
4. 验证 mock DataNode 收到完整 packet，Data 内容一致
5. 验证 Client 正确解析响应，无错误
```

**TestClientReadRDMA**：
```
1. DataNode mock 在收到读请求后，RDMA Write 4MB 数据到 Client resp_ring
2. Client 轮询 resp_doorbell，从 resp_ring 读取数据
3. 验证数据内容与 mock DataNode 写入的内容一致
```

**TestClientFallbackToTCP**：
```
rdmaEnable=false 或 RDMA 连接失败时，自动使用 TCP 路径，验证功能正常
```

**TestClientRDMAvsТCP（性能对比）**：
```
相同环境下，4MB 写操作：
- TCP 延迟（baseline）
- RDMA 延迟（目标: < TCP * 0.3）
```

### 3.3 Phase 3 验收标准

- [ ] `sdk/data/stream/stream_conn.go` 改动不影响 TCP 路径（现有所有 Client 测试通过）
- [ ] `TestClientWriteRDMA` 通过，Data 内容 100% 一致
- [ ] `TestClientReadRDMA` 通过，Data 内容 100% 一致
- [ ] `TestClientFallbackToTCP` 通过（RDMA 不可用时自动降级）
- [ ] `rdmaEnable=false` 时与改动前行为完全一致
- [ ] 端到端写延迟（rxe 环境）< TCP 延迟（不要求硬件指标，但比例要合理）

---

## Phase 4：DataNode→DataNode 副本路径

### 4.1 文件修改清单

#### `datanode/repl/repl_protocol.go`（修改，约 15 行）

**改动原则**：沿用 `getSmuxConn` 已有的函数变量注入模式，不引入新的判断逻辑模式。

```go
// ReplProtocol struct 新增一个函数变量字段（与 getSmuxConn 平级）
// 默认 nil → 走 gConnPool.GetConnect（原有 TCP path，一行不改）
sendViaRDMA func(addr string, p *proto.Packet) error

// SetRDMA 由 rdma_server.go（build tagged）在 initConnPool 时注入
// 不带 rdma tag 时此函数不存在，sendViaRDMA 永远为 nil
func (rp *ReplProtocol) SetRDMA(f func(addr string, p *proto.Packet) error) {
    rp.sendViaRDMA = f
}
```

连接建立处的改动（仿照 `getSmuxConn` 的 nil 检查，约 8 行）：

```go
// 现有代码（不改动）：
if (p.IsMarkDeleteExtentOperation() || ...) && rp.getSmuxConn != nil {
    // smux path
} else {
    conn, err = gConnPool.GetConnect(addr)  // TCP path
}

// 新增 RDMA 分支（跟在 smux else 之后，与 smux 结构对称）
// 仅当 sendViaRDMA != nil 时才进入（rdmaEnable=false 时永远不进入）
if rp.sendViaRDMA != nil {
    return rp.sendViaRDMA(addr, p)
}
```

注意：`sendViaRDMA` 的实现完全在 `datanode/rdma_server.go`（build tagged）中，
`repl_protocol.go` 本身不 import `util/rdma`，不产生 CGO 依赖。

**初始化时机**：`datanode/server.go` 的 `initConnPool()` 末尾（rdmaEnable=true 时），
仿照 smux 的 `SetSmux(...)` 调用：

```go
if s.clusterInfo.RDMAEnable {
    // startRDMAServer 在 rdma_server.go（build tagged）中定义
    // 非 rdma build 时此函数为空操作（stub）
    rdmaForwardFn := startRDMAServer(cfg)
    // packetProcessor 是 ReplProtocol 实例
    packetProcessor.SetRDMA(rdmaForwardFn)
}
```

### 4.2 Phase 4 测试要求

**TestThreeReplicaWriteRDMA**（集成测试，需 3 个 DataNode mock）：
```
1. 启动 Leader DataNode（RDMA 启用）
2. 启动 2 个 Follower DataNode mock（RDMA 启用）
3. Client 发起三副本写请求
4. Leader 通过 RDMA 转发给两个 Follower
5. 验证：所有 Follower 收到的 packet 与原始一致
6. 验证：最终响应正确返回 Client
```

**BenchmarkThreeReplicaWrite**：
```
三副本写 4MB，串行（Leader→F1→F2）：
- 目标（rxe）: < TCP 延迟的 0.5x（即收益 > 2x）
- 目标（硬件 100GbE RoCEv2）: < 600μs
```

### 4.3 Phase 4 验收标准

- [ ] `datanode/repl/repl_protocol.go` 改动不影响 TCP/smux 路径（现有测试通过）
- [ ] `TestThreeReplicaWriteRDMA` 通过，数据无损
- [ ] 三副本写延迟（硬件环境）< 600μs（TCP baseline ~2.4ms）
- [ ] Follower RDMA 连接断开后，Leader 能重新建立连接并继续写入

---

## 全局验收标准

### 功能正确性

| 测试项 | 验收条件 |
|--------|---------|
| TCP 路径回归 | 所有现有测试（datanode、sdk/data）100% 通过 |
| RDMA 写入正确性 | Packet 字段、Arg、Data 字节级一致 |
| RDMA 读取正确性 | 读取数据与写入数据字节级一致 |
| 降级机制 | `rdmaEnable=false` 时行为与改动前完全一致 |
| 连接重建 | RDMA 连接断开后 < 1s 重连，业务重试成功 |
| 内存安全 | 72h 连续写入，内存占用稳定（无 MR leak） |

### 性能基线（硬件 100GbE RoCEv2，单流）

| 场景 | TCP 基线 | RDMA 目标 |
|------|---------|----------|
| 4MB 单次写延迟 | ~800μs | < 200μs |
| 三副本写总延迟 | ~2.4ms | < 600μs |
| DataNode CPU（10GB/s 写） | ~70% | < 30% |
| 连接建立时间 | < 1ms | < 2ms |

### 代码质量

- [ ] 所有新增文件带正确 build tag（`//go:build linux && rdma`）
- [ ] stub 文件覆盖所有公开符号，非 rdma build 下无编译错误
- [ ] `go vet` 和 `staticcheck` 无报错
- [ ] CGO 调用无内存泄漏（C.malloc 与 C.free 配对，ibv_reg_mr 与 ibv_dereg_mr 配对）
- [ ] 所有 goroutine 在 Stop/Close 后退出（无 goroutine leak）

---

## 开发注意事项

### CGO 内存安全规则

1. **不传 Go 内存指针给 C**：`ibv_reg_mr` 的 buf 参数必须是 `C.malloc` 分配的地址
2. **C 字符串释放**：`C.CString` 之后必须 `defer C.free(unsafe.Pointer(cs))`
3. **ibv_poll_cq 批量轮询**：每次轮询 `MAX_POLL_WC=16` 个 WC，减少 CGO 调用频率
4. **错误码转换**：C 函数返回 errno 时用 `syscall.Errno(errno).Error()` 转为 Go error

### 并发安全规则

1. **每个 slot 同一时刻只有一个 goroutine 使用**：Client 端通过 `ReqID % NumSlots` 确定 slotIdx，同一 slotIdx 不并发（由调用方保证）
2. **ConnState.pollCh 缓冲大小 = NumSlots**：防止 pollLoop 阻塞
3. **RDMAConnPool.GetConnect 超时**：300ms，与 ConnectPool 一致

### 测试环境搭建

```bash
# 安装软件 RoCE（无硬件时用于 CI）
modprobe rdma_rxe
rdma link add rxe0 type rxe netdev lo

# 验证
rdma link show
# 期望输出: link rxe0/1 state ACTIVE physical_state LINK_UP

# 运行 Phase 1 测试
go test -tags 'linux rdma' -v ./util/rdma/... -run TestLoopback

# 运行所有 RDMA 测试（带 CI tag 跳过硬件相关测试）
go test -tags 'linux rdma' -v ./... -run RDMA
```
