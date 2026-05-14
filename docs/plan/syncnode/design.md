# syncnode 设计与开发规约

> **背景**：本服务作为**三级存储中转**核心，承接 GPFS（训练/推理热存储）↔ CubeFS（中温存储 / eval & 轻量推理）↔ BOS/TOS（冷归档）三层之间的数据移动。`cfs-sync` CLI 的同步能力被服务化、规则化、纳入 CubeFS 集群管理面，作为 lcnode 之外的新 role。
>
> **本文档用法**：作为 SDD (Specification-Driven Development) 规约。
> - **每个章节是 contract**——描述"应该是什么样"，不是"为什么这样设计"（设计取舍单独放 §10）
> - **§9 实施分期中每个子项含可执行 AC**（unit test / integration test / 手动 demo）——开发完成的判定标准
> - **§16 列出 P0 开发前需 unblock 的 TBD 项**，其他都是确定要做的

---

## 0. 设计决议表（开发前已确定）

| # | 决议 | 章节 |
|---|---|---|
| D1 | 服务启动通过 cfs-server `-c sync.json` 加 role 启动，role 字符串 = `sync`；包名仍称 syncnode；跟 lcnode 同构 | §3.2 |
| D2 | 监控经 `util/exporter` 出 Prometheus，三层切分（节点 / 任务 / backend），含告警阈值 + 看板建议 | §13 |
| D3 | rule 的 src/dst 用分字段 `{kind, ...}` 而非 URI string | §4.2 |
| D4 | Master 调度算法 = 纯负载均衡 + load score 公式；P0 单节点不启用，P1 启用 | §6.3 |
| D5 | `local` 是唯一 POSIX kind，覆盖普通本地盘 + 宿主挂载并行 FS（GPFS/Lustre/WekaFS/BeeGFS），差异通过可选字段（buffer/concurrency/directIO/fadvise）调，不为单一 FS 类型写专门代码 | §3.4 |
| D6 | TOS/BOS/OSS/COS 通过 `Backend` interface + registry 扩展，P0 用 s3 adapter 兼容（自定义 endpoint），P2 按需加 native adapter | §10.6 |
| D7 | HTTP API 鉴权 P0 不做（内部网络），AuthMiddleware hook 必须就位 | §5.1.1 |
| D8 | Master 是**现有 CubeFS Master**（与 datanode/metanode/lcnode 共用），不引入新 master 集群 | §2 / §6.0 |
| D9 | 限流四层（per-task / per-rule / per-node / per-backend），实际速率 = min(四层) | §12.4 |
| D10 | 多 syncnode 并发同一任务（fan-out）：P1 文件级 sharding，P2 字节范围 sharding | §6.4 |
| D11 | 支持"跳过 CubeFS 中间层"对偶 W4 / W6（GPFS↔ 对象存储直传），无新代码——`kind: local` ↔ `kind: s3` 的双向配置即可 | §1.2.1 / §1.2.4 |

---

## 1. 目标 & 不做的事

### 1.1 核心场景：三级存储中转

```
  ┌──────────────────────────────────────────────────────────┐
  │  HOT  (宿主挂载的 POSIX 路径) — GPFS / WekaFS / Lustre /  │
  │       BeeGFS / 本地盘，下文统称 "GPFS"                     │
  │  ─── 训练 + 高性能推理在这里跑                            │
  │      产出 model.pt / ckpt-step-N.pt / 中间结果            │
  └──────────────────────────────────────────────────────────┘
            │ syncnode 推                  ↑ syncnode 拉
            ↓ (model promotion)         (reload old model)
  ┌──────────────────────────────────────────────────────────┐
  │  WARM (CubeFS) — 集群内可大规模访问的中温层               │
  │  ─── eval / 轻量推理 / 数据集准备 直接读 CubeFS           │
  │      由 syncnode 跟 GPFS / 对象存储 双向同步              │
  └──────────────────────────────────────────────────────────┘
            │ syncnode 推                  ↑ syncnode 拉
            ↓ (cold archive)            (warm up old ckpt)
  ┌──────────────────────────────────────────────────────────┐
  │  COLD (对象存储) — BOS / TOS / S3 / 自建 MinIO            │
  │  ─── 长期归档、跨集群共享、超出 CubeFS 容量预算            │
  └──────────────────────────────────────────────────────────┘
```

> **术语约定**：本文档说"GPFS"时**泛指任何通过宿主挂载、能从 syncnode/cfs-sync 进程 POSIX 访问的文件系统**——包括但不限于 GPFS / Lustre / WekaFS / BeeGFS / 本地盘 / 任何 K8s `hostPath` 挂进来的目录。syncnode 通过统一的 **`kind: local`** backend 接入，不为某个具体 FS 写专门代码（差异只在 buffer size / 并发度 / fadvise 等可选调优字段）。

**syncnode 是这三层之间唯一的"搬运工"**，由规则 + HTTP API 驱动。

### 1.2 六个典型工作流（P0 都要覆盖）

| 编号 | 流向 | 触发 | 用例 |
|---|---|---|---|
| **W1** | GPFS → CubeFS | 定时 + 显式 API | 训练产出的 model / ckpt 提升到 CubeFS，给 eval / 轻量推理用 |
| **W2** | CubeFS → BOS/TOS | 定时 + retention | 旧 ckpt 归档；CubeFS 容量周转 |
| **W3** | BOS/TOS → CubeFS | 显式 API | 反向加载旧模型给 eval / A/B 测试 / 回滚 |
| **W4** | **GPFS → BOS/TOS（直传，跳过 CubeFS）** | 定时 + 显式 API + **cfs-sync CLI**（ad-hoc）| 见 §1.2.1 |
| **W5** | 外部数据集（BOS/TOS / GPFS）→ CubeFS | 显式 API | 准备训练数据集；让 eval 集群读取 |
| **W6** | **BOS/TOS → GPFS（反向直传，跳过 CubeFS）** | 显式 API + **cfs-sync CLI**（ad-hoc）| 见 §1.2.4 |

**注**：有些场景 CubeFS 性能够用就不上 GPFS——比如部分 eval 任务直接读 CubeFS 而无需先去 GPFS。这种情况 syncnode 只参与 W2/W3 流，W1/W4/W6 不会发生。

**W4 / W6 是对偶**：W4 是 hot→cold 直传（GPFS→对象存储），W6 是 cold→hot 反向直传（对象存储→GPFS）。两者都"跳过 CubeFS 中间层"，使用场景互补。

#### 1.2.1 W4 详解：跳过 CubeFS 中间层

**典型用例**：

- 训练日志 / TensorBoard 事件 / 实验记录文件——只为归档审计，**没有任何 eval 或推理需要读它们**
- 大量训练中间结果（checkpoint 早期版本、梯度可视化、超参搜索过程）——单次产出后基本不读
- 数据预处理产物——已经进了 CubeFS 给 eval 用，GPFS 上的原始副本可以直接归档
- 临时数据集快照——只需要做版本备份给将来回放

**跳过 CubeFS 的好处**：

- **省 CubeFS 容量**：典型一次训练 100-500 GB 中间产物，全进 CubeFS 容量周转压力大
- **省一次传输**：W1+W2 = 数据被传两次；W4 = 一次直接到冷层
- **职责清晰**：归档型数据本来就不该在 warm 层占位

#### 1.2.2 W4 的两种实现路径（都支持）

**路径 A：syncnode 规则**（用于定时 / 规模化场景）

```json
{
  "id": "w4-gpfs-direct-cold",
  "type": "sync",
  "schedule": "0 3 * * *",
  "src": { "kind": "local", "path": "/mnt/gpfs/runs/.../intermediate/" },
  "dst": { "kind": "s3", "bucket": "ckpt-archive", "prefix": "intermediate/" },
  "filter": { "minAge": "1d" },
  "afterCopy": "verify_then_delete_src"
}
```

跟所有 syncnode 规则一样：监控、retention、bandwidth limit、HTTP API 触发都有。

**路径 B：cfs-sync CLI**（用于 ad-hoc / 人工运维）

```bash
# 一次性归档某次实验的全部产物
cfs-sync sync /mnt/gpfs/runs/exp-42/ s3://ckpt-archive/exp-42/ \
    --master <none, 不需要 CubeFS>          \
    --include "*.pt,*.log,*.yaml,*.json"   \
    --max-age 7d
```

cfs-sync CLI **完全不依赖 CubeFS 集群**，可以在任何能挂 GPFS + reach 对象存储的机器上跑。不需要 syncnode 实例、不需要 master、不需要 cfs-server。

#### 1.2.3 何时用哪个

| 场景 | 推荐 |
|---|---|
| 定期同步（每天 / 每小时）| **syncnode 规则**（监控、告警、HA、retention 都内建）|
| 大规模批量同步（持续 N 小时、TB 级数据）| **syncnode 规则**（多实例并行、自动重试）|
| 一次性补归档某个目录 | **cfs-sync CLI**（5 分钟搞定，不用改配置）|
| 临时迁移 / 抢修 | **cfs-sync CLI**（命令式，立即可见效果）|
| 没装 CubeFS 集群的环境 | **cfs-sync CLI**（CLI 不依赖 master）|
| 训练流水线集成 | **syncnode HTTP API**（流程化，可状态查询）|

**两条路径共用底层 storage adapter**（`tool/cfs-sync/storage/`），行为对等：传同样的数据、同样的 md5 校验、同样的 multipart 上传——只是触发方式和监控生态不同。

#### 1.2.4 W6 详解：BOS/TOS → GPFS 反向直传

W4 的对偶：**数据从对象存储直接落到训练用的 GPFS，跳过 CubeFS 中间层**。

**典型用例**：

- **训练前预热大数据集到 GPFS**——比如 imagenet-21k、LAION-5B 这种 TB 级训练数据，从 BOS/TOS 直接拉到 GPFS 给训练进程读，不需要先进 CubeFS 再走第二跳
- **模型加载到训练 GPU**——从 BOS 上的 pretrained 模型库直接 load 到 GPFS，让训练框架的高吞吐 I/O 路径就能直接读
- **跨集群训练数据迁移**——A 集群的训练产物归到 BOS，B 集群训练前从 BOS 直接拉到 B 的 GPFS，CubeFS 中间层没必要
- **任务期临时数据集**——一次性训练用的数据，跑完即弃，没必要持久化到 CubeFS

**跳过 CubeFS 的好处**：

- **省一次传输**：W3+W1 反向（BOS→CubeFS→GPFS）= 数据被传两次；W6 = 一次直接到 hot 层
- **省 CubeFS 容量**：一次性 / 短期数据本来就不需要驻留中间层
- **省时间**：训练前数据准备阶段直接拉到训练能读的最高性能存储，少一段等待
- **bandwidth 走最直接的路径**：对象存储 → 计算节点宿主，不绕开

**两种实现路径（与 W4 完全对偶）**：

**路径 A：syncnode 规则**

```json
{
  "id": "w6-dataset-to-gpfs",
  "type": "load",
  "src": { "kind": "s3", "bucket": "datasets-cold", "prefix": "imagenet-21k/" },
  "dst": {
    "kind": "local",
    "path": "/mnt/gpfs/train-data/imagenet-21k/",
    "bufferSizeKiB": 16384,
    "concurrency": 8,
    "fadviseSequential": true
  },
  "downloadStrategy": "temp_rename",
  "bandwidthLimitMBps": 800
}
```

**路径 B：cfs-sync CLI**

```bash
# 训练流水线启动前手动拉取
cfs-sync sync s3://datasets-cold/imagenet-21k/ /mnt/gpfs/train-data/imagenet-21k/ \
    --include "*.tar,*.json,*.txt"        \
    --buffer-size-kib 16384               \
    --concurrency 8
```

**何时用 W6 vs W3+W1**：

| 数据特性 | 推荐 |
|---|---|
| 训练专用，不需要 eval 访问 | **W6**（直传，省一次拷贝）|
| 训练 + eval 都需要 | **W3+W1**（先落 CubeFS 给 eval，再促到 GPFS 给训练）|
| 一次性 / 短期使用 | **W6**（不污染 CubeFS）|
| 长期保留 + 多团队复用 | **W3+W1**（CubeFS 当共享层）|
| 数据规模 > CubeFS 单卷容量 | **W6**（CubeFS 装不下）|

#### 1.2.5 W4 + W6 = 完整的"跳过中间层"对偶

```
              GPFS  ←─── W6 (BOS→GPFS 直传) ───  BOS/TOS
                 │                                   │
                 └─── W4 (GPFS→BOS 直传) ──────────→ │

                 [CubeFS 不参与这两条 path]
```

两条 path 共享相同的实现：

- 同一个 syncnode 服务
- 同一对 backend (`local` ↔ `s3`)
- 同一套规则配置 schema
- 同一份 cfs-sync CLI 工具

P0 实现完成后**自动得到 W4 + W6**——它们就是 `kind: local` ↔ `kind: s3` 的双向 sync/load 配置，没有特殊代码。

### 1.3 目标（P0）

- 服务化部署：通过 `cfs-server -c sync.json` 启动（**role 字符串 = `sync`**，二进制/包名仍称 syncnode），注册到 Master，心跳上报
- 三类后端：`cfs`（CubeFS SDK） / `s3`（含 BOS/TOS 通过 S3-兼容 endpoint） / `local`（GPFS / WekaFS / Lustre 等任意 POSIX mount）
- 三类任务：`sync`（搬数据）/ `load`（拉数据）/ `check`（双向校验）
- 三种触发：规则定时 cron / 显式 HTTP API / Master 调度（多实例 P1）
- 多实例分布式调度（P1）：Master 负责把任务按 load score 分发到具体 syncnode
- 完整的 Prometheus 指标 + Grafana 看板 + 告警阈值

### 1.4 不做的事

- ❌ **透明 read-through cache**——客户端读冷数据自动从 S3 拉。需要 SDK + metanode 改造，性价比不划算（详见 §10.1）
- ❌ **改 proto / metanode**——不引入 `StorageClass_S3` 之类的元数据语义。文件在 CubeFS 里就是正常 SSD/HDD 文件
- ❌ **容量水位驱动 evict**——同上，按时间或规则 retain，不做 LRU
- ❌ **替换 lcnode**——lcnode 继续负责内部 lifecycle（SSD↔HDD↔BlobStore、snapshot 版本清理）；syncnode 只管"跨外部存储 / 跨文件系统"的同步

### 1.5 与现有组件的边界

| 模块 | 职责 |
|---|---|
| **GPFS / WekaFS / Lustre / 任何 hostPath 挂载的 POSIX 路径** | 训练 / 高性能推理热存储。挂载在 syncnode 宿主上，通过统一的 `kind: local` 接入。本文档中说"GPFS"即泛指此类，参见 §1.1 术语约定 |
| **lcnode** | CubeFS 内部 lifecycle：SSD↔HDD↔BlobStore，删除过期文件，snapshot 版本清理 |
| **syncnode**（新，role=sync） | 跨外部存储 / 跨文件系统同步：GPFS ↔ CubeFS ↔ 对象存储；含 W1/W2/W3/W5 主流场景 + W4 定时直传 |
| **cfs-sync** CLI | 人工运维工具：**ad-hoc 单次同步**（任意 backend 之间，含 W4 GPFS→对象存储直传）、benchmark、check。**不做 daemon 模式**——daemon 形态完全由 syncnode 承担。底层 storage adapter 与 syncnode 共用（`tool/cfs-sync/storage/`）。**不依赖 CubeFS 集群**：可以在任何能挂 GPFS + reach 对象存储的机器上跑 |

---

## 2. 架构

```
   [训练集群]              [eval / 轻量推理集群]            [运维 / 管控]
       │                          │                            │
       │ POSIX read/write         │ CubeFS SDK / FUSE         │ HTTP API
       │                          │ read                        │
       ↓                          ↓                            ↓
 ┌──────────┐              ┌──────────┐                  ┌──────────────────┐
 │  GPFS    │              │  CubeFS  │                  │  CubeFS Master   │
 │ (mount   │              │          │                  │  (现有，新增      │
 │  HOT)    │              │ (WARM)   │                  │   SyncNode 模块) │
 └────┬─────┘              └────┬─────┘                  └──┬───┬───────────┘
      │ POSIX                   │ CubeFS SDK                │   │
      │ (kind: local)           │ (kind: cfs)               │   │ 任务调度 / 心跳
      │                         │                           │   │ 节点注册
      └────────┬────────────────┘                           │   │
               │                                            │   │
               ↓                       ┌────────────────────┘   │
        ┌──────────────┐               │              ┌─────────┘
        │  syncnode    │ ←──heartbeat──┤              │
        │  node1       │               │              │
        └────┬─────────┘               └──heartbeat───┤
             │                                        │
             │ aws-sdk-go-v2                ┌─────────┤
             │                              │         ↓
             ↓                              │   ┌──────────┐
        ┌──────────┐                        │   │ syncnode │
        │  BOS/TOS │                        └──→│  node2   │
        │  (COLD)  │                            └──────────┘
        └──────────┘                                 ...
```

**重要说明**：图中 "CubeFS Master" 就是**现有的 CubeFS Master 集群**（Raft 多副本），跟 datanode / metanode / lcnode / flashnode **是同一个 Master**。本设计不引入新的 master 集群——只是在现有 master 上新增 SyncNode 节点表、新增 RPC handler、新增几个 opcode（详见 §6.0）。

**关键交互**：

- 训练 / 高性能推理 → 直接读写 GPFS（不经过 syncnode）
- eval / 轻量推理 → 直接读 CubeFS（不经过 syncnode）
- 运维 / 训练流水线 → 调 syncnode HTTP API 触发跨层移动
- syncnode → 通过 POSIX 操作 GPFS（host mount），通过 SDK 读写 CubeFS，通过 S3 协议读写 BOS/TOS
- Master（现有 CubeFS Master）→ 节点注册 + 心跳 + 多实例任务派发（P1）

**用户视角**：

- 训练 / 推理 / eval 工程师**不感知 syncnode 存在**——只关心数据在哪一层、目标层是不是有
- 运维 / 数据工程师配置 sync 规则、调 API 触发显式搬运、查任务状态
- syncnode 是后台中转，不在数据读写的关键路径上

### 2.1 部署拓扑

```
              ───────  Zone A  ──────────────
              │                              │
              │  ┌─────────────┐              │
              │  │  GPFS NSD   │              │
              │  └──────┬──────┘              │       ──── Zone B ────
              │         │ POSIX mount         │       │              │
              │  ┌──────▼──────┐              │       │  GPFS / CubeFS│
              │  │ syncnode-A1 ├──────────┐   │       │  + syncnode   │
              │  │ syncnode-A2 │ heartbeat│   │       │   pool        │
              │  │     ...     │          │   │       │   ...         │
              │  └──────┬──────┘          │   │       │              │
              │         │ HTTPS           │   │       └──────┬───────┘
              │         ▼                 │   │              │
              │  ┌─────────────┐          │   │              │
              │  │  BOS / TOS  │          │   │              │
              │  └─────────────┘          │   │              │
              │                           │   │              │
              │  ┌─────────────┐          │   │              │
              │  │   CubeFS    │←─────────┤   │              │
              │  │ meta + data │          │   │              │
              │  └─────────────┘          │   │              │
              └───────────────────────────┼───┘              │
                                          │                  │
                                          ▼                  ▼
                                   ┌────────────────────────────┐
                                   │  现有 CubeFS Master         │
                                   │  (Raft 多副本，与 datanode  │
                                   │   /metanode/lcnode 共用)    │
                                   │                            │
                                   │  - 节点注册 + 心跳收集       │
                                   │  - 任务调度（P1+）           │
                                   │  - 全局规则存储（P1+）        │
                                   └────────────────────────────┘
```

**部署单位 = zone**：一个 zone 内有它自己的 GPFS / CubeFS 实例 / 对象存储 endpoint。zone 之间不互访，互不依赖。

**每 zone 最少 2 个 syncnode 实例**（HA），实例之间无状态交互——故障转移完全由 Master 协调。**注意**：Master 不是 syncnode 私有，而是**现有 CubeFS Master**（同集群唯一），新增 SyncNode 模块挂在其下。

**单 syncnode 实例规格**（生产推荐）：

| 项 | 建议 |
|---|---|
| CPU | 8 核（4 核 task executor + 1 核 net + 余量） |
| 内存 | 16 GiB（multipart buffer + BoltDB cache + Go runtime） |
| NIC | 25 Gbps（单 task 多 part 并行能跑 ~2 GB/s）|
| 本地盘 | 100 GiB SSD（BoltDB + 临时下载文件区域，临时文件最坏占 in-flight 任务总大小）|

**网络可达性要求**：
- ✓ Master HTTP 端口（17010 或同等）
- ✓ Zone 内 CubeFS metanode + datanode（17210/17310 等）
- ✓ Zone 内 GPFS 挂载（kernel POSIX）
- ✓ 同 region 的对象存储 endpoint（HTTPS）
- ✗ 不需要跨 zone 通信
- ✗ 不需要互联 syncnode 实例

**部署形态**：

| 形态 | 说明 |
|---|---|
| 裸机 / VM | binary + systemd unit，挂 GPFS 在主机上 |
| K8s DaemonSet | 每个 GPFS 客户端节点一个 pod；用 hostPath 把 /mnt/gpfs 挂进去 |
| K8s Deployment | 几个独立 pod，每个用 CSI driver 挂 GPFS |

**P0 不做跨 zone / 跨集群同步**——一个 CubeFS 集群对应一组 syncnode pool；rules 的 src/dst endpoint 都默认在同集群可达范围内。需要跨 zone 转数据的，按"两套独立 syncnode + 共享 S3/BOS bucket"配置即可（每边各自 sync 到/从同一 bucket），不需要 syncnode 实现跨集群协议。

---

## 3. 组件分解

### 3.1 进程结构（一个 syncnode 实例）

```
syncnode process
  ├── master client (registers, heartbeats)
  ├── TCP server (port: listen)
  │     └── handles Master-dispatched task requests (proto.Packet)
  ├── HTTP server (port: httpListen)
  │     └── admin API (CubeFS-style URL + response envelope)
  ├── scheduler
  │     └── cron-loop for rule-driven tasks
  ├── task executor pool
  │     ├── SyncTask (cfs → object-store / local)
  │     ├── LoadTask (object-store / local → cfs)
  │     └── CheckTask (verify integrity)
  ├── object-store adapter pool
  │     ├── S3Adapter        (aws-sdk-go-v2; works for AWS / MinIO / S3-compatible)
  │     ├── TOSAdapter       (P2，预留)
  │     ├── BOSAdapter       (P2，预留)
  │     └── ... (按需扩展)
  ├── posix adapter
  │     └── 通用本地文件系统 + 宿主挂载的并行文件系统 (Lustre/GPFS/WekaFS/...)
  ├── rule store (loaded from config + master pull)
  └── state store (BoltDB local: task history, in-progress state)
```

### 3.2 服务启动（role-based, 与 lcnode 完全同构）

`syncnode` 作为 cfs-server 的一个 role 启动，**不是独立 binary**。运维体验跟 `lcnode` / `datanode` / `metanode` 完全一致。

#### cmd/cmd.go 扩展（必做）

```go
const (
    // 已有：
    // RoleLifeCycle = "lcnode"
    // RoleData      = "datanode"
    // ...
    RoleSync        = "sync"              // 新增（role string 用单词 "sync"，与 lcnode/datanode 命名风格略不同；意图是 role 表示动作，不是节点类型）
    ModuleSync      = "sync"              // 新增
)

// switch role:
case RoleSync:
    server = syncnode.NewServer()
    module = ModuleSync
```

#### 启动命令

```bash
# 跟 lcnode 一模一样的启动方式
cfs-server -c /etc/cubefs/sync.json
```

#### 配置文件最小骨架（`sync.json`）

```json
{
  "role": "sync",
  "listen": "17910",
  "httpListen": "17911",
  "masterAddr": "10.0.0.1:17010,10.0.0.2:17010",
  "logDir": "/cfs/log/syncnode",
  "logLevel": "info",
  "dataDir": "/cfs/data/syncnode",
  "warnLogDir": "/cfs/log/syncnode/warn",
  "exporterPort": 17912,
  ...
}
```

字段含义：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `role` | string | 是 | 固定 `"sync"`，由 cfs-server 读取派发 |
| `listen` | string | 是 | TCP 端口，处理 Master 下发的 task 包 |
| `httpListen` | string | 是 | HTTP 管理 API 端口 |
| `masterAddr` | string | 是 | 逗号分隔的 master 地址列表 |
| `logDir` | string | 是 | 标准日志目录 |
| `dataDir` | string | 是 | BoltDB + 临时下载文件目录（必须可写持久卷）|
| `exporterPort` | int | 否 | Prometheus 指标 endpoint 端口（默认与 httpListen 同）|
| `s3Defaults` / `concurrency` / `rules` | 见 §4 | — | 业务配置 |

#### 注册流程（镜像 lcnode）

```
1. cfs-server 解析 role → 创建 syncnode.NewServer()
2. server.Start(cfg):
   2.1 parseConfig
   2.2 mc = master.NewMasterClient(masters)
   2.3 ci = mc.AdminAPI().GetClusterInfo()
        → 拿到 cluster ID + 本机 IP
   2.4 mc.NodeAPI().AddSyncNode(localAddr)
        → master 持久化节点（新 RPC，见 §6）
        → 返回 nodeID
   2.5 启动 TCP listener + HTTP admin server + scheduler + executor pool
   2.6 启动心跳 goroutine：每 10s 上报 OpSyncNodeHeartbeat
3. 收到 SIGTERM:
   3.1 停止接收新任务（HTTP API 返回 503，scheduler 暂停）
   3.2 等待 in-flight 任务完成 (max graceful_shutdown_seconds, 默认 60s)
   3.3 超时 → cancel 任务，记录 interrupted 状态供下次恢复
   3.4 close BoltDB, deregister from master
   3.5 exit 0
```

### 3.3 任务模型

每个任务有唯一 `task_id`（uuid），属于以下三类。注意"方向"是 backend kind 维度的，不绑定具体的层（hot/warm/cold），同一类型的任务能搬任意两层之间：

| 类型 | 方向 | §1.2 工作流映射 |
|---|---|---|
| **Sync** | 任意 src kind → 任意 dst kind（典型是从更热的搬到更冷的）| W1 (GPFS→CubeFS) / W2 (CubeFS→BOS/TOS) / W4 (GPFS→BOS/TOS 直传) |
| **Load** | 任意 src kind → 任意 dst kind（典型是从更冷的搬到更热的）| W3 (BOS/TOS→CubeFS) / W5 (外部→CubeFS) |
| **Check** | 双向校验，不传数据 | 完整性巡检；任意两层之间 |

**Sync vs Load 的实际区别**：协议层完全对称，只是默认行为不同——

- Sync 默认 `afterCopy: keep`（不删源）、`retention` 字段对 dst 生效
- Load 默认 `downloadStrategy: temp_rename`（原子落地）、对 dst 文件做强校验
- 两者都可以显式覆盖

任务来源：

- **Rule-triggered**：cron 触发，匹配规则后生成任务
- **API-triggered**：用户 POST /admin/sync/trigger 显式生成（W3/W4/W5 主要走这条）
- **Master-dispatched**：Master 调度分发（多实例场景下）

### 3.4 支持的 src / dst kind

每个规则的 `src` / `dst` 由 `kind` 字段区分后端。**所有 kind 通过统一的 `Backend` 接口注册**（见 §10.6），新增 provider 不影响 task executor 代码。

| kind | 说明 | P0 | P2 |
|---|---|:---:|:---:|
| `cfs` | CubeFS 卷，走 SDK（meta + extent client）| ✓ | |
| `s3` | AWS S3 / S3-兼容（aws-sdk-go-v2，含 MinIO、Ceph RGW、自建 S3 网关，以及通过 S3-compat endpoint 接 TOS/BOS/OSS/COS）| ✓ | |
| `local` | 本机 POSIX 路径——既可以是普通本地盘，也可以是**宿主上挂载的并行文件系统**（**GPFS** / Lustre / WekaFS / BeeGFS / Alluxio FUSE），从 syncnode 视角都是一个 mount path。本设计的核心场景下 GPFS 作为热存储通过该 kind 接入 | ✓ | |
| `tos` / `bos` / `oss` / `cos` | 各家原生 SDK | | ✓ |

#### `local` 配置项

POSIX 后端覆盖普通本地盘与宿主挂载的并行 FS 两种场景。差异通过**可选字段**调，不区分 kind：

```json
{
  "kind": "local",
  "path": "/mnt/gpfs/runs/",
  "bufferSizeKiB": 16384,    // 可选，缺省 4096（普通本地盘）。并行 FS 推荐 16384+
  "concurrency": 8,          // 可选，缺省 1。并行 FS 推荐 4-8
  "directIO": false,         // 可选，缺省 false。普通本地盘不开；并行 FS 大文件流式可考虑（仅 src 侧有效，CubeFS dst 不支持 O_DIRECT）
  "fadviseSequential": false // 可选，缺省 false。并行 FS 大文件可开 POSIX_FADV_SEQUENTIAL + DONTNEED 避免污染 page cache
}
```

#### 部署形态

- **裸机 / VM**：直接跑 binary，挂载文件系统在主机上
- **容器 / K8s**：syncnode pod 通过 `hostPath` volume 或 CSI driver 把宿主上的并行 FS 挂进来：
  ```yaml
  spec:
    containers:
    - name: syncnode
      volumeMounts:
      - name: gpfs
        mountPath: /mnt/gpfs
    volumes:
    - name: gpfs
      hostPath: { path: /mnt/gpfs, type: Directory }
  ```
- 规则配置：`src: { kind: local, path: /mnt/gpfs/runs/, bufferSizeKiB: 16384, concurrency: 8 }`

#### 安全注意

`local` 后端能任意读写宿主文件系统——生产部署必须：

- syncnode 进程跑 dedicated UID，**不允许 root**
- 规则配置在 syncnode 启动时**强制验证**：`path` 必须在 `posix.allowedRoots` 配置项白名单内
- 配置：
  ```json
  "posix": {
    "allowedRoots": ["/mnt/gpfs", "/mnt/lustre", "/mnt/weka", "/var/cfs-backup"],
    "maxDirDepth": 20
  }
  ```
- 路径解析后必须 `filepath.HasPrefix(resolved, allowedRoot)`，**包括 symlink target**（用 `filepath.EvalSymlinks` 解析后再检查），防止符号链接跨越

---

## 4. 规则配置

### 4.1 配置文件格式

`/etc/cubefs/syncnode.json`：

```json
{
  "role": "syncnode",
  "listen": "17910",
  "httpListen": "17911",
  "masterAddr": "10.0.0.1:17010,10.0.0.2:17010",
  "logDir": "/cfs/log/syncnode",
  "logLevel": "info",
  "dataDir": "/cfs/data/syncnode",

  "s3Defaults": {
    "endpoint": "https://s3.cn-north-1.amazonaws.com.cn",
    "region": "cn-north-1",
    "accessKeyEnv": "AWS_ACCESS_KEY_ID",
    "secretKeyEnv": "AWS_SECRET_ACCESS_KEY",
    "storageClass": "STANDARD_IA"
  },

  "posix": {
    "allowedRoots": ["/mnt/gpfs", "/mnt/lustre", "/mnt/weka", "/var/cfs-backup"],
    "maxDirDepth": 20,
    "defaultBufferSizeKiB": 4096
  },

  "concurrency": {
    "maxConcurrentTasks": 8,
    "transfersPerTask": 4,
    "bandwidthLimitMBps": 200
  },

  "rules": [
    // ────── W1: GPFS → CubeFS（训练产物提升到中温层）──────
    {
      "id": "w1-gpfs-to-cubefs-models",
      "type": "sync",
      "schedule": "*/15 * * * *",
      "src": {
        "kind": "local",
        "path": "/mnt/gpfs/runs/",
        "bufferSizeKiB": 16384,
        "concurrency": 8,
        "fadviseSequential": true
      },
      "dst": { "kind": "cfs", "vol": "warm-vol", "path": "/runs/" },
      "filter": {
        "include": ["*.pt", "*.safetensors", "*.bin", "*.ckpt", "*.json", "*.yaml"],
        "exclude": ["*.tmp", "*.partial"],
        "minSize": "1MB",
        "minAge": "60s"
      },
      "retention": {
        "pattern": "model-step-{N}.pt",
        "keepLast": 10
      },
      "afterCopy": "keep"
    },

    // ────── W2: CubeFS → BOS/TOS（中温归档到冷层）──────
    {
      "id": "w2-cubefs-to-cold-archive",
      "type": "sync",
      "schedule": "0 2 * * *",
      "src": { "kind": "cfs", "vol": "warm-vol", "path": "/runs/" },
      "dst": { "kind": "s3", "bucket": "ckpt-archive", "prefix": "runs/", "storageClass": "STANDARD_IA" },
      "filter": {
        "include": ["*.pt", "*.safetensors", "*.bin"],
        "minAge": "7d"
      },
      "retention": {
        "pattern": "model-step-{N}.pt",
        "keepLast": 30
      },
      "afterCopy": "verify_then_delete_src"
    },

    // ────── W3: BOS/TOS → CubeFS（反向加载旧模型给 eval）──────
    // 无 schedule，只能 HTTP API 触发
    {
      "id": "w3-cold-reload-on-demand",
      "type": "load",
      "src": { "kind": "s3", "bucket": "ckpt-archive", "prefix": "runs/" },
      "dst": { "kind": "cfs", "vol": "warm-vol", "path": "/restored/" },
      "downloadStrategy": "temp_rename"
    },

    // ────── W4: GPFS → BOS/TOS（不经过 CubeFS 的直接归档）──────
    // 适用于训练数据中间产物，不需要让 eval 集群访问，节省 CubeFS 容量
    {
      "id": "w4-gpfs-direct-cold",
      "type": "sync",
      "schedule": "0 3 * * *",
      "src": {
        "kind": "local",
        "path": "/mnt/gpfs/intermediate/",
        "bufferSizeKiB": 16384,
        "concurrency": 8
      },
      "dst": { "kind": "s3", "bucket": "ckpt-archive", "prefix": "intermediate/", "storageClass": "STANDARD_IA" },
      "filter": {
        "minSize": "10MB",
        "minAge": "1d"
      },
      "afterCopy": "verify_then_delete_src"
    },

    // ────── W5: 外部数据集 → CubeFS（让 eval 集群读）──────
    // 用例：把 imagenet-v2 等公共数据集从 BOS 拉到 CubeFS 共享访问
    {
      "id": "w5-dataset-import",
      "type": "load",
      "src": { "kind": "s3", "bucket": "datasets", "prefix": "imagenet-v2/" },
      "dst": { "kind": "cfs", "vol": "datasets-vol", "path": "/imagenet-v2/" },
      "downloadStrategy": "temp_rename"
    },

    // ────── W6: BOS/TOS → GPFS（反向直传，跳过 CubeFS）──────
    // 训练数据直接落到 GPFS 给训练进程读，不经过 CubeFS warm 层
    {
      "id": "w6-dataset-to-gpfs",
      "type": "load",
      "src": { "kind": "s3", "bucket": "datasets-cold", "prefix": "imagenet-21k/" },
      "dst": {
        "kind": "local",
        "path": "/mnt/gpfs/train-data/imagenet-21k/",
        "bufferSizeKiB": 16384,
        "concurrency": 8,
        "fadviseSequential": true
      },
      "downloadStrategy": "temp_rename",
      "bandwidthLimitMBps": 800
    },

    // ────── 完整性巡检：CubeFS ↔ BOS 对账 ──────
    {
      "id": "weekly-integrity-check",
      "type": "check",
      "schedule": "0 4 * * 1",
      "src": { "kind": "cfs", "vol": "warm-vol", "path": "/runs/" },
      "dst": { "kind": "s3", "bucket": "ckpt-archive", "prefix": "runs/" },
      "sampleStrategy": "least_recently_checked",
      "sampleRate": 0.05,
      "onMismatch": "alert"
    }
  ]
}
```

### 4.2 字段语义

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 规则唯一标识，HTTP API 引用用 |
| `type` | `sync` / `load` / `check` | 任务类型 |
| `schedule` | cron 表达式，可空 | 空 = 只能 API 触发 |
| `src` / `dst` | `{kind, ...}` | `kind` ∈ `cfs` / `s3` / `local` /（P2）`tos` / `bos` / `oss` / `cos` |
| `<endpoint>.kind` = `cfs` | `{vol, path}` | CubeFS 卷 + 子路径 |
| `<endpoint>.kind` = `s3` | `{bucket, prefix, endpoint?, region?, storageClass?}` | endpoint/region 缺省走 `s3Defaults`；通过自定义 endpoint 也能接 MinIO/TOS/BOS/OSS/COS 等 S3-兼容服务 |
| `<endpoint>.kind` = `local` | `{path, bufferSizeKiB?, concurrency?, directIO?, fadviseSequential?}` | 本机 POSIX 路径，**必须**在 `posix.allowedRoots` 之下。可调字段适用并行文件系统（Lustre/GPFS/WekaFS） |
| `<endpoint>.kind` = `tos` / `bos` / `oss` / `cos`（P2）| 各家 SDK 参数（access key、endpoint、bucket 等）| 配置 schema 复用 §10.6 Backend 抽象 |
| `filter.include` / `exclude` | `[]string` | glob 模式，include 优先 |
| `filter.minSize` / `maxSize` | `"<N><unit>"` | KB / MB / GB |
| `filter.minAge` / `maxAge` | duration | "60s" / "1h" / "7d" |
| `retention.pattern` | string | `{N}` 表示版本号占位符 |
| `retention.keepLast` | int | 保留最新 N 个匹配 |
| `retention.keepWithin` | duration | 保留时间窗内 |
| `afterCopy` | `keep` / `verify_then_delete_src` | 沉降后是否删源文件 |
| `downloadStrategy` | `temp_rename` / `direct` | 加载策略（默认 `temp_rename`）|
| `onMismatch` | `alert` / `auto_fix` / `ignore` | check 失败行为 |
| `bandwidthLimitMBps` | int (P0)| 第 1 层限流：单个 task 内部所有 transfer 总共享的字节速率上限。多节点并发 (parallelism > 1) 时是 task 的总配额，会被 sub-task 平摊。详见 §12.4 |
| `aggregateBandwidthLimitMBps` | int (P1+) | 第 2 层限流：同 rule 跨节点的多 task 共享的总速率上限。需 master 协调 |
| `parallelism` | int，默认 1 (P1+) | 多 syncnode 并发同一 task 的目标节点数。1 = 单节点（P0 默认行为）；> 1 = master 拆 sub-task 派多个节点。详见 §6.4 |
| `shardingStrategy` | `file` (P1) / `byte_range` (P2) / `auto` (P2)，默认 `file` | parallelism > 1 时如何拆 task。`file` = 按文件 hash；`byte_range` = 单文件按字节段拆 |

### 4.3 文件下载：临时文件 + rename

`downloadStrategy: temp_rename`（默认 + 推荐）：

```
1. 目标路径 /path/to/model.pt
2. 实际写到 /path/to/.model.pt.downloading.<task_id>
3. 流式写入 + 即时校验（边写边算 md5/etag）
4. 完成后：
   a. 校验通过 → rename 到 /path/to/model.pt（原子）
   b. 校验失败 → 删除临时文件，报错
5. 中断恢复：syncnode 重启时扫 dataDir 里 in-progress 任务，
   若临时文件存在 → 续传（用 Range GET 补差额）或重传（看大小）
```

`temp_rename` 保证用户在 CubeFS 里看到 `/path/to/model.pt` 时，**要么完全是新版本、要么完全是旧版本，绝不可能是半个**。

---

## 5. HTTP 管理 API

### 5.1 风格

遵循 CubeFS 既有的 `master/api_service.go` 风格：

- 路径前缀 `/admin/syncnode/...`
- GET 用于查询，POST 用于变更
- 响应统一封装：

```json
{
  "code": 0,
  "msg": "OK",
  "data": { ... }
}
```

错误响应 `code != 0`，HTTP 状态码与 code 对齐。

### 5.1.1 鉴权策略（P0 不做，但定义触发条件）

**P0 不引入 HTTP API 鉴权**——syncnode 部署在内部网络，HTTP 端口仅供集群内运维 / Master 调用，不暴露到外网。安全边界由网络层保证（VPC 内网、安全组、firewall）。

**触发引入鉴权的条件**（出现任一即必须做）：

1. syncnode HTTP 端口需暴露到非可信网段（如出 VPC、对接外部 K8s ingress）
2. multi-tenant 部署，不同租户的运维不能互相 trigger 对方的 vol 同步
3. 安全合规审计要求所有数据面写操作可追溯到具体用户

**预留的实现路径**（不阻塞 P0）：

- 中间件层抽象 `AuthMiddleware`，P0 是 no-op；P1+ 可插入 JWT / TLS client cert / 共享 token
- HTTP 路由统一过中间件，避免遗漏端点

### 5.2 端点清单

#### 服务自身

```
GET  /admin/syncnode/stat
     → 节点信息、任务并发数、当前 in-flight、CPU/MEM/带宽占用

GET  /admin/syncnode/version
     → binary 版本、配置摘要

POST /admin/syncnode/reload
     → SIGHUP 等价，重新加载 yaml 规则
```

#### 规则管理

```
GET  /admin/syncrule/list?vol=X
     → 所有规则及上次/下次执行时间

GET  /admin/syncrule/get?id=X
     → 单规则详情 + 最近 10 次执行历史

POST /admin/syncrule/create
     body: <Rule JSON>
     → 动态创建规则（持久化到 BoltDB）

POST /admin/syncrule/update?id=X
     body: <Rule JSON>
     → 更新已有规则

POST /admin/syncrule/delete?id=X
     → 删除规则（运行中的任务不会被取消，但下次不再调度）

POST /admin/syncrule/pause?id=X
POST /admin/syncrule/resume?id=X
     → 暂停 / 恢复定时执行（手动触发仍可用）
```

#### 任务触发与查询

```
POST /admin/sync/trigger
     body: {
       "type": "sync"|"load"|"check",
       "ruleId": "ckpt-backup-daily",        // 复用已有规则定义
       "overridePrefix": "/runs/exp-42/",    // 可选：缩小范围
       "wait": false                          // 默认异步
     }
     → { "taskId": "uuid", "queued" }

POST /admin/sync/save
     body: {
       "src": "cfs://train-vol/runs/exp-42/model-step-5000.pt",
       "dst": "s3://my-models/runs/exp-42/model-step-5000.pt",
       "wait": true,
       "timeout": "300s"
     }
     → wait=true 阻塞到完成，wait=false 立即返回 taskId

POST /admin/sync/load
     body: 同上反向

GET  /admin/sync/task/list?status=running&limit=50
     → 任务列表，可按状态/规则ID过滤

GET  /admin/sync/task/get?id=<taskId>
     → 任务详情:
        {
          "id": "uuid",
          "type": "sync",
          "ruleId": "ckpt-backup-daily",
          "status": "running",
          "progress": {
            "filesTotal": 42, "filesDone": 17,
            "bytesTotal": 1.2e10, "bytesDone": 4.3e9,
            "throughputMBps": 180
          },
          "verification": { "checked": 17, "mismatch": 0 },
          "startedAt": "...", "etaSeconds": 245,
          "error": null
        }

POST /admin/sync/task/cancel?id=<taskId>
     → 优雅取消（在两个文件之间停止，不中断单文件）

POST /admin/sync/task/retry?id=<taskId>
     → 重跑失败任务
```

#### 调试与监控

```
GET  /admin/syncnode/metrics
     → Prometheus 格式指标导出（可被 exporter 抓取）

GET  /admin/sync/preview
     body: {ruleId, dryRun: true}
     → 不执行，输出将会处理的文件列表（最多 1000 个），用于核对规则
```

---

## 6. Master 协议扩展

### 6.0 范围与边界

本章所有 RPC / opcode / Raft 持久化数据，**全部在现有 CubeFS Master 集群内扩展**——不引入新的 master 进程、不引入新的 Raft 组、不引入独立的服务发现。

**沿用 lcnode / flashnode 的扩展模式**：

| 项 | 做法 |
|---|---|
| HTTP RPC | 在现有 `master/api_service.go` 加 `/syncNode/add` 等路径 |
| TCP opcode | 在 `proto/packet.go` 新增 `OpSyncNodeHeartbeat` 等常量（B-1 任务负责勘察未占用值）|
| Raft 持久化 | 复用现有 master Raft 状态机，新加 SyncNodeInfo / SyncRule / SyncTask 三个 bucket |
| 心跳检查 goroutine | 在 `master/cluster.go` 加 `c.checkSyncNodeHeartbeat()`，与 `checkLcNodeHeartbeat()` 同构 |
| SDK 调用入口 | 在 `sdk/master` 新增 `NodeAPI().AddSyncNode()` 等方法 |

**预计 master 端代码量**：~600-1000 行（参考引入 lcnode 时的代码量量级）。

**为什么不开独立的 syncnode-Master**：

| 维度 | 复用现有 Master | 独立 syncnode-Master |
|---|---|---|
| 故障域 | 跟集群一起 HA（Raft 多副本兜底） | 新 HA 体系，运维负担 ×2 |
| 节点发现 | 共用 `GetClusterInfo` + 既有注册流程 | 需要自建 service discovery |
| 运维复杂度 | 不增加新 binary / 新 Raft 组 | 翻倍 |
| Raft 状态增量 | < 1 GB（即使 1000 节点 / 100 万任务历史，详见 §7.4）| — |
| 与现有节点协议一致性 | ✓ 跟 lcnode / flashnode 完全对齐 | ✗ 用户要学新一套 |

### 6.1 节点注册

完全镜像 lcnode 模式：

```go
// proto/admin_proto.go 增加
const (
    AddSyncNode    = "/syncNode/add"
    DecommissionSyncNode = "/syncNode/decommission"
)

// SDK master 增加方法
func (api *NodeAPI) AddSyncNode(addr string) (id uint64, err error)
```

Master 持久化 SyncNode 列表（类似现有 LcNode 表）。

### 6.2 心跳

新增 OpCode：

```go
const (
    OpSyncNodeHeartbeat = 0x???    // 选未占用的值
    OpSyncNodeRunTask   = 0x???
    OpSyncNodeCancelTask= 0x???
)
```

Master 周期检查 syncnode 心跳，断连 10 分钟视为下线，把它在跑的任务标 failed 并重新调度到其他节点。

### 6.3 任务调度（多实例场景，P1 才做）

**P0**：单 syncnode 实例，自己跑自己的规则。Master 只记录"哪个节点在线"，不做调度。

**P1**：多 syncnode 实例，**Master 做负载均衡分发**（非粘性）。

#### 6.3.1 节点负载评分

心跳 (OpSyncNodeHeartbeat) 上报字段：

```go
type SyncNodeHeartbeatReport struct {
    NodeID              uint64
    Addr                string

    // 容量信号
    ActiveTaskCount     int        // 当前在跑的 task 数
    MaxConcurrentTasks  int        // 配置上限
    InflightBytes       int64      // 在跑 task 累计已分配带宽 budget

    // 资源信号
    CPUPercent          float64    // syncnode 进程 CPU 占用 0-100
    MemRSSBytes         int64
    BandwidthMBpsUsed   float64    // 最近 30s 平均
    BandwidthMBpsLimit  float64    // 配置上限

    // 健康信号
    LastTaskFailureRate float64    // 最近 5min 失败率
    BoltDBHealthy       bool       // 状态库健康
}
```

Master 计算每个节点的 **load score**（越低越优先）：

```
load = 0.4 * (ActiveTaskCount / MaxConcurrentTasks)
     + 0.3 * (BandwidthMBpsUsed / BandwidthMBpsLimit)
     + 0.2 * (CPUPercent / 100)
     + 0.1 * (LastTaskFailureRate)        // 把失败率多的节点 deprioritize
```

参数都是 0-1 标量，权重和 = 1.0。心跳间隔 10s，load 在 master 内存里维护，**不**持久化（节点重启自然清零）。

#### 6.3.2 任务派发

新任务（来自 scheduler 或 API trigger）的派发流程：

```
1. Master 收到 task 创建请求（API 触发 / scheduler 触发）
2. 过滤候选节点：
   - heartbeat 在 30s 内
   - BoltDBHealthy == true
   - ActiveTaskCount < MaxConcurrentTasks
   - （未来）满足任务 nodeLabel selector（P1.5，按需）
3. 按 load score 升序排，取最低
4. 平局（同分±0.05）→ 取最久未派发到的节点（轮询避免 hot-spot）
5. 向目标节点发 OpSyncNodeRunTask
6. 节点 ack 后 master 把 task 写入元数据（status=running, owner=<node>）
7. 节点 nack（拒绝/排队满）→ 退回候选列表第 2 名，最多重试 3 次
```

#### 6.3.3 故障转移

节点心跳超时（30s 内无心跳）→ master 把它持有的所有 `status=running` 任务：

```
- 标 status=interrupted
- 清掉 owner 字段
- 重新进入派发队列
- 派发到新节点时，新节点检查 BoltDB 里没有对应任务历史 → 从头开始
  + 若 src/dst 路径上看到 .downloading.<task_id> 临时文件 → 清理后重传
  （这里是新节点 vs 旧节点，临时文件本来就在旧节点本地，新节点不会看到
   旧 task 的中间状态，所以总是从头跑）
```

**Note**：故障转移**不是断点续传**——因为 syncnode 的进度状态是节点本地的 BoltDB，新节点没有这个状态。我们选这个简化路径的理由：

- ckpt / model 同步天然以"一个完整文件"为粒度，从头传一个文件比"跨节点协调断点"简单 10 倍
- 故障转移频率本应很低（节点崩溃才发生）
- 节点本机崩溃后重启可以续传（本地 BoltDB 还在）

#### 6.3.4 任务取消

API `POST /admin/sync/task/cancel?id=X`：

```
1. 命中本节点 → 直接 cancel
2. 命中其他节点 owner → master 转发 OpSyncNodeCancelTask 给 owner
3. owner 收到后优雅取消（两个文件之间停止）+ 上报 status=cancelled
```

#### 6.3.5 何时引入 affinity / 标签

P1 只做 load-based 分发。如果未来出现以下场景再加 nodeLabel 机制：

- 跨 AZ：某些节点只能访问特定 AZ 的 S3 endpoint
- 跨网络：某些节点专门挂 Lustre，另一些挂 WekaFS
- 安全分区：训练敏感数据的任务只能跑在特定节点

### 6.4 多 syncnode 并发同一任务（fan-out 并行）

为了**快速同步**单个大任务（e.g. 一次 5 TB 的训练 ckpt 备份），可以把任务拆给多个 syncnode 并行处理。这是 P1+ 能力，P0 默认单节点。

#### 6.4.1 概念

```
普通任务（P0 默认）：
  task → 1 个 syncnode → 全部文件 / 字节串行 + 节点内并发 part

并发任务（P1+，rule.parallelism > 1）：
  task → master 拆成 N 个 sub-task → N 个 syncnode 同时跑 → 各自处理一份分片
                                              ↓
                                       master aggregate
                                              ↓
                                       task done / failed
```

#### 6.4.2 两种 sharding 策略

**A. 文件级 sharding（P1，简单）**

适用：源目录有**多个文件**（典型 ckpt 目录 / 数据集目录）

```
列出全部 N 个文件 → 按 hash(path) % parallelism 分到 sub-task
每个 sub-task 拿一份文件子集独立跑
```

- 实现复杂度：低（每个 sub-task 就是普通 task，处理子文件集）
- 失败粒度：sub-task 内单个文件失败 → 重试该文件；sub-task 整体失败 → master 重派
- 不适合：单个超大文件（一个文件只能在一个 sub-task 里跑）

**B. 字节范围 sharding（P2，复杂但通用）**

适用：**单个超大文件** TB 级，以及 A 解决不了的场景

```
单文件 → 切成 N 段（offset, length）→ 每个 sub-task 处理一段
```

- 上传方向 (S3)：master 先 `CreateMultipartUpload` 拿 uploadId →
  各 sub-task 用同一个 uploadId 上传不同 part 编号 →
  master 在所有 sub-task 完成后 `CompleteMultipartUpload`
- 下载方向 (S3 → CubeFS / GPFS)：各 sub-task `GetObject + Range` →
  写到目标的对应 offset（CubeFS ExtentClient.Write 支持 offset；POSIX `pwrite`）→
  全文 md5 在 master 端 aggregate 校验（每 sub-task 报告自己段的 md5，组合后跟源端 etag 比对）
- 实现复杂度：中高（coordinator 状态管理 + offset 一致性 + multipart upload lifecycle）

#### 6.4.3 协议：master 作为 coordinator

不引入 worker 之间的直连，所有协调走 master：

```
1. API trigger / scheduler 创建 task
   ↓
2. master 决定是否拆分（基于 rule.parallelism + 候选节点数）
   ↓
3. master:
   - 拆出 N 个 sub-task (sub_task_id = task_id + "/" + i)
   - 每个 sub-task 选一个 syncnode（按 load score）
   - 派发 OpSyncNodeRunTask（带 shard 信息）
   ↓
4. workers 并行执行，进度心跳报给 master
   ↓
5. 任一 sub-task 失败：
   - master 重新派给其他 node（最多 3 次重试）
   - 三次都失败 → entire task 失败 → cleanup (multipart abort 等)
   ↓
6. 所有 sub-task 成功 → master 做 finalization（multipart complete、合并 md5）→
   task done
```

#### 6.4.4 配置

```json
{
  "id": "fast-archive-large-ckpt",
  "type": "sync",
  "src": { "kind": "local", "path": "/mnt/gpfs/runs/big-exp/" },
  "dst": { "kind": "s3", "bucket": "ckpt-archive", "prefix": "big-exp/" },
  "parallelism": 4,                       // 期望并发节点数
  "shardingStrategy": "file"              // "file" (P1) | "byte_range" (P2) | "auto" (P2，按文件大小切换)
}
```

或 API 触发时显式指定：

```
POST /admin/sync/trigger
  body: { "ruleId": "...", "fanOut": 8, "shardingStrategy": "auto" }
```

#### 6.4.5 收益 / 代价

| 维度 | 单节点 | N 节点并发 |
|---|---|---|
| 吞吐（理想）| 1.5-2 GB/s (单 NIC) | N × 1.5-2 GB/s (受 backend / NIC 群上限约束) |
| 5 TB 文件耗时 | ≈ 45 min | N=4 → ≈ 12 min |
| Master 调度开销 | 1 派发 | N 派发 + N×心跳进度聚合 |
| 失败恢复 | 重传整文件 | 仅重传失败 sub-task 的文件 / 字节段 |
| 实现复杂度 | 基础 | 文件级（中）/ 字节级（高）|

**何时启用并发**：

- 默认 `parallelism: 1`（不并发）
- task 数据量 > X GB **且**候选 syncnode ≥ 2 时**自动建议** parallelism > 1（运维提示，不自动启）
- 用户 / 流水线代码显式指定（流水线知道这次是 5 TB 大单子）

#### 6.4.6 与 §12.4 限流的交互

并发不绕过限流。N 个 sub-task 跑在不同节点上：

- **第 1 层 Per-Task**：rule 配置 `bandwidthLimitMBps: 200`——这是**整个 task 的总配额**，被 N 个 sub-task 平摊（每个 200/N MB/s）。需要 master 在派发时把配额带给每个 sub-task
- **第 2 层 Per-Rule** (P1+)：跨节点全 rule cap，自然限制 fan-out 的总速率
- **第 3 层 Per-Node**：每个 sub-task 受所在节点的 node-level cap 限制（与同节点其他 task 共享配额）
- **第 4 层 Per-Backend**：跨节点全局对 S3 等共享 backend 的总流量约束

所以**并发不会突破限流**——它只是让"在限流允许的速度下"用满更多 NIC。

#### 6.4.7 P0 / P1 / P2 划分

| 能力 | 阶段 |
|---|---|
| 单节点单任务 | P0 |
| 单节点内多 part 并发 (transfersPerTask) | P0 |
| 多节点 + 文件级 sharding | P1 |
| 多节点 + 字节范围 sharding | P2 |
| 自动选择 sharding 策略（auto）| P2 |

---

## 7. 状态持久化

### 7.1 三层持久化模型

```
┌──────────────────────────────────────────────────────────────────────┐
│  集群级 — 现有 CubeFS Master (Raft 多副本，与 datanode/metanode 共用)  │
│  ─────────────────────────────────────                                │
│  • SyncNode 节点注册表 (addr, nodeID, zone, state, capabilities)       │
│  • (P1+) 规则定义全局存储 + 版本号                                      │
│  • (P1+) 任务全局账本 (task_id, owner, status)——只存 ownership 元    │
│    数据，不存进度详情                                                  │
│  • 持久化机制：Raft 日志 + 周期 snapshot；与现有 master 数据共享存储     │
│  • **本设计不引入新的 master 进程**，详见 §6.0                          │
└──────────────────────────────────────────────────────────────────────┘
            ↕ heartbeat 上报 / 任务派发命令
┌──────────────────────────────────────────────────────────────────────┐
│  节点级 — 每 syncnode 一个 BoltDB（`{dataDir}/syncnode.db`）            │
│  ─────────────────────────────────────                                │
│  • 本节点规则缓存（从 Master 拉，本地副本；P0 也可来自 config 文件）     │
│  • 本节点正在跑 + 已跑任务（详细进度、错误、verify 状态）                │
│  • 任务历史（TTL 7 天，过期滚动删）                                     │
│  • in_progress 断点信息（chunk offset、multipart uploadId 等）         │
│  • 持久化机制：每次 task 状态变更触发 fsync txn                         │
└──────────────────────────────────────────────────────────────────────┘
            ↕ 进程启停时载入 / 持久化
┌──────────────────────────────────────────────────────────────────────┐
│  进程内 — runtime memory（不持久化）                                    │
│  ─────────────────────────────────────                                │
│  • Backend client connection pool（重启重建）                          │
│  • Load score（重启从 heartbeat 重新累积）                              │
│  • HTTP request 处理状态                                                │
│  • In-flight goroutine                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

### 7.2 各层数据分类详表

| 数据类型 | 存放位置 | 是否跨节点共享 | 丢失影响 |
|---|---|---|---|
| SyncNode 节点列表 | Master Raft | 是 | Master 多数挂 → 集群暂停；单 master 挂 → Raft 切主，30s 内恢复 |
| 规则定义（P1+）| Master Raft | 是 | 同上 |
| 规则定义（P0）| 节点配置文件 + BoltDB cache | 否（每节点独立配） | 节点本地恢复；rules 不会跨节点丢 |
| 任务 ownership 元数据（P1+）| Master Raft | 是 | Master 失主 → 派发暂停；不影响在跑任务 |
| 任务详细进度 | 节点 BoltDB | **否** | 单节点 BoltDB 损坏 → 该节点正在跑的任务标 failed，需要从头重传（非跨节点续传，见 §6.3.3） |
| in_progress 断点（chunk offset / uploadId）| 节点 BoltDB | **否** | 同上 |
| 任务历史 | 节点 BoltDB (TTL 7d) | 否 | 丢失只影响审计，不影响功能；可通过 `/admin/sync/task/export` 提前导出长期存档 |
| Backend client pool | 进程内存 | 否 | 重启重建，无影响 |
| Load score | Master 进程内存 | 是（master 进程） | Master 重启后 syncnode 心跳重新上报，10s 重建 |

### 7.3 关键设计点

- **节点 BoltDB 不共享**：每个 syncnode 只看自己分到的任务。这样故障转移就是"换个节点从头跑"——简单可靠，详见 §6.3.3
- **Master 不存进度详情**：避免高频写 Raft（一个 task 进度可能每秒更新十几次）。Master 只在 task **状态变更**（pending→running→done/failed）时收到节点 push
- **进程内 cache 重启可恢复**：syncnode 设计成"重启即重建"，不依赖 in-memory state 的持久性

### 7.4 容量估算

#### Master 端新增数据量

| 数据 | 单条大小 | 1000 节点规模 | 备注 |
|---|---|---|---|
| 节点表 | ~200 B | 200 KB | 包含 addr / nodeID / 加入时间 / capabilities |
| 规则定义 (P1) | ~2 KB | 1000 规则 → 2 MB | 大型集群上限 |
| 任务 ownership (P1) | ~500 B | 100 万累计 → 500 MB | 滚动 TTL 7 天后删 |
| **Master 总增量** | — | **< 1 GB** | 跟现有 master 数据相比可忽略 |

#### syncnode 端 BoltDB 容量

| 数据 | 单节点稳态量 |
|---|---|
| 规则 cache | < 5 MB |
| 任务历史（7 天）| 10-100 MB（取决于任务频率）|
| in_progress 断点 | < 5 MB（典型 < 100 个 in-flight task）|
| **单节点总量** | **50-200 MB**，远小于 dataDir 配置（推荐 100 GiB SSD）|

### 7.5 崩溃恢复

#### 节点崩溃 → 重启
syncnode 启动时：

1. 加载 rules（从 config 文件 + 跟 master 同步）
2. 扫 `tasks_active` bucket：把 status=running 的任务标为 interrupted
3. 扫 `in_progress` bucket：找到有断点信息的任务，重新入队（带恢复点）——**仅本节点持有的任务**，本机重启续传
4. 扫 `dataDir/*.downloading.*` 临时文件：跟 in_progress 信息对账，没对上的孤儿文件清理
5. 启动 backend client pool（重新建 HTTP/S3 client）
6. 重新注册到 master + 开始心跳

#### Master 切主
syncnode 端：

1. heartbeat 失败 → 自动 retry 到新 leader（masterClient 内置）
2. 期间在跑的 task 不受影响（数据面与 master 解耦）
3. 新 leader 接管后 30s 内 syncnode 重新报到 + 上报 load score
4. 派发恢复

#### BoltDB 损坏（罕见）
- 通过启动时 BoltDB 自带的健康检查发现
- 处理：备份原文件 → 删除 → 重新初始化空 DB
- 影响：该节点任务历史 + 在跑断点全丢；进行中任务被标 failed，等 master 重新派发（P1）或从头重传（P0 重启即从头）
- 集群其他节点不受影响

### 7.6 备份与导出

- **Master 数据**：跟现有 master 的 Raft snapshot + 日志一起备份，无新机制
- **节点 BoltDB**：默认**不**做跨节点 backup（任务历史不是关键数据）；运维可通过 `/admin/sync/task/export` 定期导出长期存档（合规需要时）
- **临时下载文件**：在 dataDir 下，syncnode 启动时自动清理孤儿；不需要 backup

---

## 8. 核心数据流

### 8.1 Sync (CubeFS → S3)

```
1. Scheduler 触发 rule "ckpt-backup-daily"
2. ExtentClient.OpenStream(src.vol) + meta.ListDir
3. 对每个匹配 filter 的文件：
   a. compute s3 key  = dst.prefix + relative_path
   b. HEAD s3://...: 已存在且 etag 匹配 → skip
   c. PUT via s3manager.Uploader (multipart):
        reader = io.Pipe wrap ExtentClient.Read
        uploader uses MultipartUpload(>5MB) or PutObject(<=5MB)
   d. Verify: head s3 → compare etag with computed multipart-etag
   e. Persist task progress to BoltDB
4. If rule.retention.keepLast set:
   a. List s3 dst.prefix matching retention.pattern
   b. Sort by version number extracted from pattern
   c. Delete entries beyond keepLast
5. Update task status = done, write to history bucket
```

### 8.2 Load (S3 → CubeFS)

```
1. Scheduler / API 触发 rule "pretrained-loader"
2. List s3://bucket/prefix/ → keys
3. 对每个匹配 filter 的 key:
   a. HEAD s3 → size, etag
   b. Compute cfs dst path
   c. Stat cfs dst:
       - exists and size matches → skip (or re-verify if --force)
       - exists, mismatch → continue (will overwrite via temp)
   d. Open cfs temp file: dst + ".downloading." + task_id
   e. S3 GetObject → io.Pipe → ExtentClient.Write
      - parallel Range GET for files > 100MB (use cfs-sync prefetch_reader 反向变体)
      - 边写边算 md5
   f. Verify: computed md5 == s3 etag (or multipart-etag composite)
   g. Atomic rename: temp → final via metanode.Rename
   h. Persist progress
4. Update task done
```

### 8.3 Check (双向校验)

```
1. List src + dst recursively
2. Pair entries by relative path
3. For each pair:
   a. Mismatch reasons:
      - missing on dst (src-only)
      - missing on src (dst-only, perhaps stale)
      - size mismatch
      - etag/md5 mismatch (only if sampleRate selects this file — full md5
        compute is expensive on TB files)
4. Aggregate report
5. If onMismatch == "auto_fix":
   - generate sub-tasks to bring sides in sync
6. Write report to /admin/sync/task/<id>/report endpoint
```

---

## 9. 实施分期

### P0：单节点 syncnode + 基础同步（**6-7 周** = 32 个工作日）

P0 按 8 个阶段（Phase A-H）串行交付。每个子项含**可验收标准 (AC)**——必须通过对应测试或 demo 才能 close。

每个 AC 都是可执行的（unit test / integration test / 手动 demo），不靠"代码 review 觉得 OK"通过。

#### Phase A — 骨架与配置（5 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| A-1 | `cmd/cmd.go` 加 RoleSync=`"sync"` 派发 + syncnode 包骨架（`server.go`、`NewServer()`、Start/Shutdown/Sync 三方法）| 2 天 | `cfs-server -c sync.json` 启动成功；进程不退出；`/admin/syncnode/version` 返回非空 JSON |
| A-2 | 配置加载 + 完整 schema 校验（rule、posix、s3Defaults、concurrency 全部字段）| 2 天 | 写 8 个 negative 测试（cron 非法 / kind 非法 / path 不在 allowedRoots / S3 endpoint 缺 / retention pattern 没 `{N}` / minSize 单位错 / 必填缺失 / 类型错），每个返回**特定错误码 + 错误信息**；正面测试加载 design.md §4.1 完整示例无错 |
| A-3 | exporter 独立端口 + util/exporter 注册 + 节点级 gauge 上报 | 1 天 | `curl http://addr:17912/metrics` 返回 Prometheus 文本格式；包含 `cubefs_syncnode_up`、`uptime_seconds`、`concurrent_tasks` 三个 gauge |

#### Phase B — Master 协议 + 节点注册（6 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| B-1 | **确定** 3 个新 opcode 数值（`OpSyncNodeHeartbeat`、`OpSyncNodeRunTask`、`OpSyncNodeCancelTask`）→ grep 全部 opcode 找空位 → 在 `proto/packet.go` 加常量 | 0.5 天 | 新 opcode 在 0xXX-0xXX 范围内未占用；`go vet ./proto/...` 通过 |
| B-2 | Master 端：`AddSyncNode` API + 节点表持久化（参考 lcnode 现有实现 mirror）| 2 天 | `curl -X POST master:17010/syncNode/add?addr=...` 返回 nodeID；master leader 切换后 `ListSyncNodes` 返回相同列表 |
| B-3 | syncnode 端：register loop + 失败重试 + master leader fallback | 1.5 天 | 启动后 5 秒内注册成功；master 主从切换后 30 秒内重新注册到新 leader；master 全挂时 syncnode 不 panic 持续重试 |
| B-4 | 心跳（OpSyncNodeHeartbeat，间隔 10s，含 §6.3.1 全部字段）| 1 天 | master `/admin/clusterInfo` 显示 syncnode 状态 active；杀掉 syncnode 30 秒后状态变 inactive |
| B-5 | master 端：检测 syncnode 心跳超时 + 标记 inactive | 1 天 | 集成测试：杀 syncnode，60 秒内 master 标记 inactive；重启后状态恢复 active |

#### Phase C — Backend 抽象 + 三个 P0 后端（10 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| C-1 | `Backend` interface 定义 + registry + `(kind, endpoint, region)` 共享 client pool | 1 天 | 单元测试：同一三元组多次 `New()` 返回同一 client；不同三元组返回不同 client；Close 不会影响其他持有者 |
| C-2 | s3 backend：基于 aws-sdk-go-v2，含 multipart PUT (s3manager.Uploader)、range GET、List + paginate、Head、Delete | 3 天 | 黑盒测试套件（mock S3 + 实跑 MinIO）：上传 100 MB / 1 GB / 5 GB 文件各 1 个，etag 校验通过；range GET `[off, off+size)` 返回正确字节；List 1 万个 key 完整无丢失 |
| C-3 | s3 backend：multipart upload 残骸自启动清理（`ListMultipartUploads` 扫所有相关 bucket，超过 7 天的 abort）| 1 天 | 集成测试：手工制造 3 个 in-progress multipart upload，syncnode 启动后 60 秒内全部清理；指标 `cubefs_syncnode_backend_request_total{op="abort_multipart"}` 准确计数 |
| C-4 | local backend：std `os` 包 + `copyWithBuffer` + buffer/concurrency/directIO/fadvise 可选字段；**allowedRoots 强校验**（包括 EvalSymlinks 后） | 2 天 | 负面测试：用 symlink 跨越 allowedRoots 时 backend 创建失败；正面测试：4 KiB / 4 MiB / 16 MiB / 64 MiB 文件读写正确，directIO 开关下数据一致 |
| C-5 | cfs backend：复用 cfs-sync 的 storage/cfs_linux.go 读路径 + **重写**写路径以支持并发写多文件 | 2.5 天 | 跑：1 GB ckpt 写入 cfs（target 卷），单文件耗时 ≤ 8 秒（在测试集群上）；同时 10 个文件并发写无错；写完后 Flush + CloseStream 都成功 |
| C-6 | Backend 黑盒契约测试套（Put/Get/Head/List/Delete + 1 KB-5 GB 大小矩阵），s3 / local / cfs 三个 backend 都通过 | 0.5 天 | `go test ./internal/syncnode/backend/contract` 通过 3 个 backend 的同一套测试 |

#### Phase D — 任务执行器（10 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| D-1 | TaskExecutor 框架：任务上下文、取消、进度上报、超时 | 1 天 | 单元测试：cancel context 后 task 在 1 秒内退出；进度回调每秒至少 1 次 |
| D-2 | Sync 任务：列源 → filter → 跳过未变更 → 多 part 并发 PUT → etag 校验 | 3 天 | 集成测试：100 个 100 MB 文件 sync cfs→s3 全部成功；mtime 未变的文件第二次跑被跳过；etag 不匹配的文件触发告警；100 MB 文件单文件吞吐 ≥ 200 MB/s |
| D-3 | Load 任务：列源 → 跳过相同 → 多 part 并发 Range GET → temp file → 校验 → 原子 rename | 3 天 | 集成测试：5 GB ckpt load s3→cfs 成功，本地能看到目标文件；中途 `kill -9` syncnode 后重启，临时文件被清理（不存在半截文件）；rename 后目标文件 md5 与源 etag 一致 |
| D-4 | Check 任务：列两端 → 配对 → size/mtime/etag 对比 → sampleStrategy（random / oldest / largest 三种）| 2 天 | 制造 1000 文件，刻意改 10 个的 size，check 任务能 100% 检测出不一致；fullScan 配置下完整跑完 |
| D-5 | `onMismatch: auto_fix` 在 Check 发现不一致后自动调度 sync sub-task | 1 天 | 集成测试：刻意制造 size mismatch → check 触发 → sub-task 重传 → 二次 check 通过 |

#### Phase E — 控制面（5 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| E-1 | HTTP admin API 框架：路由 + 统一 response envelope + 错误码表 + AuthMiddleware（P0 no-op 但 hook 在位）| 1 天 | 所有端点过 middleware；errors 转换为 `{code, msg}` 格式；HTTP 状态码与 code 一致 |
| E-2 | 规则 CRUD：list / get / create / update / delete / pause / resume | 1.5 天 | 每个端点单元 + 集成测试；动态创建的规则持久化到 BoltDB，重启后还在 |
| E-3 | 任务触发与查询：trigger / save / load / list / get / cancel / retry | 1.5 天 | save / load `wait=true` 阻塞到完成；cancel 后 task 30 秒内退出 |
| E-4 | 规则冲突检测（启动 + reload 时）：prefix overlap 拒绝；循环 sync (A:cfs→s3 + B:s3→cfs 相同 path) 拒绝 | 1 天 | 负面测试：写 3 种冲突配置（同 src/dst 双规则、prefix overlap、循环 sync），全部启动失败 + 报具体冲突字段 |

#### Phase F — 调度与状态（5 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| F-1 | Cron scheduler：基于 robfig/cron，规则注册 / 解注册 / 启停 | 1 天 | 配置 `*/1 * * * *` 每分钟跑一次的 rule；测试观察连续 5 分钟触发 5 次（±1 秒） |
| F-2 | BoltDB 状态持久化：rules / tasks_active / tasks_history / in_progress 四个 bucket，启动恢复 | 2 天 | 集成测试：跑到任务 50% 进度 `kill -9` → 重启 → status=interrupted 任务可见 → BoltDB 健康检查通过 |
| F-3 | 配置 reload (SIGHUP)：先 validate 整体 → 通过才切换 → 失败保留旧配置 + 上报指标 | 1 天 | 集成测试：在跑 task 1 期间发 SIGHUP 改规则 2 配置；task 1 用旧配置完成；新 task 用新配置；改配置带语法错时 reload 失败，旧规则继续工作 |
| F-4 | 任务历史 TTL（默认 7 天）+ 导出 API (`GET /admin/sync/task/export`) | 1 天 | 老于 TTL 的任务自动从 active 移到 history；history 导出 jsonl 格式正确 |

#### Phase G — 运营硬化（5 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| G-1 | Retention 策略实现（pattern + keepLast / keepWithin），**仅 sync 全部成功后才执行**| 1.5 天 | 制造 7 个 `model-step-{N}.pt` 文件，keepLast=5；跑完后 S3 上正好剩 5 个最新的；如果 sync 中途失败，retention 不触发（仍是 7 个）|
| G-2 | bandwidth limiter（**四层限流**：第 1 层 per-task + 第 3 层 per-node + 第 4 层 per-backend 节点本地；reader/writer 套 multi-`rate.Limiter`，详见 §12.4）| 2 天 | 三个独立测试：(a) 仅配 node 1000 MB/s，跑 10 GB → 实测 [900, 1100]；(b) 仅配 task 200 MB/s，跑同样 → 实测 [180, 220]；(c) node 1000 + task 200 + backend 500 共存，多 task 并发 → 实际速率 = min 三者 |
| G-3 | vol 失联错误分类 + 规则降级机制：`vol_not_found` → 标 rule degraded + 告警；`transient_network` → 指数退避重试 | 1.5 天 | 集成测试：删一个 vol，相关规则 60 秒内标 degraded；恢复 vol 后规则可手动 resume |
| G-4 | metanode.Rename 原子性验证 + 必要时降级方案（验证不原子时改 Create new → Write → Sync → Unlink old）| 1 天 | 验证测试：1000 次 rename，每次随机中途 kill -9，重启后看到的目标文件**要么完全是新内容、要么完全是旧内容**（用文件 hash 判断）|

#### Phase H — 集成测试与文档（4 天）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| H-1 | **三级链路 TB 级稳定性测试**：模拟训练产物 GPFS → CubeFS → BOS/TOS 全链路；10 个 100 GB ckpt 连续跑 W1 + W2，再触发 W3 反向加载，全程 1 小时无错 | 2 天 | 测试报告：每段链路 throughput / p99 延迟 / 错误数 / orphan temps 数 / BoltDB 大小；md5 在三层之间 100% 一致 |
| H-2 | **端到端集成 demo**：Python 脚本模拟训练流水线（在 GPFS 写 ckpt → 调 W1 触发 → 等 CubeFS 可见 → eval 读 CubeFS → 调 W2 归档 → 7 天后调 W3 反向加载验证）| 1 天 | `python examples/three_tier_pipeline.py` 一键跑通；README 含运行步骤 |
| H-3 | 部署文档：单节点 K8s YAML / 裸机 systemd unit / `sync.json` 完整字段说明 | 1 天 | 同事按文档 30 分钟内能拉起一个本地测试实例 + 跑一次 demo |

### P0 里程碑

```
Week 1   Phase A + B 启动              骨架 + Master 注册起步     5+3=8 days
Week 2   Phase B 完成 + Phase C 启动    心跳 + S3 backend
Week 3   Phase C 完成 + Phase D 启动    三 backend 通过契约 + sync 跑通
Week 4   Phase D 完成                  sync + load + check 全部
Week 5   Phase E + Phase F             控制面 + 调度
Week 6   Phase G                        运营硬化
Week 7   Phase H + buffer              集成测试 + 文档 + 修长尾 bug
```

**P0 总计 32 个工作日 ≈ 6.5 周。单工程师 7 周；双工程师 4 周（Phase A+B / C+D 并行，Phase E-G 串行）。**

#### P0 交付物清单

- [ ] `cfs-server -c sync.json` 单节点能跑
- [ ] 三个 backend（cfs / s3 / local）通过契约测试
- [ ] sync / load / check 三类任务跑通
- [ ] HTTP admin API 全部 endpoint 有集成测试（含 AuthMiddleware no-op hook）
- [ ] BoltDB 状态持久化 + 崩溃恢复
- [ ] retention / bandwidth limit / 配置 reload / 规则冲突检测全部工作
- [ ] multipart 残骸自清理
- [ ] vol 失联降级机制
- [ ] metanode.Rename 原子性验证通过 / 降级方案上线
- [ ] 三级链路 TB 级稳定性测试报告（W1 + W2 + W3 串跑）
- [ ] 端到端三级流水线 demo（GPFS → CubeFS → BOS/TOS → 反向 reload）
- [ ] 部署文档

### P1：多实例分布式调度 + 并发任务 + 跨节点限流（**5 周** = 22 个工作日）

| ID | 工作 | 工期 | 可验收标准 |
|---|---|---|---|
| P1-1 | Master 节点列表 + load score 计算（§6.3.1 公式）| 2 天 | master 内存维护 syncnode load 表；查询接口返回每个节点的 load score；杀一个 syncnode 后该节点的 load 在 30s 内消失 |
| P1-2 | Master 派发器：低 load 优先，平局轮询；新 task 入口分发；nack 重试 | 3 天 | 集成测试：3 个 syncnode，触发 10 个 task，按 load 分布；某节点 nack 后回退到次优节点 |
| P1-3 | OpSyncNodeRunTask / CancelTask 协议 + syncnode 端 handler | 2 天 | 通过 master 触发的 task 在指定 syncnode 上跑；cancel 后 30s 内停 |
| P1-4 | 节点故障转移：syncnode 心跳超时后，在它身上的 task 标 interrupted + 重新派发 | 3 天 | 集成测试：杀 syncnode 节点 → 30s 内 master 检测 → task 在其他节点上从头重传完成 |
| P1-5 | Master 主从切换容忍：syncnode 端 retry，master 接管后重建节点列表 | 2 天 | 集成测试：杀 master leader → 新 leader 在 30s 内有完整 syncnode 列表 → 派发继续 |
| P1-6 | 多实例集成测试 + 性能基准（聚合吞吐、故障转移耗时分布、调度均衡性）| 2 天 | 测试报告：3 节点聚合吞吐 ≥ 单节点 × 2.5；故障转移 p99 ≤ 60s；load 标准差 ≤ 平均值 30% |
| P1-7 | **多节点并发任务 (fan-out)** — 文件级 sharding：master 拆 sub-task → N 节点并行 → 进度聚合 → 任一 sub-task 失败重派 | 4 天 | 集成测试：100 个 1 GB 文件 + `parallelism: 4` → master 拆成 4 个 sub-task 派到 4 节点 → 完成耗时约为单节点的 1/3.5 (考虑调度开销)；杀其中 1 节点 → 该 sub-task 转其他节点完成 |
| P1-8 | **第 2 层 Per-Rule 跨节点限流** — master 周期重算 rule 配额，心跳响应里把"本节点剩余配额"发回 | 2 天 | 集成测试：rule.aggregateBandwidthLimitMBps=400，3 节点跑该 rule task → 集群总速率收敛在 [360, 440] MB/s |
| P1-9 | **第 4 层 Per-Backend 跨节点限流** — 同 rule.dst.endpoint 的所有 backend 请求总额限制 | 2 天 | 集成测试：3 节点都打同一 s3 bucket，backend.s3.bandwidthLimitMBps=600 全局，每节点限 1000 → 实测总速率 ≤ 660 (±10%) |

**P1 总计 22 天 ≈ 5 周**（含原 14 天调度 + 4 天 fan-out 并发 + 4 天跨节点限流）。

### P2：增强特性（按需，每项独立可上线）

| ID | 候选项 | 说明 | 工期 |
|---|---|---|---|
| P2-A | TOS / BOS / OSS / COS 原生 adapter | 各家专有 SDK；P0 用 s3-compat endpoint 已能用，原生 adapter 仅在需要专有特性时做 | 3-5 天/个 |
| P2-B | Task chain | onSuccess / onFailure 触发其他 rule；用例：load 后自动 verify | 3 天 |
| P2-C | Notification 抽象 | Slack / 钉钉 / 飞书 / 企微 / Email / Webhook 多 sink，去重 + 限流 | 4 天 |
| P2-D | Python training SDK | `cubefs_sync.SyncClient`：save_ckpt / load_ckpt / verify，带 progress bar | 5 天 |
| P2-E | 增量对比策略 | 配置 `compare: size_mtime / etag / full_md5`，etag 比 size_mtime 更准 | 2 天 |
| P2-F | 加密上传 | S3 SSE-S3 / SSE-KMS / 客户端加密；KMS key 配置 | 3 天 |
| P2-G | 压缩 | gzip / zstd 传输前压缩，省 S3 流量和存储 | 3 天 |
| P2-H | HTTP API 鉴权 | 中间件实装；JWT / TLS client cert / 共享 token 三选一；触发条件见 §5.1.1 | 4 天 |
| P2-I | vol 白名单 | multi-tenant 集群限制 syncnode 实例能访问的 vol；详见 §16.2 | 2 天 |
| P2-J | 配置 lint 命令 | `cfs-sync rule lint sync.json` 静态检查 + 试连接 + dry-run | 2 天 |
| P2-K | 一键暂停所有 sync | `/admin/syncnode/pause-all` + master 广播 | 1 天 |
| P2-L | **单文件字节范围 sharding (byte_range)** — 单 TB 级文件由 N 个节点并行处理：上传方向走共享 multipart uploadId + 各 sub-task 上不同 part；下载方向用 Range GET + offset write + 全文 md5 aggregate | 5-7 天 | 集成测试：5 TB 单文件 + `parallelism: 4, shardingStrategy: byte_range` → 完成耗时约为单节点的 1/3.5；中途 kill 一个 sub-task → multipart 状态保持 → 重派后接着补上传该 part；最终 etag 与源文件 md5 一致 |
| P2-M | **运行时动态调整限流** — HTTP API 修改 rule.bandwidthLimitMBps 等字段无需重启，已在跑的 task 立即生效 | 2 天 | 集成测试：跑 task 中改 `PATCH /admin/syncrule?id=X bandwidth=100` → 立即生效（监控指标 60s 内反映新速率）|

---

## 10. 关键设计取舍

### 10.0 为什么是"三级存储中转"而不是"CubeFS 缓存"

讨论早期曾考虑把 syncnode 做成"以 CubeFS 为缓存、底层是对象存储"的语义：客户端读 cold 数据时透明从对象存储拉回 CubeFS。否决理由：

1. **真实需求是数据流动，不是 cache 透明性**——训练在 GPFS 跑、eval/推理读 CubeFS、归档去对象存储。每一层各有职责，**用户清楚自己在哪一层**。透明 cache 是反过来的——用户假装只有一层，由系统暗中搬运
2. **透明 cache 需要改 SDK + metanode**（StorageClass_S3、双引用元数据），工程量 8-12 周
3. **本设计 syncnode 完全在数据面之外**——CubeFS 内的读写跟 syncnode 解耦，syncnode 挂了不影响业务读写，只是后台搬运暂停

三级存储分层的对应关系：

| 层 | 工具 | 用户感知 | 性能 | 容量经济性 |
|---|---|---|---|---|
| HOT (GPFS) | 训练 / 高性能推理直接用 | 知道在 GPFS 上 | 极高 | 贵 |
| WARM (CubeFS) | eval / 轻量推理直接读 | 知道在 CubeFS 上 | 中 | 中 |
| COLD (BOS/TOS) | 不直接访问 | 知道归档了，要先 reload | 低（要 reload）| 便宜 |

每个用户**显式知道自己读哪一层**，需要数据从别处搬过来时调 syncnode API。这是工程上最简单、最可控的模型。

### 10.1 为什么不做 read-through cache

讨论过的 lcnode 扩展方案（StorageClass_S3 + SDK 改造 + 双引用元数据）能实现完整的"以 CubeFS 为缓存、S3 为后端"语义，**但工程量是 syncnode 路线的 4-5 倍**（8-12 周 vs 2-3 周），并且：

- 触及 metanode / SDK / proto，**改动面广、回滚困难**
- 多 client 并发 read-through 时的协调机制（多个客户端同时触发同一文件 warm）需要 metanode 层互斥锁，复杂
- ML 训练的典型工作流（流水线提前知道要哪些数据）**用户能预先 warm**，透明 read-through 并非刚需

如果 P0/P1 上线后真的出现"用户必须先 warm 才能读太麻烦"的强反馈，再增量做（不影响现有架构）：
- 加一个 FUSE wrapper 层在 cfs-mount 之上，读 ENOENT 时触发 syncnode load 等待
- 复杂度比 SDK 改造低、隔离性好

### 10.2 为什么不用 lcnode 而新开 syncnode

- **职责清晰**：lcnode 是内部 lifecycle（删除/迁移到内部 BlobStore），syncnode 是外部对象存储桥接。混在一起 lcnode 包会膨胀
- **失败域隔离**：syncnode 跟外部 S3 网络打交道，故障特性（网络抖动、S3 限流、AK/SK 轮换）跟 lcnode 完全不同。一个 bug 不应该把 lifecycle 也搞挂
- **代码风险隔离**：lcnode 已在生产跑 lifecycle，syncnode 是新代码，单独包好回滚

### 10.3 为什么用 BoltDB 而不是 etcd / Raft

- syncnode 状态是**本地**的——分到这个节点的任务进度只有这个节点关心
- 多 syncnode 之间**不共享**任务进度（Master 分发后各节点独立执行）
- BoltDB 单文件、零依赖、性能足够（同步任务的状态变更频率很低）
- 节点崩溃恢复语义：重启后扫 BoltDB + 临时文件，自己恢复自己的进度

### 10.4 S3 key 命名：path 而不是 inode

- 跟之前 cache 模型不同——syncnode 是 archive/mirror 语义，用户希望在 S3 控制台能看到熟悉的路径
- 代价：CubeFS 内 rename → 下次同步会上传新 path 的对象，老 path 在 S3 上变成孤儿
- 缓解：定期的 check 任务扫 S3 prefix vs CubeFS 目录树，发现孤儿对象上报（不自动删，运维确认后手动清）

### 10.5 上传成功不删源（默认）

- 训练 ckpt 通常希望本地保留最近几个版本（快速恢复用），同时 S3 保留长期备份
- 删源是显式行为：`afterCopy: verify_then_delete_src`
- 用 retention.keepLast 控制本地版本数（独立于 S3 上的 retention）

### 10.6 ObjectStore + Posix 后端的统一抽象

为支持 S3 / TOS / BOS / OSS / COS / cfs / local 多种后端，syncnode 内部用统一接口隔离 task executor 跟具体 SDK：

```go
// internal/syncnode/backend/backend.go (新)
type Backend interface {
    // Identity
    Kind() string                          // "s3" / "tos" / "local" / ...

    // Listing
    List(ctx context.Context, prefix string, recursive bool) (<-chan Entry, error)

    // Read path
    Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error)
    Head(ctx context.Context, key string) (size int64, etag string, mtime time.Time, err error)

    // Write path
    Put(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (etag string, err error)

    // Mutations
    Delete(ctx context.Context, key string) error
    Rename(ctx context.Context, oldKey, newKey string) error   // 仅 posix + cfs 支持；object store 通常 copy+delete

    // Capabilities
    Capabilities() Caps                    // 不同后端能力差异（range GET、multipart、原子 rename 等）

    Close() error
}

type Entry struct {
    Key   string
    Size  int64
    Mtime time.Time
    ETag  string
    IsDir bool        // 仅 posix 后端用，object store 全是 false
}

type PutOptions struct {
    StorageClass  string                  // S3 family: STANDARD / STANDARD_IA / GLACIER 等
    ContentType   string
    Metadata      map[string]string
    Multipart     bool                    // 自动决定也可强制
    PartSizeMiB   int                     // multipart 时单 part 大小
}

type Caps struct {
    RangeRead       bool   // 是否支持 GET with Range header（几乎所有 object store 都支持）
    Multipart       bool   // 是否支持 multipart upload
    AtomicRename    bool   // 同 backend 内的 rename 是否原子（posix=yes, object store=no）
    ListMaxKeys     int    // 单次 List 上限（S3 默认 1000）
    StrongConsistency bool // PUT 后立即可读
}
```

#### 注册机制

```go
// internal/syncnode/backend/registry.go
var registry = map[string]Constructor{}

type Constructor func(cfg BackendConfig) (Backend, error)

func Register(kind string, c Constructor) { registry[kind] = c }
func New(kind string, cfg BackendConfig) (Backend, error) {
    c, ok := registry[kind]
    if !ok { return nil, fmt.Errorf("unknown backend: %s", kind) }
    return c(cfg)
}
```

每个 backend 实现在自己的 `init()` 注册：

```go
// internal/syncnode/backend/s3/s3.go
func init() { backend.Register("s3", New) }
func New(cfg backend.BackendConfig) (backend.Backend, error) { ... }

// internal/syncnode/backend/tos/tos.go  (P2)
func init() { backend.Register("tos", New) }
```

#### P0 实现清单

| Backend | P0 | 说明 |
|---|:---:|---|
| `cfs` | ✓ | 复用现有 `tool/cfs-sync/storage/cfs_linux.go` 的 ExtentClient 集成，但写路径要重做（cfs-sync 当前是单 goroutine 串行写，syncnode 性能要求并行写多文件）|
| `s3` | ✓ | 用 aws-sdk-go-v2 + s3manager.Uploader for multipart |
| `local` | ✓ | std `os` 包 + `copyWithBuffer`，可配 buffer/concurrency/directIO/fadvise 覆盖普通本地盘 + 并行 FS（Lustre/GPFS/WekaFS）|

#### P2 扩展（按需）

| Backend | SDK | 备注 |
|---|---|---|
| `tos` | volcengine-go-sdk-tos | 字节 TOS，专有 SDK；部分场景可用 S3-compat 模式直接走 `s3` adapter |
| `bos` | baidubce-sdk-go-bos | 百度 |
| `oss` | aliyun-oss-go-sdk | 阿里 |
| `cos` | tencentyun-cos-go-sdk-v5 | 腾讯 |

**注意**：上述云厂家都提供 "S3-compatible" endpoint，技术上可以**先**让用户用 `kind: "s3"` + 自定义 endpoint 接入。专门的 adapter 仅在需要厂家专有特性（如 TOS 的对象元数据扩展、BOS 的归档恢复 API）时才写。

#### 实现工期估算

| 阶段 | 工期 |
|---|---|
| 接口 + registry 抽象 | 1 天 |
| cfs / s3 / local 三个 P0 backend | 已含在 §9 Phase C 中 |
| 单个新 backend (TOS / BOS / OSS / COS) | 3-5 天/个，主要是 SDK 适配 + 测试 |

#### 配置时的 backend 寻址

规则的 src/dst 配置直接对应 `Backend.Kind()`：

```json
{ "kind": "s3", "endpoint": "https://tos-s3-cn-beijing.volces.com", "bucket": "...", "prefix": "..." }
```

这种"用 s3 adapter 接 TOS 的 s3-compat endpoint"是 P0 上线就能用的临时方案。P2 加 `kind: "tos"` 后用户可平滑切换（rules 改一个字段，src 数据不变）。

---

## 11. 安全 & 运维

### 11.1 凭据管理

S3 AK/SK 三种获取方式（优先级）：

1. **AWS IRSA / 实例元数据**（如果跑在 EKS / EC2，最安全）
2. **环境变量**（`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`）
3. **配置文件直接写**（仅测试用，生产严禁）

凭据轮换：syncnode 监听 SIGHUP 重读环境变量；或定期从配置文件刷新。

### 11.2 网络隔离

- syncnode → S3 / TOS / BOS / OSS / COS：HTTPS，**优先走云厂家的内网 endpoint**（VPC PrivateLink、内网 OSS endpoint 等）。出公网 endpoint 是默认 fallback，但成本和延迟都更高
- syncnode → Master：内部 TCP，跟 lcnode 一致
- syncnode → CubeFS SDK：跟普通 client 一样走 metanode/datanode
- syncnode → 本机 POSIX：`local` backend 直接走文件系统调用。**必须经过 §3.4 `posix.allowedRoots` 白名单**（含 EvalSymlinks 后校验），不允许相对路径或路径穿越

### 11.3 审计日志

每次 sync/load 操作记入 audit log（参考 lcnode 的 `auditlog.LogLcNodeOp`）：

```
2026-05-13 14:23:10 INFO syncnode audit: vol=train-vol path=/runs/exp-42/model-step-5000.pt
  size=2147483648 src=cfs dst=s3 etag=ab12...cd op=sync duration=124s status=ok
```

---

## 12. 可扩展性与容量规划

### 12.1 单节点容量模型

**理论上限**（单 syncnode 实例）：

| 维度 | 上限 | 主要限制源 |
|---|---|---|
| 双向带宽 | NIC 单向带宽 ÷ 2 | 通常 25 Gbps NIC ≈ 1.5 GB/s 单向稳态 |
| CPU 吞吐 | ~500 MB/s / 核 | md5 / etag 计算 + 序列化 |
| BoltDB 写吞吐 | ~10K txn / 秒 | fsync 顺序写 |
| 并发 task 数 | `maxConcurrentTasks` (默认 8) | 配置项 |
| 单 task 内并发 part | `transfersPerTask` (默认 4) | 配置项 |

**实测预期**（25 Gbps NIC、8C16G、SSD）：

| 工作负载 | 吞吐 / QPS |
|---|---|
| 顺序大文件 sync (单 task)| 1.5-2 GB/s |
| 8 个 task 并发大文件 | 接近 NIC 单向上限 |
| 小文件 sync (<10 MB) | 200-500 个 / 秒 |
| Check 任务（仅 metadata 对比，全量）| 1000-3000 个 / 秒 |

**瓶颈优先级**（典型负载）：

1. **NIC**（多数情况下，syncnode 主要是 net I/O 服务）
2. **后端服务**（CubeFS DP 拥塞 / S3 prefix 限流 / GPFS NSD 带宽）
3. **CPU**（仅大量小文件 + md5 全文校验场景）
4. BoltDB（基本不会成为瓶颈；如果观察到要 batch txn 优化）

### 12.2 横向扩展

加 syncnode 实例：

| 节点数 | 聚合吞吐（理想）| 实际效率 | 备注 |
|---|---|---|---|
| 1 | 1.5 GB/s | 100% | 单点，无 HA |
| 2 | 3 GB/s | 95-100% | HA 配置最小 |
| 3 | 4.5 GB/s | 90-95% | master 调度有少量开销 |
| 5 | 7-8 GB/s | 85-90% | 推荐稳态 |
| 10 | 13-15 GB/s | 80-85% | 大规模训练集群 |
| 50 | 50+ GB/s | 60-70% | 后端开始成为瓶颈 |

**线性扩展的边界**：

| 真正限制 | 出现规模 | 应对 |
|---|---|---|
| Master 派发能力 | > 100 节点 | 单 master 派发 ~10K task/s 远超实际，不会到这 |
| CubeFS 后端 DP / 集群带宽 | 中规模 | 联系 CubeFS 容量规划侧 |
| 后端 S3 prefix 限流（5500 req/s/prefix）| 大量小文件场景 | 规则 dst.prefix 按 date / hash 分片 |
| GPFS NSD 聚合带宽 | 多 syncnode 都打同一 GPFS | 计划 GPFS 客户端节点数量 + IO 调度 |

### 12.3 流量压力场景与应对

#### 场景 A：突发大量 task（训练结束一波 ckpt 同步）

```
1000 个 ckpt 文件 × 1 GB 同时进 sync 队列
```

数据流：

```
HTTP API trigger / cron fires
    ↓
Master pending queue (P1+ 才用，P0 单节点直入)
    ↓
按 load score 分发到 N 个 syncnode
    ↓
节点本地排队（BoltDB 持久化）
    ↓
执行槽放出 → task executor (max=8) → backend
```

容量边界：

- 节点内 `maxConcurrentTasks` 满 → 排队（队列 size 是 `maxQueueSize` 配置，默认 10000）
- 节点队列满 → 向 master nack，master 派给次优节点
- 全集群所有节点都满 → master 拒绝新 task，HTTP API 返回 429 + 告警

应对配置：

```json
"concurrency": {
  "maxConcurrentTasks": 8,
  "maxQueueSize": 10000,
  "transfersPerTask": 4,
  "bandwidthLimitMBps": 200
}
```

监控：`cubefs_master_synctask_pending_queue_size`、`cubefs_syncnode_concurrent_tasks{node}`

#### 场景 B：单个 TB 级超大文件

```
1 个 10 TB ckpt 文件
```

行为：

- 单 task 占用单节点一个 concurrent slot
- 单文件内启动 N 路 multipart part 并发（默认 4-8，可调 `transfersPerTask`）
- **不分片到多节点**：单文件不跨节点拆分（P0/P1 都不做）

容量考虑：

- 单节点 NIC 上限即为单文件上限（约 1.5-2 GB/s）
- 10 TB 文件耗时 ≈ 1.5 小时（25 Gbps NIC）
- 期间该节点的其他 task 仍可用其他 concurrent slot

应对配置：

```json
"backend": {
  "s3": {
    "partSizeMiB": 64,        // 大文件用大 part，省 multipart 元开销
    "maxConcurrentParts": 8   // 单 task 内 part 并发
  }
}
```

**未来扩展点**（P2+）：若 10+ TB 单文件成为常态，引入 "single-file fan-out across nodes"——一个 task 拆成 N 个 partition，每个 partition 在不同节点上跑。复杂度高，等需求出现再做。

#### 场景 C：S3 / BOS / TOS 限流（429 SlowDown）

```
大量 PUT 到同一 S3 prefix → AWS S3 5500 req/s 上限触发
```

行为：

- backend 收到 429 → 内置 exponential backoff（aws-sdk-go-v2 默认行为）
- 持续 3 分钟仍 429 → task 标 failed
- 指标：`cubefs_syncnode_backend_request_total{kind=s3, result=error_4xx}`

应对：

1. **规则配置层**：把 `dst.prefix` 按 date / hash 分片
   ```json
   "dst": { "kind": "s3", "bucket": "...", "prefix": "runs/2026-05-13/" }
   ```
2. **运维层**：联系云厂家提升 prefix 上限（AWS S3 自动 scaling）
3. **限速层**：`bandwidthLimitMBps` 主动放缓

#### 场景 D：GPFS 突然变慢 / 不可用

```
GPFS metadata 服务器抖动 / 网络问题
```

行为：

- local backend 文件操作超时
- 错误分类为 `transient_network` → 退避重试（最多 5 次）
- 持续失败超过 5 分钟 → 规则标 `degraded`
- 不影响 CubeFS ↔ BOS 的规则继续跑

应对：

- 监控 `cubefs_syncnode_backend_request_duration_seconds_bucket{kind=local}` p99 延迟
- 规则降级机制 (§G-3) 避免一颗 GPFS 故障拖垮整个 syncnode

#### 场景 E：CubeFS 数据卷满（W3 反向加载触发）

```
W3 反向加载，BOS → CubeFS，CubeFS 卷容量满
```

行为：

- ExtentClient.Write 返回 ENOSPC
- task 标 failed，`error_type=disk_full`
- 告警立即触发，规则降级到 paused

应对：

- 监控 CubeFS 卷容量水位告警（CubeFS 现有指标）
- W1 / W3 规则都要配 retention 控制 CubeFS 上的版本数
- Master 端：syncnode 心跳上报"上次 task 触发的卷容量错误"，master 把同卷的规则全部暂停

### 12.4 限流与背压

#### 12.4.1 四层限流体系

每个 transfer 实际速率 = `min(task, rule, node, backend)` 四层限流的最严约束。

```
┌──────────────────────────────────────────────────────────────┐
│ 第 1 层 Per-Task — rule.bandwidthLimitMBps                    │
│   单 task 内部所有 part / chunk / file 共享                    │
│   控制单个搬运任务的资源占用                                   │
│   ── P0 支持                                                  │
├──────────────────────────────────────────────────────────────┤
│ 第 2 层 Per-Rule — rule.aggregateBandwidthLimitMBps           │
│   同一 rule 下所有并发 task 共享（P1+ 多节点场景）              │
│   控制业务侧"这个同步通道"的总占用                              │
│   ── P1+ 支持（需 master 协调，因为同 rule 可能跨节点）         │
├──────────────────────────────────────────────────────────────┤
│ 第 3 层 Per-Node — concurrency.bandwidthLimitMBps             │
│   单 syncnode 节点上所有 task 共享                             │
│   保护节点自身的 NIC、CPU、不挤占同机业务                       │
│   ── P0 支持                                                  │
├──────────────────────────────────────────────────────────────┤
│ 第 4 层 Per-Backend — backend.<kind>.bandwidthLimitMBps       │
│   同一 (kind, endpoint) 三元组的所有请求共享                   │
│   防止打爆 S3 prefix / GPFS NSD 等共享后端                     │
│   ── P0 支持基础（每节点本地）；P1+ 支持跨节点全局              │
└──────────────────────────────────────────────────────────────┘
```

**配置示例**：

```json
{
  "concurrency": {
    "bandwidthLimitMBps": 1000,           // 第 3 层：本节点上限 1 GB/s
    "maxConcurrentTasks": 8,
    "maxQueueSize": 10000,
    "transfersPerTask": 4
  },
  "backend": {
    "s3": {
      "bandwidthLimitMBps": 500,          // 第 4 层：所有 s3 backend 共享 500 MB/s
      "maxConcurrentRequests": 64
    }
  },
  "rules": [
    {
      "id": "w1-ckpt",
      "bandwidthLimitMBps": 200,          // 第 1 层：本规则单个 task 200 MB/s
      "aggregateBandwidthLimitMBps": 800, // 第 2 层 (P1+)：跨节点该规则总 800 MB/s
      ...
    }
  ]
}
```

#### 12.4.2 实现机制

每层都是一个 `golang.org/x/time/rate.Limiter` (token bucket)。reader / writer 包装层在每次 `Read` / `Write` 前向**所有适用的 limiter 申请配额**：

```go
// 伪代码：reader 包装
type LimitedReader struct {
    inner   io.Reader
    limiters []*rate.Limiter   // [task, rule, node, backend]
}

func (r *LimitedReader) Read(p []byte) (int, error) {
    n, err := r.inner.Read(p)
    if n > 0 {
        for _, l := range r.limiters {
            l.WaitN(ctx, n)        // 阻塞直到所有 layer 都批准 n 个字节
        }
    }
    return n, err
}
```

注意点：

- limiter 是**字节配额**（不是 ops 配额），桶容量按 burst 设置（默认 = 1 秒带宽）
- `WaitN` 走 ctx，task 被 cancel 时立即返回
- 第 2 / 4 层跨节点的实现要走 master：每节点心跳报告"已用带宽"，master 计算"剩余配额"在心跳响应里发回——周期 10s，精度足够（不需要每字节同步）

#### 12.4.3 限流应用矩阵

| 场景 | 主要 binding 层 |
|---|---|
| 单个 ckpt 备份 task | 第 1 层（task 内部限速）|
| 同一 rule 下 100 个文件 | 第 1 层 × 100 个 task + 第 2 层全 rule cap |
| 一个 syncnode 节点跑 8 个 task | 第 3 层（node 级）|
| 所有 syncnode 都打同一 S3 bucket | 第 4 层（backend 级，关键防 429）|

#### 12.4.4 被动背压（出现拥塞时的链式行为）

```
backend 慢          →  task 内 worker 阻塞（被 limiter 卡住或 backend 5xx）
                      ↓
task 进度停滞       →  上报 task_stuck 告警（>5 min 无进度）
                      ↓
节点新 task 入不来  →  Master nack，派给其他节点
                      ↓
全集群都饱和       →  HTTP API 返回 429，alert
```

#### 12.4.5 P0 vs P1 vs P2

| 层 | P0 | P1 | P2 |
|---|:---:|:---:|:---:|
| 第 1 层 Per-Task | ✓ | | |
| 第 3 层 Per-Node | ✓ | | |
| 第 4 层 Per-Backend（节点本地）| ✓ | | |
| 第 2 层 Per-Rule（跨节点）| | ✓ | |
| 第 4 层 Per-Backend（跨节点全局）| | ✓ | |
| 动态调整（运行时改限速）| | | ✓ |

### 12.5 部署规模建议

按集群训练规模：

| 集群规模 | syncnode 实例数 | 部署形态 |
|---|---|---|
| < 100 GPU | 2 (HA) | 单 zone，2 个独立节点 |
| 100-500 GPU | 3-5 | 单 zone |
| 500-2000 GPU | 5-10 | 单 zone 或多 zone（按物理位置）|
| > 2000 GPU | 按数据流分组每组 3-5 个 | 多 zone（业务拆分而非物理拆分）|

每实例规格已在 §2.1 列出（8C16G + 25 Gbps + 100 GiB SSD）。

**评估表**：先按"每 100 个 GPU 配 1 个 syncnode"起步，上线后看吞吐 + 队列指标，按需扩容。

### 12.6 验证扩展性的方法（P0 测试）

P0 不交付多实例（P1 才有），但 P0 测试要验证**单节点的天花板符合理论值**，为 P1 扩展打 baseline：

1. **天花板基准**：单节点跑满载 1 小时（8 个 task 并发，每个 1 GB 文件循环），观察聚合吞吐稳定在 NIC 80%+
2. **背压验证**：触发 100 个 task 排队，确认 nack 行为正确、队列不爆炸
3. **慢后端验证**：在 backend 加 50 ms 延迟模拟，确认 task 没卡死 + 限流生效
4. **长跑稳定性**：连续 7×24 小时跑，确认 goroutine 数、内存、BoltDB 大小都平稳

---

## 13. 监控指标（Prometheus）

### 12.1 指标导出

通过 cfs-server 共用的 exporter 体系（`util/exporter`）注册到 master 配置的 Consul/Prometheus 端，跟 lcnode 注册流程一致。指标 endpoint 默认走 `httpListen`（或独立 `exporterPort`）。

启动日志会输出：

```
exporter registered: cubefs_syncnode, scrape http://10.0.0.10:17912/metrics
```

### 12.2 指标分层

#### 节点级（gauge，每节点一份）

```promql
cubefs_syncnode_up{node}                            gauge   # 1 = 在线，0 = 进程死
cubefs_syncnode_uptime_seconds{node}                gauge
cubefs_syncnode_concurrent_tasks{node}              gauge
cubefs_syncnode_concurrent_tasks_limit{node}        gauge
cubefs_syncnode_bandwidth_mbps_used{node}           gauge
cubefs_syncnode_bandwidth_mbps_limit{node}          gauge
cubefs_syncnode_cpu_percent{node}                   gauge
cubefs_syncnode_mem_rss_bytes{node}                 gauge
cubefs_syncnode_temp_files_orphan{node}             gauge   # 残留临时文件数
cubefs_syncnode_boltdb_size_bytes{node}             gauge
```

#### 任务级（counter / histogram，按 rule / type / backend 维度切分）

```promql
cubefs_syncnode_tasks_total{node, rule_id, type, backend, status}                counter
                            # status ∈ done | failed | cancelled | interrupted

cubefs_syncnode_task_duration_seconds_bucket{rule_id, type, backend, le}         histogram
                            # le ∈ 10, 60, 300, 1800, 3600, 7200, 86400
cubefs_syncnode_task_duration_seconds_sum{rule_id, type, backend}                counter
cubefs_syncnode_task_duration_seconds_count{rule_id, type, backend}              counter

cubefs_syncnode_bytes_transferred_total{rule_id, type, backend, direction}       counter
                            # direction ∈ in (load) | out (sync)

cubefs_syncnode_files_processed_total{rule_id, type, backend, result}            counter
                            # result ∈ ok | skipped | verify_fail | error
```

#### Backend 级（按 backend kind 切分）

```promql
cubefs_syncnode_backend_request_total{kind, op, result}                          counter
                            # kind ∈ s3 | local | cfs (P2 加 tos/bos/oss/cos)
                            # op ∈ get | put | head | list | delete | range_get
                            # result ∈ ok | error_4xx | error_5xx | error_timeout

cubefs_syncnode_backend_request_duration_seconds_bucket{kind, op, le}            histogram
cubefs_syncnode_backend_retry_total{kind, op}                                    counter
cubefs_syncnode_backend_throttle_seconds_total{kind}                             counter  # bandwidth limiter 阻塞累计
```

#### Master 调度（仅 master 端，P1）

```promql
cubefs_master_synctask_dispatch_total{result}                                    counter
                            # result ∈ scheduled | no_capable_node | retry_exhausted
cubefs_master_synctask_pending_queue_size                                        gauge
cubefs_master_synctask_failover_total                                            counter
cubefs_master_syncnode_count{state}                                              gauge
                            # state ∈ active | inactive
```

### 12.3 推荐告警阈值

| 红线 | 级别 | 说明 |
|---|---|---|
| `task_failure_rate > 5%` (5min) | warning | `tasks_total{status="failed"}/sum(tasks_total)` |
| `task_failure_rate > 20%` | critical | 同上 |
| `backend_request_errors{result="error_5xx"} rate > 100/min` | warning | 通常云厂限流或 S3 region 故障 |
| `concurrent_tasks == concurrent_tasks_limit` 持续 10min | warning | 积压，要么扩并发要么加节点 |
| `temp_files_orphan > 50` | warning | 崩溃恢复机制可能有问题 |
| `synctask_failover_total rate > 1/min` (5min) | critical | 节点不稳定，频繁飘动 |
| `boltdb_size_bytes > 5 GiB` | warning | 任务历史没回收 |

### 12.4 看板建议

最少做三个 Grafana 看板：

1. **集群概览**：在线节点数、总并发、总吞吐、总失败率
2. **节点详情**：单节点 CPU/MEM/带宽/任务数随时间分布
3. **规则详情**：单规则的成功率、平均耗时、字节数、retention 命中

---

## 14. 验收标准

### P0 上线门槛（每条都可量化判定）

**正确性**
- [ ] 三个 backend (cfs / s3 / local) 通过统一契约测试（1 KB–5 GB 大小矩阵，全部 PASS）
- [ ] **三级链路 1 TB 总数据量** 端到端 W1 → W2 → W3 跑通，md5 在三层之间 100% 一致
- [ ] 中途 `kill -9` 后重启，未完成任务的目标文件**不存在半截**（rename 原子或 temp 文件被清理）
- [ ] 启动检测：3 种规则冲突配置（同 src/dst 双规则、prefix overlap、循环 sync）全部拒绝启动
- [ ] 启动检测：8 种 negative 配置（cron 非法、kind 非法、path 越界等）全部返回特定错误码

**稳定性**
- [ ] 单 syncnode 实例连续跑 7×24 小时无 OOM、无 panic、无 goroutine 泄漏（runtime.NumGoroutine 平稳）
- [ ] BoltDB 大小连续 7 天稳定（任务历史 TTL 生效）
- [ ] 不会因 multipart 残骸把 S3 存储费拉高（启动清理工作）

**性能**
- [ ] 单文件吞吐：1 GB ckpt sync ≤ 5 秒（要求 ≥ 200 MB/s）
- [ ] 单文件吞吐：5 GB ckpt load ≤ 25 秒（要求 ≥ 200 MB/s）
- [ ] 并发：8 个 task 并发，聚合吞吐 ≥ 1.5 GB/s（在测试 NIC 50 Gbps 上）
- [ ] bandwidth_limit 200 MB/s 实测稳定在 [180, 220] MB/s（±10%）

**功能完整性**
- [ ] retention `keepLast=5` 跑 100 次 sync 后 S3 上正好 5 个最新版本
- [ ] retention 在 sync 部分失败时**不**执行（防止删除应保留的版本）
- [ ] HTTP admin API 全部端点（rule CRUD、task trigger/get/cancel、preview）有集成测试通过
- [ ] AuthMiddleware hook 在位（P0 是 no-op 实现，但中间件链已经把所有路由覆盖）
- [ ] vol 失联场景：删 vol 后 60 秒内规则标 degraded，恢复后可手动 resume

**运维**
- [ ] 配置 reload (SIGHUP) 配置错误时不中断在跑任务
- [ ] 部署文档：同事按文档 30 分钟内能拉起本地实例 + 跑通 demo
- [ ] Prometheus 指标三层（节点 / 任务 / backend）全部上报；Grafana 看板 JSON 模板提供

### P1 上线门槛

继承 P0 全部验收项，外加：

- [ ] 3 节点部署，10 个 task 触发，load score 标准差 ≤ 平均值 30%
- [ ] 节点故障：kill 一个 syncnode → 60 秒内 task 重新派发到其他节点 → 重传完成
- [ ] master 主从切换：kill leader → 30 秒内新 leader 接管 → 派发继续
- [ ] 多节点聚合吞吐 ≥ 单节点 × (节点数 × 0.8)（线性扩展系数 80%）

---

## 15. 风险

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| S3 限流（429 SlowDown）| 中 | 同步变慢 | 指数退避重试，bandwidth_limit 主动节流 |
| 大文件下载中断后续传失败 | 低 | 部分文件需重传 | temp_rename 策略 + ETag 校验 |
| 时钟漂移导致 schedule 紊乱 | 低 | 任务跑两次或漏跑 | 用 BoltDB 记录 last_run_at，重启后基于此判断而非纯 cron |
| 配置加载时新规则覆盖运行中规则 | 低 | 运行任务被打断 | reload 时只更新调度信息，运行中任务不动 |
| syncnode 与 cfs-sync CLI 双重使用导致冲突 | 中 | 同文件被多端同时改 | syncnode 维护"正在处理的文件路径"集合，cfs-sync CLI 通过 LOCK 文件协调 |
| BoltDB 单文件损坏（电源故障）| 低 | 任务历史丢失 | sync write 模式 + 启动时校验，损坏则备份重建（任务历史可舍）|
| AK/SK 泄漏 | 中 | 数据泄漏 | 优先用 IRSA / IMDS，配置文件凭据加权限 0600 |
| **Master 状态膨胀** | 低 | Master Raft 写延迟变高 | 心跳上报只 push **状态变化**（不是每次进度更新），任务详细进度只在节点 BoltDB，Master 只存 ownership 元数据。§7.4 估算 1000 节点 / 100 万累计任务下 Master 增量 < 1 GB，可接受 |
| **Master 失主期间任务派发暂停** | 中 | 派发延迟 5-30s | Raft 选举本身就 5-30s；已派发的任务不受影响；新任务在 syncnode 端 retry 直到新 leader 选出 |
| 引入 syncnode 后 Master 心跳压力上升 | 低-中 | Master CPU 占用上升 | 每 syncnode 每 10s 一次心跳，载荷小；与 lcnode 心跳协议同构，可参考 lcnode 现网压力评估 |


---

## 16. TBD（P0 开发前需 unblock）

下列三项**对 P0 范围没有阻塞**——P0 用默认行为先做，未来按需替换。仅在这里登记，进入开发后由 owner 拍板。

| # | 议题 | 默认行为（P0）| 替代方案 | 触发拍板时机 |
|---|---|---|---|---|
| O1 | 跨节点断点续传 | 故障转移就从头重传（§6.3.3）| Master 持久化任务进度，新节点接管时可续传 | P1 上线后看故障转移耗时数据 |
| O2 | 配置 reload 影响在跑 task 的边界 | 旧 task 用旧 backend 配置完成 | 中断旧 task → 用新配置重启 | 出现实际场景再拍 |
| O3 | 任务依赖链 (task chain) | 不支持，每 task 独立 | onSuccess / onFailure 钩子（P2-B 已列）| P0 上线后用户反馈推动 |

---

## 17. Last Updated

`2026-05-13` — initial consolidated spec for SDD-based development. W6 (BOS/TOS → GPFS 反向直传) added as the dual of W4; no new code needed since `kind: local` ↔ `kind: s3` configurations already cover it via the universal Backend abstraction.
