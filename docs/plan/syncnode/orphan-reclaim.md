# Syncnode 孤儿分片回收 + Admission 设计

> 状态：进行中 · 起始 2026-05-26 · 跨多轮推进 · branch `ft_bugfix_syncnode`
> 触发事件：test-hb 上 `ai-training-dataloader-mplwgjqf-1779755621663671921` 4 个 shard 全部因 OOMKilled 永久卡死在 `running`

## 1. 背景与目标

### 1.1 现象

test-hb 上 `ai-training-dataloader` rule(seed-dataset stage `size=200g numjobs=8 iodepth=16 bs=4m`)派发到 4 个 syncnode,4 个 pod 全部被 cgroup OOM kill(exitCode 137 + reason OOMKilled,4GiB pod limit)。pod 重启后:

- master DB 里 4 个 shard 仍是 `running` 状态,`updatedAt == createdAt`,从未更新
- 9 个 syncnode 报 `runningTasks=0`,master 调 `sync_node_tasks` 全返回 null
- 任务永久卡住,需要人工介入

### 1.2 根因(状态分裂 + 3 个协议洞)

| 编号 | 缺陷 | 表现 |
|---|---|---|
| 1 | master 缺孤儿 shard 回收 | shard owner 失联后 master 不主动检测 / 标 failed |
| 2 | syncnode 重启后不向 master reconcile 自己持有的 task 列表 | 现有 `recovery.go:Recover()` 把本地任务标 failed 并 `pushFailedTerminal()` 上报,但 OOM 后 pod 还未起来时 master 已无从感知 |
| 3 | syncnode 接活前不做资源 admission | 任何超出 pod cgroup limit 的 rule 都会让接活的 syncnode 静默 OOM,不能在派发阶段拒绝 |
| 4 | test-hb syncnode 资源偏小 | 4GiB / 2 core,跑 size=200g/numjobs=8 类负载内存不够 |

### 1.3 目标

让 master 在 syncnode 异常退出 / 拒绝接活时具备自愈能力:

- 任何 shard 因 owner 失联 → master 在心跳超时窗口内自动标 `failed`,parent 聚合 `failed`(**不重派**)
- syncnode 接活前估算 rule fioStages 峰值内存,超 pod limit 一定比例(70%)→ 拒绝
- syncnode 拒绝 → master 把 shard 转给下一个候选 syncnode(用现有 `SyncDispatcher.Dispatch()` 的重试循环);所有候选都拒绝才标 `failed`
- test-hb syncnode pod 升到 12GiB / 4 core,缓解资源压力(不替代上述修复,只是把阈值推高)

### 1.4 不做什么

- ❌ 不实现 shard 自动重派(用户明确决策"只标 failed,不重派",由用户 UI 自己 retry)
- ❌ 不改动 fio rule 模板默认值(`ai-training-dataloader` 的 200g 是合理的训练集模拟规模)
- ❌ 不引入 K8s scheduler / 节点级资源调度(syncnode 是 DaemonSet,没节点选择问题,只是 pod 内 cgroup 限制)
- ❌ 不动 SyncTask 状态机的语义(只在终态判定上加超时保险)
- ❌ 不改 BoltDB schema(syncnode 侧已有 task 持久化,够用)
- ❌ 不在本轮做 master 任务的 BoltDB 持久化(ledger 还是内存 LRU,重启清空 — 那是另一个问题)

## 2. 关键代码接缝点

> 来自前置代码调研(2 个 Explore agent 报告)

### 2.1 Master 侧

| 文件:行 | 角色 |
|---|---|
| `master/bench_task_ledger.go:72` `BenchTaskRecord` | **需新增 `Owner` 字段** 记录派发到哪个 syncnode |
| `master/bench_task_ledger.go:35-42` `BenchTaskStatus` | 已有 `Running/Succeeded/Failed/Cancelled`,够用 |
| `master/bench_task_ledger.go` `Fail(taskID, errMsg)` (~line 241) | 复用,标 failed |
| `master/sync_task_ledger.go:94` `SyncTaskRecord` | 已有 `Owner` 字段,**需补 `UpdatedAt int64`** |
| `master/sync_rule_store.go:260` `recordTaskTerminal` | 复用,标 failed |
| `master/sync_dispatcher.go:332` `SyncDispatcher.Dispatch()` | 已有候选循环 line:343-368,**扩展拒绝错误码识别** |
| `master/bench_dispatch.go:29/52` `dispatchBenchTask/Shards` | 用 `pickActiveSyncNode` 单点派发,**改造为候选循环** |
| `master/sync_node_task.go:34` `handleSyncNodeTaskResponse` | 进度上报入口,**新增 OpSyncNodeReject 分支** |
| `master/sync_node_task.go:55` `handleSyncNodeHeartbeatResp` | 心跳上报入口 |
| `master/cluster.go:1133` `checkSyncNodeHeartbeat` | 每 30s 后台 worker,**挂入孤儿扫描** |
| `master/cluster.go:966` `defaultIntervalToCheckHeartbeat` = 30s | 节拍参考 |

### 2.2 Syncnode 侧

| 文件:行 | 角色 |
|---|---|
| `syncnode/server.go:139` `Start/doStart` | 启动入口 |
| `syncnode/server.go:206` initExecutorAndRunner | **挂入启动 reconcile**(本地 task list → POST master) |
| `syncnode/bolt/recovery.go:53` `Recover()` | 已有"重启 = 失败上报"骨架,**校验是否对 bench task 也生效** |
| `syncnode/task_handler.go:365 HandleConn` → `:386 handleRunTask` → `:439 TriggerBench` | dispatch 接收链 |
| `syncnode/task_handler.go:412-420` TriggerWithRule 前 | **插入 admission check** |
| `syncnode/task_handler.go:217 admitOrRetry` | 现有 ErrQueueFull 拒绝路径,**新增 ErrInsufficientMemory 错误码** |
| `syncnode/task_handler.go:271 pushFailedTerminal` | 复用,通知 master 拒绝 |
| `syncnode/spec/bench_rule.go:55-90 BenchRule` / `:223-252 FIOStage` | rule 结构,admission 估算的输入 |
| `syncnode/snapshot.go:62-75` | 现有 CPU/Mem 使用率采样 |
| `syncnode/util/loadutil.GetContainerMemoryLimitBytes()` | **复用读 cgroup mem limit** |
| `syncnode/master_client.go:358 sendHeartbeat` 每 10s | 心跳协议 |

### 2.3 协议字段新增

| 字段 / 错误码 | 位置 | 用途 |
|---|---|---|
| `OpSyncNodeReject`(新 OpCode) | `proto/admin_proto.go` | syncnode → master 拒绝接活 |
| `OpSyncNodeReconcile`(新 OpCode) | 同上 | syncnode 启动时上报本地任务列表 |
| `ErrInsufficientMemory` | `syncnode/errors.go` 或 `proto/error.go` | admission 拒绝原因 |
| `BenchTaskRecord.Owner string` | `master/bench_task_ledger.go:72` | 派发时记录,孤儿检测时用 |
| `SyncTaskRecord.UpdatedAt int64` | `master/sync_task_ledger.go:94` | 心跳超时判定 |

## 3. 分阶段实施

### Phase A · 单点改动(可独立合并)

| Task | 范围 | 状态 |
|---|---|---|
| A1: test-hb syncnode 资源 override 到 12Gi/4 core | `envs/test-hb/syncnode/terragrunt.hcl` | 进行中 |
| A2: 本执行文档 | 本文件 | 进行中 |

### Phase B · cubefs 代码改动

| Task | 范围 |
|---|---|
| B1: master 加 `BenchTaskRecord.Owner` + `SyncTaskRecord.UpdatedAt` 字段 |
| B2: master `checkSyncNodeHeartbeat` 挂入 `checkOrphanShards()`: 扫描 SyncTaskLedger + BenchTaskLedger,Running 且 (UpdatedAt 超阈值 OR Owner 节点失活) → 标 failed |
| B3: 定义 `OpSyncNodeReject` opcode + handler,syncnode 拒绝时 master 转下一个候选(SyncDispatcher 循环已有;BenchTask 派发改造) |
| B4: 定义 `OpSyncNodeReconcile` opcode + handler,syncnode 启动时上报本地 task list,master diff 后把本地 DB 有但 syncnode 不认的标 failed |
| B5: syncnode admission check:`handleRunTask` 前估 fioStages 峰值内存 vs `GetContainerMemoryLimitBytes()` × 0.7,超阈值返回 `OpSyncNodeReject` |
| B6: syncnode 启动时调 `Recover()` 后,主动调用 reconcile 上报 |

### Phase C · 构建 + 部署 + E2E(test-k3d)

| Task | 范围 |
|---|---|
| C1: dev_bd 构建 `cubefs:v3.5.3.1.rc17`,push 到 registry |
| C2: bump `cubefs-deploy/_envcommon/images.hcl` 的 `cubefs_image` |
| C3: `make ENV=test-k3d apply-master apply-syncnode` |
| C4: E2E 用例(见 §4) |

### Phase D · commit + push

| Task | 范围 |
|---|---|
| D1: cubefs 仓库按 feat/fix 拆 commit + push 到 `ft_bugfix_syncnode` |
| D2: cubefs-deploy 仓库 **不提交**(images.hcl 和 test-hb override 留本地) |

### Phase E · 待用户确认后 test-hb 部署

| Task | 范围 |
|---|---|
| E1: 在 test-hb apply syncnode(资源 + 新镜像) |
| E2: 重放 ai-training-dataloader 看是否端到端 OK |

## 4. test-k3d E2E 验收用例

### 4.1 正常 rule 跑通(回归)

跑 `aiCheckpointSeqWrite` 或 `posixSequentialBaseline`(小 size),应该 succeed。

### 4.2 高资源 rule 被 admission 拒绝

构造一个 rule:`size=20g numjobs=8 iodepth=16`(预估峰值 > test-k3d syncnode 内存 70%),触发后:
- master 看到 shard 立即被标 failed,error 含 `insufficient memory`
- 4 个候选 syncnode 全都拒绝时,parent 标 failed
- 不发生 OOMKilled

### 4.3 模拟 syncnode 死亡,master 自动标 failed

跑一个长 runtime rule(`runtime=300`),启动后:
- `kubectl delete pod -n storage-cfs cubefs-syncnode-xxx --force`
- 60s 内(2 × 心跳间隔 30s)master 应自动把孤儿 shard 标 failed
- parent task 标 failed
- 不出现永久卡死

### 4.4 syncnode 重启后 reconcile 生效

跑一个 rule,中途:
- pod restart(`kubectl delete pod ...`,OnDelete 策略下不会自动起,手动 delete + 等 pod 自起)
- 启动后 syncnode 应在第一次心跳前 POST reconcile,master 立即把对应 shard 标 failed
- 不依赖 30s 超时扫描

## 5. 风险与缓解

| 风险 | 缓解 |
|---|---|
| BenchTaskLedger 是内存 FIFO,master 重启后所有任务记录丢失,孤儿扫描扫不到 | 不在本轮解决,master 重启目前就是会丢任务记录(已知问题);本轮只解决 syncnode 异常导致的卡死,不解决 master 异常 |
| admission 估算公式不准(libaio 实际峰值难精确算) | 取 conservative 估计 `bs × iodepth × numjobs × 1.5`(1.5 是 fio buffer + 内核 pagecache 放大系数),× 多 stage 并发时取最大 stage |
| OpSyncNodeReject / Reconcile 新 opcode 与旧 syncnode 不兼容 | test-k3d 全栈用同一镜像版本,无兼容性问题;test-hb 部署时需要同步升级 master + syncnode 镜像 |
| reconcile 上报时 master 还没起来 | syncnode 启动时 master_client 已有重连逻辑,reconcile 跟着心跳的重试机制走 |
| 孤儿扫描误判(syncnode 心跳延迟但实际还活着)| 阈值取 3 × 心跳间隔 = 90s(syncnode 心跳是 10s),足够容错 |

## 6. 进度

| 时间 | 事项 |
|---|---|
| 2026-05-26 上午 | 完成根因分析 + 用户决策对齐(标 failed 不重派,syncnode 拒绝 master 重选) |
| 2026-05-26 上午 | 完成 master / syncnode 代码调研(2 个 Explore agent) |
| 2026-05-26 上午 | 起草本执行文档 + 开始 Phase A |

## 7. 下一步

1. 完成 Phase A 两个独立任务(test-hb 资源 override + 本文档)
2. 开始 Phase B:按 B1 → B2 → B3 → B4 → B5 → B6 顺序在 cubefs 仓库改代码
3. 改完 Phase B 后请用户在 dev_bd 上构建镜像(用户操作)
4. Phase C E2E 测试由本会话执行
