# Scheduler 主备高可用方案设计

本文给出 BlobStore Scheduler 控制面（磁盘修复、均衡、下线、手工迁移、卷巡检）的主备高可用设计。
当前 Scheduler 通过配置静态指定 Leader；Leader 宕机后迁移类任务无法自动接任。
本文方案用 clustermgr 上的租约选举替代静态主，使选民节点可自动抢主与接任，同时保持 Blob 删除与 Shard 修补的多节点 Kafka 消费。

**范围**：机制与任务拆解。实现接口与状态机细节可在后续代码 PR 中落地。

## 1. 要保护的范围

只有 Leader 在跑、且状态已在 clustermgr 的功能需要自动接任：


| 需要接任              | 原因                                                             |
| ----------------- | -------------------------------------------------------------- |
| 磁盘修复、均衡、磁盘下线、手工迁移 | 队列在主进程内存里。任务本体在 KV，主死之后没有人 collect、prepare、finish，也没有人接受续租     |
| 卷巡检               | 批次在主进程内存里，checkpoint 在 KV。主死之后巡检停止，进行中的一批可以丢掉，下次从 `nextVid` 继续 |
| 正常盘拓扑、Kafka 积压监控  | 只为主上的均衡和观测服务                                                   |


不收进选主、继续在每个节点上跑：


| 保持现状             | 原因                                                |
| ---------------- | ------------------------------------------------- |
| Blob 删除、Shard 修补 | 已经是 Kafka 消费组。节点退出后分区会再分配。把它们收成单主会降低吞吐，也会在切换时中断消费 |
| 服务注册、卷缓存、坏盘缓存    | 每个节点都要有。删除和修补读本机卷缓存                               |


scheduler 选用主备模式，不做多主分片。

`VolTaskLocker`、按 IDC 的 work 队列、每盘并发都假设进程内只有一个写者。（同一条 `Prepared` 任务若被两个进程 `Acquire`，clustermgr 的卷锁挡不住，因为卷锁在 prepare 时已经加上。）

## 2. 多节点如何抢主

目标形态就是租约选举，不再保留静态主双轨，也不做 `election_enable` 一类开关。谁是主只看 clustermgr 上的租约记录；配置里的 `services.leader` 废弃，运行期与启动路径都不读它。

选民是配置里的 `services.candidates`（见 §2.1）。租约由 clustermgr 新建模块 `schedulermgr` 专用路径读写（Campaign / Renew / Get），逻辑键仍为 `scheduler/leader`；**不走**通用 `SetKV`。clustermgr 用 Raft 复制该状态，scheduler 不再自建共识。具体 HTTP / Raft apply 接口在实现阶段落地（Campaign / Renew / Get）。

记录字段：


| 字段          | 含义                                          |
| ----------- | ------------------------------------------- |
| `node_id`   | 当前主的配置 id                                   |
| `host`      | 当前主的 `service_register.host`，供反代和 worker 使用 |
| `epoch`     | 单调递增的任期                                     |
| `expire_at` | 租约到期时间                                      |


现有 `SetKV` 在 Raft apply 时直接覆盖。抢主不能先 Get 再 Set，后写入的会盖掉先写入的：

```go
// blobstore/clustermgr/kv.go:60-91
func (s *Service) KvSet(c *rpc.Context) {
	// ...
	err = s.raftNode.Propose(ctx, base.EncodeProposeInfo(s.KvMgr.GetModuleName(), kvmgr.OperTypeSetKv, data, base.ProposeContext{ReqID: span.TraceID()}))
	// ...
}
```

```go
// blobstore/clustermgr/kvmgr/applier.go:57-71
		case OperTypeSetKv:
			kvSetArgs := &clustermgr.SetKvArgs{}
			// ...
			t.taskPool.Run(t.getTaskIdx(kvSetArgs.Key), func() {
				errs[idx] = t.Set(kvSetArgs.Key, kvSetArgs.Value)
				wg.Done()
			})
```

需要新增一次 apply 内的比较写入：期望值匹配，或键不存在，或 `expire_at` 已过，才写入新记录并把 epoch 加一。比较发生在状态机里，不在客户端。同一 key 的写入已按哈希进同一执行槽串行，CAS 必须进入同一槽。

每个**选民**一个循环：

1. 读租约。
2. 空、过期，或持有者是自己且快到期：发起比较写入。
3. 写入成功：本进程进入该 epoch 的主。
4. 持有者是自己：按 1 秒周期续约，把 `expire_at` 往后推。续约也走比较写入，期望 epoch 必须仍是自己的。
5. 续约失败，或连不上 clustermgr：立即卸任。网络分区里的旧主不能继续 `Acquire`。

建议租约 10 秒，续约 1 秒。租约要盖住 worker 的续租周期（5 秒）一次失败，又让新主不用等分钟级。

冷启动或租约键为空时，各选民同时 CAS，先写入成功者成为第一任主，不依赖配置里谁“本该是主”。从今天的静态主升级时：去掉对 `services.leader` 的依赖后滚动重启选民即可；旧主若仍按静态逻辑发任务，会在租约与 epoch 校验（第 3 节）下被挡住，直到它也升到选举版本。

### 2.1 选民范围：`candidates` 与 `members`

实际部署里，scheduler 常与 clustermgr 同机部署。clustermgr 可能有很多节点，但 **scheduler 选主与 clustermgr 的 Raft 成员无关**：前者是 `schedulermgr` 租约上的竞选/续约，后者是另一套共识。同机只是运维习惯，不是代码耦合。

因此会出现：集群里跑着很多 scheduler 进程（为了分摊 Blob 删除、Shard 修补的 Kafka 分区），但控制面抢主只希望 **3 或 5** 个节点参与。

**配置分工：**


| 字段                    | 含义                                                        |
| --------------------- | --------------------------------------------------------- |
| `services.candidates` | **选民表**。仅这些 `node_id` 跑租约循环、预建控制面、可成为主。推荐 3 或 5           |
| `services.members`    | **更大集合**（运维可见的 scheduler 节点列表，可含永久从）。不决定谁竞选，也不作为卷缓存扇出唯一来源 |
| `services.node_id`    | 本机 id。∈ `candidates` → 选民；否则 → 永久从                        |
| `services.leader`     | 废弃，运行期不读                                                  |


示例：

```json
{
  "node_id": 10,
  "candidates": {
    "1": "10.0.0.1:9800",
    "2": "10.0.0.2:9800",
    "3": "10.0.0.3:9800"
  },
  "members": {
    "1": "10.0.0.1:9800",
    "2": "10.0.0.2:9800",
    "3": "10.0.0.3:9800",
    "10": "10.0.0.10:9800"
  }
}
```

规则：

1. 只有 `node_id ∈ candidates` 的进程才竞选；建议各选民的 `candidates` 集合一致。误写入会参与竞选，漏写则永远当不了主。
2. 永久从：不在 `candidates`，仍注册到 clustermgr、跑删除和修补、反代到租约 `host`；不创建控制面管理器。宜出现在 `members` 或至少完成 `service_register`。
3. 建议 `candidates ⊆ members`，便于对照；校验上选民必须在 `candidates`，`leader` 不再参与 `fixServices`。
4. 小集群若总共只有 2～3 台 scheduler，可令 `candidates` 与 `members` 相同，不必再拆永久从。

**是否有必要把选民限制为 3/5：**


| 维度      | 选民很多（例如等于全部同机 CM 节点）             | 选民 3 或 5   |
| ------- | -------------------------------- | ---------- |
| 切换时延    | 主要由租约 TTL（约 10 秒）决定，候选数几乎不缩短等待   | 相同         |
| 竞选开销    | 过期瞬间多人同时 Campaign，多余 propose 被拒绝 | 竞争面小       |
| 备机资源    | 每个选民都要预建控制面管理器（见第 4 节），随 N 增长    | 可控         |
| 运维面     | 任意一台同机进程都能当主，滚动、配错更难排查           | 主备角色清晰     |
| 删除/修补吞吐 | 与选民数无关；靠加入 Kafka 消费组的进程数         | 消费节点可以多于选民 |


结论：**有必要在部署上把选民收窄到 3 或 5**，不是为了“选得更快”，而是为了控制备机成本与运维边界。用独立 `candidates` 正是为了让 `members` 仍可表示更大集合，而不把所有同机进程都拉进竞选。

**卷缓存扇出：** 当前只通知配置算出的 `followerHosts`。改造后（A1）**强制**用 `GetService(SCHEDULER)` 取注册表主机并去掉自己，不把 `members`/`candidates` 当作扇出表。永久从依赖注册表扇出与 A2 过期刷新，避免长期旧映射。

## 3. 如何避免多主

两层，缺一不可：

1. **租约只有一个 epoch 能写成功。** 比较写入在 Raft apply 中执行，同一时刻最多一个 `node_id` 持有未过期租约。
2. **旧任期的进程必须停止发任务。** 只靠“新主等一会儿”不够。旧主如果还活着并且还能访问 worker，会继续把 doing 队列里的任务发出去。因此：
  - 领取、准备、完成、收集新任务之前，读一次租约，本地 epoch 不一致就拒绝。
  - 卸任时立刻停掉修复、均衡、下线、手工迁移、巡检和 Kafka 监控的循环，并清空这四类迁移的内存队列。
  - 卸任不得调用今天的 `Service.Close`。`Close` 会把 Blob 删除和 Shard 修补的 consumer 一起停掉。控制面关闭要单独一条路径：

```go
// blobstore/scheduler/startup.go:353-366
// Close close service safe
func (svr *Service) Close() {
	log.Infof("stop scheduler service")
	svr.blobDeleteMgr.Close()
	svr.shardRepairMgr.Close()
	if !svr.leader {
		return
	}
	svr.CloseKafkaMonitors()
	svr.balanceMgr.Close()
	svr.diskRepairMgr.Close()
	svr.diskDropMgr.Close()
	svr.manualMigMgr.Close()
	svr.inspectMgr.Close()
}
```

- 续约失败的原因包括 clustermgr 不可达。旧主在分区侧无法续约，必须停循环，而不是用本地缓存继续当主。

`VolTaskLocker` 继续留在进程内。集群互斥由“只有一个 epoch 在跑控制面”保证，不另做锁服务。

## 4. 控制面管理器的创建与启停

当前机制 `NewService`（`scheduler/startup.go`）按配置静态主分支：非主于 `RunTask` 之后直接返回：

```go
// blobstore/scheduler/startup.go:149-217
	if err = svr.RunTask(); err != nil {
		return nil, err
	}

	if !svr.leader {
		return
	}

	err = svr.NewKafkaMonitor(conf.ClusterID)
	// ... 创建 balance / diskDrop / diskRepair / manualMigrate / inspect ...
	err = svr.waitAndLoad()
	// ...
	go svr.Run()
```

结果是：非主进程里根本没有 Kafka 监控、均衡、磁盘下线、磁盘修复、手工迁移、卷巡检这些对象。选举方案下任意选民都可能升主；若仍按启动时的 `conf.IsLeader()` 决定是否创建，备机升主时会空指针或只能重启进程才能接任，自动 Failover 不成立。必须拆开创建与运行（见下）。

### 4.1 阶段划分


| 阶段    | 做什么                                             | 谁做                          | 何时    |
| ----- | ----------------------------------------------- | --------------------------- | ----- |
| 常驻面   | 注册、卷/坏盘缓存、Blob 删除、Shard 修补、`RunTask`            | 所有 scheduler 进程（含永久从）       | 启动时一次 |
| 控制面创建 | 构造 Kafka 监控与五个管理器对象；**不** `Load`、**不** `Run`    | 仅选民（`node_id ∈ candidates`） | 启动时一次 |
| 控制面运行 | `waitAndLoad`（或等价等待 + `Load`）、`Run`、加载正常盘拓扑、开监控 | 当前租约持有者                     | 每次升主  |
| 控制面停止 | 停五个管理器与 Kafka 监控循环，清空迁移内存队列；**不**停删除/修补         | 刚卸任的原主                      | 每次卸任  |


进程退出仍走完整 `Close`。卸任不得调用当前的整段 `Close`（见第 3 节）。

### 4.2 启动路径

唯一路径（无静态主分支）：

1. 所有节点：创建常驻面并 `RunTask`。
2. 本机是选民：继续创建控制面对象，挂到 `Service` 上；然后启动租约循环，**不要**在 `NewService` 里调用 `waitAndLoad` / `Run`。
3. 本机是永久从：不创建控制面对象，不跑租约循环；HTTP 反代到租约 `host`。
4. 租约循环里竞选成功 → 走第 5 节接任顺序（等待、`Load`、`Run`）。
5. 续约失败 → 走第 3 节卸任（只停控制面）。

拓扑里当前用启动时的 `Leader: conf.IsLeader()` 决定是否 `loadNormalDisks`。改为跟运行期身份走：升主后再加载/刷新正常盘；卸任后停止为均衡服务的正常盘刷新。卷缓存与坏盘缓存仍常驻。删除 `Service.leader` / `conf.IsLeader()` 在启动与请求路径上的用法，运行期身份只来自本进程是否持有未过期租约。

与现有 `NewService`（`scheduler/startup.go`）的对照如下。左侧是当前的骨架，右侧是改造后的骨架（伪代码，表示结构变化，不是最终实现）：

**当前：**

```go
func NewService(conf *Config) (*Service, error) {
	svr := &Service{
		leader:        conf.IsLeader(),   // 启动写死
		leaderHost:    conf.Leader(),     // 来自 services.leader
		followerHosts: conf.Follower(),
	}
	// 常驻面：拓扑(Leader: conf.IsLeader())、删/修、register
	svr.RunTask()

	if !svr.leader { // 非配置主直接返回
		return svr, nil
	}

	// 仅配置主：创建监控 + 五个管理器
	svr.NewKafkaMonitor(...)
	svr.balanceMgr = NewBalanceMgr(...)
	svr.diskDropMgr = NewDiskDropMgr(...)
	svr.diskRepairMgr = NewDiskRepairMgr(...)
	svr.manualMigMgr = NewManualMigrateMgr(...)
	svr.inspectMgr = NewVolumeInspectMgr(...)

	svr.waitAndLoad() // 启动时就 Load
	go svr.Run()      // 启动时就跑控制面
	return svr, nil
}
```

**改造后：**

```go
func NewService(conf *Config) (*Service, error) {
	svr := &Service{
		// 不再设置 leader / leaderHost 静态字段
		// followerHosts 可改为运行期从注册表或选民表派生；反代目标读租约 host
		candidate: conf.IsCandidate(), // node_id ∈ services.candidates
	}
	// 常驻面：拓扑不再用 conf.IsLeader()；删/修、register 不变
	svr.RunTask()

	if !svr.candidate { // 永久从：无控制面、无租约循环
		return svr, nil
	}

	// 选民：只创建控制面对象，不 Load、不 Run
	svr.NewKafkaMonitor(...) // 或延到升主再 Start；对象先挂上
	svr.balanceMgr = NewBalanceMgr(...)
	svr.diskDropMgr = NewDiskDropMgr(...)
	svr.diskRepairMgr = NewDiskRepairMgr(...)
	svr.manualMigMgr = NewManualMigrateMgr(...)
	svr.inspectMgr = NewVolumeInspectMgr(...)

	go svr.leaseLoop() // 竞选 / 续约 / 失败则 demoteControlPlane()
	// waitAndLoad + Run 挪到竞选成功之后（见第 5 节）
	return svr, nil
}

// 租约循环内（示意）
func (svr *Service) onElected(epoch uint64) {
	svr.waitAndLoad() // 或 wait + Load
	go svr.Run()
	// 再加载正常盘拓扑、启动监控循环
}

func (svr *Service) demoteControlPlane() {
	// 只停监控 + 五个管理器并清空队列；不停 blobDeleteMgr / shardRepairMgr
	// 不得调用今天的 Service.Close()
}
```

逐项对比：


| 点                | 原有                                   | 改造后                                   |
| ---------------- | ------------------------------------ | ------------------------------------- |
| 主身份              | `conf.IsLeader()` 启动写死到 `svr.leader` | 废弃；持有未过期租约才是主                         |
| `RunTask` 之后早退条件 | `!svr.leader`（非配置主）                  | `!candidate`（非选民 / 永久从）               |
| 五个管理器 + Kafka 监控 | 仅配置主创建，并立刻 `waitAndLoad` + `Run`     | 选民创建对象；`waitAndLoad` + `Run` 仅在升主后    |
| 反代目标             | 启动时的 `leaderHost`（`services.leader`） | 租约记录里的 `host`                         |
| 卸任               | 无（进程不换主）                             | `demoteControlPlane()`，不是整段 `Close()` |
| 拓扑正常盘            | 启动时按 `IsLeader()` 决定是否加载             | 升主后再加载/刷新，卸任后停刷新                      |




### 4.3 对象是否每次升主重建

不重建，**选民启动时创建一次，卸任只 Stop/清空队列，再升主再次** `Load` **+** `Run`。

- 优点：升主时少一次构造与依赖注入，接任路径更短。
- 约束：卸任必须把 prepare / work / finish 与巡检内存态清干净，避免下一任期残留旧 epoch 的任务指针。`Load` 是从 KV 重建队列的唯一入口。
- 若某管理器 Close 后不能再次 `Run`，实现上改为“可停可开”，或卸任后置空、升主时再 `New*Mgr`。这属于实现细节，完成标准是：同一进程经历「备 → 主 → 备 → 主」两次，第二次发放的任务只来自当时 KV，不出现上一任期的内存幽灵任务。

永久从不预建控制面，避免 N 很大时每台同机进程都占一份均衡/修复管理器的内存。

## 5. 旧主消失后新主如何接任

旧主崩溃时不会主动卸任，租约自然过期。任意**选民**在看到过期后竞选成功。永久从不参与竞选。

新主的顺序固定，复用现有 `waitAndLoad`，不另写一套恢复。与当前的差别是：这段逻辑从 `NewService` 里挪到“竞选成功”之后执行（见第 4 节）。

```go
// blobstore/scheduler/startup.go:220-226
func (svr *Service) waitAndLoad() error {
	//why:service stop a task lease period to make sure all worker release task
	//so there will not a task run on multiple worker
	log.Infof("start waitAndLoad")
	time.Sleep(proto.TaskLeaseExpiredS * time.Second)
	return svr.load()
}
```

1. 比较写入成功，记下 epoch 和 host。控制面对象此时应已存在（选民启动时已创建）。
2. 停止把请求反代出去，开始自己处理。
3. 等待 `max(旧租约剩余时间, TaskLeaseExpiredS)`。旧主已崩溃时，这一等待的主要目的是让 worker 的 10 秒租约过期并停手，避免新队列和旧 worker 同时搬同一卷。
4. 从 clustermgr 加载未完成的修复、均衡、下线、手工迁移，装入 prepare / work / finish（`MigrateMgr.Load` 按 KV 状态分入 prepare / work / finish 队列）。
5. 启动这些循环和巡检。巡检从 KV 里的 checkpoint 继续，内存中未完成的一批不恢复。
6. 加载正常盘拓扑，启动 Kafka 监控。

KV 中的状态与接任后的队列：


| KV 状态           | 新主行为                                                                                                                  |
| --------------- | --------------------------------------------------------------------------------------------------------------------- |
| `Inited`        | 放入 prepare，重新分配目标                                                                                                     |
| `Prepared`      | 放入 work，可被 worker 再次领取。数据若已部分拷贝，再执行一次                                                                                 |
| `WorkCompleted` | 放入 finish。`UpdateVolume` 发现映射已经是目标位置时按成功继续，然后解锁并删任务。磁盘修复只有 finish 开始后才会把该状态写入 KV；写入之前崩溃则 KV 仍是 `Prepared`，会再发给 worker |
| 巡检 checkpoint   | 从 `nextVid` 开始下一批。丢失的是旧主内存里那一批的修补消息，下一轮扫到仍会补                                                                          |


目标是旧主消失后大约 30 秒内，新主开始对外发放迁移任务：租约 10 秒，加上现有的 10 秒 worker 租约等待，再加上一次任务加载。加载时间随任务条数增加。

旧主若是活着被抢掉任期，而不是崩溃，它在续约失败的那一次循环里走第 3 节的卸任。新主仍等待满 10 秒再 `Load`，给旧 worker 停手留出时间。

## 6. 正在执行的任务怎么续租

续租状态只在当前主的 work 队列里。切换时不把 doing 队列同步给新主。

### 6.1 blobnode worker 逻辑不变

blobnode worker 现有发现与停手语义保持即可（`api/scheduler` 随机选注册表节点；`task_runner_mgr.go` `renewalTask`）：


| 时机                             | 行为                              |
| ------------------------------ | ------------------------------- |
| 每 5 秒续租整次失败（超时、连不上、反代失败、旧主拒绝等） | `StopAllAliveRunner`，本机迁移任务全部停止 |
| 某个 task id 续租返回错误              | 只停该 runner                      |
| 任务停手                           | 已拷贝的数据留在目标盘。调度状态以 KV 为准         |


切换时的主路径：

1. worker 仍随机打 `SCHEDULER` 注册表，请求可能落到 Follower / 永久从 / 旧主 / 新主。
2. Follower / 永久从把领取、续租、完成等反代到租约里的 `host`（**scheduler 侧必须改**；不能再用启动时的静态 `services.leader`）。
3. 已卸任的旧主对写路径（含 renewal / complete / acquire）按 epoch 拒绝。
4. worker 续租失败后停手；新主 `Load` 后把 KV 里的 `Prepared` 再发放、`WorkCompleted` 进 finish → **任务重入**，不依赖旧 runner 把续租续上。

因此：打到旧主被拒绝、本次执行放弃、靠 KV 重入，是预期行为，不是缺陷。旧 worker 若在停手前又发一次 `POST /task/complete`，必须由**新任期**主处理；旧主已卸任则拒绝。新主 finish 按卷映射是否已更新做重入，重复完成不会把映射改乱。

**本期硬性要求在 scheduler**：反代读租约 `host`；旧主拒绝；新主 wait + `Load`。主地址为空或连不上时反代返回明确错误，不 panic。`/stats` 仍本机；`/stats/leader` 由当前主应答。

### 6.2 可选优化：worker 优先打租约 host

不改 worker 的代价是：切换窗口里更容易大面积 `StopAllAliveRunner`，要等到随机打中能转到新主的节点后才恢复，Failover 更钝。

可选优化（非正确性门槛）：blobnode 读取 `scheduler/leader` 的 `host`，领取 / 续租 / 完成等优先直连，失败再退回注册表随机。这与第 8 节 F2 对应，可排在控制面跑通之后。

Shard 修补和 Blob 删除没有续租，切换控制面时 consumer 不停。它们不包含在「执行中任务的续租」里。

卷缓存有一个独立的故障：当前 Leader 同步通知每个 Follower，任一失败则 `HTTPUpdateVolume` 失败，finish 可能因此反复重试并把卷留在锁定状态。主备切换不解决这个问题，还会在滚动重启备节点时碰到它。实现时要先改成：本机卷缓存更新失败才返回错误；通知 Follower 失败只记录并异步重试。Follower 对过期的 vid 从 clustermgr 刷新，避免删除和修补长期使用旧映射。选民收窄后，不在 members 里的永久从更依赖这条刷新路径。

## 7. clustermgr 边界


| clustermgr 做                                                 | clustermgr 不做                                     |
| ------------------------------------------------------------ | ------------------------------------------------- |
| 用 Raft + `schedulermgr` 保存租约，Campaign/Renew 保证一个 epoch 一个持有者 | 不运行 scheduler 的任务循环，也不在主宕机时主动调用 scheduler         |
| 继续保存迁移任务、巡检 checkpoint、任务开关、卷锁和卷映射                           | 不感知 blobnode 的续租。续租仍是 worker 到 scheduler 主进程      |
| 继续提供 `SCHEDULER` 与 `BLOBNODE` 服务列表                           | 不把“谁是 scheduler 主”写进服务注册；主身份只在 `schedulermgr` 租约里 |
| 不限制有多少 scheduler 注册                                          | 不解释 `candidates`/`members`；选民范围是 scheduler 本地配置   |


scheduler 在租约读写失败时卸任，把“看不到 clustermgr”当成“我不是主”。这和 clustermgr 自己非主节点不处理写请求是同一方向：写路径跟着 Raft 的主，调度路径跟着租约的主。

## 8. 任务拆解

对应后续的代码任务。合入即启用选举，不设兼容开关。杀主、双主、选民+永久从演练验收。


| 编号  | 内容                                                       | 依赖         | 完成标准                                           |
| --- | -------------------------------------------------------- | ---------- | ---------------------------------------------- |
| S0  | 冻结 `LeaderRecord` 和选主接口：竞选、续约、卸任、读取当前任期                  | 无          | 字段与第 2 节一致，错误分为非主、epoch 不一致、租约丢失               |
| S1  | clustermgr 新建 `schedulermgr`：Campaign / Renew / Get      | S0         | 并发竞选只有一方成功；不走通用 `SetKV`；原 KV 行为不变              |
| A1  | 卷缓存扇出改为尽力而为；host 来自 `GetService(SCHEDULER)`              | 无，可与 S1 并行 | 备节点不可达时，已写入 clustermgr 的卷映射仍能 finish 并解锁       |
| A2  | 扇出失败异步重试；Follower / 永久从卷缓存过期后向 clustermgr 刷新             | A1         | 删除和修补在通知丢失后一个刷新周期内看到新映射；注册表内节点均可自愈             |
| C1  | 引入 `candidates` 选民表；`members` 为更大集合；废弃 `services.leader` | S0         | 仅 candidates 内跑租约循环；永久从只消费 Kafka 并反代           |
| C2  | 选民启动时创建控制面对象但不 `Run`；升主再 `Load`/`Run`；卸任只停控制面            | C1、S0      | 同一进程「备→主→备→主」第二次任务只来自 KV；永久从不占控制面内存            |
| D1  | 租约循环：竞选、1 秒续约、失败即卸任；删除 `NewService` 静态主早退分支              | S1、C1、C2   | 启动后必经选举才发控制面任务；非选民不竞选                          |
| D2  | 领取、准备、完成、收集任务前校验 epoch                                   | D1         | 旧 epoch 的 Acquire 和 Complete 失败                |
| E1  | 升主后按第 5 节等待并 `Load`，再启动控制面                               | D1、C2      | `Inited` / `Prepared` / `WorkCompleted` 进入对应队列 |
| E2  | 卸任只停控制面                                                  | S0、C2      | Blob 删除和 Shard 修补的 consumer 在卸任后仍在             |
| E3  | 反代目标改为租约 host                                            | D1         | 运行期反代只读租约 host；连不上时返回错误；worker 仍随机打注册表也能经反代到主  |
| F1  | `/stats/leader` 由当前主应答；租约 host 可读（供观测与可选 F2）             | S0         | 非主反代或返回当前租约信息；不阻塞 worker 不动的主路径                |
| F2  | （可选）blobnode 优先向租约 host 领取和续租                            | F1、E3      | 失败再退回注册表随机；不改也不影响正确性，只改善切换体感                   |
| H1  | 指标：是否为主、是否选民、epoch、租约剩余时间                                | D1         | 没有主时可告警；选民数与注册 scheduler 数可对照                  |
| G1  | 双主测试                                                     | D2         | 两个进程同时竞选只有一个 epoch 能发任务                        |
| G2  | 备节点宕机时的 finish 测试                                        | A1         | 卷能解锁                                           |
| G3  | 杀掉当前主的预发演练（worker 保持现网随机发现）                              | E1、E2、E3   | 约 30 秒内新主开始发放；worker 因续租失败停手后任务经 KV 重入；卷映射仍在   |
| G4  | 选民 3 台 + 永久从若干台的演练                                       | C1、C2、E1   | 只有选民能升主；杀掉主后由另一选民接任；永久从消费不中断                   |
| G5  | 从静态主配置滚动升级到选举版本的演练                                       | D1、E3      | 升级中至多短暂无主；升级完成后无进程再按 `services.leader` 发任务     |


建议顺序：A1 与 S0/S1 并行。C1/C2 与 S1 并行合入后，再做 D1、D2。E1、E2、E3 依赖 C2/D1。G3/G4/G5 在控制面切换跑通后做。部署上选民固定 3 或 5，Kafka 消费节点按删除/修补吞吐另扩，不必等于选民数。