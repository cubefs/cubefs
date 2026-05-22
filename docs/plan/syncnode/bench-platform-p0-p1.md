# Bench 平台 P0/P1 阶段性执行文档

> 状态：草案 → 待 review 后进入开发
> 起始日期：2026-05-22
> 涉及仓库：cubefs（syncnode + master + 镜像）、cubefs-dashboard、cubefs-deploy
> 上游需求：将 cubefs 作为产品级测试平台，覆盖 GPFS（local）+ S3 两侧的大规模压测；当前 bench 能力（fio/mdtest/s3-ops/shard）无法满足跨 shard 聚合统计、压力曲线协调、对象大文件、SLA 判定等核心场景。

---

## 1. 目标

让 syncnode 的 bench 能力达到产品级测试平台水平，支撑以下用户旅程：

1. AI agent 通过 MCP 工具一键创建/触发/查询 bench，并在 Claude 对话内得到结构化结果。
2. 人通过 dashboard 页面查看实时进度（live 曲线）+ 终态结果（含尾延迟、错误分桶、SLA 判定）。
3. 跨 shard 的延迟统计在聚合后仍然准确（HDR 直方图合并，不是简单拼接 p99）。
4. stage 切换有显式 barrier；steady stage 内可按目标 IOPS / BW ramp-up，找拐点。
5. 大对象 S3 压测可调 multipart part size / 并发 + range-GET。
6. GPFS 压测覆盖 IOR（POSIX + MPI-IO），不再只有 mdtest（元数据）+ fio（单机）。
7. 给 BenchRule 配 SLA（硬字段），跑完自动出 PASS/FAIL，CI 可消费。

## 2. 范围边界（不做什么）

显式排除，避免范围漂移：

- ❌ **不做 CLI**：人用 dashboard，AI 用 MCP，CLI 重复建设。
- ❌ **不做 chaos 注入**：依赖 chaos-mesh，超出本期。
- ❌ **不做跨版本基线 / regression detection**：dashboard 内只展示终态 + SLA，不做历史趋势对比。
- ❌ **不做 trace 回放**、**不做 PDF 报告导出**、**不做 GPFS native 命令**、**不做 versioned bucket / cross-region copy** 等 P2 项。
- ❌ **不动 SyncRule / 同步任务**的执行链路（MCP 只读 + 触发）。

## 3. 关键设计决策（已 review 拍板）

| # | 议题 | 决策 |
|---|---|---|
| A | CLI | **不做** |
| MCP | server 跑哪 | **独立二进制 `cubefs-mcp`**，本地拉起，Claude Desktop/Code stdio 集成；HTTP 连 master REST |
| MCP | 工具范围 | **bench + sync + 集群状态查询**（cluster.health / node_list / metrics_query） |
| B | 实时指标承载 | **走 Prometheus**（syncnode pod `/metrics`），减少 master 压力；dashboard Grafana panel；最终结果 master REST |
| C | 直方图库 | **`github.com/HdrHistogram/hdrhistogram-go`**（合并 bucket 用），同时 Prometheus 端用自定义 bucket histogram |
| D | IOR 镜像 | **syncnode pod multi-container**（cubefs-bench-tools sidecar），共享 hostPath；syncnode 通过 exec 进 sidecar 跑命令 |
| E | Barrier 协议 | **Consul KV + session**：每 shard 写 ready key，master watch 全集；60s 超时则 quorum 放行 + 任务打 `degraded` 标记 |
| F | SLA 表达式 | **硬字段**：`p99_ms_max` / `p999_ms_max` / `bw_mibs_min` / `iops_min` / `error_rate_max` |
| 精度 | Prometheus histogram | **自带 histogram + 多 bucket**（30 bucket 覆盖 100us~10s），p99.9 误差可控（5-10%） |

## 4. 验收标准（总）

满足以下全部条件视为本期交付完成：

- [ ] MCP server 二进制可在本地跑，Claude Desktop/Code 配置后可执行 bench/sync/cluster 全部 tool
- [ ] dashboard 上 bench 任务页面有「实时曲线 tab」（Grafana iframe 或 embed panel）+「终态结果 tab」（p50/95/99/99.9/99.99/max + 错误分桶 + SLA badge）
- [ ] HDR 直方图跨 shard 合并后 p99 与单 shard 的 p99 数学一致（单元测试 + 集成测试）
- [ ] stage 切换 barrier 在 test-k3d 环境验证（3 shard 启动时间偏移 10s+，stage 内同时起跑）
- [ ] ramp-up 模式 0→目标 BW 渐进，Prometheus 上能看到匀速上升曲线
- [ ] S3 端可配 PartSize / Concurrency / RangeSize，bench 结果中体现这三个参数对吞吐的影响
- [ ] IOR sidecar 在 test-k3d 跑通：4-rank `ior -t 1m -b 1g -F` 成功，结果回流 BenchStageResult
- [ ] SLA 硬字段在 dashboard 配置，跑完后 dashboard 显示 PASS/FAIL；MCP `bench.evaluate_sla` 返回结构化结果
- [ ] 全部组件镜像在 test-k3d apply 通过；不破坏现有 sync / posix-compliance 链路
- [ ] 文档：本文件 + `docs/plan/syncnode/bench-mcp-tools.md`（MCP tool schema）+ deploy 仓 `docs/` 对应小节

---

## 5. 分阶段任务

### Sprint 1（P0 核心，目标 2 周）

#### 1.1 MCP server 骨架（cubefs/cmd/cubefs-mcp）
- 新建 `cubefs/cmd/cubefs-mcp/main.go`，stdio MCP transport
- 引入 MCP go SDK（`github.com/mark3labs/mcp-go` 或 anthropic 官方）
- 配置：通过环境变量 `CUBEFS_MASTER_ADDR` + `CUBEFS_AUTH_TOKEN`
- 暴露 ping tool 验证链路

#### 1.2 MCP tools - bench
- `bench.list_rules` → master `GET /benchRule/list`
- `bench.create_rule(rule_json)` → master `POST /benchRule/create`
- `bench.update_rule(id, rule_json)` → `POST /benchRule/update`
- `bench.trigger(rule_id)` → 返回 `task_id`
- `bench.get_task_status(task_id)` → `{ state, progress_pct, current_stage, elapsed_sec, eta_sec }`
- `bench.get_result(task_id)` → 完整 `BenchResult`（含每 stage、p50/95/99/999/9999/max、bw、iops、err_by_kind）
- `bench.evaluate_sla(task_id, sla)` → `{ pass, reasons }`

#### 1.3 MCP tools - sync + cluster
- `sync.list_rules` / `sync.trigger(rule_id)` / `sync.get_task_status(task_id)` / `sync.get_result(task_id)`
- `cluster.health()` → master + metanode + datanode 状态摘要
- `cluster.node_list(role)` → 节点清单
- `cluster.metrics_query(promql, range)` → Prometheus passthrough（master 端代理鉴权）

#### 1.4 HDR 直方图跨 shard 聚合
- 引入 `github.com/HdrHistogram/hdrhistogram-go`
- syncnode bench shard：op 完成时 `hdr.RecordValue(latUs)`；stage 结束序列化 bucket counts（base64 gzip）
- shard → master 报告 payload 增加 `latency_histogram` 字段
- master 端 stage 收齐所有 shard histogram，`hdr.Merge()` 后计算 p50/95/99/999/9999/max
- 替换现有 `simpleHistogram` 路径，但保留 stage 内单 shard 显示
- 单元测试：合并后 p99 与全样本 p99 数学一致（误差 < 1%）

#### 1.5 Prometheus 指标
- syncnode pod 暴露 `:9090/metrics`（或选一个未占用端口）
- 指标：
  ```
  syncnode_bench_op_latency_seconds_bucket{task_id, shard, stage, op, le}
  syncnode_bench_op_latency_seconds_count / sum
  syncnode_bench_op_bytes_total{task_id, shard, stage, op}
  syncnode_bench_op_errors_total{task_id, shard, stage, op, kind}
  syncnode_bench_stage_state{task_id, shard, stage, state}  # 0=pending 1=running 2=done 3=failed
  ```
- bucket 边界：30 个，几何分布覆盖 100us~10s
- cubefs-deploy/modules/monitoring：prometheus scrape job 加 syncnode pod target；grafana dashboard 加 bench 面板（live BW / IOPS / lat percentiles / err rate）

#### 1.6 Barrier + ramp-up（Consul）
- BenchRule schema 增加：
  ```go
  type StageControl struct {
      RampUpSec      int     // 0 = 不 ramp-up
      SteadySec      int     // ramp-up 完成后稳态秒数（runtime 不变）
      RampDownSec    int
      TargetIOPS     int     // ramp-up 目标值，0 表示不限速
      TargetBwMiBs   float64
      WaitForPeers   bool    // true 才走 barrier
      BarrierTimeoutSec int  // 默认 60
  }
  ```
- syncnode 进入 stage 前：consul `kv.Put("bench/{task_id}/{stage}/ready/{shard_idx}", "")` + session attach
- master 端 watch `bench/{task_id}/{stage}/ready/` 前缀；收齐 N 个或超时后写 `bench/{task_id}/{stage}/release`
- syncnode block on `kv.Get(release)` 才进入 stage 主循环
- ramp-up：`golang.org/x/time/rate` 令牌桶，速率线性增长

#### 1.7 BenchRule SLA 字段（dashboard + cubefs）
- spec/bench_rule.go 新增：
  ```go
  type BenchSLA struct {
      P99MsMax     float64 `json:"p99MsMax,omitempty"`
      P999MsMax    float64 `json:"p999MsMax,omitempty"`
      BwMiBsMin    float64 `json:"bwMiBsMin,omitempty"`
      IopsMin      int     `json:"iopsMin,omitempty"`
      ErrorRateMax float64 `json:"errorRateMax,omitempty"`  // 0~1
      AppliesTo    string  `json:"appliesTo,omitempty"`  // stage name pattern, "" = all
  }
  ```
- BenchRule 新增 `SLA []BenchSLA`
- master 收完 task 终态后跑 SLA evaluator，结果写回 task；dashboard 加 PASS/FAIL badge
- MCP `bench.evaluate_sla` 复用同一段逻辑

### Sprint 2（P0 收尾 + S3 大对象 + IOR，目标 1.5 周）

#### 2.1 S3 multipart + range-GET
- backend/s3 暴露 `PartSize int64`、`PartConcurrency int`
- BenchRule.ObjStage 新增 `PartSize / PartConcurrency / GetRangeSize`
- bench_s3.go：put 时按 PartSize 走 multipart manager；get 时按 GetRangeSize 走 range GET（随机 offset）

#### 2.2 IOR sidecar 镜像
- 新建 `cubefs/docker/bench-tools/Dockerfile`：openmpi + IOR + mdtest + fio + s3bench
- cubefs-deploy/modules/cubefs-syncnode/daemonset.tf：syncnode DaemonSet 加 sidecar container（共享 `mountPath` hostPath + `/dev/shm`）
- syncnode executor 新增 `bench_ior.go`：通过 `kubectl exec`（in-cluster ServiceAccount）→ sidecar → mpirun ior，解析 IOR stdout summary
- BenchRule 新增 `IORStages []IORStage`，参考 mdtest 结构

#### 2.3 dashboard 实时曲线 + 终态结果页
- BenchTaskDrawer 新增「实时曲线」tab：Grafana iframe / `<iframe>` embed `/d/cubefs-bench?orgId=1&var-task_id=xxx&kiosk=tv`
- 「终态结果」tab：升级 BenchStageCharts 增加 p99.9 / p99.99 / max；新增「错误分桶」表格；新增 SLA badge

### Sprint 3（P1，目标 2 周）

#### 3.1 客户端资源指标 + 服务端关联视图
- syncnode `/metrics` 增加 `process_cpu_seconds_total` / `process_resident_memory_bytes` / net counters（用 prometheus client default collectors）
- dashboard bench 页面联动展示同时间窗的 master/metanode/datanode 已有 Grafana panel（query by task time range）

#### 3.2 Soak 模式 + checkpoint
- BenchRule 新增 `Soak { Enabled bool, Duration string, CycleStages []string }`
- syncnode bench loop：cycle 循环执行 stage 直到达到 duration；每 N 分钟向 master push 一个 checkpoint（含累计 op 数 + 当前指标）
- master 持久化 checkpoint；syncnode 重启可从最近 checkpoint 续跑

#### 3.3 混合 small + large 负载
- ObjStage / FIOStage 新增 `SubWorkloads []SubWorkload{ Weight, ObjectSize, RW, ... }`
- bench 内部按 weight 分配 worker goroutine

#### 3.4 预热 / cache drop / 错误归因
- BenchRule 新增 `Warmup BenchWarmup{ DropCaches bool, WarmupSec int }`
- syncnode 在 stage 前可选执行 `sync; echo 3 > /proc/sys/vm/drop_caches`（要求 privileged 或 sidecar 提供）
- 错误分桶：`syncnode_bench_op_errors_total` label `kind`：`throttle_4xx` / `server_5xx` / `timeout` / `network` / `checksum` / `cancel`

#### 3.5 场景模板库
- dashboard 内置模板 JSON（HPC checkpoint / AI training / video ingest / backup / data-lake）
- BenchRuleCreateDialog 新增「从模板创建」入口

## 6. 风险与依赖

| 风险 | 影响 | 缓解 |
|---|---|---|
| Consul 不可用 / 网络抖动 | barrier 卡死 | 60s 超时 + quorum 放行 + degraded 标记；不阻塞任务 |
| Prometheus histogram bucket 边界不合适 | p99.9 误差大 | 30 bucket geometric 100us~10s；上线后观察实际分布再调 |
| sidecar 镜像膨胀 | pod 启动慢 | bench-tools 镜像独立 tag；alpine + 必要库；预估 < 400MB |
| MCP SDK 选型不稳定 | 后续维护 | 优先选 anthropic 官方或社区 star > 1k 的 |
| HDR 库性能 | 高 QPS 下 RecordValue 锁竞争 | 每 worker 独立 hdr.Histogram，stage 结束本地合并 |
| kubectl exec sidecar 鉴权 | RBAC 不足 | syncnode ServiceAccount 加 `pods/exec` 权限；监控 token TTL |

## 7. 进度跟踪

| Sprint | 阶段 | 状态 |
|---|---|---|
| S1.1 | MCP server 骨架 | 待启动 |
| S1.2 | MCP tools - bench | 待启动 |
| S1.3 | MCP tools - sync + cluster | 待启动 |
| S1.4 | HDR 直方图聚合 | 待启动 |
| S1.5 | Prometheus 指标 | 待启动 |
| S1.6 | Barrier + ramp-up | 待启动 |
| S1.7 | SLA 字段 + evaluator | 待启动 |
| S2.1 | S3 multipart + range-GET | 待启动 |
| S2.2 | IOR sidecar 镜像 | 待启动 |
| S2.3 | dashboard 实时 + 终态页 | 待启动 |
| S3.* | P1 全部 | 待启动 |

## 8. 后续可选 backlog（明确不在本期范围）

- chaos-mesh 集成
- 基线 / regression detection / 跨版本趋势
- trace 回放
- HTML/PDF 报告导出
- GPFS native 命令压测
- versioned bucket / cross-region copy
- CLI 工具
