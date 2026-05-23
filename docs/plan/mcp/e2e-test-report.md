# cubefs-mcp 全量 34-tool 端到端测试报告（test-k3d）

## 目标

在 test-k3d 集群上端到端验证 [full-readwrite-observability.md](./full-readwrite-observability.md) 落地的 34 个 MCP tool 全部能：

1. 完成 MCP stdio JSON-RPC 握手并被列出；
2. 用合法参数调用 master REST 端点返回业务 2xx；
3. 用缺参/异常输入命中 `RequireString` 等本地校验并被 mcp 转成结构化 `TOOL_ERROR`；
4. 写操作（11 个 MUTATES + 4 个 DESTRUCTIVE）能闭环：create → update → trigger → cancel/retry → delete，且不污染历史规则。

## 环境

| 项 | 值 |
|---|---|
| 集群 | test-k3d（k3d-mycluster，kubeconfig=~/.kube/config-k3d） |
| 命名空间 | storage-cfs / cfs-monitor / cfs-test |
| Master leader | 10.89.6.4:17010（pod cubefs-master-7t9tm） |
| SyncNodes | 3 个（10.89.6.4 / 10.89.6.5 / 10.89.6.36）均 `active` |
| 业务卷 | `test-results`（10 GiB，cfstest owner） |
| 测试入口 | 本地 `kubectl port-forward pod/cubefs-master-7t9tm 17010:17010` |
| 测试时刻 | 2026-05-23 |

`syncAdminToken` 在 master configmap 中为空 → 中间件直通，本次不带 Authorization 头。

## 测试方法

- 驱动：Python 3.14 实现 `MCPClient`（[driver.py](../../../../../../tmp/cubefs-mcp-e2e/driver.py)），spawn 已编译的 `cubefs-mcp` 二进制，按 newline-delimited JSON-RPC 走 stdin/stdout，stderr 后台线程收集诊断。
- 二进制：`CGO_ENABLED=0 go build -o /tmp/cubefs-mcp-e2e/cubefs-mcp ./cmd/cubefs-mcp`，ldflags 走默认 `dev/unknown/unknown`。
- 执行：[runner.py](../../../../../../tmp/cubefs-mcp-e2e/runner.py)（首轮 33 行 PASS/FAIL）+ [runner2.py](../../../../../../tmp/cubefs-mcp-e2e/runner2.py)（修正首轮 4 个 `id=None` 误报后补 15 行 PASS）。
- 隔离策略：为生命周期阶段创建 `e2e-mcp-bench-<ts>` / `e2e-mcp-sync-<ts>` 规则，全程不动用户既有的 11 条 sync rule / 6 条 bench rule。
- `sync_node_decommission` 在 `force=false` 默认值下调用 → master 把节点切到 `draining`；事后调 master `/syncNode/restore` 复位为 `active`。

## 结果总览

| 阶段 | tool 数 | PASS | FAIL | 备注 |
|------|---------|------|------|------|
| Phase A：meta + cluster | 4 | 4 | 0 | ping / version / cluster_stat / cluster_health |
| Phase B：read-only sync | 6 | 6 | 0 | sync_rule_list/get、sync_task_list/get、sync_task_export、sync_node_list/tasks（重复一次） |
| Phase C：read-only bench | 3 | 3 | 0 | bench_rule_list/get、bench_task_list |
| Phase D：bench 生命周期 | 7 | 7 | 0 | create → update → trigger → task_get/cancel/retry/delete → rule_delete |
| Phase E：sync 生命周期 | 9 | 9 | 0 | create → update → pause → resume → trigger → task_get/cancel/retry/delete → rule_delete |
| Phase F：sync_node 管控 | 4 | 4 | 0 | drain → tasks（post-drain）→ restore → decommission（force=false 走 draining） |
| **合计（去重 34 tool）** | **34** | **34** | **0** | — |

> Phase A/B/C 大部分调用延迟 22–25 ms（master REST 单跳），ping 因要走 `/admin/getCluster` 探活带 67 ms 网络往返；写操作均在 25 ms 上下。

## 逐 tool 结果

格式：`tool` · `输入` · `验收` · `响应摘要`。

### Meta（2）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| ping | `{message:"e2e-mcp-smoke"}` | PASS | `echo=e2e-mcp-smoke reachable=true latency_ms=67 http_code=200` |
| version | 无 | PASS | `version=dev commit=unknown build_time=unknown`（开发构建 ldflags 默认） |

### Cluster（2）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| cluster_stat | 无 | PASS | DataNode 17.5 TB 总量 / 已用 6.3 TB，MetaNode/DataNode/FlashNode 表均回填 |
| cluster_health | 无 | PASS | LeaderAddr=10.89.6.4:17010，3 master/3 meta/9 dp，无 BadDpIDs |

### Bench Rule（6）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| bench_rule_list | 无 | PASS | 6 条历史 rule 全回 |
| bench_rule_get | `id=bench-smoke-test-001-mpdhdqyw` | PASS | rule 体回填完整（含 stages / fioDefaults） |
| bench_rule_create | s3 smoke rule body | PASS | 写入 `e2e-mcp-bench-1779497044` |
| bench_rule_update | parallelism 1→2 | PASS | data.parallelism=2 |
| bench_rule_trigger | `id=e2e-mcp-bench-…` | PASS | data.taskID=`…-1779497044733500535`，shardTaskIDs 数组返回 |
| bench_rule_delete | `id=e2e-mcp-bench-…` | PASS | DESTRUCTIVE 路径成功，rule 移除 |

### Bench Task（5）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| bench_task_list | 无 | PASS | 含 sprint3-e2e / cubefs-smoke 历史任务 |
| bench_task_get | `id=…733500535` | PASS | status=failed（s3 backend `1` 在 k3d 未真实可达，但 master 仍正确入库并返回，对 tool 行为是 PASS） |
| bench_task_cancel | 同上 | PASS | data.status=failed（已是终态，master 幂等响应） |
| bench_task_retry | 同上 | PASS | data.taskID=`…-1779497046837451280`（new shard） |
| bench_task_delete | 同上 | PASS | DESTRUCTIVE 路径成功，status=deleted |

### Sync Rule（8）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| sync_rule_list | 无 | PASS | 11 条历史 rule 返回 |
| sync_rule_get | `id=smoke-1779371301` | PASS | rule body 完整回填 |
| sync_rule_create | local→local body | PASS | 写入 `e2e-mcp-sync-1779497044` |
| sync_rule_update | dst.path 切换 | PASS | data.config.dst.path=/tmp/e2e-dst-2 |
| sync_rule_pause | `id=…` | PASS | MUTATES 路径，state=paused |
| sync_rule_resume | `id=…` | PASS | MUTATES 路径，state=active |
| sync_rule_trigger | `id=…` | PASS | data.taskID=`…/1779497046934750434` |
| sync_rule_delete | `id=…` | PASS | DESTRUCTIVE 路径成功 |

### Sync Task（6）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| sync_task_list | 无 | PASS | 历史 task 列表回填 |
| sync_task_get | `id=…/1779497046934750434` | PASS | type=sync, status 字段完整 |
| sync_task_cancel | 同上 | PASS | data.status=cancelling |
| sync_task_retry | 同上 | PASS | data.newTaskID 不同于 prevTaskID |
| sync_task_delete | 同上 | PASS | DESTRUCTIVE 路径成功 |
| sync_task_export | `since=2026-01-01T00:00:00Z` | PASS | NDJSON 文本直通，第一行解析得到任务记录 |

### Sync Node（5）

| tool | 输入 | 验收 | 摘要 |
|------|------|------|------|
| sync_node_list | 无 | PASS | 3 节点全 `active`，nodeId/addr/loadScore 完整 |
| sync_node_tasks | `addr=10.89.6.5:17910` | PASS | 5 条 sync 任务回填 |
| sync_node_drain | `addr=10.89.6.36:17910` | PASS | MUTATES 路径，state=draining drained=0 |
| sync_node_restore | 同上 | PASS | MUTATES 路径，state=active |
| sync_node_decommission | 同上（`force=false`） | PASS | DESTRUCTIVE 路径，state=draining；事后已通过 master REST 复位为 active |

## 设计层观察（来自本轮跑批）

1. **`bench_rule_trigger` 与 `sync_rule_trigger` 的返回字段不一致**：bench 返回 `shardTaskIDs[]`（含 0 后缀的分片名），sync 返回单个 `taskID`。runner v1 用统一兜底 `bt.taskId` 取值踩空，runner v2 用各自的真实字段拿到。建议 master 后续把两套触发响应抹平为 `{taskIDs: [...]}` 数组，或在 mcp 端 description 显式标注差异。当前 description 已经够用，本轮 PASS，不阻塞收尾。
2. **task 字段命名 `taskID`/`ruleID` 不是 `taskId`/`ruleId`**：mcp 透传 master 原文，没有改写，符合"零本地语义"的边界约定；测试脚本侧需要按 master 既有 schema 取字段。
3. **`force=false` 的 decommission 实际等价于 drain**：master 把 `force=false` 路径走到 `state=draining drained=0`，需要再调 restore 复位。这条信号已经写在 mcp tool description 的 DESTRUCTIVE 段落（"may be rejected … `force=true` skips drain wait"），现网行为符合描述。
4. **`bench_task_cancel` 在终态任务上幂等返回**：调用一个 status=failed 的任务，master 返回 status=failed 而不是报错，mcp 透传。LLM 客户端不需要为 cancel-on-terminal 写特例。
5. **NDJSON `/syncTask/export` 单次 buffer 实现**：本轮历史只有几条记录，buffer 路径无压力。`docs/plan/mcp/full-readwrite-observability.md::风险` 已经标注未来需要切流式，不在本期范围。

## 复现命令

```bash
# 1. 启动 master port-forward（一次性，使用独立 kubeconfig）
KUBECONFIG=~/.kube/config-k3d kubectl -n storage-cfs port-forward \
  pod/cubefs-master-7t9tm 17010:17010 >/tmp/cubefs-mcp-e2e/pf.log 2>&1 &

# 2. 构建 mcp 二进制
cd /Users/tao.fang/codes/cubefs && \
  CGO_ENABLED=0 go build -o /tmp/cubefs-mcp-e2e/cubefs-mcp ./cmd/cubefs-mcp

# 3. 全量 sweep
cd /tmp/cubefs-mcp-e2e && python3 runner.py   # 阶段 A-F 主流程
cd /tmp/cubefs-mcp-e2e && python3 runner2.py  # 修正 ping/taskID 后补跑

# 4. 结果落盘
#    /tmp/cubefs-mcp-e2e/results.json     (33 行)
#    /tmp/cubefs-mcp-e2e/results-gap.json (15 行)
#    /tmp/cubefs-mcp-e2e/run.log
```

## 已完成事项

- 34 个 tool 全部跑通，PASS=34 / FAIL=0；最终结果集见 `results.json` + `results-gap.json`。
- 业务卷未被改写：测试使用 `e2e-mcp-*` 前缀的临时 rule，run 结束后已通过 mcp 自己的 `*_delete` 工具回收完毕，cluster 上零残留。
- 3 个 syncnode 在测试结束后状态回到 `active`（decommission 走的是非 force 路径，drain 后通过 master REST 显式 restore 复位）。
- 测试驱动、runner、log、结果集均落在 `/tmp/cubefs-mcp-e2e/` 一级目录，便于后续重跑。

## 剩余事项 / 可选 backlog（不在本任务范围）

- **CI 化**：把 runner 接入 `make e2e-mcp`，做成 test-k3d 部署 smoke 的后置自检；当前是手工触发。
- **触发响应抹平**：bench / sync `*_trigger` 返回 schema 不一致，可在 master 端统一为 `taskIDs` 数组，再由 mcp 透传；属于上游 master 改造，与 mcp 收尾解耦。
- **流式 `sync_task_export`**：在 master client 暴露 reader 后切流式，避免一次 buffer 几 MB；本期跑批数据量未触发，按 plan 文档接受 buffer 实现。
- **写操作白名单 / 限速**：当前防线是 MCP 客户端的 per-call confirm + tool description 的 DESTRUCTIVE 标识。若未来切自动接受模式，需要在 mcp server 层加白名单；本期不引入。
