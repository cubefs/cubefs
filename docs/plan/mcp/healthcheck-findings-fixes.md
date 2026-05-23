# test-k3d 集群体检发现的三个问题与修复方案

> 触发场景:在 test-k3d 上跑 `/tmp/cubefs-mcp-e2e/healthcheck.py`(基于 `~/.local/bin/cubefs-mcp` 的 stdio MCP wrapper)做集群体检时发现的三个独立问题。三处都属于"设计或接口收口不全",不是单点 case;一次修掉。
>
> 修复顺序: **P1 → P2 → P0**(便宜的先做,P0 涉及 master + mcp 两个面的改动最大)。

## 背景与发现

体检脚本通过 stdio 调 `~/.local/bin/cubefs-mcp` 触达 master,扫描了 ping / cluster_health / sync_rule_list / bench_rule_list / sync_task_list / bench_task_list / sync_node_list 共 7 个工具。结果聚合后发现:

1. **P0(安全)** — `sync_rule_list` / `sync_rule_get` 返回值里 `config.dst.accessKey` / `config.dst.secretKey` 为**明文**。意味着任何拿到 master 读权限的人(包括把这个 envelope 喂给 LLM 的 MCP 调用)都能看到 S3 凭证。
2. **P1(可用性)** — `ping` 工具把 `message` 设成 `mcp.Required()`,LLM 想做一次纯连通性探测都必须先想一个字符串塞进去,语义上 message 只是 echo,没必要强制。体检脚本直接踩到 `TOOL_ERROR: required argument "message" not found`。
3. **P2(观测一致性)** — `SyncRule` envelope 已经带 `state` / `lastRunAt` / `lastRunStatus` 三个观测字段,`BenchRule` envelope 没有。导致 dashboard / MCP 客户端看 sync 和看 bench 用两套不一样的"最近一次运行"字段集,体检脚本里也不得不对两边分别做兼容判断。

## 修复方案

### P1 — `ping.message` 改成可选(默认 `"healthcheck"`)

- 文件: `cmd/cubefs-mcp/internal/tools/ping.go`
- 改动:
  - `mcp.WithString("message", …)` 去掉 `mcp.Required()`。
  - tool 体内把 `req.RequireString("message")` 换成 `req.GetString("message", "healthcheck")`,与项目里 `sync_rule_list` 等读类工具的可选参数读取风格一致。
  - 描述更新为 "Optional arbitrary string echoed back; defaults to `healthcheck` when omitted."
- 验证:
  - 重新编译 + 安装 binary → 重连 MCP wrapper → `ping {}` 返回 `reachable=true`,`echo` 字段值为 `healthcheck`。

### P2 — `benchRuleView` 补 `lastRunAt` / `lastRunStatus`

> bench rule **没有 pause/active 的状态机**(全靠 `enabled` + 手动 trigger),所以**不**给它造一个 `state` 字段。只补和 sync 对齐的 `lastRunAt` / `lastRunStatus` 两个观测字段,从 `benchTaskLedger` 现拉取最新终态任务即可,**不写新的持久化字段**。

- 文件: `master/api_service_bench.go`
- 改动:
  - `benchRuleView` 增加:
    ```go
    LastRunAt     int64  `json:"lastRunAt,omitempty"`
    LastRunStatus string `json:"lastRunStatus,omitempty"`
    ```
  - 新建私有 helper `latestBenchRun(ledger *BenchTaskLedger, ruleID string) (int64, string)`:
    - `ledger.List(ruleID, "")` 取全部 → 过滤 `ParentTaskID != ""` (跳过 shard,只看 parent / 单 task) → 选 `UpdatedAt` 最大且 `Status` 为终态(failed / cancelled / succeeded)的那条。
    - 返回 `(UpdatedAt, string(Status))`;无终态记录返回 `(0, "")`,前端按 `omitempty` 自然不展示。
  - `newBenchRuleView` / `newBenchRuleViews` 都改成接受 `ledger *BenchTaskLedger`(显式注入,不依赖全局),内部调上面的 helper 填两个字段。
  - 四个 handler(`listBenchRules` / `getBenchRule` / `createBenchRule` / `updateBenchRule`)调用点统一传 `m.cluster.benchTaskLedger`。
- 不改动:
  - `spec.BenchRule` 本体不动 → 不影响 rocksdb 持久化、raft 同步、其他模块对 spec 的导入。
- 验证:
  - 跑一次手动 trigger → 等任务终止 → 调 `bench_rule_list`,确认对应 rule 的 `lastRunAt` / `lastRunStatus` 出现且与 `bench_task_list` 中该 rule 最新终态记录一致。

### P0 — SyncRule 凭证字段在 master 端 redact + cubefs-mcp 兜底 redact

防线两层:**master 出口处先 redact**(根因,谁调都看不到明文);**cubefs-mcp 出口再 redact 一次**(防 master 漏改、防其他 master 接口绕路把同字段带出去时 mcp 这一层兜得住)。

#### 1) master 侧:加 syncRuleView + 深拷贝 + 替换

- 文件: 新增 `master/sync_rule_view.go`,改 `master/api_service_sync_rule.go`
- 设计:
  - `SyncRuleCache.Get` / `List` 返回的是 cache 内部指针,**不能直接改**(会把存储里的 accessKey 也改成 `***`),所以视图层必须深拷贝。
  - 引入 `type syncRuleView struct { *proto.SyncRule }` 太薄,这里直接用值类型 copy 然后改字段更清晰:
    ```go
    func redactedSyncRule(in *proto.SyncRule) *proto.SyncRule {
        if in == nil { return nil }
        out := *in                  // shallow copy of value
        out.Config = in.Config      // SyncRuleConfig 是值类型,这里已自动深拷贝
        if out.Config.Src.AccessKey != "" { out.Config.Src.AccessKey = redactedMask }
        if out.Config.Src.SecretKey != "" { out.Config.Src.SecretKey = redactedMask }
        if out.Config.Dst.AccessKey != "" { out.Config.Dst.AccessKey = redactedMask }
        if out.Config.Dst.SecretKey != "" { out.Config.Dst.SecretKey = redactedMask }
        return &out
    }
    func redactedSyncRules(in []*proto.SyncRule) []*proto.SyncRule { /* map redactedSyncRule */ }
    const redactedMask = "***"
    ```
  - 注意 `SyncRuleConfig.ShardPrefixes` 是 slice,赋值后两份共用底层数组,但本次只改 string 字段不会写到 slice,所以无需 clone slice。
- 调用点:
  - `createSyncRule` 末尾 `sendOkReply(w, r, newSuccessHTTPReply(rule))` → 改成 `newSuccessHTTPReply(redactedSyncRule(rule))`
  - `updateSyncRule` 末尾 `&updated` → `redactedSyncRule(&updated)`
  - `listSyncRules` 末尾 `rules` → `redactedSyncRules(rules)`
  - `getSyncRule` 末尾 `rule` → `redactedSyncRule(rule)`
  - `transitionSyncRule` (pause/resume) 末尾 `&updated` / `existing` → 同样 redact
  - `triggerSyncRule`:如果响应里带回 rule 体,也要 redact;否则跳过。
- 不动持久化路径:`syncPutSyncRuleInfo` / cache.Put 仍写入明文,凭证可用、调度可用。
- 测试:`master/api_service_sync_rule_test.go` 已有 list/get/update 用例,补两个 case:
  - 写入带 accessKey/secretKey 的 rule → list 返回里两个字段值为 `"***"`、cache 内部仍是明文(直接读 cache.Get(id).Config.Dst.AccessKey 校验)。

#### 2) cubefs-mcp 侧:在 forward 出口扫一遍 JSON,把 accessKey / secretKey 改 `***`

- 文件: `cmd/cubefs-mcp/internal/tools/tools.go`
- 设计:
  - 不在 `rawJSONResult` 里无脑改(那是泛用 helper,export NDJSON 也走这里逻辑要分叉),只针对 sync_rule / sync_task 系列 + bench_rule 系列(以防 BenchRule 未来加凭证)的 forwarder 包一层。
  - 新增 `forwardGetRedacted` / `forwardPostRedacted` / `forwardPostJSONRedacted`:
    - 拿到 body 后,若 `json.Valid(body)` 走通用 `redactSensitiveJSON(body)` 然后再 `mcp.NewToolResultText(...)`。
    - `redactSensitiveJSON`:`var v any; json.Unmarshal(body, &v)` → 递归遍历 map/slice,把 key 为 `accessKey` / `secretKey` / `AccessKey` / `SecretKey` 的字符串值替换为 `"***"` → 重新 marshal。HDR / 二进制场景不会命中(它们不是 json string)。
  - 改 `sync_rule_list.go` / `sync_rule_get.go`(如果有)/ `sync_rule_create.go` / `sync_rule_update.go` 调用 redacted 版本。
  - bench_rule 系列暂时保持 forwardGet,但 redactor 函数公开,后续如果 bench rule 加凭证一行切换即可。
- 也要处理 mcp 把整个 body 转给 LLM 之外的写路径吗? 不需要 — mcp 不存任何东西,LLM 看到什么就是什么,redact 一次足够。
- 测试:`cmd/cubefs-mcp/internal/tools/...` 单测里加一个表驱动 case 检查 `redactSensitiveJSON` 对嵌套 / array / 缺字段三种 case 的行为。

## 风险与回滚

- P1 改可选参数:向后兼容,旧调用者传 message 还是会拿到 echo。无风险。
- P2 加字段:envelope 只增不减,前端按需消费。无破坏性。
- P0:
  - 读路径:dashboard 现在如果直接拿 `accessKey` 回写更新 rule,会把 `***` 当成新值写回去。**前置检查**: 在改前 grep `cubefs-dashboard/frontend` 里 `accessKey` 的消费方式,若有"读了再改了再 PUT"的模式必须对 dashboard 同步加"看到 `***` 不发送"的过滤;若 dashboard 只在 create 时填、不在 list 里回写,则零风险。
  - 写路径不动,持久化值不变。
- 回滚:`git revert` 三个 commit 即可,无 schema 变化。

## 实现 / 验证步骤

1. **P1 改 ping** → 编译 cubefs-mcp → `go install ./cmd/cubefs-mcp` 到 `~/.local/libexec/cubefs-mcp` → 通过 `/tmp/cubefs-mcp-e2e/healthcheck.py` 验证 ping 无 message 也通。
2. **P2 改 benchRuleView** → 单测 + 重新部署 master 镜像 → 触发一次 bench rule → list 检查 lastRunAt/lastRunStatus 出现。
3. **P0 master + mcp redact** → 单测先过 → master 镜像 + mcp binary 同时更新 → `sync_rule_list` 看到 `accessKey: "***"`、cache 内仍是明文(看 master 日志或 trigger 一次 sync 验证凭证还能用)。
4. 全部完成后再跑一次完整体检脚本(`/tmp/cubefs-mcp-e2e/healthcheck.py`),确认三个问题都消失。

## 验收结果(test-k3d, 2026-05-23)

- P1 ✅ — `ping {}` 不传 message,wrapper 直连 healthcheck.py 返回 `{"ok":true,"raw_err":null}`,默认 echo `healthcheck`。
- P2 ✅ — master 重建为 `cubefs:v3.5.3.1.rc10` 后触发 `bench-smoke-test-001-mpdhdqyw`,`bench_rule_get` 返回 `lastRunAt: 1779506692531`、`lastRunStatus: "failed"`,与 `bench_task_list` 中该 rule parent task 的 UpdatedAt / Status 一致。
- P0 ✅ — 绕过 cubefs-mcp 直 `curl master:17010/syncRule/list`:11 条 rule、6 个带凭证字段全部 `"***"`、零 plain leak;mcp 兜底层在老 master 镜像(rc8/9)环境下也已能完整脱敏。

## 不做什么

- **不**改 spec.BenchRule schema(避免 rocksdb 兼容性问题)。
- **不**给 BenchRule 加 pause/active 状态机(本次只补观测字段,状态机是新需求)。
- **不**在 master 加凭证轮转/加密落盘(超出本次范围,但是 follow-up: 凭证应进 secret store 而不是 rocksdb 明文)。
- **不**改其他 mcp 工具的 redact 范围(只针对 sync_rule / sync_task / bench_rule 系列)。

## Follow-up backlog(本次不做)

- 凭证落盘加密 / 走外部 secret store。
- BenchRule 加 pause 状态机(若运维真有需求)。
- master 通用响应中间件加 redact(把当前 syncRule 的手工 redact 收成框架级)。
