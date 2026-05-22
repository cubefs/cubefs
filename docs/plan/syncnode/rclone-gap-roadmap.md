# rclone 能力对齐 roadmap（syncnode P0 / P1）

> 目标：补齐 syncnode 与 rclone 的关键能力缺失（共 6 项），让 rclone 用户能在不丢能力的前提下迁移到 syncnode，并解决当前几个对生产可用性影响最大的 footgun。

## 背景

syncnode 已稳定：cfs/s3/local 三 backend、sync/load/check/bench/move 五种 type、filter/retention/concurrency/checksum/分片/重试/checkpoint 等基础设施齐备。

与 rclone 对照（详见对话记录中的 gap analysis）后识别出若干能力空白；本文档只聚焦 **P0+P1 共 6 项**，其它（额外 backend、crypt、chunker、union、cleanup、dedupe、mount/serve、时间窗带宽、POSIX 元数据 mode/uid/gid/xattr/ACL）一律进 backlog，不在本期。

## 范围

**做（6 项）**

| # | 子项 | 优先级 | 用户痛点 |
|---|------|--------|----------|
| 1 | OnSymlink 处理策略可配 | P0 | 当前 local backend 静默跳过 symlink，footgun |
| 2 | dry-run | P0 | type=move 带删除却无演练；高风险规则裸奔 |
| 3 | OnExisting 策略可配（`--ignore-existing` / `--update` / `overwrite`） | P0 | rclone 用户最常用的"增量"开关迁不过来 |
| 4 | server-side copy（S3 CopyObject + CFS inode-level） | P0 | 同 region/同集群迁移流量经本地，差几个数量级 |
| 5 | metadata 保留（mtime 起步） | P1 | 迁移后 mtime 丢失；阻断 `--update` 完整闭环 |
| 6 | 真 sync 语义（type=mirror + delete-extraneous） | P1 | 镜像备份场景做不了；当前 sync ≈ rclone copy |

**不做（明确放弃）**

- 新增 backend（gcs / oss / sftp / webdav / ftp）—— 按需开项目，进 backlog
- crypt / chunker / union / combine —— backlog
- cleanup / dedupe / hashsum CLI —— backlog
- mount / serve（SFTP/WebDAV/HTTP/S3）—— 与 CSI/POSIX client 重叠，明确放弃
- 时间窗口带宽调度（rclone `--bwlimit "08:00,512k …"`）—— backlog
- POSIX 元数据 mode/uid/gid/xattrs/ACL 保留 —— 留 P2，本期只做 mtime

## 不做的边界（防漂移）

- **不动 master 协议**：`/syncRule/{create,trigger,delete}` / `/syncTask/list` HTTP API 字段保持兼容；新增字段都加在 `proto.SyncRuleConfig` 上，老规则 Unmarshal 时新字段默认零值，行为不变。
- **不动 BoltDB schema**：checkpoint 二进制布局不变；新字段如需持久化走单独 bucket 或外挂 JSON。
- **不动 dashboard 既有表单结构**：sync/load/check/bench/move 五个 type 的表单只新增字段，不重构。
- **不引入"任意 backend 任意能力"的运行时探测**：能力靠 `Caps` 结构静态声明 + executor 显式分支，禁止反射。
- **不动 cubefs-dashboard 后端**：所有新字段纯透传 JSON，dashboard 后端无 Go 改动。

## 子项详情

### 子项 1：OnSymlink 处理策略可配

**背景**：当前 `syncnode/backend/local/local.go` List 时 `d.Type()&os.ModeSymlink != 0` → 直接 skip；resolve 时 `EvalSymlinks` 撞 AllowedRoots → 报错。两条路径都是硬编码，用户不知道源里的 symlink 没被同步。

**设计**：

```go
// proto/sync_rule.go
type SyncRuleConfig struct {
    ...
    OnSymlink string `json:"onSymlink,omitempty"` // ""(=skip) | "skip" | "follow" | "error"
}
```

- `skip`（默认，向后兼容）：List 跳过；resolve 不入 symlink 路径
- `follow`：List 把 symlink 当成它指向的目标（用 `os.Stat` 而非 `os.Lstat`）；resolve 沿用 `EvalSymlinks` 但允许跨 AllowedRoots（仍需文档警告）
- `error`：List 遇到 symlink → 立即报错；任务失败而非静默丢数据

仅 `local` backend 实现；s3/cfs 上 symlink 不适用，OnSymlink 字段无效（不报错，warn 日志即可）。

**测试**：
- 单测：`local_test.go` 在 AllowedRoots 内造一个指向同 root 内文件的 symlink，三策略各验一次
- 单测：`local_test.go` 造跨 root 的 symlink，`follow` 报错 / `skip` 跳过 / `error` 报错

**完成标准**：`go test ./syncnode/backend/local/... -run Symlink` 全绿。

---

### 子项 2：dry-run

**背景**：type=move 一旦触发即删 src，万一规则配错（src/dst 写反、filter 误匹配）无法挽回。

**设计**：

```go
type SyncRuleConfig struct {
    ...
    DryRun bool `json:"dryRun,omitempty"`
}
```

- `DryRun=true` 时：executor 仍走完 List + filter + 决策（要传 / 跳过 / 删除），但**任何对 dst 的写、对 src 的删都不执行**；改为把 (action, srcKey, dstKey, size, reason) 一行行写进 task event log。
- 任务最终状态 `succeeded`，附 `dryRun=true` 标记，前端可展示"本次为演练"。
- `type=move` 若配 `DryRun=true`：日志和事件里写"src 将被删除"但不真删。

**实现位置**：`syncnode/executor/sync_task.go` 的 `syncOneFile` 和 `runRetention` 都加 `if t.DryRun { logPlanned(...); return nil }` 短路。

**测试**：
- 单测：构造 3 文件 src + 空 dst，`DryRun=true` 跑完后断言 dst 仍空、event log 含 3 条 PLAN_PUT
- 单测：type=move + DryRun=true，断言 src 未被删

**完成标准**：`go test ./syncnode/executor/... -run DryRun` 全绿；test-k3d e2e 通过。

---

### 子项 3：OnExisting 策略可配

**背景**：当前 `shouldSkipExistingDst`（`syncnode/executor/sync_task.go:567`）是硬编码"size+checksum/ETag 匹配才跳"，覆盖了 rclone `--checksum`，但**不覆盖** `--ignore-existing`（裸跳）和 `--update`（按 mtime）。

**设计**：

```go
type SyncRuleConfig struct {
    ...
    OnExisting string `json:"onExisting,omitempty"`
    // "" (=verify_then_skip, 向后兼容)
    // "verify_then_skip"
    // "always_skip"      // rclone --ignore-existing
    // "newer_only"       // rclone --update
    // "overwrite"        // rclone --ignore-times
}
```

`shouldSkipExistingDst` 升级为策略派发：

| OnExisting | 跳过条件 |
|---|---|
| `verify_then_skip`（默认） | dst 存在 + size 匹配 + checksum/ETag 匹配 |
| `always_skip` | dst 存在 |
| `newer_only` | dst 存在 + dst.Mtime ≥ src.Mtime |
| `overwrite` | 永不跳 |

**互斥校验**（validateTask）：
- `type=move` 强制锁 `OnExisting="verify_then_skip"`（其它策略可能丢数据：always_skip 会让 src 不被删；overwrite 浪费；newer_only 错过覆写）
- 用户在 type=move 下传 `OnExisting != "" && != "verify_then_skip"` → 报错

**newer_only 容差**：跨 backend mtime 精度差异（S3 秒级，local 纳秒）→ 设计 1s 容差，"src.Mtime > dst.Mtime + 1s 才传"。

**测试**：
- 单测 `shouldSkipExistingDst_*`：四策略 × 四种 dst 状态（无 / 不同 size / 同 size 不同 checksum / 全匹配）的真值表
- 单测：type=move + OnExisting=always_skip → validateTask 报错

**完成标准**：`go test ./syncnode/executor/... -run ShouldSkip` 全绿。

---

### 子项 4：server-side copy

**背景**：同 backend 同 region 的"大文件搬家"，syncnode 当前必经本地（Get→Put），相当于把对象数据全量过一遍 syncnode pod。S3 有 `CopyObject` / multipart copy，CFS 同 master 同 vol 可以 inode-level clone。

**设计**：

```go
// syncnode/backend/backend.go
type Caps struct {
    ...
    ServerSideCopy bool
}

type ServerSideCopier interface {
    // 同 backend 实例内 srcKey → dstKey；调用方保证 src/dst 属于同一个 Backend
    ServerSideCopy(ctx context.Context, srcKey, dstKey string, opts PutOptions) (PutResult, error)
}
```

- `s3`：实现 ServerSideCopy
  - 文件 ≤ 5GB → 单次 `CopyObject`
  - 文件 > 5GB → multipart copy（UploadPartCopy 循环）
- `cfs`：本期**先不做 inode-level**（涉及 metanode API，单独立项）；返回 `ErrBackendUnsupported`，executor 自动 fallback 走 Get/Put。Caps.ServerSideCopy=false。
- `local`：不实现（hardlink 不可靠 + 跨 mount 无意义），Caps.ServerSideCopy=false。

**executor 决策**：在 `syncOneFile` 开头判断：

```go
if t.Src == t.Dst /* 同实例 */ && t.Src.Capabilities().ServerSideCopy {
    if copier, ok := t.Src.(ServerSideCopier); ok {
        result, err := copier.ServerSideCopy(ctx, srcKey, dstKey, putOpts)
        if !errors.Is(err, ErrBackendUnsupported) {
            return err  // 成功 / 失败都不走 Get/Put fallback
        }
    }
}
// fallback: 走原 Get/Put 路径
```

注意：rule 的 src/dst 是两个 SyncEndpointConfig，即便 kind 都是 s3、bucket 不同也算同 backend（如果共享同一个 client/endpoint）。**判等条件**用 `Src.SameInstance(Dst) bool` 接口，避免直接对比指针。

**测试**：
- 单测 `s3_test.go`：用现有 fake S3 server 模拟 CopyObject，断言 PUT 走 copy 而非分体 GET+PUT
- 单测：>5GB 文件触发 multipart copy 路径
- 单测：跨 endpoint（不同 s3 server）→ SameInstance 返回 false，走 fallback

**完成标准**：`go test ./syncnode/backend/s3/... -run ServerSideCopy` 全绿。

---

### 子项 5：metadata 保留（mtime 起步）

**背景**：当前 `backend.PutOptions` 只有 ContentType/Metadata/StorageClass；mtime 不传，dst 文件以"写入时刻"为 mtime，迁移场景丢失"何时产生"信息；同时阻断 OnExisting=newer_only 的完整闭环。

**设计**：

```go
type PutOptions struct {
    ...
    Mtime *time.Time  // nil = 不设置（保持现状行为）
}
```

- `local`：`Put` 完成后 `os.Chtimes(dstPath, time.Now(), *opts.Mtime)` 写 mtime
- `s3`：写入 `x-amz-meta-syncnode-mtime: <RFC3339Nano>` user-metadata（S3 没有原生 mtime 概念）；List/Head 解析这个 header 填回 `Entry.Mtime`（如有 user-metadata 优先于 LastModified）
- `cfs`：调用 `SetAttr` 接口写 mtime（CubeFS Posix 语义原生支持）

**调用方修改**：`syncnode/executor/sync_task.go` `syncOneFile` 准备 PutOptions 时填 `Mtime: &entry.Mtime`（entry 来自 src List）。

**dashboard**：本期**不加 UI 字段**，默认全开（PutOptions.Mtime 总是从 src 复制）；后续如要 opt-out 再加 `PreserveMtime bool`。

**测试**：
- 单测：local→local，writeFile 时间 t1，sync 后读 dst mtime 应等于 t1（容差 1ms）
- 单测：local→s3，s3 Head 应返回 t1 = Entry.Mtime
- 单测：s3→local，s3 user-metadata mtime 解析正确

**完成标准**：`go test ./syncnode/... -run Mtime` 全绿。

---

### 子项 6：真 sync 语义（type=mirror）

**背景**：当前 `type=sync` 行为是 "src 有的复制到 dst"，但 **dst 多余的不删**，是 rclone copy 而非 rclone sync。需要"完整镜像"的用户做不了。

**设计**：

新增 `TaskTypeMirror TaskType = "mirror"`：

```go
case TaskTypeMirror:
    if err := e.runSync(ctx, t); err != nil {
        return err
    }
    return e.deleteExtraneous(ctx, t)
```

`deleteExtraneous`：
- List dst（全量）
- List src（全量）→ 建索引（srcKey → bool）
- 遍历 dst：若 rebaseSrcKey(dstKey) 不在 src 索引里，删 dst

**安全**：
- 默认 `DryRun=true`（如果用户未显式设 DryRun）—— 首跑必须演练，演练 OK 后用户改成 false 再跑一次。
- 添加 `Confirm bool` 字段在 type=mirror 下必须为 true 才会真删（双重确认）。

**validateTask**：
- `type=mirror` 白名单
- `type=mirror` 且 `DryRun=false` 且 `Confirm=false` → 报错

**冲突检查**：mirror 对 src/dst 占用与 sync 一致，沿用现有逻辑。

**测试**：
- 单测 `TestRunMirror_DeleteExtraneous`：src=[a,b]，dst=[a,b,c]，mirror 后 dst=[a,b]，c 被删
- 单测：dry-run 模式下 c 不被真删，event log 含 PLAN_DELETE
- 单测：type=mirror 无 Confirm → validateTask 报错

**完成标准**：`go test ./syncnode/executor/... -run Mirror` 全绿；test-k3d e2e 通过。

---

## 分阶段计划

| 波次 | 子项 | 并行度 | 估计镜像版本 |
|------|------|--------|--------------|
| 1 | #1 symlink + #3 OnExisting + #5 mtime + #4 server-side copy | 4 个 sub-agent 并行 | cubefs `v3.5.3.1.rc6` |
| 2 | #2 dry-run | 1 sub-agent | cubefs `v3.5.3.1.rc7` |
| 3 | #6 mirror | 1 sub-agent（依赖 #2） | cubefs `v3.5.3.1.rc8` |
| 4 | dashboard UI 三字段 + type=mirror 选项 | 1 sub-agent | dashboard `v1.0.5.rc4` |
| 5 | 编译 / push / bump deploy / apply test-k3d / e2e | 主 agent 串行 | — |

## 完成标准

1. cubefs `go test ./syncnode/...` 全绿（含所有新增测试）
2. dashboard 前端在 test-k3d 集群中可创建：
   - type=mirror 规则
   - 任意 type 下的 onSymlink / onExisting / dryRun 配置项
3. test-k3d e2e 覆盖：
   - **symlink**：local→local，源含 symlink，三策略行为符合预期
   - **dry-run**：type=move + dryRun=true，src 未删 / dst 未写 / event log 含 plan
   - **OnExisting**：四策略 × 已有/不同 size/同 size 不同内容 真值表
   - **server-side copy**：s3→s3 同 endpoint，pod 出流量显著低于对照组（看 Prometheus）
   - **mtime**：local→s3→local 往返，mtime 守恒（容差 1s）
   - **mirror**：dst 多余文件被删；dry-run 首跑不真删
4. 老规则零回归：现有的 sync/load/check/bench/move 规则继续跑（cfs/s3/local 三 backend 各 1 条 happy path）

## 当前进度

- [x] roadmap 落地（本文档）
- [ ] 波次 1：symlink / OnExisting / mtime / server-side copy
- [x] 波次 2：dry-run done
- [x] 波次 3：mirror done（`TaskTypeMirror` + `runMirror`/`deleteDstExtras` + `MirrorStats` + 配置白名单 + 单测；validateTask 锁定 AfterCopy=verify_then_skip；taskIsDestructive 含 mirror；master 冲突检测无须改动）
- [x] 波次 4：dashboard UI done（SyncRuleCreateDialog 暴露 type=mirror、onSymlink、onExisting、dryRun、confirm 字段及 i18n 提示；DryRun/Confirm 互斥与破坏性确认提示走 inline alert；后端字段 omitempty 透传）
- [ ] 波次 5：镜像 + 部署 + e2e（镜像 bump 后到 test-k3d 跑 mirror dry-run → confirm-apply 全链路；前置依赖：波次 4 完成）

## 风险

| 风险 | 缓解 |
|------|------|
| OnExisting=newer_only 跨 backend mtime 精度差异（S3 秒，local 纳秒） | 1s 容差；newer_only 必须配 mtime 保留（子项 5）才有意义，否则永远不跳 |
| server-side copy S3 5GB 上限 | multipart copy fallback；不可走时报错而非静默退化 |
| mirror delete-extraneous 误删 | 默认 DryRun=true；Confirm 双重确认字段；validateTask 强制校验 |
| s3 user-metadata mtime 与 LastModified 冲突 | 优先级：x-amz-meta-syncnode-mtime > LastModified；缺失时回退 |
| 并行 4 个 sub-agent 改 SyncRuleConfig 同一结构 | 字段名先在本文档定下，agent 按名严格添加；最后合并验证 `go build` |
| cfs server-side copy 本期不做 | 已说明；Caps.ServerSideCopy=false，自动 fallback；不阻塞 |

## 字段最终名称（agent 必须严格用这些名）

```go
type SyncRuleConfig struct {
    // 已有字段...

    OnSymlink   string `json:"onSymlink,omitempty"`    // "" | "skip" | "follow" | "error"
    OnExisting  string `json:"onExisting,omitempty"`   // "" | "verify_then_skip" | "always_skip" | "newer_only" | "overwrite"
    DryRun      bool   `json:"dryRun,omitempty"`
    Confirm     bool   `json:"confirm,omitempty"`      // type=mirror 实跑必须 true
}

type PutOptions struct {
    // 已有字段...
    Mtime *time.Time
}

type Caps struct {
    // 已有字段...
    ServerSideCopy bool
}

const TaskTypeMirror TaskType = "mirror"
```

i18n key 名（dashboard）：
- `ruleOnSymlinkHint`
- `ruleOnExistingHint`
- `ruleDryRunHint`
- `ruleTypeMirrorHint`
- `ruleConfirmHint`
