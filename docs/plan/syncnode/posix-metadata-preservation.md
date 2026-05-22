# POSIX 元数据保留（P2）

> 目标：在 P1 已完成的 mtime 保留之上，补齐 `mode / owner(uid+gid) / xattr` 三项 POSIX 元数据的跨 backend 保留，使 syncnode 在 rclone migration 场景下能完整 round-trip 文件属性。
>
> ACL 在本期降级处理（见"不做"）。

## 背景

P1 mtime 保留之后，唯一阻断"rclone 用户完整迁移"的就是其它 POSIX 属性：mode、owner、xattr。rclone 通过 `--metadata` 把这些塞进 S3 user metadata（`x-amz-meta-mode/uid/gid/mtime`），实现 local↔s3 的属性 round-trip。syncnode 已经用同样的模式（`x-amz-meta-syncnode-mtime`）做完 mtime，本期把这个模式扩展到其它三项。

## 范围

**做（3 项）**

| # | 子项 | 用户痛点 |
|---|------|----------|
| 1 | mode 保留 | 文件权限位（rwxrwxrwx + suid/sgid/sticky）丢失 |
| 2 | owner 保留（uid+gid 合并） | 跨节点同步后所有权变成执行进程身份 |
| 3 | xattr 保留 | 用户扩展属性（包括 POSIX ACL 所在的 system.posix_acl_access）丢失 |

**不做（明确放弃，进 backlog）**

- **通用 ACL 翻译**：POSIX ACL ↔ S3 ACL 是两套完全不同的模型，cross-backend 翻译几乎不可能正确。POSIX ACL 通过 xattr 自然带（local↔local、local↔cfs 同 POSIX 体系内可用）。S3 canned ACL（`private/public-read/...`）作为 endpoint-level 独立配置，**不进 `PreserveXxx` 开关**——rclone 也是这个做法。
- **POSIX→S3 grantee 映射**：需要 user identity registry，超范围。
- **Windows ACL / NTFS 权限**：不在 syncnode 支持范围。
- **拆分 PreserveUID / PreserveGID**：使用场景极少；统一 `PreserveOwner`。

## 不做的边界（防漂移）

- **不动 master 协议**：新字段加在 `proto.SyncRuleConfig`，老规则 Unmarshal 时零值，行为不变。
- **不动 BoltDB schema**：metadata 不持久化到 checkpoint（每次任务执行时从 source 实时读取）。
- **不引入 user identity registry**：uid/gid 是裸数字，跨集群语义靠用户自己保证。
- **不动 dashboard 后端**：纯字段透传。dashboard 前端按 mtime 的相同模式加 4 个 boolean + 1 个枚举。
- **不动 backend Caps 接口的形状**：只新增 cap bit，不重组现有字段。

## 设计

### 1. 协议层：`proto.SyncRuleConfig` 新字段

```go
type SyncRuleConfig struct {
    // 已有 mtime / OnSymlink / OnExisting / DryRun / Confirm / ...

    PreserveMode  bool `json:"preserveMode,omitempty"`
    PreserveOwner bool `json:"preserveOwner,omitempty"` // uid+gid 合并
    PreserveXattr bool `json:"preserveXattr,omitempty"`

    // OnMetadataUnsupported: 目标 backend 不支持某项元数据时的处理
    // ""(=warn) | "warn" | "skip" | "error"
    //   warn  : 记日志、计入 stats、继续传文件主体
    //   skip  : 整个文件跳过
    //   error : 任务失败
    OnMetadataUnsupported string `json:"onMetadataUnsupported,omitempty"`
}
```

向后兼容：所有字段 `omitempty`，老规则零值 = 不保留 = 行为不变。

### 2. Backend 抽象层

**`PutOptions` 扩展**（与 `Mtime *time.Time` 同模式）：

```go
type PutOptions struct {
    // 已有 Mtime *time.Time ...

    Mode   *uint32             // POSIX file mode；nil = 不设置
    UID    *uint32             // POSIX uid；nil = 不设置
    GID    *uint32             // POSIX gid；nil = 不设置
    Xattrs map[string][]byte   // xattr name → raw bytes；nil/empty = 不设置
}
```

`*uint32` 而非 `uint32`：区分"不保留"和"保留为 0"（root/root 是合法值）。

**`Caps` 扩展**（与 `NativeMtimeWrite` 同模式）：

```go
type Caps struct {
    // 已有 NativeMtimeWrite bool ...

    NativeModeWrite  bool  // 持久化 PutOptions.Mode 后 Stat/List 能取回
    NativeOwnerWrite bool  // 同上 UID/GID
    NativeXattrWrite bool  // 同上 Xattrs
}
```

**Stat 接口**（新增）——避免污染 Head：

```go
type Backend interface {
    // 已有 Head/Get/Put/List/Delete/...

    // Stat returns full POSIX-style metadata. Backends that don't natively
    // support a field return zero/nil for it. Implementations:
    //   - local: syscall.Lstat + listxattr/getxattr
    //   - cfs:   mw.InodeGet + mw.XAttrList/XAttrGet
    //   - s3:    HeadObject → 解析 user metadata header
    Stat(ctx context.Context, key string) (Stat, error)
}

type Stat struct {
    Size  int64
    ETag  string
    Mtime time.Time

    Mode   *uint32             // nil = backend 不支持或未设置
    UID    *uint32
    GID    *uint32
    Xattrs map[string][]byte
}
```

`Head` 保留不动（避免破坏现有调用方），`Stat` 作为新接口可选实现；executor 用 `Stat` 时优先调用，没有实现就用 `Head` + 假装元数据空。

### 3. Backend 实施细节

| Backend | mode 写 | mode 读 | owner 写 | owner 读 | xattr 写 | xattr 读 |
|---------|---------|---------|----------|----------|----------|----------|
| **local** | `syscall.Chmod` | `Lstat` → `Mode()` | `syscall.Lchown` | `Lstat` → `Sys().(*Stat_t)` | `syscall.Setxattr` | `Listxattr`+`Getxattr` |
| **cfs** | `mw.Setattr` (`AttrMode`) | `mw.InodeGet` | `mw.Setattr` (`AttrUid/Gid`) | `mw.InodeGet` | `mw.XAttrSet_ll` | `mw.XAttrList_ll` + `mw.XAttrGet_ll` |
| **s3** | `x-amz-meta-syncnode-mode`=八进制字符串 | HeadObject metadata | `x-amz-meta-syncnode-uid/gid`=十进制 | HeadObject | `x-amz-meta-syncnode-xattrs`=base64(JSON) | HeadObject |

#### s3 header 命名与读 fallback 链

写：只写 `x-amz-meta-syncnode-{mode,uid,gid,xattrs}`，归属明确、不污染 namespace。

读 fallback 顺序：

1. `x-amz-meta-syncnode-{mode,uid,gid,xattrs}` — 我们自己写的
2. `x-amz-meta-{mode,uid,gid}` — rclone naked 命名（xattr 在 rclone 里没标准编码，跳过 fallback）
3. 都没有 → 字段为 nil（表示该对象没保留过此元数据）

价值：rclone 写的 bucket 我们能读出 mode/uid/gid，反之亦然。

#### s3 xattr 编码方案（确认：单 header JSON）

```
x-amz-meta-syncnode-xattrs = base64(JSON({"user.foo": base64(value_bytes), "system.posix_acl_access": base64(...)}))
```

S3 user metadata 限制（总 ≤ 2KB）：
- 编码后 size 计算：`len("x-amz-meta-syncnode-xattrs") + len(base64_payload)`
- 加上 mode/uid/gid/mtime/sha256 等已有 header，留给 xattr 的预算大约 1.5KB
- 超限按 `OnMetadataUnsupported` 走

### 4. Executor 流程

每个 sync task 处理一个文件时：

```
src.Stat(key) → SrcStat{mode, uid, gid, xattrs, mtime, ...}
                  ↓ filter by PreserveXxx flags
                  ↓
dst.Put(key, body, PutOptions{
    Mtime:  &SrcStat.Mtime,    // 已有
    Mode:   &SrcStat.Mode      if PreserveMode
    UID:    &SrcStat.UID       if PreserveOwner
    GID:    &SrcStat.GID       if PreserveOwner
    Xattrs: SrcStat.Xattrs     if PreserveXattr
})

// Caps 检查：dst.Capabilities().NativeModeWrite==false 但 PreserveMode==true
//   → 按 OnMetadataUnsupported 走（warn/skip/error）
```

**server-side copy 路径**：S3→S3 同 bucket 的 `CopyObject` 默认拷贝 user metadata（含我们的 syncnode-* header），无需额外处理。跨 backend 的 server-side copy 不存在，所以不用考虑跨 backend 的情况。

**Chown 权限**：syncnode pod 通常以 root 运行（DaemonSet），Chown 不会失败。非 root 跑测试时 Chown EPERM → 按 `OnMetadataUnsupported` 走（warn 默认）。

**xattr namespace 过滤**：

- 默认保留 `user.*`（用户命名空间）
- `system.posix_acl_access` / `system.posix_acl_default`（POSIX ACL）：默认保留
- `security.*`（SELinux 等）：默认跳过（运行时 LSM 重新生成）
- `trusted.*`：默认跳过（特权命名空间）
- 不做配置项暴露，按上面策略硬编码；如有需要后续再扩

### 5. Dashboard 透传

`cubefs-dashboard` 后端：完全无 Go 改动（字段 JSON 透传）。

前端：新增 4 个表单字段（按 `OnSymlink/OnExisting` 的模式）：

- `preserveMode` (switch)
- `preserveOwner` (switch)
- `preserveXattr` (switch)
- `onMetadataUnsupported` (select: warn/skip/error)

i18n 加 5 个 key（中英对照）：

- `rulePreserveModeHint`
- `rulePreserveOwnerHint`
- `rulePreserveXattrHint`
- `ruleOnMetadataUnsupportedHint`
- `ruleOnMetadataUnsupportedOptions` (warn/skip/error)

## 分阶段任务

### Phase 1：backend 抽象层 + local backend

1. `proto.SyncRuleConfig` 加 4 字段（PreserveMode/Owner/Xattr + OnMetadataUnsupported）
2. `backend.PutOptions` 加 Mode/UID/GID/Xattrs
3. `backend.Caps` 加 NativeModeWrite/OwnerWrite/XattrWrite
4. `backend.Stat` 接口 + `backend.Stat` struct 新增
5. local backend 实现 Stat / Put 时 Chmod+Chown+setxattr
6. backend contract test 扩 metadata round-trip 矩阵

### Phase 2：cfs backend

1. cfs Stat 实现（InodeGet + XAttrList）
2. cfs Put 时 SetAttr(mode/uid/gid) + XAttrSet
3. cfs server-side copy 路径走同样的 metadata pass-through
4. 单测覆盖

### Phase 3：s3 backend

1. s3 Stat 实现（HeadObject metadata 解析 + rclone naked fallback）
2. s3 Put 时把 metadata 编码进 user-metadata header
3. xattr 编码 / 解码 + 2KB 限制处理
4. ServerSideCopy 路径自动继承 user metadata（验证默认行为 + 单测）
5. CopyObject 重写 metadata 时（rclone naked → syncnode-prefix 归一化）

### Phase 4：executor 编排

1. sync_task 流程：Stat → 按开关填 PutOptions
2. mirror_task 同步走 metadata 通路
3. Caps 不支持时按 OnMetadataUnsupported 走
4. xattr namespace 过滤（user.*/posix_acl_*/skip system,trusted,security 非 acl）

### Phase 5：dashboard 前端 + i18n

1. `BenchRuleCreateDialog.vue` 加 4 字段
2. i18n zh/en 增 5 key

### Phase 6：镜像构建 + 部署 + e2e

1. cubefs/cubefs-dashboard 镜像 bump rc
2. `_envcommon/images.hcl` bump（**不 commit cubefs-deploy**）
3. `make ENV=test-k3d apply-master apply-metanode apply-datanode-hdd apply-objectnode apply-syncnode`
4. e2e driver 扩 3 个 case：preserve-mode / preserve-owner / preserve-xattr
5. 跨 backend round-trip case：local→s3→local（mode/uid/gid/xattr 全保留）

## 测试矩阵

| 场景 | src | dst | 验证点 |
|------|-----|-----|--------|
| local→local mode | local | local | dst 文件 stat().Mode() == src.Mode() |
| local→cfs mode | local | cfs | dst InodeGet.Mode == src.Mode |
| local→s3 mode | local | s3 | dst HeadObject metadata syncnode-mode 解析回 == src.Mode |
| s3→local mode (rclone naked) | s3 (写入时只有 `x-amz-meta-mode`) | local | dst Chmod 后 stat == header |
| local→cfs owner | local | cfs | dst Stat.UID/GID == src |
| local→s3 owner | local | s3 | metadata syncnode-uid/gid 正确 |
| local→local xattr | local | local | getxattr(dst) == getxattr(src) for each |
| local→s3 xattr | local | s3 | metadata syncnode-xattrs base64 decode 正确 |
| s3→local xattr | s3 | local | xattr round-trip 完整 |
| xattr 超 2KB | local | s3 | OnMetadataUnsupported=error → 任务失败 |
| Caps 不匹配 | local（含 mode） | (假) s3-no-mode-write | warn 模式 → 仅记日志 |

## 当前进度

- 2026-05-22: 落 plan doc + 决策对齐
- Phase 1: pending
- Phase 2-6: pending

## 风险

- **xattr namespace 默认黑白名单**：硬编码 `user.*` + POSIX ACL + 跳过 `security/trusted/system(其它)`。如果用户业务依赖 `trusted.*`（罕见）需要后续放开 → 留作 backlog 看真实需求。
- **Chown 在非 root 进程下 EPERM**：syncnode pod 通常 root，但本地开发 / 集成测试需要 sudo 或 fallback 到 warn 模式（默认就是 warn）。
- **S3 user-metadata 2KB 上限**：xattr 数量/长度大的文件会触发 OnMetadataUnsupported；用户可在规则里改成 skip 让大文件跳过、或 error 整体失败。
- **cfs xattr API 性能**：`mw.XAttrList_ll` 是同步元数据 RPC，大量小文件场景下可能成为瓶颈；先实现，后续按 perf 数据决定是否引入 batch。
- **POSIX ACL 跨集群 uid 不一致**：xattr 保留是 raw bytes，POSIX ACL 内部的 uid 不会被翻译；目标集群 uid namespace 不一致时 ACL 语义会漂移。文档中显式标注此风险。
