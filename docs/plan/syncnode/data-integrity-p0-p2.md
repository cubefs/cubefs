# SyncNode 数据完整性 P0-P2 设计实现方案（SDD）

> Status: Draft v1
> Date: 2026-05-21
> Owner: tao.fang
> Affected repos: `cubefs`（核心）/ `cubefs-dashboard`（仅前端字段）/ `cubefs-deploy`（仅 rc 号 bump，无配置变更）

## 1. Background & Goals

用户的核心场景是**数据搬运 / move（即 `afterCopy=verify_then_delete_src`）**。当前 syncnode 在「verify-then-delete-src」语义下只对比 size，不查校验和；网络抖动 / 半写文件 / 静默 bit-flip 都可能让"通过验证"的目标对象与源不一致，但源在本轮就被删除——这是数据搬运链路上**唯一可能丢数据的位置**，必须先解决。在此基础上 P1/P2 把"传输中源被改"和"个别失败导致整批失败 / 中断后从 0 重传"两个常见痛点也补上。

### 1.1 P0 目标（强制 checksum 校验，数据搬运的安全网）
- 在传输管道里同步计算 src 端 checksum；
- 落盘后用 dst 端的原生 checksum（S3 ETag / CFS CRC / local SHA256）做端到端比对；
- 比对通过后才允许删除 src（即仅在严格校验通过的前提下执行 verify_then_delete_src 语义）；
- 若不一致：保留源、标记任务失败、上报 Metric，永远不删源；
- 跳过逻辑（idempotent re-run skip）从"size+ETag"升级为"size+checksum"，避免 same-size mutation 被错跳。

### 1.2 P1 目标（源被改检测，避免拷半截 / 拷错版本）
- 传输前 Head 一次（拿 size + mtime + etag）；
- 传输后再 Head 一次（同 key），如果 size / mtime / etag 与传输前快照不一致，判定为「source mutated mid-transfer」；
- 用户可选三种处理：`fail` / `skip`（跳过此文件，不删源）/ `retry`（最多 N 次后判失败）；
- 目的：彻底杜绝"边写边搬"导致目标侧出现半截或旧版本数据。

### 1.3 P2 目标（per-file retry + 多文件 / 大文件断点续传）
- 单个文件失败不再立即把整批任务标记 failed，而是按规则配置的 `maxRetries` + 指数退避 N 次，仍失败才计入 FilesFailed；
- 已有 `bolt.InProgressStore`（文件级 + s3 multipart UploadID）只持久化、不消费——P2 把消费侧补齐：
  - s3 dst：multipart UploadID 续传（manager.Uploader 内置），靠的是把 UploadID 写入 InProgress；
  - cfs/local dst：从已写 BytesDone 偏移开始 Get range read + 追加写；
  - 多文件断点：任务级 already-done 文件清单从 task_store 取（已实现的 idempotency check 已经覆盖）；
- 单 rc 中断（kill -9 / OOM / 节点宕）后，下一次 fire 同 rule 时自动 resume。

## 2. Non-Goals (v1)

为防止范围漂移，以下功能**第一版不实现**，未来另立设计：

- rclone bisync（双向同步、删除传播、冲突标记）；
- chunk-level checksum tree（仅做整文件 checksum，不做分块树）；
- 加密 / 压缩 / 服务端拷贝（s3 server-side copy 上限 5GiB，且 mode 复杂）；
- SFTP / WebDAV / Azure Blob backend；
- 跨 rule 全局去重 / 内容寻址；
- 客户端 ETag 计算（MD5-of-MD5 用以匹配 AWS 多段 ETag）——dst 是 s3 时只比 size + 我们计算的 sha256（写入 Metadata）；
- check task 流程不变（本 SDD 不动 check_task.go，check 已有 ETag 比对，足够）。

## 3. Architecture impact per repo

| Repo | 影响范围 | 需要 commit 提交？ |
|---|---|---|
| `cubefs` | `proto/sync_rule.go`（schema +4 字段）、`syncnode/backend/backend.go`（接口 +2 / 类型 +3）、`syncnode/backend/{cfs,s3,local}/*.go`（各实现 GetChecksum + 写入端 sha256）、`syncnode/executor/{executor.go,sync_task.go}`（流程改写 + per-file retry + resume wiring）、`syncnode/tasks/runner.go`（Task 字段透传） | **是**（用户只许提交 cubefs） |
| `cubefs-dashboard` | `frontend/src/pages/.../syncManage/components/SyncRuleCreateDialog.vue`（新增 4 个表单字段 + 透传到 generatedJson + fillFormFromConfig）、`frontend/src/i18n/lang/{zh,en}/index.js`（key 文案） | 否（dashboard 仓库改动不在本次提交范围） |
| `cubefs-deploy` | 仅 `_envcommon/images.hcl` 的 cubefs rc 号 +1，无 ConfigMap / Helm values 变更（所有新参数走 SyncRule，不走 syncnode `sync.json`） | 否（按硬约束 images.hcl 不 commit） |

> Dashboard 后端是**纯透传**（`map[string]interface{}` + master 转存），新字段无需 dashboard 后端改动 / migration。

## 4. Backend interface delta（cubefs/syncnode/backend/backend.go）

### 4.1 接口新增方法

```go
// GetChecksum returns the backend-native (or computed) checksum for key.
// Returns ErrKeyNotFound if missing. Algorithm is implementation-defined:
//   - cfs:   crc32 (CubeFS internal); fallback to streaming sha256 if unavailable
//   - s3:    ETag if it's a single-part object (md5 hex); for multipart, the
//            sha256 stored in object Metadata (key: x-amz-meta-syncnode-sha256)
//   - local: sha256 hex computed on read (cached on file mtime+size key in mem)
//
// `algorithm` echoes which algo was used so the executor can decide whether
// the value is comparable across endpoints.
GetChecksum(ctx context.Context, key string) (sum string, algorithm string, err error)
```

### 4.2 PutOptions 扩展

```go
type PutOptions struct {
    // ... 现有字段 ...

    // ComputeChecksum tells the backend to compute a sha256 alongside the
    // upload and return it in PutResult. Object stores additionally persist
    // it as user metadata (`x-amz-meta-syncnode-sha256`) so future Heads /
    // GetChecksum calls can read it back without re-streaming. POSIX
    // backends just return the value (no on-disk metadata).
    ComputeChecksum bool
}
```

### 4.3 Put 返回值改为结构化

```go
// 旧签名： Put(...) (etag string, err error)
// 新签名：
type PutResult struct {
    ETag        string // backend-native (s3 etag, empty for POSIX/CFS)
    Checksum    string // sha256 hex (only set if PutOptions.ComputeChecksum)
    Algorithm   string // "sha256" when Checksum populated; "" otherwise
    BytesPut    int64  // for sanity: bytes the backend acknowledged
}

Put(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (PutResult, error)
```

### 4.4 Caps 扩展

```go
type Caps struct {
    // ... 现有字段 ...

    // NativeChecksum reports whether GetChecksum returns a fast (O(1))
    // server-side value. s3 = true (ETag), cfs = true (crc32), local =
    // false (must stream). Used by the executor to decide whether to
    // skip a sha256 compute on the Get side when the native checksum
    // already exists on the dst side.
    NativeChecksum bool
}
```

### 4.5 错误新增

```go
var ErrChecksumMismatch = errors.New("backend: checksum mismatch")
```

> 各实现细节见 §6（Backend impls）。

## 5. Schema delta（cubefs/proto/sync_rule.go）

`SyncRuleConfig` 新增 4 个字段，全部 `omitempty`：

```go
type SyncRuleConfig struct {
    // ... 现有字段 ...

    // ChecksumMode controls how strict the post-copy verification is.
    //   ""        → "size_etag"  legacy default; size + (etag when both
    //                            sides have one); P0 fallback when both
    //                            backends lack NativeChecksum AND
    //                            ComputeChecksum=false.
    //   "size_etag"
    //   "strong"  → ALWAYS compute sha256 on the source side during
    //               transfer; compare against dst checksum (native or
    //               metadata-stored sha256). REQUIRED for AfterCopy =
    //               verify_then_delete_src to actually delete src; with
    //               any other value the executor refuses to delete.
    ChecksumMode string `json:"checksumMode,omitempty"`

    // OnSourceMutated controls behaviour when the source key changes
    // (size/mtime/etag) between the pre-transfer Head and the post-
    // transfer Head.
    //   ""       → "fail" default
    //   "fail"   → error the file; counted in FilesFailed, never deletes src
    //   "skip"   → log + skip; counted in FilesSkipped (with a special
    //              SkippedSamples reason tag); does not delete src
    //   "retry"  → re-fetch and re-upload up to MaxRetries; counts as failed
    //              after exhaustion
    OnSourceMutated string `json:"onSourceMutated,omitempty"`

    // MaxRetries is the per-file retry cap. 0 → 0 retries (1 attempt total),
    // current behaviour. P2 default (when omitted) is 3 with exponential
    // backoff (1s, 2s, 4s, max 30s).
    MaxRetries int `json:"maxRetries,omitempty"`

    // ResumeEnabled toggles the breakpoint-resume code path. Default off
    // for safety; operators opt in. When true:
    //   - executor consults bolt.InProgressStore at file start and resumes
    //     from BytesDone (POSIX/CFS) or UploadID (s3 multipart);
    //   - on each successful Put, the breakpoint is cleared.
    ResumeEnabled bool `json:"resumeEnabled,omitempty"`
}
```

`spec.SyncRuleConfig` 通过 type alias 自动继承（无需改 syncnode/spec/types.go）。

## 6. Backend implementations

### 6.1 `backend/local`（POSIX）

```
Put(opts.ComputeChecksum=true):
  - Tee reader: io.TeeReader(body, sha256.New()) → io.Copy 到目标 file
  - 关闭文件后从 hash 取 hex → PutResult.Checksum
  - PutResult.Algorithm = "sha256"
GetChecksum(key):
  - 打开文件 → io.Copy 到 sha256.New() → hex
  - 内存里用 (path, mtime_ns, size) → sha256 做轻量缓存（map+lock，cap 1000，LRU evict）
Caps.NativeChecksum = false
```

### 6.2 `backend/s3`

```
Put(opts.ComputeChecksum=true):
  - 走 manager.Uploader（已实现 multipart）
  - Wrap body 为 sha256-tee reader：io.TeeReader 在管道头部计算
  - 把 hex 结果通过 PutObjectInput.Metadata["syncnode-sha256"] 写入对象元数据
    （manager.Uploader 也支持 SetMetadata，单段 / 多段都行）
  - PutResult.Checksum = hex, PutResult.Algorithm = "sha256"
GetChecksum(key):
  - HeadObject → 读 Metadata["syncnode-sha256"]
  - 命中：返回 sha256
  - 未命中：返回 ETag（仅当为单段 ETag，即 hex 32 位无 "-"），algorithm="md5"
  - 否则：返回 ErrChecksumMismatch（强校验拒绝）
Caps.NativeChecksum = true
```

> 不修改既有 multipart 阈值（64MiB/16MiB）。

### 6.3 `backend/cfs`

```
Put(opts.ComputeChecksum=true):
  - sha256-tee reader 同 local；CFS 当前 Put 走 fuse mount 写文件
  - 目录结构落盘后没有元数据通道 → 把 sha256 写到伴随文件
    `<key>.syncnode.sha256`（隐藏小文件，仅 64B）
  - 后续 GetChecksum 优先读伴随文件
GetChecksum(key):
  - 先读 `<key>.syncnode.sha256`，命中即返回
  - 未命中：流读文件计算 sha256，并写回伴随文件（best-effort，写失败不报错）
Caps.NativeChecksum = false（伴随文件只是缓存，不算 native）
```

> 伴随文件路径策略和 list 排除：所有 `*.syncnode.sha256` 在 List 时自动 skip（在 cfs backend 的 List 实现里加一行 filter；与 hidden file 风格一致）。

## 7. Executor flow（cubefs/syncnode/executor/sync_task.go）

新流程伪码（仅展示 syncOneFile，整体 runSync 框架不变）：

```go
func (e *Executor) syncOneFile(ctx, t, entry, r, p) error {
    dstKey := rebaseKey(...)

    // 1) Pre-Head src（P1）—— 仅当 OnSourceMutated != "" 时启用
    var srcPre headSnapshot
    if t.OnSourceMutated != "" {
        srcPre = headSnapshotOf(ctx, t.Src, entry.Key)
    }

    // 2) Idempotency: skip if dst already matches
    if dstSize, dstETag, _, herr := t.Dst.Head(...); herr == nil {
        if shouldSkip(entry, dstSize, dstETag, t.ChecksumMode, t.Src, t.Dst, dstKey) {
            // skip path（含强校验：调用 GetChecksum 比对）
            return nil
        }
    } else if !errors.Is(herr, ErrKeyNotFound) { return ... }

    // 3) Resume? P2 wiring
    var resumeOffset int64
    var resumeUploadID string
    if t.ResumeEnabled && e.inprogress != nil {
        if bp, err := e.inprogress.Get(ctx, breakpointKey(t.ID, entry.Key)); err == nil {
            resumeOffset = bp.BytesDone
            resumeUploadID = bp.UploadID
        }
    }

    // 4) Per-file retry loop（P2）
    var lastErr error
    for attempt := 0; attempt <= t.MaxRetries; attempt++ {
        if attempt > 0 {
            backoffSleep(ctx, attempt) // 1s,2s,4s,...,30s
        }
        result, err := transferOnce(ctx, t, entry, dstKey, resumeOffset, resumeUploadID)
        if err == nil {
            // 5) Post-Head src（P1）—— check mutation
            if t.OnSourceMutated != "" {
                srcPost := headSnapshotOf(ctx, t.Src, entry.Key)
                if mutated(srcPre, srcPost) {
                    // delete dst (we wrote a stale copy), set lastErr accordingly
                    _ = t.Dst.Delete(ctx, dstKey)
                    lastErr = ErrSourceMutated
                    if t.OnSourceMutated == "skip" {
                        // counted as skipped
                        atomic.AddInt64(&p.FilesSkipped, 1); return nil
                    }
                    if t.OnSourceMutated == "fail" {
                        return lastErr
                    }
                    // "retry" → loop continues, srcPre = srcPost for next attempt
                    srcPre = srcPost
                    continue
                }
            }

            // 6) Strong checksum verify（P0）
            if t.ChecksumMode == "strong" {
                dstSum, dstAlgo, gerr := t.Dst.GetChecksum(ctx, dstKey)
                if gerr != nil { lastErr = gerr; continue }
                if !checksumEqual(result.Checksum, result.Algorithm, dstSum, dstAlgo) {
                    _ = t.Dst.Delete(ctx, dstKey)
                    lastErr = ErrChecksumMismatch
                    continue
                }
            }

            // 7) AfterCopy = verify_then_delete_src（P0 升级）
            if t.AfterCopy == AfterCopyVerifyThenDeleteSrc {
                if t.ChecksumMode != "strong" {
                    return errors.New("verify_then_delete_src requires checksumMode=strong")
                }
                if derr := t.Src.Delete(ctx, entry.Key); derr != nil { return derr }
            }

            // 8) Clear breakpoint
            if t.ResumeEnabled && e.inprogress != nil {
                _ = e.inprogress.Delete(ctx, breakpointKey(t.ID, entry.Key))
            }
            atomic.AddInt64(&p.FilesDone, 1)
            return nil
        }
        lastErr = err
        // Update breakpoint on partial progress (transferOnce surfaces it via result.BytesAcked / result.UploadID)
        if t.ResumeEnabled && e.inprogress != nil && result.PartialBytes > 0 {
            _ = e.inprogress.Put(ctx, &Breakpoint{
                TaskID:    t.ID,
                Key:       breakpointKey(t.ID, entry.Key),
                BytesDone: result.PartialBytes,
                UploadID:  result.UploadID,
            })
            resumeOffset = result.PartialBytes
            resumeUploadID = result.UploadID
        }
    }
    return lastErr
}
```

`transferOnce` 是把现有 Get→io.Pipe→Put 那段抽出来的小函数；把 `ComputeChecksum=true`（当 `ChecksumMode=="strong"`）传到 PutOptions，把 result 拼回去。

> **校验拒绝兜底（防呆）**：在 `validateTask`（executor.go）增加：`if t.AfterCopy == AfterCopyVerifyThenDeleteSrc && t.ChecksumMode != "strong" → return error`。这把"老用户没读 release note 就升级"导致的静默回退堵住——必须显式选 `strong` 才能开 verify_then_delete_src。

## 8. Bolt checkpoint wiring

`bolt.Breakpoint.Key` 复用为 `<taskID>:<entryKey>` 复合键以支持多文件同任务下的并发写入：

- 现有 schema 不动（Key 只是 string）；
- 在 executor 里加 helper：`func breakpointKey(taskID, entryKey string) string { return taskID + ":" + entryKey }`；
- bbolt 单 bucket 仍可承载（每文件 1 条记录，删除即时）。

executor 拥有 `inprogress bolt.InProgressStore` 字段（P2 新增），通过 `WithInProgressStore(s)` Option 注入；nil 表示禁用 resume（保持向后兼容、单元测试静默通过）。

## 9. Dashboard 改动（cubefs-dashboard 前端）

仅 `frontend/src/pages/cfs/clusterOverview/clusterInfo/syncManage/components/SyncRuleCreateDialog.vue` + i18n。

### 9.1 表单 emptyForm() 新增字段

```js
checksumMode: '',     // '' | 'size_etag' | 'strong'
onSourceMutated: '',  // '' | 'fail' | 'skip' | 'retry'
maxRetries: 0,
resumeEnabled: false,
```

### 9.2 表单 UI（紧跟在「拷贝后处理」el-form-item 之后）

- 校验模式（el-select）：空 / size_etag / strong（hint：搬运语义必须 strong）；
- 源文件中改后处理（el-select）：空 / fail / skip / retry；
- 单文件最大重试次数（el-input-number, 0~10）；
- 启用断点续传（el-switch）。

当 `form.afterCopy === 'verify_then_delete_src' && form.checksumMode !== 'strong'` 时显示 `el-alert` 红字警告："搬运语义需 checksumMode=strong，否则后端会拒绝"。

### 9.3 generatedJson 透传

```js
if (this.form.checksumMode) payload.checksumMode = this.form.checksumMode
if (this.form.onSourceMutated) payload.onSourceMutated = this.form.onSourceMutated
if (this.form.maxRetries > 0) payload.maxRetries = this.form.maxRetries
if (this.form.resumeEnabled) payload.resumeEnabled = true
```

### 9.4 fillFormFromConfig（编辑/查看模式回填）

```js
this.form.checksumMode    = config.checksumMode    || ''
this.form.onSourceMutated = config.onSourceMutated || ''
this.form.maxRetries      = config.maxRetries      || 0
this.form.resumeEnabled   = !!config.resumeEnabled
```

### 9.5 i18n key（zh + en，各 4 条）

- `sync.checksumMode` / `.checksumModeHint` / `.onSourceMutated` / `.onSourceMutatedHint` / `.maxRetries` / `.resumeEnabled`。

> 后端 / migration **零改动**（dashboard 是 map[string]interface{} 透传）。

## 10. 测试策略

| 层级 | 用例 | 期望 |
|---|---|---|
| backend/local 单测 | Put(ComputeChecksum=true) → GetChecksum 一致；篡改文件后 GetChecksum 改变 | 通过 |
| backend/s3 单测（minio CI） | Put 后 HeadObject 看到 `x-amz-meta-syncnode-sha256`；GetChecksum 单段 / 多段都返回正确值 | 通过 |
| backend/cfs（test-k3d 起 fuse） | Put 后 `<key>.syncnode.sha256` 伴随文件存在；List 不返回伴随文件 | 通过 |
| executor sync_task 单测 | (1) checksumMode=strong + 篡改 dst → 任务 failed + dst 自动 Delete + src 不删；(2) AfterCopy=verify_then_delete_src 时 ChecksumMode != strong → validateTask 拒绝；(3) OnSourceMutated=retry 中途改 src → 重试 N 次最终失败；(4) ResumeEnabled + 中断后再跑 → 从 Breakpoint.BytesDone 续传，bytes 计数器无重复 | 全绿 |
| 集成测（test-k3d）| local→cfs / cfs→s3 / s3→cfs 三对组合，跑一份 8GiB 数据集 strong 模式，断电（kill -9 syncnode pod）后再起，检查最终 sha256 校验通过 + 没有源文件丢失 | 通过 |

## 11. Rollout

1. cubefs 仓库改动按下文 Wave 推进，提交一个 feat commit，push origin/ft_support_rdma；
2. dev_bd 拉新代码，`make build && make image version=v3.5.3.rcN push=1`（N = 当前 +1）；
3. 本地 `cubefs-deploy/_envcommon/images.hcl` 把 cubefs_image rc 号 +1（**不 commit**）；
4. `ENV=test-k3d make apply-syncnode`（仅重启 syncnode DaemonSet，master/metanode/datanode 不影响）；
5. 起来后跑集成测试（§10 最后一行）；
6. 跑挂了 → rc 号再 +1，回到第 2 步（不回滚）。

dashboard 前端改动跟着下次 dashboard 镜像 rc 走（不阻塞 cubefs 上线，因为新字段缺省值 = 旧行为）。

## 12. Wave / Subagent 任务分解

并行 Wave 1（独立改动，可并发跑）：
- **A** [cubefs] `syncnode/backend/backend.go`：新增 GetChecksum / 改 Put 返回值 / Caps.NativeChecksum / PutOptions.ComputeChecksum / ErrChecksumMismatch；同步修 `backend_test.go` 中签名相关编译错误（mock 实现）；
- **C** [cubefs] `proto/sync_rule.go`：SyncRuleConfig +4 字段，全部 omitempty；
- **D** [cubefs] `syncnode/bolt/inprogress.go`：注释从 "P0 stores breakpoints but executor doesn't yet read" 改为 "consumed by executor when Task.ResumeEnabled"；新增 `breakpointKey(taskID, entryKey)` helper（放 executor 包里，bolt 包不用动 schema）。

依赖 Wave 1 完成后跑 Wave 2：
- **A2** [cubefs] backend 三个实现（local / s3 / cfs）补齐 GetChecksum + ComputeChecksum 路径 + Caps.NativeChecksum；
- **B** [cubefs] `syncnode/executor/{executor.go,sync_task.go}` 主流程改写 + per-file retry + resume wiring + validateTask 防呆；
- **B2** [cubefs] `syncnode/tasks/runner.go::buildTask` 把 ChecksumMode / OnSourceMutated / MaxRetries / ResumeEnabled 透传到 `executor.Task`（Task struct 也要加这 4 字段）；
- **B3** [cubefs] `syncnode/executor/sync_task_test.go` 加 §10 用例。

Wave 3（前端，独立于 cubefs）：
- **E** [dashboard] `SyncRuleCreateDialog.vue` 新表单字段 + generatedJson + fillFormFromConfig + 校验提示；
- **F** [dashboard] zh / en i18n key。

## 13. 已知限制 / 后续 backlog（不在本次实施范围）

- chunk-level checksum tree（处理超大文件部分坏损）；
- s3 server-side copy（避开 Get→Put 流量）；
- multipart upload 续传时 part-list 校验（manager.Uploader 不暴露 ListParts，需自己写 SDK 调用）；
- `ChecksumMode=strong` 下 algorithm 不一致（src=sha256, dst=md5/etag）的「降级比对」策略——目前是直接 ErrChecksumMismatch，更友好的策略可在 v2 里加 `AlgorithmFallback` 字段；
- bench/load/check 三类任务的 checksum 升级（本 SDD 仅升级 sync）。

