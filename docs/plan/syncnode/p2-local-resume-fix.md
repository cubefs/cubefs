# P2 设计缺陷修复：syncnode local backend 支持 resume offset

> Status: 实施中
> 负责人: syncnode P2 修复专项
> 关联设计: `docs/plan/syncnode/data-integrity-p0-p2.md` §6.4（local backend）、§7（executor 流程）

## 1. 背景

`data-integrity-p0-p2.md` 已经为 syncnode 落地 P2 断点续传：
- `Task.ResumeEnabled` 在 `bolt.InProgressStore` 中维护 `Breakpoint{BytesDone, UploadID}`；
- `syncnode/executor/sync_task.go::syncOneFile` 在重试前从 breakpoint 读取 `resumeOffset / resumeUploadID`；
- 失败时把 `resumeOffset + io.Copy n bytes` 作为 `partialBytes` 回写 breakpoint。

但执行链路上 **`resumeOffset` 仅传给了 `t.Src.Get`**（让源端跳到指定 offset 开始 range-read），从未传到目标 `t.Dst.Put`。具体来说 `transferOnce` 的写入路径是：

```
rc, _ := t.Src.Get(ctx, key, resumeOffset, 0)   // 源端从 offset N 开始读
n, _ := io.Copy(pipeWriter, rc)                 // pipe 中转
res, _ := t.Dst.Put(ctx, dstKey, pipeReader, entry.Size, putOpts)
```

而 `local.Put` 一律：
1. 创建随机 temp 文件；
2. 把 body 全部写进 temp（隐含从偏移 0 开始）；
3. `os.Rename(temp, dst)` 原子替换；
4. 失败时把 temp 删除。

结果：
- 即使前一次写到一半，dst 上根本没有任何"半成品"可以续传（temp 被清理）；
- 即使保留了 partial，executor 第二次重试也只会把 `(entry.Size - resumeOffset)` 字节从源拉过来，被 Put 当成完整新文件覆盖 dst，最终文件长度 = `entry.Size - resumeOffset`，与源不一致；
- 这是**设计层缺陷**：`backend.Backend.Put` 接口缺少表达"从指定偏移续写"的语义，且 `local.Put` 的原子写策略与续传不兼容。

> cfs/s3 backend 的 Put 也存在同类问题（cfs Put 先 truncate；s3 multipart 未持久化 UploadID）。本次修复**仅收敛 local backend**，cfs/s3 的同类缺陷在另外的 issue 跟踪，避免一次性扩大改动范围。

## 2. 范围 / 目标

### 做
- 在 `backend.PutOptions` 新增 `ResumeOffset int64` 字段（向后兼容：默认 0 = 旧行为）。
- 在 `backend.Caps` 新增 `ResumeOffsetWrite bool`，声明该后端是否在 Put 时尊重 `ResumeOffset`。
- 改造 `syncnode/backend/local/local.go::Put`：
  - 使用确定性 partial 文件名 `<dst>.syncnode.partial`，而不是随机 hex 后缀；
  - `ResumeOffset == 0`：truncate 后从头写入（原行为）；
  - `ResumeOffset > 0`：打开既有 partial，校验大小 ≥ ResumeOffset，`Seek(ResumeOffset)` 之后续写；
  - `ComputeChecksum=true` 且 `ResumeOffset>0`：先把 partial 已有的 `[0, ResumeOffset)` 字节灌进同一个 sha256 sink，再 tee body 进去，保证最终 PutResult.Checksum 是**整文件**的 sha256；
  - 成功完成后 `os.Rename(partial, dst)`；
  - 失败时**不再清理 partial**（保留供下次续传）；
  - 同时把 `Capabilities().ResumeOffsetWrite = true`。
- `syncnode/executor/sync_task.go::transferOnce` 中把 `resumeOffset` 写到 `putOpts.ResumeOffset`，让真正支持的 backend（如 local）能续写。对不支持的 backend 没有行为变化（field 被忽略），保持兼容。
- 单测覆盖：
  - 写 50 KiB → 失败留 partial → `ResumeOffset=50KiB` 续写 50 KiB → 最终文件 100 KiB 字节级等于源；
  - `ComputeChecksum + ResumeOffset>0` 路径返回的 sha256 与源整文件 sha256 一致；
  - `ResumeOffset > 既有 partial size` 时返回错误（防呆）；
  - `ResumeOffset == 0` 不依赖既有 partial，且仍然能 Rename 成 dst；
  - `Caps.ResumeOffsetWrite == true`。

### 不做
- 不动 `backend/cfs/cfs.go`：cfs Put 的 truncate-first 设计另案处理。Caps 中 `ResumeOffsetWrite` 默认 false。
- 不动 `backend/s3/s3.go`：s3 multipart resume 另案处理。Caps 中 `ResumeOffsetWrite` 默认 false。
- 不动 `executor/sync_task.go` 中的 breakpoint 编排（已有的 ResumeEnabled / Breakpoint 持久化 / 重试循环结构），仅做 `putOpts.ResumeOffset = resumeOffset` 的最小补丁。
- 不动 dashboard / cubefs-deploy。

### 不影响的边界
- 现有的 `backend.PutOptions` / `backend.Caps` 都是 struct，新增字段为零值默认 → 不破坏调用方。
- `local.Put` 在 `ResumeOffset==0` 路径上**逻辑等价**于现状，仅 partial 文件名由随机改为确定性。两次完整 Put 不会并发写同 key（syncnode 任务编排已保证），所以确定性命名是安全的。
- `local.List`（recursive + shallow）已在源头**过滤掉 `.syncnode.partial` 后缀**的条目（见下文 Phase B 第 4 步），partial 文件对所有 List 调用方（executor、retention、dashboard、contract 测试）透明，不再要求调用方知道这个后缀。这是一处由测试用例（`TestRunSync_RetentionNotAppliedAfterFailure` 在新 partial 残留语义下断言 `len(keys)==5` 不通过）暴露出来的设计缺口，已一并收敛。
- contract 测试无需改动：现有所有 case 都使用 `ResumeOffset==0`，行为保持一致。

## 3. 分阶段任务

### Phase A：interface 扩展（影响 backend.go）
1. `backend.PutOptions` 增加 `ResumeOffset int64`，并在 doc 注释中说明语义、与各后端的兼容性。
2. `backend.Caps` 增加 `ResumeOffsetWrite bool`。

### Phase B：local backend 落地
1. `local.go::Put`：
   - 引入 `partialSuffix = ".syncnode.partial"`，partial 文件路径 = `dst + partialSuffix`；
   - 拆分 `ResumeOffset==0` 与 `>0` 两条分支，复用 `copyWithBufferCtx`；
   - ComputeChecksum + ResumeOffset 的 sha256 续算逻辑（先把 `[0, ResumeOffset)` 灌进同一个 sink）；
   - `ResumeOffset>0` 时若 partial 大小 > ResumeOffset，先 `Truncate(partial, ResumeOffset)` 再 Seek 续写，避免上次崩溃前多写的字节污染最终文件；
   - 失败保留 partial（用户/operator 可基于 mtime+size 判断是否清理）。
2. `local.go::Capabilities`：`ResumeOffsetWrite: true`。
3. 删除旧的 `tempName()`（无引用残留），同时移除 `crypto/rand` 导入。
4. `local.go::walkShallow` / `walkRecursive`：在条目遍历时跳过 `*.syncnode.partial`。partial 是 Put 的实现细节，不应出现在 List 的契约结果里——否则 executor / retention / dashboard / contract 测试都得知道这个后缀，是典型的抽象泄露。`local_test.go::TestList_HidesPartialFiles` 钉住这一行为。

### Phase C：executor 透传
1. `executor/sync_task.go::transferOnce`：
   - `putOpts.ResumeOffset = resumeOffset`；
   - 仅当 `t.Dst.Capabilities().ResumeOffsetWrite` 时设置（避免误导其他 backend；不过 PutOptions 字段未识别时也只是被忽略，所以是 belt-and-suspenders）。

### Phase D：测试
1. `local_test.go::TestPut_ResumeOffset_HappyPath`：50KiB→partial→50KiB 续写→100KiB 整文件等于源；
2. `TestPut_ResumeOffset_PreservesChecksum`：同样 50+50 拼接，`ComputeChecksum=true` 返回值等于 sha256(整文件)；
3. `TestPut_ResumeOffset_StalePartial`：partial 大小 < ResumeOffset 时返回 `ErrConfigInvalid`；
4. `TestPut_ResumeOffset_StalePartialMissing`：partial 完全不存在 + `ResumeOffset>0` 时返回 `ErrConfigInvalid`；
5. `TestPut_ResumeOffset_FreshStartLeavesNoPartial`：ResumeOffset=0 + 成功 Put 后 partial 已被 rename 走，不残留；
6. `TestPut_ResumeOffset_TruncatesExtraBytes`：partial 多出的"上次崩溃前多写的"字节被 Truncate 掉，最终文件 = 源；
7. `TestCapabilities` 追加 `ResumeOffsetWrite == true`；
8. `TestList_HidesPartialFiles`：recursive + shallow 都不会把 `*.syncnode.partial` 暴露给调用方。

### Phase E：plan doc 落地（即本文）

## 4. 验收标准

- `cd /Users/tao.fang/codes/cubefs && go build ./syncnode/...` 退出码 0。
- `cd /Users/tao.fang/codes/cubefs && go test ./syncnode/backend/local/...` 全部通过，含新测试。
- `cd /Users/tao.fang/codes/cubefs && go test ./syncnode/executor/...` 全部通过（PutOptions.ResumeOffset 透传无回归）。
- 新增测试用例覆盖 resume 路径（写 50KB→中断→resumeOffset=50KB 继续→最终 100KB 字节级匹配源文件）。
- 本 plan doc 持续更新进度。

## 5. 当前进度

- [x] Phase A：backend.PutOptions / Caps 扩展
- [x] Phase B：local backend Put 改造 + Caps 报告
- [x] Phase C：executor transferOnce 透传 ResumeOffset
- [x] Phase D：local_test.go 新增 resume 用例
- [x] Phase E：plan doc

`go build ./syncnode/...` 通过；`go test ./syncnode/backend/local/...` 全绿；`go test ./syncnode/executor/...` 全绿（受 P2 改动影响的子集）。

## 6. 已知限制 / 后续 backlog

1. **cfs backend Put 同样需要续传支持**：当前 cfs Put 先 truncate；executor 给 cfs dst 写 breakpoint 后续传仍然会数据出错。需新开 issue 修复（思路：cfs Put 接受 ResumeOffset 后跳过 truncate，并通过 `ec.Write(ino, offset=ResumeOffset, ...)` 续写）。
2. **s3 backend multipart UploadID 续传**：当前 s3 Put 使用 `manager.Uploader` 抽象，不暴露 UploadID。需 fork uploader 或自行管理 multipart 状态，是较大改动。新开 issue 跟踪。
3. **partial 文件 GC**：当前 partial 文件在失败后保留，operator 需要按需清理；可在 list 工具或 reload 流程中添加 GC（>N 天未更新的 partial 清理）。
4. **跨节点恢复**：partial 文件依赖单节点本地盘，节点级 crash + reschedule 到其他节点不能续传——这与 syncnode 任务本来就 sticky 到节点一致，符合现网约束。
5. 本修复并未触及 executor 层 `transferResult.partialBytes` 的统计口径。当 dst 是 local backend 时，partialBytes 是 io.Copy 经过 pipe 的字节数，等价于"已写入 partial 的字节数"，与 BytesDone 语义一致；其他 backend 的 partialBytes 是源端读出的字节数，与目标 dst 的实际写入可能不一致——这是 cfs/s3 修复一起回收的工作，本次保持不变。

## 7. 验证记录

```
$ cd /Users/tao.fang/codes/cubefs
$ go build ./syncnode/...
# 退出码 0

$ go vet ./syncnode/...
# 无输出

$ go test ./syncnode/backend/local/... -count=1
ok      github.com/cubefs/cubefs/syncnode/backend/local   0.391s

$ go test ./syncnode/backend/... -count=1
ok      github.com/cubefs/cubefs/syncnode/backend         0.801s
ok      github.com/cubefs/cubefs/syncnode/backend/cfs     0.404s
ok      github.com/cubefs/cubefs/syncnode/backend/contract 0.989s
ok      github.com/cubefs/cubefs/syncnode/backend/local   1.301s
ok      github.com/cubefs/cubefs/syncnode/backend/s3      1.374s

$ go test ./syncnode/executor/... -count=1
ok      github.com/cubefs/cubefs/syncnode/executor        14.597s
```

新增/修改的 local 测试用例：
- TestCapabilities（新增 `ResumeOffsetWrite == true` 断言）
- TestPut_ResumeOffset_HappyPath
- TestPut_ResumeOffset_PreservesChecksum
- TestPut_ResumeOffset_StalePartial
- TestPut_ResumeOffset_StalePartialMissing
- TestPut_ResumeOffset_FreshStartLeavesNoPartial
- TestPut_ResumeOffset_TruncatesExtraBytes
- TestList_HidesPartialFiles
