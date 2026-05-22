# SyncRule Type=move 一等公民化

> 目标：把 rclone-move 语义从 `sync + afterCopy=verify_then_delete_src + checksumMode=strong` 三字段组合升格为顶层 `type=move`，避免用户误配，统一心智。

## 背景

- 现状：移动语义靠组合配置实现，UI 中藏在 sync 类型下面的二级选项里。
- `validateTask`（`syncnode/executor/executor.go:649`）强制 `verify_then_delete_src ⇒ checksumMode=strong`，但用户必须自己选两次。
- 用户反馈：dashboard 同步管理类型下拉没有 move，可发现性差。

## 范围

**做：**
- cubefs：`TaskTypeMove` 常量；`validateTask` 校验并自动锁定 afterCopy/checksumMode；`Run()` 分发到 `runSync`；冲突检查兼容 move；单元测试。
- dashboard 前端：`type` 下拉新增 move；move 类型下隐藏 afterCopy/checksumMode（或显示为只读锁定）；i18n hint。
- cubefs-deploy：bump `cubefs_image` 与 `dashboard_image`；apply-syncnode + apply-monitoring；test-k3d 端到端验证。

**不做：**
- 后端 master 不增加任何 type 白名单（保持当前"接受任意 string"的兼容性）。
- 不动 dashboard 后端（纯透传，无须改）。
- 不为旧规则做数据迁移——历史 `type=sync + afterCopy=verify_then_delete_src` 规则继续按 sync 路径跑，不强制转换为 move。
- 不实现可视化 move-history、迁移进度等扩展能力。

## 不做的边界（防漂移）

- 不引入 move 专属的 ledger/统计指标；复用 sync 的 progress/metrics。
- 不修改既有 sync 规则的字段语义；afterCopy=verify_then_delete_src 在 type=sync 下继续可用，作为旧路径兼容。

## 设计要点

### cubefs 后端

1. `executor/executor.go`
   - 加 `TaskTypeMove TaskType = "move"`
   - `validateTask` 新增对 move 的处理：
     - 如果 `t.Type == TaskTypeMove` 且用户设的 `AfterCopy != "" && AfterCopy != AfterCopyVerifyThenDeleteSrc` → 报错（防"我选 move 又另设 afterCopy"的歧义）
     - 如果 `t.Type == TaskTypeMove` 且用户设的 `ChecksumMode != "" && ChecksumMode != "strong"` → 报错
     - 否则强制 `t.AfterCopy = AfterCopyVerifyThenDeleteSrc`、`t.ChecksumMode = "strong"`
   - `validateTask` 的 type 白名单加 `TaskTypeMove`
   - `Run()` switch 加 `case TaskTypeMove: runErr = e.runSync(...)`
2. 冲突检查（`master/sync_rule_conflicts.go`）：move 对 src/dst 的占用与 sync 一致，duplicate/prefix-overlap 检查均按"非 check"分支处理，已天然支持，无需改。
3. `runner.go` 的 `TriggerAs(wantType)` 路径：仅 sync/load 两条 alias，move 不引入 alias，靠 `/syncRule/trigger` 通用路径触发。

### dashboard 前端

1. `SyncRuleCreateDialog.vue`
   - `type` 下拉加 `<el-option label="move" value="move">`
   - hint：`move：迁移 src → dst，写后强校验（sha256）并删除源；等价 type=sync + afterCopy=verify_then_delete_src + checksumMode=strong`
   - `type === 'move'` 时：
     - 隐藏 afterCopy 表单项（写死 verify_then_delete_src 由后端兜底）
     - 隐藏 checksumMode 表单项（写死 strong）
     - 隐藏"verifyDeleteRequiresStrong"那条 alert（约束已被类型本身保证）
   - `submit` 时 `type === 'move'` 不再透传 afterCopy/checksumMode（让后端 validateTask 自动锁定）
2. i18n
   - `zh/index.js` sync 区块加 `ruleTypeMoveHint`
   - `en/index.js` 同步加英文版

### 测试

cubefs syncnode：
- `validateTask` 单元：move + 合法/冲突 afterCopy/checksumMode 组合
- end-to-end：`TestRunSync_AfterCopyVerifyThenDeleteSrc_*` 改写或新增 move 版本：构造 `Type=TaskTypeMove`，跑完后断言 dst 写入 + src 删除

部署后 test-k3d e2e：
- 通过 dashboard UI 创建 type=move 规则；触发；观察 syncnode 日志 + dst 写入 + src 删除

## 完成标准

1. cubefs `go test ./syncnode/executor/...` 全绿（含新增 move 测试）
2. dashboard 前端在 test-k3d 集群中可创建 type=move 规则
3. 规则触发后：
   - dst 文件出现（sha256 与 src 一致）
   - src 文件被删除
   - 任务状态 `done`
4. UI 自检：选 move 类型时 afterCopy/checksumMode 表单项不出现；保存的规则 JSON 中 type=move、不携带 afterCopy/checksumMode（或后端会自动补齐）

## 验证步骤

1. cubefs：`cd /Users/tao.fang/codes/cubefs && go test ./syncnode/executor/ -run 'Move|VerifyThenDelete' -count=1 -v`
2. 构 cubefs `v3.5.3.1.rc4` 镜像 → push
3. 构 dashboard `v1.0.5.rc3` 镜像 → push
4. cubefs-deploy：bump `_envcommon/images.hcl`（cubefs+dashboard）
5. `ENV=test-k3d make apply-syncnode apply-monitoring`
6. dashboard UI 验证表单显示
7. 创建一条 type=move 规则（src=cfs:/move-src/，dst=cfs:/move-dst/），触发，断言 src 已删除、dst 已写入

## 当前进度

- [x] 设计落地
- [x] cubefs executor 改动 + 测试（rc4 `f438bf2d8`：TaskTypeMove + validateTask 自动锁定 + skip 路径 delete 修复）
- [x] cubefs syncnode config 白名单补 "move"（rc5 `9b2bd2954`：`validRuleTypes` + 错误消息更新）
- [x] dashboard 前端 + i18n（rc3：SyncRuleCreateDialog 新增 move 选项 + zh/en 文案）
- [x] 镜像构建 + push（`cubefs:v3.5.3.1.rc5`、`cubefs-dashboard:v1.0.5.rc3`，均在 `hub.shiyak-office.com/storage/`）
- [x] images.hcl bump（不 commit；cubefs-deploy `_envcommon/images.hcl` 已更新到 rc5）
- [x] apply-syncnode / apply-monitoring（test-k3d 三个 syncnode pod Ready on rc5）
- [x] e2e 验证 src 删除 + dst 写入（2026-05-22）

### e2e 验证结果（2026-05-22, test-k3d, cubefs:v3.5.3.1.rc5）

| 场景 | 结果 |
|------|------|
| Happy path：3 文件 local→local | status=succeeded，src 清空，dst sha256 全部匹配 |
| 类型白名单：`type=transfer` | executor 拒绝：`invalid task.Type: "transfer"` |
| AfterCopy 自动锁定：`type=move + afterCopy=keep` | validateTask 拒绝：`type=move forbids afterCopy="keep"` |
| Master 存储层 | `/syncRule/create` 接受任意 type（按设计，校验在 syncnode dispatch 时执行） |

## 风险

- 历史规则用 `type=sync + afterCopy=verify_then_delete_src` 的不会自动转 move，仍可工作，不破坏兼容。
- master 不做 type 白名单，意味着错拼的 type（如 `mov`）只会在 syncnode 触发时报错（validateTask）；这与 sync/load 行为一致，不引入新风险。
