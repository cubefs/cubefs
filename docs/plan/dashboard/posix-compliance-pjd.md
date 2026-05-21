# dashboard POSIX 兼容性测试（pjd-fstest）独立路径设计

> **背景**：bench 子系统专注吞吐 / IOPS / 延迟等**性能**指标，输出数字。POSIX 兼容性测试（pjd-fstest）是**合规性**测试，输出 pass / fail / skip，验证 cubefs 提供的文件系统语义是否符合 POSIX 标准（mkdir / rename / chmod / chown / symlink 等系统调用）。两者在数据模型、调度方式、结果展示上都不一样，**不能塞进同一个框架**。
>
> 本文设计**独立路径**：dashboard 端独立菜单 + 一次性 K8s Job + 第三方 pjd-fstest 测试套件 + 独立 MySQL 表。**不动 cubefs master、不动 syncnode**。

---

## 0. 决议表

| # | 决议 | 备注 |
|---|---|---|
| C1 | pjd 与 bench 完全解耦 | 模型 / API / 入口分离 |
| C2 | dashboard 端独立菜单"POSIX 兼容性" | 不放在"测试管理"下 |
| C3 | 用第三方 pjd-fstest 套件作为唯一执行器 | 不重造测试用例，定期跟踪上游 |
| C4 | 单次运行 = 一个 K8s Job + 一个挂载目标卷的 Pod | 不引入常驻 worker |
| C5 | 结果只存 dashboard MySQL，不进 cubefs master raft | 合规结果 ≠ 集群状态 |
| C6 | 失败用例可下钻：测试名 + 期望 vs 实际 + 系统调用 | 给 cubefs 实现侧反馈用 |
| C7 | 支持多次运行历史 + 版本对比 | 帮助回归追溯 |

---

## 1. 概念与边界

### 1.1 与 bench 的区别

| 维度 | bench | POSIX 合规 (pjd) |
|---|---|---|
| 输出 | 数字（IOPS / BW / latency） | 布尔（pass / fail），加失败用例列表 |
| 调度 | master + syncnode 集群派发 | 单 K8s Job，无需集群调度 |
| 存储 | 走 syncnode → master raft（rule）+ ledger（task） | 走 dashboard MySQL |
| 入口 | 测试管理菜单 | 新增"POSIX 兼容性"菜单 |
| 配置 | rule + parallelism + stages | 测试套件版本 + 目标卷 + 子集过滤 |
| 失败语义 | 性能不达标 | 实现 bug → 创 issue |

### 1.2 选用 pjd-fstest 作为执行器

第三方现成、社区认可（ceph / fuse / nfs / glusterfs 都用它做合规验证）。下载源：`http://download.ceph.com/qa/pjd-fstest-20090130-RC-aclfixes.tgz`。

**不要重造**：不在 syncnode 里写第二套合规测试。

---

## 2. 架构

```
┌────────────────────────────────────────────────────────────────┐
│ dashboard frontend                                              │
│  ├ 菜单：POSIX 兼容性                                            │
│  │   ├ 运行历史列表（最近 N 次，pass/fail 数 + 完成时间）         │
│  │   ├ 详情抽屉（按测试套件分组，失败用例列表 + 系统调用）        │
│  │   └ 新建运行表单（选目标卷 / 套件版本 / 是否过滤子集）         │
│  └ HTTP → dashboard backend                                     │
└────────────────────┬───────────────────────────────────────────┘
                     │
┌────────────────────▼───────────────────────────────────────────┐
│ dashboard backend (Go)                                          │
│  ├ POST /posixCheck/run     创建 K8s Job，落 run 记录            │
│  ├ GET  /posixCheck/list    历史运行列表                         │
│  ├ GET  /posixCheck/get/:id 单次运行结果 + 失败用例              │
│  ├ POST /posixCheck/cancel/:id  删除 Job + 标记 cancelled        │
│  └ MySQL 表：posix_check_run / posix_check_failure              │
└────────────────────┬───────────────────────────────────────────┘
                     │ K8s API
┌────────────────────▼───────────────────────────────────────────┐
│ K8s Job (一次性)                                                 │
│  ├ pod: hub.shiyak-office.com/storage/pjd-fstest:<version>      │
│  │   ├ initContainer：挂载目标 cubefs 卷到 /mnt/target           │
│  │   ├ main：cd /mnt/target/<unique-tmp> && prove -r --formatter │
│  │   │      TAP::Formatter::JSON ../pjd-fstest/tests > result.json│
│  │   └ sidecar: kubectl cp 把 result.json 推回 dashboard         │
│  │              （或 dashboard backend 用 K8s client 直接读 logs）│
│  └ 完成后 Job ttlSecondsAfterFinished 自动清理                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 数据模型

### 3.1 `posix_check_run`（一次运行的元信息）

```sql
CREATE TABLE posix_check_run (
  id           BIGINT PRIMARY KEY AUTO_INCREMENT,
  cluster_id   BIGINT NOT NULL,
  target_vol   VARCHAR(128) NOT NULL,
  mount_subdir VARCHAR(256),
  suite_image  VARCHAR(256) NOT NULL,             -- e.g. pjd-fstest:20090130
  k8s_job_name VARCHAR(128),
  status       VARCHAR(32) NOT NULL,              -- pending|running|done|failed|cancelled
  pass_count   INT DEFAULT 0,
  fail_count   INT DEFAULT 0,
  skip_count   INT DEFAULT 0,
  total_count  INT DEFAULT 0,
  duration_sec INT DEFAULT 0,
  trigger_user VARCHAR(64),
  created_at   DATETIME NOT NULL,
  finished_at  DATETIME,
  INDEX (cluster_id, created_at)
);
```

### 3.2 `posix_check_failure`（失败用例明细）

```sql
CREATE TABLE posix_check_failure (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  run_id      BIGINT NOT NULL,
  test_file   VARCHAR(256) NOT NULL,        -- e.g. tests/rename/00.t
  test_number INT NOT NULL,                 -- TAP test number within file
  description VARCHAR(512),
  syscall     VARCHAR(64),                  -- e.g. rename / chmod / symlink
  expected    TEXT,
  actual      TEXT,
  INDEX (run_id)
);
```

> 不与 cubefs master raft 关联 — 全部在 dashboard MySQL。

---

## 4. HTTP API

完整路径 `/api/cubefs/console/posixCheck/*`，鉴权走 dashboard 现有 v-auth 体系（参考 sync 的做法，加 `CFS_POSIXCHECK_*` 系列 auth_code + migration seed）。

### 4.1 `POST /posixCheck/run`

```json
请求：
{
  "cluster_name": "test-k3d",
  "target_vol":   "ltptest",
  "mount_subdir": "compliance-2026-05",
  "suite_image":  "hub.shiyak-office.com/storage/pjd-fstest:20090130",
  "test_filter":  ["rename/*", "chmod/*"]          // 可选，缺省全部
}
响应：
{ "code": 0, "data": { "id": 42, "status": "pending", "k8s_job_name": "posix-check-42" } }
```

后端动作：
1. 在 MySQL 写一条 `posix_check_run` 记录（`status=pending`）
2. 调 K8s API 创建 Job（manifest 见 §5）
3. 异步监听 Job 状态变化，更新 `status` + 完成时拉日志解析 TAP
4. 解析后批量插入 `posix_check_failure`

### 4.2 `GET /posixCheck/list?cluster=X&limit=20`

按 `created_at desc` 返回最近运行，每条含 pass/fail/skip 汇总。

### 4.3 `GET /posixCheck/get?id=42`

返回 run 详情 + failure 列表（按 `test_file, test_number` 排序），前端按测试目录折叠展示。

### 4.4 `POST /posixCheck/cancel?id=42`

`kubectl delete job posix-check-42 -n cfs-monitor` + 标记 `status=cancelled`。

---

## 5. K8s Job manifest 模板

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: posix-check-{{ run_id }}
  namespace: cfs-monitor
  labels:
    app: posix-check
    run-id: "{{ run_id }}"
spec:
  ttlSecondsAfterFinished: 1800
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: pjd
        image: "{{ suite_image }}"
        command: ["/bin/bash", "-c"]
        args:
        - |
          set -e
          cd /mnt/target/{{ mount_subdir }}
          prove -r --formatter TAP::Formatter::JSON \
            --merge --timer \
            /opt/pjd-fstest/tests \
            > /tmp/result.json 2>&1 || true
          cat /tmp/result.json
        volumeMounts:
        - name: target
          mountPath: /mnt/target
      volumes:
      - name: target
        # CSI 挂载示例（取决于目标 vol 接入方式）
        csi:
          driver: csi.cubefs.com
          volumeAttributes:
            volName: "{{ target_vol }}"
            masterAddr: "{{ master_addr }}"
```

### 5.1 第三方 pjd-fstest 镜像（一次性构建）

```dockerfile
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
      build-essential perl libacl1-dev wget make ca-certificates \
      libtap-formatter-junit-perl libtap-formatter-html-perl \
      libjson-perl \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
RUN wget -q http://download.ceph.com/qa/pjd-fstest-20090130-RC-aclfixes.tgz \
 && tar xzf pjd-fstest-20090130-RC-aclfixes.tgz \
 && mv pjd-fstest-20090130-RC pjd-fstest \
 && cd pjd-fstest && make
ENV PATH="/usr/bin:$PATH"
```

构建一次推到内部 hub，长期复用。

---

## 6. TAP 输出解析

`prove` 默认输出 TAP（Test Anything Protocol）。`--formatter TAP::Formatter::JSON` 转 JSON：

```json
{
  "tests": [
    {
      "file": "tests/chmod/00.t",
      "result": { "pass": 12, "fail": 0, "total": 12 }
    },
    {
      "file": "tests/rename/01.t",
      "result": { "pass": 8, "fail": 1, "total": 9, "failures": [
        { "number": 5, "description": "rename returns EACCES", "expected": "0 EACCES", "actual": "0 EPERM" }
      ]}
    }
  ]
}
```

dashboard backend `internal/posixcheck/parse.go` 解析这个结构 → 写两张 MySQL 表。

> 如果第三方镜像没装 `TAP::Formatter::JSON` 模块，回退到原始 TAP 文本 + 自己写正则解析器。

---

## 7. 前端

新页面 `frontend/src/pages/cfs/.../posixCompliance/`：

- `index.vue`：左侧菜单"POSIX 兼容性"，主区域 = 历史列表（el-table）+ "新建运行"按钮
- `RunDialog.vue`：选目标卷 / 套件镜像版本 / 测试子集
- `RunDetailDrawer.vue`：右侧抽屉，含三 tab：
  - **概览**：pass / fail / skip / 耗时 / 用了什么镜像
  - **失败用例**：按目录折叠（chmod / rename / mkdir / symlink ...）→ 每条显示 test_number + description + expected vs actual
  - **原始日志**：K8s Job pod log 全文（折叠）

权限码（沿用 sync 模式）：
- `CFS_POSIXCHECK_LIST` / `CFS_POSIXCHECK_RUN` / `CFS_POSIXCHECK_CANCEL` / `CFS_POSIXCHECK_GET`
- 角色管理界面新增"POSIX 兼容性"分组

---

## 8. 实施分期

### Phase 1：MVP（最小可用）

- 第三方 pjd-fstest 镜像构建 + 推 hub（半天）
- dashboard backend：MySQL 表 + `/posixCheck/run` + `/posixCheck/list` + `/posixCheck/get`（1 天）
- dashboard backend：K8s client 创建/监听 Job + TAP 解析（1 天）
- migration：4 个 `CFS_POSIXCHECK_*` permission seed + 新 migration ID（半天）
- 前端：历史列表 + 新建表单（半天）

**AC**：在 test-k3d 上点"运行"→ 5 分钟内能看到 pass/fail 数字。

### Phase 2：可视化

- 失败用例下钻抽屉（半天）
- 原始日志查看（半天）
- 测试子集过滤（半天）

**AC**：失败时能看到具体 syscall + 行号，方便回到 cubefs 代码定位。

### Phase 3：长期演进

- 历史趋势图（每周一次跑，看 pass 数随版本变化）
- 跟 CI 集成（PR 合入前跑一次 POSIX 合规）
- 多卷并发跑（横向规模化）

---

## 9. 不做的事

- 不重写 pjd-fstest 测试用例（用第三方）
- 不把 pjd 测试结果塞进 bench `BenchTaskLedger`（语义错位）
- 不在 cubefs master 加 `/posixCheck/*` API（dashboard 端独立，避免与 master 耦合）
- 不要求 syncnode 参与（pjd 是单点 mount 测试，syncnode 是数据搬运角色，混在一起反而把责任搞乱）
- 不做"自动判定 cubefs 实现是否符合 POSIX"——只产出 pass/fail 报告，符合性由人判断（有的 fail 是 cubefs 已知不支持的 corner case，标 known-issue 即可）

---

## 10. 风险与回退

- **风险 1**：第三方 pjd-fstest 套件版本陈旧（2009 年）。已有项目都用这个版本，stick to it；若发现 corner case 缺失，自行 fork 一份。
- **风险 2**：K8s Job 失败时 logs 拉取超时。设 Job `activeDeadlineSeconds=3600`，Job timeout 后 backend 主动标 `status=failed`。
- **风险 3**：pjd 测试需要 root（chown / chmod 模式位）。Job pod 必须 `runAsUser: 0` + `privileged: true`。test-k3d 默认 PSS 是 baseline，需要 namespace 开 `pod-security.kubernetes.io/enforce: privileged`。
- **回退**：本设计独立模块，删除 backend handler + 前端页面 + 两张 MySQL 表即可彻底回退，不影响 bench / sync / master。
