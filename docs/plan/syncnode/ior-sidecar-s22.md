# S2.2 — IOR / mdtest 独立 sidecar 接入 bench 平台

## 背景

Sprint 2 任务 S2.2。用户已决策：IOR / mdtest 不进 syncnode 主镜像，作为同 Pod 内独立 sidecar 容器与 syncnode 共享 hostPath / PVC 挂载点。

## 范围

仅本任务做：
- 新增 `cubefs-bench-tools` sidecar 镜像（含 IOR / mdtest / openmpi / tini / 一个 Go 写的 HTTP runner）。
- syncnode 侧新增 `IORStage` 规则字段 + `bench_ior.go` 执行器；通过 `http://127.0.0.1:18000/run` 调 sidecar。
- cubefs-deploy 侧把 sidecar 容器加进 syncnode DaemonSet；变量化 sidecar 镜像；**不改 images.hcl**（留 TODO）。
- 单元测试：mock HTTP server，覆盖 IOR JSON 输出解析。

不做：
- 不构建镜像、不部署、不修改 images.hcl。
- 不 commit / push 任何一个仓库。
- 不动 cubefs-dashboard。

## 完成标准

- `go build ./...` + `go vet ./syncnode/... ./cmd/bench-tools-runner/...` 通过。
- `go test ./syncnode/executor/... -run IOR` 通过。
- deploy 仓库 `tofu fmt -recursive modules/cubefs-syncnode/` 通过。
- 交付报告列出新增/修改文件清单。

## 关键设计

### sidecar runner（Go HTTP server）

- 路径：`cmd/bench-tools-runner/main.go`。
- 监听 `127.0.0.1:18000`（与 syncnode 同 Pod 共享 network namespace，主容器直连）。
- 路由：
  - `GET /healthz` → 200。
  - `POST /run` 请求体：
    ```json
    { "tool": "ior"|"mdtest", "args": [...], "workdir": "/cfs/posix-bench/..." }
    ```
    响应体：
    ```json
    { "exitCode": 0, "stdout": "...", "stderr": "...", "durationSec": 12.3 }
    ```
- entrypoint：`tini -- /usr/local/bin/bench-tools-runner`。

### IORStage 字段（spec/bench_rule.go 新增）

```go
type IORStage struct {
    Name      string
    Tool      string   // "ior" | "mdtest"
    Args      []string // 直接透传给 ior/mdtest（不含 -O summaryFormat=JSON）
    NumTasks  int      // mpirun -n
    Runtime   int
    Skip      bool
    ExtraArgs string
    Control   StageControl // barrier 知悉，节流/ramp 不生效
}
```

BenchRule 新增 `IORStages []IORStage`，新增 `BenchStorageIOR = "ior"`。

### bench_ior.go 执行器

- 函数签名对齐：`runBenchIOR(ctx, rule, taskID, shardIdx, shardTotal, pushIntervalSec)`。
- 入口逻辑参考 `bench_posix.go` / `bench_mdtest.go`：mkdir workDir、barrier、SetStageState、IncErr、ClassifyError。
- 调用 sidecar：标准 `net/http` POST，IOR/mdtest 强制 `-O summaryFormat=JSON`（IOR 3.3+ 支持）。
- 解析 IOR JSON 输出：`summary[].operation` / `bwMiB` / `tIOPS` / `latency`（IOR 的字段名）；
  mdtest 在 IOR 输出格式下也是同一 JSON schema，复用解析。
- 出 BenchStageResult：`ThroughputMBs`（MiB/s 视作 MB/s，与 fio 路径一致）、`OpsPerSec`、`Latency.{P50,P95,P99,Mean}`、`DurationSec`。
- 失败 → IncErr(kind="other") / state=failed。

### deploy 改动

- `modules/cubefs-syncnode/daemonset.tf` 增加第二容器 `bench-tools`：
  - 镜像 = `var.bench_tools_image`
  - command = `["/usr/local/bin/bench-tools-runner"]`
  - volumeMount：复用 syncnode 的 `bench-mount` 挂载到 `/cfs/posix-bench`（条件：当 `bench_mount_pvc_name != ""`）。
  - resources 默认 cpu=4 / mem=8Gi，可调。
  - hostNetwork 已在 pod spec，sidecar 自动共享 → 主容器走 `127.0.0.1:18000` 即可。
- `variables.tf` 新增：
  - `bench_tools_image` (string, no default)
  - `bench_tools_resources`（与 `resources` 同结构，含默认）
- `_envcommon/syncnode.hcl` 暂留 TODO 注释，**不修改 images.hcl**。

## 风险

- IOR 在 ASCII 输出下没有结构化 JSON；强制 `-O summaryFormat=JSON` 需要 IOR ≥3.3。Dockerfile 必须 build IOR 3.3+。
- runner 选择 hostNetwork 后，sidecar 监听 `127.0.0.1` 也会暴露到 host 的回环；与同节点其他 Pod 隔离风险低（仍是 host loopback），但要在 README 注明端口占用。
- 共享 bench-mount PVC 才能让 sidecar 在 syncnode 指定的 workDir 内执行；如果用户没开启 `bench_mount_pvc_name`，sidecar 也起，但没共享存储 → 文档需要说明这点。

## 进度

- [x] 调研：bench_posix.go / bench_mdtest.go / executor.go runBench 分派 / metrics / barrier / daemonset / variables / images.hcl
- [x] Part A.1 Dockerfile + entrypoint
- [x] Part A.1 bench-tools-runner main.go
- [x] Part A.2 spec/bench_rule.go IORStage + BenchRule.IORStages + BenchStorageIOR
- [x] Part A.3 executor/bench_ior.go
- [x] Part A.4 executor.go switch 分支补 IOR
- [x] Part A.5 bench_ior_test.go
- [x] Part B.1 daemonset.tf sidecar 容器
- [x] Part B.2 variables.tf 新增 bench_tools_image / resources
- [x] 验证：go build / go vet / go test -run IOR / tofu fmt
