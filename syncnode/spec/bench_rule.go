// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package spec

// BenchStorageType identifies which storage system the benchmark targets.
type BenchStorageType string

const (
	BenchStorageS3     BenchStorageType = "s3"
	BenchStoragePosix  BenchStorageType = "posix"
	BenchStorageSDK    BenchStorageType = "sdk"
	BenchStorageMdtest BenchStorageType = "mdtest"
	// BenchStorageIOR runs IOR / mdtest via the cubefs-bench-tools sidecar.
	// The syncnode main container POSTs to http://127.0.0.1:18000/run; the
	// sidecar shells out to /usr/local/bin/ior or /usr/local/bin/mdtest with
	// `-O summaryFormat=JSON` so the executor can parse structured output.
	BenchStorageIOR BenchStorageType = "ior"
)

// BenchRule is the persisted configuration for a distributed benchmark task.
// StorageType selects the executor path (S3/SDK object ops vs POSIX fio).
// Parallelism controls the number of shards dispatched across nodes.
type BenchRule struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	StorageType BenchStorageType `json:"storageType"` // "s3" | "posix" | "sdk"
	Parallelism int              `json:"parallelism"` // shard count
	Output      BenchOutput      `json:"output"`
	CreatedAt   int64            `json:"createdAt"`
	UpdatedAt   int64            `json:"updatedAt"`

	// S3 / SDK only
	BackendID string     `json:"backendID,omitempty"`
	KeyPrefix string     `json:"keyPrefix,omitempty"`
	Stages    []ObjStage `json:"stages,omitempty"`

	// POSIX only
	MountPath   string     `json:"mountPath,omitempty"`
	FIODefaults FIOConfig  `json:"fioDefaults,omitempty"`
	FIOStages   []FIOStage `json:"fioStages,omitempty"`

	// Mdtest only — distributed metadata benchmark via MPI.
	// MountPath above is reused as the working directory root.
	MdtestDefaults *MdtestConfig `json:"mdtestDefaults,omitempty"`
	MdtestStages   []MdtestStage `json:"mdtestStages,omitempty"`

	// IOR only — IOR / mdtest workloads executed via the cubefs-bench-tools
	// sidecar (HTTP at http://127.0.0.1:18000). MountPath above is reused as
	// the working directory root; SidecarEndpoint overrides the sidecar URL
	// in tests / non-standard deployments.
	IORDefaults     *IORConfig `json:"iorDefaults,omitempty"`
	IORStages       []IORStage `json:"iorStages,omitempty"`
	SidecarEndpoint string     `json:"sidecarEndpoint,omitempty"`

	// BackendEndpoint is resolved at dispatch time by the master / dashboard
	// from BackendID. The syncnode uses it to build the actual backend client.
	// Not persisted; populated transiently in the dispatch payload.
	BackendEndpoint *EndpointConfig `json:"backendEndpoint,omitempty"`

	// SLA is the list of pass/fail criteria evaluated against the terminal
	// task result. Empty/nil means "no SLA configured" — the task reports
	// no SLAResult at all. See BenchSLA / BenchSLAResult for semantics.
	SLA []BenchSLA `json:"sla,omitempty"`

	// S3.2 — Soak 长跑配置。Soak.DurationSec == 0 时整个字段视为禁用，对现有
	// bench 流程零影响。结构见同文件末尾的 SoakControl 定义；append-only。
	Soak SoakControl `json:"soak,omitempty"`

	// S3.4 — CacheDrop stage 间 page cache 清理。CacheDrop == nil 或
	// CacheDrop.Enabled == false 时全链路 no-op。结构见同文件末尾的
	// CacheDropSpec 定义；append-only。
	CacheDrop *CacheDropSpec `json:"cache_drop,omitempty"`

	// RC8 #119 — 原始 JSON 字节，仅在内存中流转，不参与 JSON marshal/unmarshal。
	//
	// master 的 POST /benchRule/create|update handler 在严格反序列化通过后，
	// 把请求 body 的原始字节存到这里；BenchRuleStore 在 raft 持久化时与
	// BenchRule 一起序列化进 rocksdb（见 master/bench_rule_store.go 的
	// storedBenchRule 包装）；GET handler 通过 wrapper response 把 RawJSON
	// 以 "rawJSON" 字段对外暴露，供 dashboard / debug 字节级对照。
	//
	// 使用 `json:"-"` 是关键：
	//  1. POST body 里写 rawJSON 字段时被 DisallowUnknownFields 拒绝，避免
	//     调用方伪造 RawJSON；
	//  2. dispatch payload 透传 BenchRule 给 syncnode 时 RawJSON 不会被
	//     重复打包；
	//  3. 内部 syncPut 路径走单独的 storedBenchRule wrapper，不依赖该字段
	//     的 JSON tag。
	RawJSON string `json:"-"`
}

// BenchSLA encodes one pass/fail criterion checked against the aggregated
// terminal stage results of a bench task. All numeric fields are optional:
// a zero value means "no constraint on this dimension". A stage passes a
// BenchSLA only when every non-zero constraint is satisfied for that stage.
//
// AppliesTo selects which stages this criterion applies to. It uses
// path.Match glob syntax (`*`, `?`, `[abc]`). An empty string matches all
// stages. The full BenchSLA fails if AppliesTo matches no stage (the user
// almost certainly mis-typed a stage name — surface it loudly rather than
// silently pass).
type BenchSLA struct {
	P99MsMax     float64 `json:"p99MsMax,omitempty"`     // p99 latency ceiling, milliseconds
	P999MsMax    float64 `json:"p999MsMax,omitempty"`    // p99.9 latency ceiling, milliseconds
	BwMiBsMin    float64 `json:"bwMiBsMin,omitempty"`    // throughput floor, MiB/s
	IopsMin      int     `json:"iopsMin,omitempty"`      // ops/sec floor
	ErrorRateMax float64 `json:"errorRateMax,omitempty"` // error rate ceiling, 0..1
	AppliesTo    string  `json:"appliesTo,omitempty"`    // stage-name glob, "" = all
}

// ObjStage is one phase of an S3/SDK benchmark (e.g. write, read, delete).
type ObjStage struct {
	Name       string  `json:"name"`
	Ops        []ObjOp `json:"ops"`
	NumJobs    int     `json:"numjobs"`
	Runtime    int     `json:"runtime"`
	NumObjects int     `json:"numObjects"`
	ObjectSize ObjSize `json:"objectSize"`
	DeleteAll  bool    `json:"deleteAll,omitempty"`

	// Control carries S1.6 ramp / steady / ramp-down + throttle + barrier
	// settings. All zero values preserve the legacy stage semantics
	// (Runtime-based duration, no throttling, no cross-shard barrier).
	Control StageControl `json:"control,omitempty"`

	// S3.4: Warmup 在 stage 主测量前执行一段不计入结果的预热负载。nil 时无预热。
	// 详见同文件末尾的 WarmupSpec 定义；append-only：仅在 struct 末尾追加。
	Warmup *WarmupSpec `json:"warmup,omitempty"`
}

// ObjOp is one operation type within a stage, with a relative weight used
// to distribute worker goroutines across op types.
//
// S2.1 字段扩展：
//   - PartSizeMiB 仅对 "put_multipart" 生效；0 时回落到 defaultMultipartPartMiB。
//   - RangeOffset / RangeSize 仅对 "get_range" 生效；两者均 > 0 时按 range 读，
//     否则回落到等价于 "get" 的全量读。
//   - ListPrefix / ListMaxKeys 仅对 "list" 生效；ListMaxKeys=0 时使用
//     defaultListMaxKeys。
//
// 所有新增字段都带 omitempty，保持向后兼容（旧 rule JSON 不需要修改）。
type ObjOp struct {
	Type   string `json:"type"` // "put" | "put_multipart" | "get" | "get_range" | "delete" | "head" | "list"
	Weight int    `json:"weight"`

	// S2.1: multipart put 分片大小（MiB）。仅当 Type == "put_multipart" 时生效。
	PartSizeMiB int `json:"part_size_mib,omitempty"`

	// S2.1: range get 偏移/长度（字节）。仅当 Type == "get_range" 时生效。
	// 两者均 > 0 时透传给 backend.Get(ctx, key, off, size)；否则等价于全量读。
	RangeOffset int64 `json:"range_offset_bytes,omitempty"`
	RangeSize   int64 `json:"range_size_bytes,omitempty"`

	// S2.1: list 操作的 Prefix 与 MaxKeys。仅当 Type == "list" 时生效。
	// ListPrefix 为空时使用 stage 的 keyPrefix；ListMaxKeys=0 时使用默认 1000。
	ListPrefix  string `json:"list_prefix,omitempty"`
	ListMaxKeys int    `json:"list_max_keys,omitempty"`

	// S3.3: SizeClass 为该 op 打标签（small / medium / large 等），透传到
	// class 维度的 Prometheus 指标，便于 dashboard 在单 stage 内区分混合负载
	// （例：90% small put + 10% large put）。空字符串视为 "default"，不影响
	// 现有 stage 行为，仅作为标签维度新增。append-only：仅在 struct 末尾追加。
	SizeClass SizeClass `json:"size_class,omitempty"`
}

// ObjSize configures object sizes for a stage. Exactly one of Fixed or
// (Min+Max+Dist) should be set.
type ObjSize struct {
	Fixed int64  `json:"fixed,omitempty"`
	Min   int64  `json:"min,omitempty"`
	Max   int64  `json:"max,omitempty"`
	Dist  string `json:"dist,omitempty"` // "fixed" | "uniform" | "pareto"
}

// FIOConfig holds default fio parameters applied to all POSIX stages unless
// a stage provides its own override value.
type FIOConfig struct {
	IOEngine         string `json:"ioengine"`
	IODepth          int    `json:"iodepth"`
	NumJobs          int    `json:"numjobs"`
	BS               string `json:"bs,omitempty"`
	Size             string `json:"size"`
	Runtime          int    `json:"runtime"`
	Direct           int    `json:"direct"`
	ExtraArgs        string `json:"extraArgs,omitempty"`
	CleanupAfterDone bool   `json:"cleanupAfterDone,omitempty"`
}

// FIOStage is one fio workload phase. Non-zero / non-empty fields override
// the corresponding FIODefaults value.
type FIOStage struct {
	Name        string `json:"name"`
	RW          string `json:"rw"`
	BS          string `json:"bs"`
	RWMixRead   int    `json:"rwmixread,omitempty"`
	ReuseFiles  bool   `json:"reuseFiles"`
	SourceStage string `json:"sourceStage,omitempty"`
	Skip        bool   `json:"skip,omitempty"`
	IODepth     int    `json:"iodepth,omitempty"`
	NumJobs     int    `json:"numjobs,omitempty"`
	Size        string `json:"size,omitempty"`
	Runtime     int    `json:"runtime,omitempty"`
	Direct      int    `json:"direct,omitempty"`

	// Control: only the barrier knobs (WaitForPeers / BarrierTimeoutSec)
	// have any effect for fio stages — fio drives its own runtime + IO
	// shaping; ramp/throttle fields are ignored here. Kept on the struct
	// so a single rule schema covers all three storage types.
	Control StageControl `json:"control,omitempty"`

	// S3.3: Mixed 在单 stage 内表达"多个 size 类组件"的混合负载。非空时
	// 走串行时间分片执行：按 weight 把 Runtime 拆给每个组件依次跑一次 fio，
	// 每段记录到 class 维度的 metrics。为空时保持原有单 BS/RW 行为不变。
	// append-only：仅在 struct 末尾追加，向后兼容旧 rule JSON。
	Mixed []FIOMixedComponent `json:"mixed,omitempty"`

	// S3.4: Warmup 在 stage 主测量前执行一段不计入结果的预热负载。nil 时无预热。
	// 详见同文件末尾的 WarmupSpec 定义；append-only：仅在 struct 末尾追加。
	Warmup *WarmupSpec `json:"warmup,omitempty"`
}

// BenchOutput specifies which percentiles to include in the reported results.
type BenchOutput struct {
	Percentiles []float64 `json:"percentiles"`
}

// MdtestConfig holds default mdtest parameters applied to all mdtest stages
// unless a stage provides its own override value. MpiBin / MdtestBin default
// to PATH lookup ("mpirun" / "mdtest"). NumTasks is the MPI process count
// (mpirun -n) and should typically equal the number of CPU cores or above.
type MdtestConfig struct {
	MpiBin    string `json:"mpiBin,omitempty"`
	MdtestBin string `json:"mdtestBin,omitempty"`
	NumTasks  int    `json:"numTasks,omitempty"`
	ExtraArgs string `json:"extraArgs,omitempty"`
}

// MdtestStage is one mdtest workload phase. Non-zero / non-empty fields
// override the corresponding MdtestDefaults value; unset fields fall back
// to mdtest's own built-in defaults.
type MdtestStage struct {
	Name        string `json:"name"`
	Skip        bool   `json:"skip,omitempty"`
	Iterations  int    `json:"iterations,omitempty"`  // -i
	NumItems    int    `json:"numItems,omitempty"`    // -n (per task)
	ItemsPerDir int    `json:"itemsPerDir,omitempty"` // -I
	Depth       int    `json:"depth,omitempty"`       // -z
	Branching   int    `json:"branching,omitempty"`   // -b
	WriteBytes  int64  `json:"writeBytes,omitempty"`  // -w
	ReadBytes   int64  `json:"readBytes,omitempty"`   // -e
	OnlyFiles   bool   `json:"onlyFiles,omitempty"`   // -F (file-only)
	OnlyDirs    bool   `json:"onlyDirs,omitempty"`    // -d (dir-only)
	UniqueDir   bool   `json:"uniqueDir,omitempty"`   // -u
	NumTasks    int    `json:"numTasks,omitempty"`    // overrides defaults
	ExtraArgs   string `json:"extraArgs,omitempty"`

	// Control: only the barrier knobs (WaitForPeers / BarrierTimeoutSec)
	// have any effect for mdtest stages — mpirun + mdtest drive their own
	// pacing. Kept for schema parity with ObjStage / FIOStage.
	Control StageControl `json:"control,omitempty"`
}

// StageControl holds the S1.6 cross-shard barrier + ramp + throttle knobs
// applied around a single bench stage. All fields are optional; the
// zero-valued StageControl reproduces the legacy stage behaviour.
//
// Semantics:
//
//   - WaitForPeers / BarrierTimeoutSec: when WaitForPeers is true, every
//     shard registers its readiness for the stage in a Consul KV-backed
//     barrier and blocks until all peers do the same (or
//     BarrierTimeoutSec elapses, default 60s). On timeout the stage still
//     runs — the executor logs a warning rather than refusing to start,
//     so a partial cluster can finish a degraded measurement.
//
//   - RampUpSec / SteadySec / RampDownSec: ramp the throttle's target rate
//     linearly from 0 → target over RampUpSec, hold target for SteadySec,
//     then linearly ramp target → 0 over RampDownSec. If all three are
//     zero the stage falls back to its legacy Runtime-based duration and
//     no rate shaping is applied. Only the S3 path consumes the ramp
//     fields; fio / mdtest drive their own pacing.
//
//   - TargetIOPS / TargetBwMiBs: throttle the per-op execution rate. When
//     both are zero the limiter is a no-op (legacy unthrottled
//     behaviour). When both are non-zero TargetIOPS wins (the limiter
//     ignores TargetBwMiBs). When only TargetBwMiBs is set the executor
//     converts to ops/sec via the caller-supplied average op size.
type StageControl struct {
	RampUpSec         int     `json:"rampUpSec,omitempty"`
	SteadySec         int     `json:"steadySec,omitempty"`
	RampDownSec       int     `json:"rampDownSec,omitempty"`
	TargetIOPS        int     `json:"targetIops,omitempty"`
	TargetBwMiBs      float64 `json:"targetBwMiBs,omitempty"`
	WaitForPeers      bool    `json:"waitForPeers,omitempty"`
	BarrierTimeoutSec int     `json:"barrierTimeoutSec,omitempty"`
}

// HasRampSchedule reports whether the stage uses the S1.6 ramp / steady /
// ramp-down windows for its own duration. When false the legacy Runtime
// field controls how long the stage runs.
func (c StageControl) HasRampSchedule() bool {
	return c.RampUpSec > 0 || c.SteadySec > 0 || c.RampDownSec > 0
}

// HasThrottle reports whether the stage requests rate shaping. When false
// the executor uses an unlimited (no-op) limiter.
func (c StageControl) HasThrottle() bool {
	return c.TargetIOPS > 0 || c.TargetBwMiBs > 0
}

// IORConfig holds default IOR / mdtest parameters applied to all IOR stages
// unless a stage provides its own override value. NumTasks is the mpirun -n
// process count (only used when UseMpi=true on the stage).
type IORConfig struct {
	UseMpi     bool   `json:"useMpi,omitempty"`     // wrap in mpirun -n N
	MpiBin     string `json:"mpiBin,omitempty"`     // default "mpirun"
	NumTasks   int    `json:"numTasks,omitempty"`   // mpirun -n
	TimeoutSec int    `json:"timeoutSec,omitempty"` // 0 = no timeout
	ExtraArgs  string `json:"extraArgs,omitempty"`  // appended to every stage's args
}

// IORStage is one IOR / mdtest workload phase executed via the
// cubefs-bench-tools sidecar.
//
// Tool selects the binary inside the sidecar (`ior` or `mdtest`). Args are
// passed verbatim to that binary; the executor will ensure
// `-O summaryFormat=JSON` is present so the sidecar's output can be parsed
// structurally. Targets, when non-empty, are translated to `-o <path>` for
// `ior` and `-d <path>` for `mdtest`; when empty the stage runs against
// rule.MountPath.
//
// Only the StageControl barrier knobs (WaitForPeers / BarrierTimeoutSec) have
// any effect — IOR drives its own runtime + IO shaping. The ramp / throttle
// fields are kept for schema parity with ObjStage / FIOStage / MdtestStage.
type IORStage struct {
	Name       string       `json:"name"`
	Tool       string       `json:"tool"`           // "ior" | "mdtest"
	Args       []string     `json:"args,omitempty"` // passed verbatim
	Targets    []string     `json:"targets,omitempty"`
	Runtime    int          `json:"runtime,omitempty"`
	Skip       bool         `json:"skip,omitempty"`
	UseMpi     bool         `json:"useMpi,omitempty"`     // override defaults
	NumTasks   int          `json:"numTasks,omitempty"`   // override defaults
	TimeoutSec int          `json:"timeoutSec,omitempty"` // override defaults
	ExtraArgs  string       `json:"extraArgs,omitempty"`
	Control    StageControl `json:"control,omitempty"`
}

// ---------------------------------------------------------------------------
// S3.2 Soak — append-only block. New structs MUST go below this anchor; do
// not insert above (avoids merge conflicts with S3.3 / S3.4 which also append
// at file tail). See docs/plan or 任务卡片 S3.2 for the contract.
// ---------------------------------------------------------------------------

// SoakControl 配置 Soak 长跑模式（小时~天级持续测试）。
//
// 语义：
//   - DurationSec > 0 即开启 Soak；总目标运行时长（秒）。
//   - CheckpointInterval：checkpoint 持久化间隔（秒），0 时由 runtime 取默认值 60。
//   - ResumeFromCheckpoint：进程重启时是否尝试从 SoakStore 恢复已记录的进度。
//   - MaxRestartCount：单 stage 单次 run 内的最大重启次数。stage 回调返回错误
//     时累加；超过此上限后 stage 被标记为 failed，整次 Soak 终止。0 表示不允许
//     重启（initial 尝试一次后即终止）。
//
// 当 DurationSec == 0 时，所有相关字段都不生效；保持现有 bench 流程 100% 不变。
type SoakControl struct {
	DurationSec          int  `json:"duration_sec,omitempty"`
	CheckpointInterval   int  `json:"checkpoint_interval_sec,omitempty"`
	ResumeFromCheckpoint bool `json:"resume_from_checkpoint,omitempty"`
	MaxRestartCount      int  `json:"max_restart_count,omitempty"`
}

// Enabled reports whether this stage rule has Soak mode turned on. Centralised
// so executor callsites can avoid open-coding the DurationSec > 0 check.
func (s SoakControl) Enabled() bool {
	return s.DurationSec > 0
}

// EffectiveCheckpointIntervalSec returns CheckpointInterval or a sensible
// default (60s) when unset / negative. Centralised so the runner and tests
// agree on the same fallback.
func (s SoakControl) EffectiveCheckpointIntervalSec() int {
	if s.CheckpointInterval > 0 {
		return s.CheckpointInterval
	}
	return 60
}

// SoakRule 是 BenchRule 顶层 Soak 字段的别名（保留以备 S3.3/S3.4 扩展时挂内嵌
// 子结构使用）。当前仅暴露 SoakControl 本身。
//
// 注意：BenchRule.Soak 字段在 BenchRule 结构体末尾以追加方式插入，见同文件下
// BenchRule 定义末行的 `Soak SoakControl` 字段。

// ---------------------------------------------------------------------------
// S3.3 Mixed workload — append-only block. New structs MUST go below this
// anchor; do not insert above (避免与 S3.2 / S3.4 在文件末尾追加时冲突)。
// S3.4 应在本块末尾继续追加 `// S3.4 ... append-only block` 锚点。
// ---------------------------------------------------------------------------

// SizeClass 给 op 打 size 维度标签，便于 metrics 按 small / medium / large
// 区分聚合。仅用作 label 字符串，不参与执行调度逻辑本身。
type SizeClass string

const (
	SizeClassSmall  SizeClass = "small"
	SizeClassMedium SizeClass = "medium"
	SizeClassLarge  SizeClass = "large"
)

// FIOMixedComponent 描述 FIO 混合负载中的一个组件。一个 FIOStage 内可挂多个
// FIOMixedComponent，bench_fio executor 按 Weight 将 stage Runtime 拆成时间片，
// 依次串行执行，每段独立写入 class 维度的 metrics。
//
// 字段语义：
//   - Name：组件名，作为 fio --name 与 metric op label 后缀使用，唯一区分本组件。
//   - SizeClass：size 维度的 metrics 标签（small / medium / large）；空时记为
//     "default"。
//   - Weight：时间分片权重；总 Runtime 按 Weight/SumWeight 拆给本组件。
//   - BlockSize：fio --bs，如 "4k" / "16m"。
//   - IODepth / NumJobs / RW / Size：对应 fio 参数，未设置时回落到外层
//     FIOStage / FIODefaults。
type FIOMixedComponent struct {
	Name      string    `json:"name"`
	SizeClass SizeClass `json:"size_class,omitempty"`
	Weight    int       `json:"weight"`
	BlockSize string    `json:"bs"`
	IODepth   int       `json:"iodepth,omitempty"`
	NumJobs   int       `json:"numjobs,omitempty"`
	RW        string    `json:"rw"`
	Size      string    `json:"size,omitempty"`
}

// ClassLabel 把 SizeClass 转成稳定的 metric label 值；空时返回 "default" 以
// 保证 class 标签永不为空（Prometheus label 空串会带来 dashboard 拼接困扰）。
func (c SizeClass) ClassLabel() string {
	if c == "" {
		return "default"
	}
	return string(c)
}

// ---------------------------------------------------------------------------
// S3.4 Warmup/CacheDrop — append-only block. 在此锚点后追加 warmup / cache drop
// 相关的结构体；务必只追加，不要修改 S3.2 / S3.3 已有代码。
// 后续 sprint 应在本块末尾继续追加 `// S3.5 ... append-only block` 锚点。
// ---------------------------------------------------------------------------

// WarmupSpec 描述 stage 启动前的预热行为。
// 预热期间产生的指标会带 stage="<原 stage>-warmup" 标签，
// 不进入 SLA 评估，也不计入终态 stage 汇总。
type WarmupSpec struct {
	DurationSeconds int     `json:"duration_seconds,omitempty"` // 预热秒数，0 = 不预热
	TargetQPS       float64 `json:"target_qps,omitempty"`       // 预热 QPS（不限速则 0）
	SubsetRatio     float64 `json:"subset_ratio,omitempty"`     // 0~1，预热只跑前 X% 的文件集（默认 0.1）
}

// CacheDropSpec 控制 stage 之间是否清理客户端缓存。
type CacheDropSpec struct {
	Enabled          bool `json:"enabled,omitempty"`            // 默认 false，需要显式打开
	BetweenStages    bool `json:"between_stages,omitempty"`     // stage 之间 drop（默认 true 当 Enabled）
	BeforeFirstStage bool `json:"before_first_stage,omitempty"` // 首个 stage 之前也 drop
	DropLevel        int  `json:"drop_level,omitempty"`         // 1/2/3，默认 3
}
