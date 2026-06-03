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

package executor

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/hist"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// runBenchS3 runs an S3/object storage benchmark for a single shard.
// b is the already-acquired backend for rule.BackendID; the caller is
// responsible for constructing it from the backend pool. shardTotal is
// the cluster-wide shard count for cross-shard barrier coordination
// (see syncnode/barrier).
func runBenchS3(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx, shardTotal int, b backend.Backend, pushIntervalSec int) (*spec.BenchShardResult, error) {
	result := &spec.BenchShardResult{
		ShardIdx:  shardIdx,
		Status:    "running",
		StartedAt: time.Now().UnixMilli(),
	}

	keyPrefix := renderKeyPrefix(rule.KeyPrefix, taskID, shardIdx)

	// keyRing holds the set of object keys written during put stages so that
	// subsequent get / delete stages have keys to operate on.
	keyRing := make([]string, 0, 1024)
	var keyRingMu sync.Mutex

	for stageIdx, stage := range rule.Stages {
		if ctx.Err() != nil {
			result.Status = "failed"
			result.Error = "context cancelled"
			break
		}
		// rc8 #120: rule 级 CacheDrop 在每个 stage 进入前触发，按位置区分
		// "before_first"（首个 stage）/ "between"（后续 stage）。MaybeDropCaches
		// 内部根据 spec.BeforeFirstStage/BetweenStages 决定是否实际 drop；
		// CacheDrop=nil 或 Enabled=false 时整链路 no-op。失败仅写指标不阻断 stage。
		dropWhere := "between"
		if stageIdx == 0 {
			dropWhere = "before_first"
		}
		MaybeDropCaches(ctx, taskID, rule.CacheDrop, dropWhere)
		// S1.6: cross-shard barrier before stage start when requested.
		// On error (ctx cancel) we bail; on barrier timeout the helper
		// logs + returns nil so a partial cluster keeps moving.
		shardID := strconv.Itoa(shardIdx)
		if err := waitForPeers(ctx, taskID, stage.Name, shardID, shardTotal, stage.Control); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q barrier: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		sr, err := runObjStage(ctx, stage, b, keyPrefix, &keyRing, &keyRingMu, taskID, shardIdx, pushIntervalSec)
		if err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		result.Stages = append(result.Stages, *sr)
	}

	if result.Status == "running" {
		result.Status = "done"
	}
	result.DoneAt = time.Now().UnixMilli()
	return result, nil
}

// renderKeyPrefix substitutes {taskID} and {shardIdx} placeholders in the
// configured key prefix template. Falls back to a default pattern when the
// template is empty.
func renderKeyPrefix(template, taskID string, shardIdx int) string {
	if template == "" {
		template = "bench-{taskID}/shard-{shardIdx}/"
	}
	r := strings.NewReplacer("{taskID}", taskID, "{shardIdx}", strconv.Itoa(shardIdx))
	return r.Replace(template)
}

// stageHist is the per-stage latency accumulator. Backed by an HDR Recorder
// so master can merge across shards losslessly via syncnode/hist.MergeSnapshots.
// One Recorder per stage isolates op keys to that stage.
type stageHist struct {
	rec *hist.Recorder
}

func newStageHist() *stageHist { return &stageHist{rec: hist.NewRecorder()} }

func (h *stageHist) record(stage, op string, d time.Duration) {
	h.rec.RecordLatency(stage, op, d)
}

// runObjStage executes one object-storage stage: it spawns worker goroutines
// (proportional to each op's weight × NumJobs) and drives them until the
// stage's runtime or object-count limit is reached.
func runObjStage(
	ctx context.Context,
	stage spec.ObjStage,
	b backend.Backend,
	keyPrefix string,
	keyRing *[]string,
	keyRingMu *sync.Mutex,
	taskID string,
	shardIdx int,
	_ int, // pushIntervalSec reserved for future live-push
) (*spec.BenchStageResult, error) {
	sr := &spec.BenchStageResult{Name: stage.Name}

	// rc8 #120: stage 主测量前的预热。stage.Warmup 为 nil 或 DurationSeconds<=0
	// 时 runObjStageWarmup 立即返回；预热耗时不计入 DurationSec（t0 在预热完成
	// 后才取）。S3 路径与 FIO 不同：backend.Put 可在每个 loop 调用一次真实 IO，
	// 故 loopFn 在预热窗口内执行真实的小对象 PUT，对 keyPrefix 下的 warmup-* 键
	// 进行写入；写入的 key 不进入 keyRing（避免污染 stage 主测量的工作集）。
	runObjStageWarmup(ctx, stage, b, keyPrefix, taskID, shardIdx)

	t0 := time.Now()

	// Per-stage HDR recorder feeds both the BenchStageResult.Latency
	// percentiles for this shard AND the HDRBuckets snapshot the master
	// merges across shards.
	sh := newStageHist()
	var totalOps atomic.Int64
	var totalBytes atomic.Int64
	var totalErrors atomic.Int64
	var objSeq atomic.Int64

	// Mark the stage running; cleared at the end (done/failed) below.
	SetStageState(taskID, shardIdx, stage.Name, StageStateRunning)

	sizeFn := resolveObjSize(stage.ObjectSize)

	totalWeight := 0
	for _, op := range stage.Ops {
		totalWeight += op.Weight
	}
	if totalWeight == 0 || stage.NumJobs == 0 {
		SetStageState(taskID, shardIdx, stage.Name, StageStateDone)
		return sr, nil
	}

	// S1.6 ramp / throttle integration.
	//
	// - When the stage declares HasRampSchedule(), the stage duration is
	//   driven by the ramp schedule (RampUp + Steady + RampDown) instead
	//   of stage.Runtime. A driver goroutine retargets the limiter as
	//   the schedule advances.
	// - When HasThrottle() is set, every op blocks on limiter.Wait before
	//   issuing IO. Otherwise NewLimiter returns the no-op limiter and
	//   Wait passes through.
	avgOpBytes := averageObjSize(stage.ObjectSize)
	limiter := NewLimiter(stage.Control, avgOpBytes)

	// done is closed when the stage should stop (schedule complete,
	// stage.Runtime elapsed, or ctx done).
	done := make(chan struct{})
	useRamp := stage.Control.HasRampSchedule()
	stageRuntime := time.Duration(stage.Runtime) * time.Second

	var stageBudget time.Duration
	if useRamp {
		stageBudget = computeRampSchedule(stage.Control, resolveTargetPerSec(stage.Control, avgOpBytes), stageRuntime).totalDuration()
	} else {
		stageBudget = stageRuntime
	}

	if stageBudget > 0 {
		timer := time.NewTimer(stageBudget)
		go func() {
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
			close(done)
		}()
	} else {
		go func() {
			<-ctx.Done()
			close(done)
		}()
	}

	// Launch the ramp driver in parallel with the workers (no-op when
	// the stage didn't request a ramp — NewLimiter already returned the
	// unlimited shim in that case, so SetLimit calls are no-ops).
	if useRamp {
		sched := computeRampSchedule(stage.Control, resolveTargetPerSec(stage.Control, avgOpBytes), stageRuntime)
		go runRampDriver(ctx, limiter, sched, done)
	}

	objLimit := int64(stage.NumObjects)
	deadline := time.Time{}
	if stageBudget > 0 {
		deadline = time.Now().Add(stageBudget)
	}

	var wg sync.WaitGroup
	for _, op := range stage.Ops {
		numWorkers := stage.NumJobs * op.Weight / totalWeight
		if numWorkers == 0 {
			numWorkers = 1
		}
		opType := op.Type
		// S2.1: capture 整个 op（含 PartSizeMiB / Range* / List* 字段）；
		// Go 1.22+ 已有每轮迭代独立作用域，但显式 capture 与 opType 对齐，
		// 让闭包读到的就是该 op 的配置，避免后续维护误用外层变量。
		op := op
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for {
					select {
					case <-done:
						return
					default:
					}
					if objLimit > 0 && totalOps.Load() >= objLimit {
						return
					}
					if !deadline.IsZero() && time.Now().After(deadline) {
						return
					}

					// S1.6 throttle: block until the token bucket
					// allows another op. Unlimited limiters pass
					// through. ctx cancellation propagates as an
					// error here — we treat it as "stage stop" and
					// exit the worker cleanly.
					if err := limiter.Wait(ctx); err != nil {
						return
					}

					var opErr error
					var opBytes int64
					opStart := time.Now()

					switch opType {
					case "put":
						sz := sizeFn(rng)
						seq := objSeq.Add(1)
						key := fmt.Sprintf("%sobj-%d", keyPrefix, seq)
						body := &fixedReader{remaining: sz}
						_, opErr = b.Put(ctx, key, body, sz, backend.PutOptions{})
						if opErr == nil {
							opBytes = sz
							keyRingMu.Lock()
							*keyRing = append(*keyRing, key)
							keyRingMu.Unlock()
						}

					case "put_multipart":
						// S2.1: 强制 multipart 上传，PartSizeMiB 来自 ObjOp，
						// 0 时回落到 defaultMultipartPartMiB。backend.PutOptions
						// 已具备 Multipart + PartSizeMiB 字段，s3 后端会据此走
						// CreateMultipartUpload + UploadPart 分段路径。
						sz := sizeFn(rng)
						seq := objSeq.Add(1)
						key := fmt.Sprintf("%sobj-%d", keyPrefix, seq)
						body := &fixedReader{remaining: sz}
						partMiB := op.PartSizeMiB
						if partMiB <= 0 {
							partMiB = defaultMultipartPartMiB
						}
						_, opErr = b.Put(ctx, key, body, sz, backend.PutOptions{
							Multipart:   true,
							PartSizeMiB: partMiB,
						})
						if opErr == nil {
							opBytes = sz
							keyRingMu.Lock()
							*keyRing = append(*keyRing, key)
							keyRingMu.Unlock()
						}

					case "get":
						keyRingMu.Lock()
						ring := *keyRing
						keyRingMu.Unlock()
						if len(ring) == 0 {
							continue
						}
						key := ring[rng.Intn(len(ring))]
						rc, err := b.Get(ctx, key, 0, 0)
						if err != nil {
							opErr = err
						} else {
							n, _ := io.Copy(io.Discard, rc)
							rc.Close()
							opBytes = n
						}

					case "get_range":
						// S2.1: range read。op.RangeOffset / op.RangeSize 同时
						// > 0 时透传给 backend.Get；否则回落到全量读，保留
						// 与现有 "get" 一致的语义。
						keyRingMu.Lock()
						ring := *keyRing
						keyRingMu.Unlock()
						if len(ring) == 0 {
							continue
						}
						key := ring[rng.Intn(len(ring))]
						off, size := op.RangeOffset, op.RangeSize
						if off < 0 || size < 0 {
							off, size = 0, 0
						}
						rc, err := b.Get(ctx, key, off, size)
						if err != nil {
							opErr = err
						} else {
							n, _ := io.Copy(io.Discard, rc)
							rc.Close()
							opBytes = n
						}

					case "delete":
						keyRingMu.Lock()
						if len(*keyRing) == 0 {
							keyRingMu.Unlock()
							continue
						}
						idx := rng.Intn(len(*keyRing))
						key := (*keyRing)[idx]
						// Swap-remove to avoid O(n) slice shift.
						last := len(*keyRing) - 1
						(*keyRing)[idx] = (*keyRing)[last]
						*keyRing = (*keyRing)[:last]
						keyRingMu.Unlock()
						opErr = b.Delete(ctx, key)

					case "head":
						keyRingMu.Lock()
						ring := *keyRing
						keyRingMu.Unlock()
						if len(ring) == 0 {
							continue
						}
						key := ring[rng.Intn(len(ring))]
						sz, _, _, err := b.Head(ctx, key)
						opErr = err
						if err == nil {
							opBytes = sz
						}

					case "list":
						// S2.1: list 操作。Prefix 为空时使用 stage 的 keyPrefix；
						// MaxKeys=0 时使用 defaultListMaxKeys。list 不属于带宽
						// 型操作：opBytes 固定为 0，IOPS 仍计 1 次。我们读取
						// channel 上限 maxKeys 个 entry（或直至 channel 关闭）
						// 以避免无界遍历影响 stage 节拍。
						prefix := op.ListPrefix
						if prefix == "" {
							prefix = keyPrefix
						}
						maxKeys := op.ListMaxKeys
						if maxKeys <= 0 {
							maxKeys = defaultListMaxKeys
						}
						ch, err := b.List(ctx, prefix, false)
						if err != nil {
							opErr = err
						} else {
							count := 0
							for e := range ch {
								if e.Err != nil {
									opErr = e.Err
									break
								}
								count++
								if count >= maxKeys {
									break
								}
							}
							// list 完成（成功或被 maxKeys 截断）：opBytes 为 0，
							// totalOps 仍 +1，反映一次 list 调用。
							_ = count
						}

					default:
						log.LogWarnf("bench s3 [%s shard=%d]: unknown op type %q", taskID, shardIdx, opType)
						return
					}

					elapsed := time.Since(opStart)
					if opErr != nil {
						if ctx.Err() != nil {
							return
						}
						totalErrors.Add(1)
						IncErr(taskID, shardIdx, stage.Name, opType, ClassifyError(opErr))
						// S3.4: 额外写入更细粒度的错误归因 metric（不破坏旧 kind 维度）。
						observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, opType, ClassifyErr(opErr))
						log.LogWarnf("bench s3 [%s shard=%d] op=%s: %v", taskID, shardIdx, opType, opErr)
					} else {
						sh.record(stage.Name, opType, elapsed)
						ObserveBenchOp(taskID, shardIdx, stage.Name, opType, elapsed, opBytes)
						// S3.3: 同步写入 class 维度的指标，op.SizeClass 透传为
						// label；class 为空时 ObserveBenchOpClass 内部归为
						// "default"，保留旧 dashboard 不受影响。
						ObserveBenchOpClass(taskID, shardIdx, stage.Name, opType, op.SizeClass.ClassLabel(), elapsed.Seconds(), opBytes)
						totalOps.Add(1)
						totalBytes.Add(opBytes)
					}
				}
			}()
		}
	}

	// If DeleteAll is set, drain the entire keyRing after stage workers finish.
	if stage.DeleteAll {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				keyRingMu.Lock()
				if len(*keyRing) == 0 {
					keyRingMu.Unlock()
					return
				}
				key := (*keyRing)[0]
				*keyRing = (*keyRing)[1:]
				keyRingMu.Unlock()

				opStart := time.Now()
				if err := b.Delete(ctx, key); err != nil {
					if ctx.Err() != nil {
						return
					}
					totalErrors.Add(1)
					IncErr(taskID, shardIdx, stage.Name, "delete", ClassifyError(err))
					// S3.4: 错误归因 metric。
					observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "delete", ClassifyErr(err))
					log.LogWarnf("bench s3 deleteAll [%s shard=%d]: %v", taskID, shardIdx, err)
				} else {
					elapsed := time.Since(opStart)
					sh.record(stage.Name, "delete", elapsed)
					ObserveBenchOp(taskID, shardIdx, stage.Name, "delete", elapsed, 0)
					totalOps.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	dur := time.Since(t0)
	sr.DurationSec = dur.Seconds()
	sr.TotalOps = totalOps.Load()
	sr.TotalBytes = totalBytes.Load()
	sr.Errors = totalErrors.Load()
	if dur.Seconds() > 0 {
		sr.OpsPerSec = float64(sr.TotalOps) / dur.Seconds()
		sr.ThroughputMBs = float64(sr.TotalBytes) / (1024 * 1024) / dur.Seconds()
	}

	// Build HDR snapshots first; master uses them to recompute merged
	// percentiles. Shard-local percentiles below are computed from the
	// same recorder so they stay consistent with what the master will see
	// for THIS shard.
	sr.HDRBuckets = sh.rec.SnapshotStage(stage.Name)
	sr.Latency = aggregateStageLatency(sh, stage.Name)

	if totalErrors.Load() > 0 && sr.TotalOps == 0 {
		SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
	} else {
		SetStageState(taskID, shardIdx, stage.Name, StageStateDone)
	}
	return sr, nil
}

// aggregateStageLatency merges the HDR histograms of every op recorded under
// stage into a single per-stage view, then projects p50/95/99/999/9999/max
// + mean back into the BenchLatencyResult. Per-op percentiles are not
// reported on the shard summary — Prometheus already carries that signal
// per-label; we only need stage-level here.
func aggregateStageLatency(sh *stageHist, stage string) spec.BenchLatencyResult {
	snaps := sh.rec.SnapshotStage(stage)
	if len(snaps) == 0 {
		return spec.BenchLatencyResult{}
	}
	blobs := make([][]byte, 0, len(snaps))
	for _, b := range snaps {
		blobs = append(blobs, b)
	}
	merged, _, err := hist.MergeSnapshots(blobs)
	if err != nil || merged.TotalCount() == 0 {
		return spec.BenchLatencyResult{}
	}
	return spec.BenchLatencyResult{
		Mean:  merged.Mean(),
		P50:   float64(merged.ValueAtPercentile(50)),
		P95:   float64(merged.ValueAtPercentile(95)),
		P99:   float64(merged.ValueAtPercentile(99)),
		P999:  float64(merged.ValueAtPercentile(99.9)),
		P9999: float64(merged.ValueAtPercentile(99.99)),
		Max:   float64(merged.Max()),
	}
}

// averageObjSize returns the canonical "average op size" for a stage,
// used by NewLimiter to convert MiB/s targets into ops/sec. Picks Fixed
// when set, the midpoint of Min/Max when both are set, or 4 KiB as the
// last-resort default. Mirrors the size policy in resolveObjSize so the
// throttle math stays consistent with the actual generator.
func averageObjSize(s spec.ObjSize) int {
	if s.Fixed > 0 {
		return int(s.Fixed)
	}
	// 与 resolveObjSize 同源：Min==Max（用 min/max 表达固定大小）必须生效。
	// 原 Max>Min 在相等时落到 4KiB 兜底，会让带宽限速（TargetBwMiBs）按 4KiB
	// 反算 ops/sec、严重高估目标速率、限速失效。放宽到 Max>=Min 保持一致。
	if s.Min > 0 && s.Max >= s.Min {
		return int((s.Min + s.Max) / 2)
	}
	return 4096
}

// resolveTargetPerSec turns the throttle knobs into a single ops/sec
// number for the ramp driver. Mirrors NewLimiter's selection rules:
// TargetIOPS wins over TargetBwMiBs; bandwidth converts via avgOpBytes.
// Returns 0 when no throttle is configured — the ramp driver treats
// that as "no rate to ramp" and only honours the time budget.
func resolveTargetPerSec(c spec.StageControl, avgOpBytes int) float64 {
	if c.TargetIOPS > 0 {
		return float64(c.TargetIOPS)
	}
	if c.TargetBwMiBs > 0 && avgOpBytes > 0 {
		return (c.TargetBwMiBs * 1024 * 1024) / float64(avgOpBytes)
	}
	return 0
}

// S2.1 默认值集中管理：
//   - defaultMultipartPartMiB：未在 ObjOp.PartSizeMiB 指定时，put_multipart 的
//     分片大小（MiB）。8 MiB 是 AWS / 火山 / 阿里 S3 的常见 part 下限+性能甜点。
//   - defaultListMaxKeys：未在 ObjOp.ListMaxKeys 指定时，list 操作单次最多读取
//     的 entry 数。1000 与 S3 ListObjectsV2 单页默认上限一致。
const (
	defaultMultipartPartMiB = 8
	defaultListMaxKeys      = 1000
)

// fixedReader streams exactly `remaining` bytes without ever allocating the
// whole object: Read fills the caller-provided buffer, so memory stays
// O(len(p)) regardless of object size. This lets large-object S3 put flow
// through the multipart uploader holding only part-sized buffers, instead of
// pinning numjobs × objectSize in RAM — the root cause of the admission
// OOM-estimate blowup. Payload content is irrelevant for a throughput bench
// (bytes are never verified or compressed), so we skip the memset; whatever
// is already in the reused transfer buffer goes out the wire.
type fixedReader struct{ remaining int64 }

func (r *fixedReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	r.remaining -= n
	return int(n), nil
}

// resolveObjSize returns a function that generates object sizes according to
// the ObjSize configuration. Falls back to 4 KiB when no size is specified.
func resolveObjSize(s spec.ObjSize) func(*rand.Rand) int64 {
	if s.Fixed > 0 {
		return func(_ *rand.Rand) int64 { return s.Fixed }
	}
	// Min==Max（用 min/max 表达固定大小，而非 Fixed 字段）也必须生效：原先
	// 判断 Max>Min，相等时落到下面的 4KiB 兜底——曾导致 objectSize=64MiB 的
	// rule 实际只写 4KiB 对象（静默退化，bench 结果完全失真）。这里放宽到
	// Max>=Min，span==0 时返回固定 Min，避免 rng.Int63n(0) panic。
	if s.Min > 0 && s.Max >= s.Min {
		span := s.Max - s.Min
		return func(rng *rand.Rand) int64 {
			if span <= 0 {
				return s.Min
			}
			return s.Min + rng.Int63n(span)
		}
	}
	return func(_ *rand.Rand) int64 { return 4096 } // default 4 KiB
}

// ---------------------------------------------------------------------------
// S3.4 error attribution — append-only hooks. observeErrorAttr 调用点已在
// op 错误分支和 deleteAll 错误分支内联，向新的 cubefs_bench_error_attr_total
// 指标双写错误归因。旧的 syncnode_bench_op_errors_total{kind=...} 保持不变。
// ---------------------------------------------------------------------------

// runObjStageWarmup 为 S3 stage 提供 RunWarmup 接入点。spec.Warmup 为 nil 或
// DurationSeconds<=0 时立即返回（RunWarmup 也会做同样的 no-op 守卫，这里只是
// 提前短路，避免无谓地构造 loopFn）。loopFn 在窗口内对 backend 发起小对象
// PUT，让预热真正打到对象存储；warmup 写入的 key 以 "warmup-" 前缀且不进
// keyRing，从而不污染 stage 主测量的工作集。warmup 错误仅由 RunWarmup 内部
// 写 observeWarmupOp，不影响 stage 状态。
func runObjStageWarmup(ctx context.Context, stage spec.ObjStage, b backend.Backend, keyPrefix, taskID string, shardIdx int) {
	if stage.Warmup == nil || stage.Warmup.DurationSeconds <= 0 {
		return
	}
	// warmup 写入大小：取 ObjectSize 的"平均"作为代表；resolveObjSize 已经处理
	// Fixed / Min,Max / 默认 4KiB 的回落，所以单次 PUT 必有合法 size。
	sizeFn := resolveObjSize(stage.ObjectSize)
	var warmSeq atomic.Int64
	loop := func(ctx context.Context) error {
		// 用本地 rng 避免与主测量 worker 抢同一个全局 rand（rand.New 廉价）。
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		sz := sizeFn(rng)
		seq := warmSeq.Add(1)
		key := fmt.Sprintf("%swarmup-%d", keyPrefix, seq)
		body := &fixedReader{remaining: sz}
		_, err := b.Put(ctx, key, body, sz, backend.PutOptions{})
		return err
	}
	defaultWarmupRunner.run(ctx, taskID, strconv.Itoa(shardIdx), stage.Name, stage.Warmup, loop)
}
