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
	"bytes"
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
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// runBenchS3 runs an S3/object storage benchmark for a single shard.
// b is the already-acquired backend for rule.BackendID; the caller is
// responsible for constructing it from the backend pool.
func runBenchS3(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx int, b backend.Backend, pushIntervalSec int) (*spec.BenchShardResult, error) {
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

	for _, stage := range rule.Stages {
		if ctx.Err() != nil {
			result.Status = "failed"
			result.Error = "context cancelled"
			break
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

// simpleHistogram is a coarse fixed-bucket latency histogram for measuring
// operation latency without external dependencies. Bucket width is 4000 µs;
// 256 buckets cover 0 – ~1024 ms.
type simpleHistogram struct {
	mu      sync.Mutex
	count   int64
	sumUs   int64
	buckets [256]int64
}

func (h *simpleHistogram) record(d time.Duration) {
	us := d.Microseconds()
	h.mu.Lock()
	h.count++
	h.sumUs += us
	idx := int(us / 4000)
	if idx >= len(h.buckets) {
		idx = len(h.buckets) - 1
	}
	h.buckets[idx]++
	h.mu.Unlock()
}

// percentile returns the p-th percentile latency in microseconds (p ∈ [0,100]).
func (h *simpleHistogram) percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	target := int64(float64(h.count) * p / 100.0)
	var cumulative int64
	for i, cnt := range h.buckets {
		cumulative += cnt
		if cumulative >= target {
			// Return the midpoint of the bucket in µs.
			return float64(i)*4000 + 2000
		}
	}
	return float64(len(h.buckets)-1)*4000 + 2000
}

func (h *simpleHistogram) mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	return float64(h.sumUs) / float64(h.count)
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
	t0 := time.Now()
	sr := &spec.BenchStageResult{Name: stage.Name}

	hist := &simpleHistogram{}
	var totalOps atomic.Int64
	var totalBytes atomic.Int64
	var totalErrors atomic.Int64
	var objSeq atomic.Int64

	sizeFn := resolveObjSize(stage.ObjectSize)

	totalWeight := 0
	for _, op := range stage.Ops {
		totalWeight += op.Weight
	}
	if totalWeight == 0 || stage.NumJobs == 0 {
		return sr, nil
	}

	// done is closed when the stage should stop (runtime elapsed or ctx done).
	done := make(chan struct{})
	if stage.Runtime > 0 {
		timer := time.NewTimer(time.Duration(stage.Runtime) * time.Second)
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

	objLimit := int64(stage.NumObjects)
	deadline := time.Time{}
	if stage.Runtime > 0 {
		deadline = time.Now().Add(time.Duration(stage.Runtime) * time.Second)
	}

	var wg sync.WaitGroup
	for _, op := range stage.Ops {
		numWorkers := stage.NumJobs * op.Weight / totalWeight
		if numWorkers == 0 {
			numWorkers = 1
		}
		opType := op.Type
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

					var opErr error
					var opBytes int64
					opStart := time.Now()

					switch opType {
					case "put":
						sz := sizeFn(rng)
						seq := objSeq.Add(1)
						key := fmt.Sprintf("%sobj-%d", keyPrefix, seq)
						body := bytes.NewReader(make([]byte, sz))
						_, opErr = b.Put(ctx, key, body, sz, backend.PutOptions{})
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
						log.LogWarnf("bench s3 [%s shard=%d] op=%s: %v", taskID, shardIdx, opType, opErr)
					} else {
						hist.record(elapsed)
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
					log.LogWarnf("bench s3 deleteAll [%s shard=%d]: %v", taskID, shardIdx, err)
				} else {
					hist.record(time.Since(opStart))
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
	sr.Latency = spec.BenchLatencyResult{
		Mean: hist.mean(),
		P50:  hist.percentile(50),
		P95:  hist.percentile(95),
		P99:  hist.percentile(99),
		P999: hist.percentile(99.9),
	}
	return sr, nil
}

// resolveObjSize returns a function that generates object sizes according to
// the ObjSize configuration. Falls back to 4 KiB when no size is specified.
func resolveObjSize(s spec.ObjSize) func(*rand.Rand) int64 {
	if s.Fixed > 0 {
		return func(_ *rand.Rand) int64 { return s.Fixed }
	}
	if s.Min > 0 && s.Max > s.Min {
		return func(rng *rand.Rand) int64 {
			return s.Min + rng.Int63n(s.Max-s.Min)
		}
	}
	return func(_ *rand.Rand) int64 { return 4096 } // default 4 KiB
}
