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

package master

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/syncnode/hist"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// BenchTaskLedgerCap is the default maximum number of bench task records
// kept in memory. Oldest entries are evicted (LRU by insertion order)
// when the cap is reached.
const BenchTaskLedgerCap = 1000

// BenchTaskStatus mirrors the bench executor's terminal status strings.
// Values are kept byte-identical to any future wire protocol so JSON
// travels unchanged.
type BenchTaskStatus string

const (
	BenchTaskStatusRunning   BenchTaskStatus = "running"
	BenchTaskStatusSucceeded BenchTaskStatus = "succeeded"
	BenchTaskStatusFailed    BenchTaskStatus = "failed"
	BenchTaskStatusCancelled BenchTaskStatus = "cancelled"
)

// IsTerminal returns true for end-state values.
func (s BenchTaskStatus) IsTerminal() bool {
	switch s {
	case BenchTaskStatusSucceeded, BenchTaskStatusFailed, BenchTaskStatusCancelled:
		return true
	}
	return false
}

// BenchShardResult carries one shard's summary produced by a bench
// executor. Populated when the node reports back; empty until then.
type BenchShardResult struct {
	ShardIdx int                     `json:"shardIdx"`
	NodeAddr string                  `json:"nodeAddr"`
	Output   string                  `json:"output,omitempty"` // raw fio / s3bench JSON output
	Error    string                  `json:"error,omitempty"`
	Duration int64                   `json:"duration,omitempty"` // milliseconds
	Status   string                  `json:"status,omitempty"`   // shard terminal status
	Stages   []spec.BenchStageResult `json:"stages,omitempty"`   // copied from the shard's BenchResult so the parent record carries per-shard per-stage metrics (throughput / IOPS / latency) for fan-out visualisation
}

// BenchTaskRecord is the master-side view of one bench task (or shard).
// It is stored in BenchTaskLedger and returned by /benchTask/get.
//
// Fan-out: when Parallelism > 1, the trigger creates one parent record
// (ShardTotal > 0) and N shard records (ParentTaskID non-empty). The
// shard IDs use the "<parentID>/<idx>" format mirroring SyncFanout.
// Parent status is derived from shards: failed > cancelled > succeeded.
type BenchTaskRecord struct {
	TaskID      string                 `json:"taskID"`
	RuleID      string                 `json:"ruleID"`
	Status      BenchTaskStatus        `json:"status"`
	CreatedAt   int64                  `json:"createdAt"`
	UpdatedAt   int64                  `json:"updatedAt"`
	DoneAt      int64                  `json:"doneAt,omitempty"`
	Error       string                 `json:"error,omitempty"`
	BenchResult *spec.BenchShardResult `json:"benchResult,omitempty"` // populated on terminal
	// Fan-out fields (only set when Parallelism > 1)
	ShardTotal   int                `json:"shardTotal,omitempty"`   // parent: total shards
	ShardsDone   int                `json:"shardsDone,omitempty"`   // parent: shards completed
	ParentTaskID string             `json:"parentTaskID,omitempty"` // shard: parent ID
	Shards       []BenchShardResult `json:"shards,omitempty"`       // parent: shard summaries
	// SLAResult is populated by the master after a task reaches a terminal
	// state and the originating rule has SLA configured. Nil means "rule
	// had no SLA" so dashboards can render a "no SLA" badge instead of
	// forcing a pass/fail color. For fan-out tasks the field is only set
	// on the parent record once all shards have rolled up.
	SLAResult *spec.BenchSLAResult `json:"slaResult,omitempty"`
	// AggregatedStages holds the parent-level merged stage results,
	// computed from every shard's HDR snapshots (lossless percentile merge)
	// + summed throughput/IOPS/ops/bytes/errors. Populated by
	// CompleteShardAndAggregate once all shards have terminated. The
	// per-shard Stages remain on Shards[*].Stages for fan-out visualisation;
	// dashboards that want the single "what did the task achieve" view use
	// AggregatedStages directly.
	AggregatedStages []spec.BenchStageResult `json:"aggregatedStages,omitempty"`
}

// BenchTaskLedger is a bounded LRU store for BenchTaskRecords. Entries
// are evicted by insertion order when the capacity is exceeded. All
// methods are safe for concurrent use.
type BenchTaskLedger struct {
	mu    sync.RWMutex
	tasks map[string]*BenchTaskRecord
	order []string // insertion order for FIFO eviction
	cap   int
}

// NewBenchTaskLedger returns a new ledger capped at cap records. If cap
// is <= 0 the default BenchTaskLedgerCap is used.
func NewBenchTaskLedger(cap int) *BenchTaskLedger {
	if cap <= 0 {
		cap = BenchTaskLedgerCap
	}
	return &BenchTaskLedger{
		tasks: make(map[string]*BenchTaskRecord),
		cap:   cap,
	}
}

// Add inserts or updates a record. On insert, if the ledger is full the
// oldest entry is evicted. UpdatedAt is always refreshed.
func (l *BenchTaskLedger) Add(r *BenchTaskRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now().UnixMilli()
	if _, ok := l.tasks[r.TaskID]; !ok {
		// new entry — evict oldest if at cap
		if len(l.order) >= l.cap {
			oldest := l.order[0]
			l.order = l.order[1:]
			delete(l.tasks, oldest)
		}
		if r.CreatedAt == 0 {
			r.CreatedAt = now
		}
		l.order = append(l.order, r.TaskID)
	}
	r.UpdatedAt = now
	cp := *r
	l.tasks[r.TaskID] = &cp
}

// Get returns a copy of the record for taskID, or nil if absent or
// evicted.
func (l *BenchTaskLedger) Get(taskID string) *BenchTaskRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	r, ok := l.tasks[taskID]
	if !ok {
		return nil
	}
	cp := *r
	return &cp
}

// List returns copies of all records matching the optional ruleID and
// status filters. Empty string means "no filter".
func (l *BenchTaskLedger) List(ruleID, status string) []*BenchTaskRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]*BenchTaskRecord, 0, len(l.tasks))
	for _, r := range l.tasks {
		if ruleID != "" && r.RuleID != ruleID {
			continue
		}
		if status != "" && string(r.Status) != status {
			continue
		}
		cp := *r
		out = append(out, &cp)
	}
	return out
}

// Cancel transitions a running task to cancelled. Returns true if the
// status was actually changed, false if the task is absent or already
// terminal.
func (l *BenchTaskLedger) Cancel(taskID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.tasks[taskID]
	if !ok {
		return false
	}
	if r.Status != BenchTaskStatusRunning {
		return false
	}
	r.Status = BenchTaskStatusCancelled
	r.UpdatedAt = time.Now().UnixMilli()
	return true
}

// Remove deletes a single task record from the ledger. Idempotent — a no-op
// when the task is absent. Caller is responsible for cascading to fan-out
// shard records ("<parentID>/<N>") separately.
func (l *BenchTaskLedger) Remove(taskID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.tasks[taskID]; !ok {
		return
	}
	delete(l.tasks, taskID)
	for i, id := range l.order {
		if id == taskID {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
}

// Fail transitions a running task to failed with the given error message.
// Returns true if the status was actually changed, false if the task is
// absent or already terminal.
func (l *BenchTaskLedger) Fail(taskID, errMsg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.tasks[taskID]
	if !ok {
		return false
	}
	if r.Status.IsTerminal() {
		return false
	}
	r.Status = BenchTaskStatusFailed
	r.Error = errMsg
	r.UpdatedAt = time.Now().UnixMilli()
	return true
}

// Complete transitions a running task to its terminal state and stores the
// BenchShardResult returned by the executor. The status is derived from the
// result: if result.Error is non-empty the task is marked failed; otherwise
// it is marked succeeded. Returns true if the status was actually changed,
// false if the task is absent or already terminal.
func (l *BenchTaskLedger) Complete(taskID string, result spec.BenchShardResult) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.tasks[taskID]
	if !ok {
		return false
	}
	if r.Status.IsTerminal() {
		return false
	}
	now := time.Now().UnixMilli()
	if result.Error != "" {
		r.Status = BenchTaskStatusFailed
		r.Error = result.Error
	} else {
		r.Status = BenchTaskStatusSucceeded
	}
	cp := result
	r.BenchResult = &cp
	r.DoneAt = now
	r.UpdatedAt = now
	return true
}

// SetSLAResult stores the SLA evaluation outcome on a terminal record.
// Returns true when the record exists and the field was written. A nil
// result is allowed (and stored as nil) so callers can explicitly mark
// "evaluation attempted but rule had no SLA configured" semantics if
// desired; current callers only invoke SetSLAResult with non-nil values.
func (l *BenchTaskLedger) SetSLAResult(taskID string, result *spec.BenchSLAResult) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.tasks[taskID]
	if !ok {
		return false
	}
	r.SLAResult = result
	r.UpdatedAt = time.Now().UnixMilli()
	return true
}

// AddShards inserts a parent record and N shard records atomically. The
// parent record must have ShardTotal == len(shardIDs) and Status==running.
// Each shard record is seeded with the parent's RuleID and marked running.
// Eviction applies per-insertion; the parent is inserted first.
func (l *BenchTaskLedger) AddShards(parent *BenchTaskRecord, shardIDs []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now().UnixMilli()
	insertOne := func(r *BenchTaskRecord) {
		if _, ok := l.tasks[r.TaskID]; ok {
			return
		}
		if len(l.order) >= l.cap {
			oldest := l.order[0]
			l.order = l.order[1:]
			delete(l.tasks, oldest)
		}
		if r.CreatedAt == 0 {
			r.CreatedAt = now
		}
		r.UpdatedAt = now
		cp := *r
		l.tasks[r.TaskID] = &cp
		l.order = append(l.order, r.TaskID)
	}
	insertOne(parent)
	for i, sid := range shardIDs {
		shard := &BenchTaskRecord{
			TaskID:       sid,
			RuleID:       parent.RuleID,
			Status:       BenchTaskStatusRunning,
			ParentTaskID: parent.TaskID,
			CreatedAt:    now,
		}
		_ = i
		insertOne(shard)
	}
}

// CompleteShardAndAggregate marks a shard record as terminal and, if all
// sibling shards have completed, updates the parent record to its aggregate
// terminal status. Returns the parentTaskID and whether the parent is now
// fully done. Returns ("", false) when taskID is not a known shard record.
//
// The caller must call Complete (or Fail/Cancel) on the shard BEFORE calling
// this so the shard's final status is already written.
func (l *BenchTaskLedger) CompleteShardAndAggregate(shardID string) (parentID string, allDone bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	shard, ok := l.tasks[shardID]
	if !ok || shard.ParentTaskID == "" {
		return "", false
	}
	parentID = shard.ParentTaskID
	parent, ok := l.tasks[parentID]
	if !ok {
		return parentID, false
	}
	if parent.Status.IsTerminal() {
		return parentID, true
	}
	// Count completed shards and collect shard summaries.
	doneSoFar := 0
	anyFailed := false
	anyCancelled := false
	var shardSummaries []BenchShardResult
	for _, rec := range l.tasks {
		if rec.ParentTaskID != parentID {
			continue
		}
		if rec.Status.IsTerminal() {
			doneSoFar++
			summary := BenchShardResult{Error: rec.Error, Status: string(rec.Status)}
			// Derive ShardIdx from the "<parent>/<N>" task ID suffix.
			if slash := strings.LastIndex(rec.TaskID, "/"); slash >= 0 {
				if idx, aerr := strconv.Atoi(rec.TaskID[slash+1:]); aerr == nil {
					summary.ShardIdx = idx
				}
			}
			if rec.BenchResult != nil {
				summary.NodeAddr = rec.BenchResult.NodeAddr
				if rec.BenchResult.DoneAt > rec.BenchResult.StartedAt {
					summary.Duration = rec.BenchResult.DoneAt - rec.BenchResult.StartedAt
				}
				// Carry per-stage metrics up so the parent record holds the
				// data the dashboard needs for shard × stage visualisation.
				if len(rec.BenchResult.Stages) > 0 {
					summary.Stages = rec.BenchResult.Stages
				}
			}
			shardSummaries = append(shardSummaries, summary)
		}
		switch rec.Status {
		case BenchTaskStatusFailed:
			anyFailed = true
		case BenchTaskStatusCancelled:
			anyCancelled = true
		}
	}
	parent.ShardsDone = doneSoFar
	parent.UpdatedAt = time.Now().UnixMilli()
	if doneSoFar < parent.ShardTotal {
		return parentID, false
	}
	// All shards done — derive parent status.
	now := time.Now().UnixMilli()
	if anyFailed {
		parent.Status = BenchTaskStatusFailed
		parent.Error = "one or more shards failed"
	} else if anyCancelled {
		parent.Status = BenchTaskStatusCancelled
	} else {
		parent.Status = BenchTaskStatusSucceeded
	}
	parent.Shards = shardSummaries
	parent.AggregatedStages = mergeShardStages(shardSummaries)
	parent.DoneAt = now
	parent.UpdatedAt = now
	return parentID, true
}

// mergeShardStages folds N shards' per-stage results into one summary per
// stage name. Throughput / IOPS / ops / bytes / errors are summed; latency
// percentiles come from a lossless HDR merge across every shard that
// recorded HDR snapshots for that stage. Stages without HDR data (fio,
// mdtest) get the simple-average latency across shards so dashboards still
// have something to render.
//
// Stage ordering follows the first shard that contributes a given stage
// name — preserves the original pipeline order in the result.
func mergeShardStages(shards []BenchShardResult) []spec.BenchStageResult {
	if len(shards) == 0 {
		return nil
	}
	// componentAgg accumulates one FIO-mixed component across all shards that
	// reported it. Latency is averaged with sample weighting (mean across
	// shards) — fio mixed paths don't ship HDR snapshots per component, so an
	// unweighted shard-mean is the best honest approximation. Components are
	// keyed by Name within a stage; SizeClass / Weight are copied from the
	// first shard that contributed and assumed identical across shards (they
	// come from the rule config, not from the run).
	type componentAgg struct {
		comp            spec.BenchComponentResult
		latencySumP50   float64
		latencySumP95   float64
		latencySumP99   float64
		latencySumP999  float64
		latencySumP9999 float64
		latencySumMax   float64
		latencySumMean  float64
		latencyCount    int
	}
	type agg struct {
		stage           spec.BenchStageResult
		hdrBlobsByOp    map[string][][]byte
		latencySumP50   float64
		latencySumP95   float64
		latencySumP99   float64
		latencySumP999  float64
		latencySumP9999 float64
		latencySumMax   float64
		latencySumMean  float64
		latencyCount    int
		durationSum     float64
		durationCount   int
		// componentOrder preserves the per-component order of the first shard
		// that contributed each component, so the parent's MixedComponents
		// reads the same as the rule's stage.Mixed declaration.
		componentOrder  []string
		componentByName map[string]*componentAgg
	}
	order := make([]string, 0)
	byName := make(map[string]*agg)
	for _, sh := range shards {
		for _, stg := range sh.Stages {
			a, ok := byName[stg.Name]
			if !ok {
				a = &agg{
					stage:           spec.BenchStageResult{Name: stg.Name},
					hdrBlobsByOp:    make(map[string][][]byte),
					componentByName: make(map[string]*componentAgg),
				}
				byName[stg.Name] = a
				order = append(order, stg.Name)
			}
			a.stage.TotalOps += stg.TotalOps
			a.stage.TotalBytes += stg.TotalBytes
			a.stage.Errors += stg.Errors
			a.stage.ThroughputMBs += stg.ThroughputMBs
			a.stage.OpsPerSec += stg.OpsPerSec
			a.durationSum += stg.DurationSec
			a.durationCount++
			// Latency fallback aggregation: only used when no HDR data is
			// present for this stage at all. We collect sums unconditionally
			// and decide which path to emit in the post-loop step.
			if stg.Latency.P50 > 0 || stg.Latency.Mean > 0 {
				a.latencySumP50 += stg.Latency.P50
				a.latencySumP95 += stg.Latency.P95
				a.latencySumP99 += stg.Latency.P99
				a.latencySumP999 += stg.Latency.P999
				a.latencySumP9999 += stg.Latency.P9999
				a.latencySumMax += stg.Latency.Max
				a.latencySumMean += stg.Latency.Mean
				a.latencyCount++
			}
			for op, blob := range stg.HDRBuckets {
				if len(blob) == 0 {
					continue
				}
				a.hdrBlobsByOp[op] = append(a.hdrBlobsByOp[op], blob)
			}
			// #121: per-component aggregation for FIO mixed stages. Each shard
			// reported MixedComponents on this stage; sum throughput / ops /
			// bytes / errors across shards, track max duration (wall-clock
			// honesty, same logic as stage.DurationSec), and shard-average
			// latency. SizeClass / Weight come from the rule and are identical
			// across shards.
			for _, comp := range stg.MixedComponents {
				ca, ok := a.componentByName[comp.Name]
				if !ok {
					ca = &componentAgg{
						comp: spec.BenchComponentResult{
							Name:      comp.Name,
							SizeClass: comp.SizeClass,
							Weight:    comp.Weight,
						},
					}
					a.componentByName[comp.Name] = ca
					a.componentOrder = append(a.componentOrder, comp.Name)
				}
				ca.comp.TotalOps += comp.TotalOps
				ca.comp.TotalBytes += comp.TotalBytes
				ca.comp.Errors += comp.Errors
				ca.comp.ThroughputMBs += comp.ThroughputMBs
				ca.comp.OpsPerSec += comp.OpsPerSec
				if comp.DurationSec > ca.comp.DurationSec {
					ca.comp.DurationSec = comp.DurationSec
				}
				if comp.Latency.P50 > 0 || comp.Latency.Mean > 0 {
					ca.latencySumP50 += comp.Latency.P50
					ca.latencySumP95 += comp.Latency.P95
					ca.latencySumP99 += comp.Latency.P99
					ca.latencySumP999 += comp.Latency.P999
					ca.latencySumP9999 += comp.Latency.P9999
					ca.latencySumMax += comp.Latency.Max
					ca.latencySumMean += comp.Latency.Mean
					ca.latencyCount++
				}
			}
		}
	}

	out := make([]spec.BenchStageResult, 0, len(order))
	for _, name := range order {
		a := byName[name]
		stg := a.stage
		// Duration: use the max across shards so the value still reads as
		// "wall-clock the stage took". Summing would mislead users into
		// thinking the cluster spent N×duration when shards ran in parallel.
		// Approximation: average is closer than sum but worse than max;
		// use shard average × shard count is unhelpful. Pick max for honesty.
		var durMax float64
		for _, sh := range shards {
			for _, s := range sh.Stages {
				if s.Name == name && s.DurationSec > durMax {
					durMax = s.DurationSec
				}
			}
		}
		stg.DurationSec = durMax
		if len(a.hdrBlobsByOp) > 0 {
			stg.HDRBuckets = make(map[string][]byte) // do not republish; mark presence only if needed downstream
			// Merge ALL ops into a single stage-wide histogram so the
			// stage-level percentile is comparable across stages with
			// different op mixes. Per-op percentiles remain visible via
			// Prometheus.
			allBlobs := make([][]byte, 0)
			for op, blobs := range a.hdrBlobsByOp {
				allBlobs = append(allBlobs, blobs...)
				_ = op // op-keyed snapshots intentionally not persisted on parent
			}
			merged, _, mergeErr := hist.MergeSnapshots(allBlobs)
			if mergeErr == nil && merged.TotalCount() > 0 {
				stg.Latency = spec.BenchLatencyResult{
					Mean:  merged.Mean(),
					P50:   float64(merged.ValueAtPercentile(50)),
					P95:   float64(merged.ValueAtPercentile(95)),
					P99:   float64(merged.ValueAtPercentile(99)),
					P999:  float64(merged.ValueAtPercentile(99.9)),
					P9999: float64(merged.ValueAtPercentile(99.99)),
					Max:   float64(merged.Max()),
				}
			}
			// Drop the placeholder map — we don't republish op-keyed
			// snapshots on the parent because the per-shard HDRBuckets are
			// still available on Shards[*].Stages.
			stg.HDRBuckets = nil
		} else if a.latencyCount > 0 {
			n := float64(a.latencyCount)
			stg.Latency = spec.BenchLatencyResult{
				Mean:  a.latencySumMean / n,
				P50:   a.latencySumP50 / n,
				P95:   a.latencySumP95 / n,
				P99:   a.latencySumP99 / n,
				P999:  a.latencySumP999 / n,
				P9999: a.latencySumP9999 / n,
				Max:   a.latencySumMax / n,
			}
		}
		// #121: emit per-component breakdown on the parent record. Order
		// follows the first shard that contributed each component, so it
		// matches the rule's stage.Mixed declaration.
		if len(a.componentOrder) > 0 {
			stg.MixedComponents = make([]spec.BenchComponentResult, 0, len(a.componentOrder))
			for _, cname := range a.componentOrder {
				ca := a.componentByName[cname]
				if ca.latencyCount > 0 {
					n := float64(ca.latencyCount)
					ca.comp.Latency = spec.BenchLatencyResult{
						Mean:  ca.latencySumMean / n,
						P50:   ca.latencySumP50 / n,
						P95:   ca.latencySumP95 / n,
						P99:   ca.latencySumP99 / n,
						P999:  ca.latencySumP999 / n,
						P9999: ca.latencySumP9999 / n,
						Max:   ca.latencySumMax / n,
					}
				}
				stg.MixedComponents = append(stg.MixedComponents, ca.comp)
			}
		}
		out = append(out, stg)
	}
	return out
}
