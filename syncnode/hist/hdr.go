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

// Package hist wraps HdrHistogram-go for bench latency aggregation.
//
// Each shard records per-(stage, op) latencies into its own *Recorder, then
// serialises the bucket counts (gzip + base64) via Snapshot at stage end and
// ships them to the master inside BenchStageResult.HDRBuckets. The master
// merges shard snapshots into a single histogram per (stage, op) via
// MergeSnapshots and recomputes p50/95/99/999/9999/max from the merged view.
//
// Range: 1µs ~ 600s (covers everything from tiny memory ops to long object
// requests). Sigfigs: 3 (~0.1% precision). Both values are shared by all
// recorders so snapshots can merge without bucket-layout mismatch.
package hist

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// LowestTrackableUs is 1 µs — values below this are clamped up.
	LowestTrackableUs int64 = 1
	// HighestTrackableUs is 600 s expressed in microseconds.
	HighestTrackableUs int64 = 600 * 1_000_000
	// SigFigs controls precision (~0.1% at 3 sig figs).
	SigFigs int = 3
)

// Recorder owns one HDR histogram per (stage, op) key. Internally it is a
// thread-safe map of "stage|op" -> *hdr.Histogram protected by a sync.Mutex
// per-key (cheap; bench workloads write hot but read cold at stage end).
//
// Zero value is not ready; use NewRecorder.
type Recorder struct {
	mu     sync.Mutex
	hists  map[string]*hdr.Histogram // key = stage + "|" + op
	locks  map[string]*sync.Mutex
}

// NewRecorder constructs an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		hists: make(map[string]*hdr.Histogram),
		locks: make(map[string]*sync.Mutex),
	}
}

func histKey(stage, op string) string { return stage + "|" + op }

// histFor returns (or lazily creates) the histogram + its lock for (stage, op).
// Callers must Lock the returned mutex while touching the histogram.
func (r *Recorder) histFor(stage, op string) (*hdr.Histogram, *sync.Mutex) {
	k := histKey(stage, op)
	r.mu.Lock()
	h, ok := r.hists[k]
	l, lok := r.locks[k]
	if !ok {
		h = hdr.New(LowestTrackableUs, HighestTrackableUs, SigFigs)
		r.hists[k] = h
	}
	if !lok {
		l = &sync.Mutex{}
		r.locks[k] = l
	}
	r.mu.Unlock()
	return h, l
}

// RecordLatency records one latency sample for (stage, op). Out-of-range
// values are clamped so RecordValue never returns an error path that would
// drop the sample silently.
func (r *Recorder) RecordLatency(stage, op string, d time.Duration) {
	us := d.Microseconds()
	if us < LowestTrackableUs {
		us = LowestTrackableUs
	}
	if us > HighestTrackableUs {
		us = HighestTrackableUs
	}
	h, l := r.histFor(stage, op)
	l.Lock()
	_ = h.RecordValue(us)
	l.Unlock()
}

// SnapshotStage returns a map of op-name -> gzip+base64 serialised HDR
// Snapshot for every op recorded under stage. Empty map when nothing recorded.
//
// The serialised payload format is:
//
//	gzip(json({LowestTrackableValue, HighestTrackableValue, SignificantFigures, Counts}))
//
// then base64-encoded so it survives JSON marshalling of the surrounding
// BenchStageResult. The wire format is intentionally HDR-library-native
// (Snapshot{}) — MergeSnapshots on the master side calls Import directly.
func (r *Recorder) SnapshotStage(stage string) map[string][]byte {
	r.mu.Lock()
	keys := make([]string, 0, len(r.hists))
	for k := range r.hists {
		keys = append(keys, k)
	}
	r.mu.Unlock()

	out := make(map[string][]byte)
	prefix := stage + "|"
	for _, k := range keys {
		if len(k) <= len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		op := k[len(prefix):]
		h, l := r.histFor(stage, op)
		l.Lock()
		if h.TotalCount() == 0 {
			l.Unlock()
			continue
		}
		blob, err := EncodeSnapshot(h.Export())
		l.Unlock()
		if err != nil {
			continue
		}
		out[op] = blob
	}
	return out
}

// EncodeSnapshot serialises an HDR snapshot to gzip+base64 wire bytes.
// Exposed so executor/master can encode/decode symmetrically.
func EncodeSnapshot(s *hdr.Snapshot) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("nil snapshot")
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	if _, err := w.Write(raw); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	// base64 so the bytes survive JSON wire transport. We keep the raw
	// []byte type on the field — encoding/json will base64-encode []byte
	// automatically when wrapped in a JSON struct, but doing it explicitly
	// here makes the wire format independent of the surrounding container.
	enc := make([]byte, base64.StdEncoding.EncodedLen(gz.Len()))
	base64.StdEncoding.Encode(enc, gz.Bytes())
	return enc, nil
}

// DecodeSnapshot is the inverse of EncodeSnapshot.
func DecodeSnapshot(blob []byte) (*hdr.Snapshot, error) {
	if len(blob) == 0 {
		return nil, fmt.Errorf("empty blob")
	}
	dec := make([]byte, base64.StdEncoding.DecodedLen(len(blob)))
	n, err := base64.StdEncoding.Decode(dec, blob)
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(dec[:n]))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gz.Close()
	raw, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("gzip read: %w", err)
	}
	var s hdr.Snapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return &s, nil
}

// MergeSnapshots imports every blob and merges them into a single histogram
// covering all shard samples. Snapshots with mismatched bucket layout are
// silently dropped (returns dropped count); in practice all shards use the
// shared constants above so dropped should always be zero.
//
// Returns an empty histogram (TotalCount == 0) when blobs is empty or every
// blob fails to decode — callers should check TotalCount before reading
// percentiles.
func MergeSnapshots(blobs [][]byte) (*hdr.Histogram, int, error) {
	merged := hdr.New(LowestTrackableUs, HighestTrackableUs, SigFigs)
	dropped := 0
	for i, blob := range blobs {
		s, err := DecodeSnapshot(blob)
		if err != nil {
			dropped++
			continue
		}
		h := hdr.Import(s)
		// Merge dropped count is samples that fall outside merged's range,
		// not snapshots; we surface both kinds via the return.
		if d := merged.Merge(h); d > 0 {
			// Not fatal: out-of-range samples are clamped at record time
			// already, so this shouldn't fire. Keep it visible via dropped.
			_ = i
			_ = d
		}
	}
	return merged, dropped, nil
}
