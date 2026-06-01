// Copyright 2018 The CubeFS Authors.
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

package exporter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabelsMetricKeyStableAcrossMapIteration(t *testing.T) {
	a := map[string]string{"cluster": "c1", "volname": "v1"}
	b := map[string]string{"volname": "v1", "cluster": "c1"}

	if labelsMetricKey("metric", a) != labelsMetricKey("metric", b) {
		t.Fatal("labelsMetricKey should be independent of map iteration order")
	}
}

func TestLabelsMetricKeyEmptyLabels(t *testing.T) {
	if got := labelsMetricKey("only_name", nil); got != "only_name" {
		t.Fatalf("expected only name, got %q", got)
	}
}

func TestLabelsMetricKeyReusesCachedString(t *testing.T) {
	labels := map[string]string{"cluster": "c1", "volname": "v1"}

	first := labelsMetricKey("latency_hist", labels)
	for i := 0; i < 100; i++ {
		if got := labelsMetricKey("latency_hist", labels); got != first {
			t.Fatalf("expected cached key reuse, iteration %d", i)
		}
	}
}

func TestLabelsMetricKeyReusesAcrossDifferentMapsWithSameContent(t *testing.T) {
	a := map[string]string{"cluster": "c1"}
	b := map[string]string{"cluster": "c1"}

	ka := labelsMetricKey("m", a)
	kb := labelsMetricKey("m", b)
	if ka != kb {
		t.Fatalf("expected same key for equivalent labels, got %q vs %q", ka, kb)
	}
}

func TestFNV64Uses64BitOffset(t *testing.T) {
	require.Equal(t, uint64(14695981039346656037), uint64(fnvOffset64))
}

func TestLabelsMetricKeyHashCollisionDoesNotCrosstalk(t *testing.T) {
	oldMap := metricKeyByHash
	oldFn := hashNameAndLabelsFn
	t.Cleanup(func() {
		metricKeyMu.Lock()
		metricKeyByHash = oldMap
		metricKeyMu.Unlock()
		hashNameAndLabelsFn = oldFn
	})

	const forcedHash uint64 = 0xcafebabe12345678
	hashNameAndLabelsFn = func(string, []string, map[string]string) uint64 {
		return forcedHash
	}

	metricKeyMu.Lock()
	metricKeyByHash = make(map[uint64][]metricKeyEntry)
	metricKeyMu.Unlock()

	keyA := labelsMetricKey("metric", map[string]string{"a": "1"})
	keyB := labelsMetricKey("metric", map[string]string{"b": "2"})
	require.NotEqual(t, keyA, keyB, "different labels must not share cached key on hash collision")

	keyBAgain := labelsMetricKey("metric", map[string]string{"b": "2"})
	require.Equal(t, keyB, keyBAgain)

	keyOtherName := labelsMetricKey("other_metric", map[string]string{"a": "1"})
	require.NotEqual(t, keyA, keyOtherName, "different metric names must not share cached key on hash collision")
}

func TestLabelsMetricKeySeededCollisionEntryMisses(t *testing.T) {
	labels := map[string]string{"x": "wanted"}
	keys, pool := sortedLabelKeys(labels)
	defer releaseLabelKeys(pool)
	labelsFP := buildLabelsFingerprint(keys, labels)
	h := hashNameAndLabels("m", keys, labels)

	metricKeyMu.Lock()
	oldBucket := metricKeyByHash[h]
	metricKeyByHash[h] = []metricKeyEntry{{
		name:     "m",
		labelsFP: "decoy=1\x00",
		key:      "m\x00decoy=1\x00",
	}}
	metricKeyMu.Unlock()
	t.Cleanup(func() {
		metricKeyMu.Lock()
		metricKeyByHash[h] = oldBucket
		metricKeyMu.Unlock()
	})

	got := labelsMetricKey("m", labels)
	require.Equal(t, joinNameLabelsFP("m", labelsFP), got)
	require.NotEqual(t, "m\x00decoy=1\x00", got)
}

func TestFingerprintMatches(t *testing.T) {
	labels := map[string]string{"b": "2", "a": "1"}
	keys, pool := sortedLabelKeys(labels)
	defer releaseLabelKeys(pool)

	fp := buildLabelsFingerprint(keys, labels)
	require.True(t, fingerprintMatches(keys, labels, fp))
	require.False(t, fingerprintMatches(keys, labels, "a=9\x00b=2\x00"))
}

func TestHashNameAndLabelsStable(t *testing.T) {
	a := map[string]string{"b": "2", "a": "1"}
	b := map[string]string{"a": "1", "b": "2"}

	keysA, poolA := sortedLabelKeys(a)
	releaseLabelKeys(poolA)
	keysB, poolB := sortedLabelKeys(b)
	releaseLabelKeys(poolB)

	ha := hashNameAndLabels("m", keysA, a)
	hb := hashNameAndLabels("m", keysB, b)
	if ha != hb {
		t.Fatalf("hash mismatch: %x vs %x", ha, hb)
	}
}

func TestLabelsMetricKeyAllocsOnCacheHit(t *testing.T) {
	labels := map[string]string{"cluster": "c1", "volname": "v1"}
	_ = labelsMetricKey("latency_hist", labels)

	const runs = 1000
	allocs := testing.AllocsPerRun(runs, func() {
		_ = labelsMetricKey("latency_hist", labels)
	})
	const maxAllocsPerHit = 0.1
	avg := float64(allocs) / runs
	t.Logf("allocs per labelsMetricKey (cache hit): %v", avg)
	require.LessOrEqual(t, avg, maxAllocsPerHit)
}

func TestCounterPublishUsesPoolAndMetricCache(t *testing.T) {
	oldEnabled := enabledPrometheus
	enabledPrometheus = true
	t.Cleanup(func() { enabledPrometheus = oldEnabled })

	ch := make(chan *Counter, 1)
	oldCh := CounterCh
	CounterCh = ch
	t.Cleanup(func() { CounterCh = oldCh })

	const counterName = "ut_counter_pool"
	c := NewCounter(counterName)
	c.AddWithLabels(3, map[string]string{"vol": "v1"})

	select {
	case m := <-ch:
		require.Equal(t, metricsName(counterName), m.name)
		require.EqualValues(t, 3, m.val)
		require.NotEmpty(t, m.metricKey)
		metric1 := m.Metric()
		metric2 := m.Metric()
		require.Same(t, metric1, metric2)
	default:
		t.Fatal("expected counter publish on channel")
	}
}

func TestGaugePublishUsesPool(t *testing.T) {
	oldEnabled := enabledPrometheus
	enabledPrometheus = true
	t.Cleanup(func() { enabledPrometheus = oldEnabled })

	ch := make(chan *Gauge, 1)
	oldCh := GaugeCh
	GaugeCh = ch
	t.Cleanup(func() { GaugeCh = oldCh })

	const gaugeName = "ut_gauge_pool"
	g := NewGauge(gaugeName)
	g.SetWithLabels(7, map[string]string{"vol": "v1"})

	select {
	case m := <-ch:
		require.Equal(t, metricsName(gaugeName), m.name)
		require.InDelta(t, 7, m.val, 0)
		require.NotEmpty(t, m.metricKey)
	default:
		t.Fatal("expected gauge publish on channel")
	}
}

func TestHistogramPublishUsesPool(t *testing.T) {
	oldEnabled := enabledPrometheus
	enabledPrometheus = true
	t.Cleanup(func() { enabledPrometheus = oldEnabled })

	ch := make(chan *Histogram, 1)
	oldCh := HistogramCh
	HistogramCh = ch
	t.Cleanup(func() { HistogramCh = oldCh })

	const histName = "ut_hist_pool"
	publishHistogram(metricsName(histName), 1234, map[string]string{"vol": "v1"})

	select {
	case m := <-ch:
		require.Equal(t, metricsName(histName), m.name)
		require.InDelta(t, 1234, m.val, 0)
		require.NotEmpty(t, m.metricKey)
	default:
		t.Fatal("expected histogram publish on channel")
	}
}

func BenchmarkLabelsMetricKeyCacheHit(b *testing.B) {
	labels := map[string]string{"cluster": "c1", "volname": "v1"}
	_ = labelsMetricKey("latency_hist", labels)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = labelsMetricKey("latency_hist", labels)
	}
}
