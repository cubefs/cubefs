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

import "testing"

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
	t.Logf("allocs per labelsMetricKey (cache hit): %v", float64(allocs)/runs)
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
