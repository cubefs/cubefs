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
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// fakeSampler returns scripted values so tests can drive deterministic
// counter deltas without touching /proc.
type fakeSampler struct {
	cpu  float64
	rss  uint64
	rx   uint64
	tx   uint64
	disk map[string]diskIO
	fds  int32
	err  error
}

func (f *fakeSampler) CPUPercent() (float64, error)             { return f.cpu, f.err }
func (f *fakeSampler) RSSBytes() (uint64, error)                { return f.rss, f.err }
func (f *fakeSampler) NetCounters() (uint64, uint64, error)     { return f.rx, f.tx, f.err }
func (f *fakeSampler) DiskCounters() (map[string]diskIO, error) { return f.disk, f.err }
func (f *fakeSampler) NumFDs() (int32, error)                   { return f.fds, f.err }

// gather collects the bench registry and indexes metric families by name for
// concise assertions.
func gatherByName(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := BenchRegistry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

// findByLabel returns the first metric in mf whose label matches name=value,
// or nil if none.
func findByLabel(mf *dto.MetricFamily, name, value string) *dto.Metric {
	if mf == nil {
		return nil
	}
	for _, m := range mf.GetMetric() {
		for _, l := range m.Label {
			if l.GetName() == name && l.GetValue() == value {
				return m
			}
		}
	}
	return nil
}

// TestClientMetricsRegistered confirms the eight client-side series land on
// the bench registry alongside the op metrics — that's the contract scrapers
// see at /metrics/bench.
func TestClientMetricsRegistered(t *testing.T) {
	// Drive one sample so vec counters get at least one child series.
	s := &fakeSampler{
		cpu:  50,
		rss:  1024,
		rx:   100,
		tx:   200,
		disk: map[string]diskIO{"sda": {ReadBytes: 10, WriteBytes: 20}},
		fds:  42,
	}
	var prevRx, prevTx uint64
	diskPrev := map[string]diskIO{}
	haveSeed := false
	// Two ticks: first seeds counters, second emits deltas so the vec
	// metric actually has a child series visible to Gather().
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)
	s.disk = map[string]diskIO{"sda": {ReadBytes: 30, WriteBytes: 50}}
	s.rx = 1100
	s.tx = 1200
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)

	want := []string{
		"syncnode_client_cpu_usage_ratio",
		"syncnode_client_mem_rss_bytes",
		"syncnode_client_net_rx_bytes_total",
		"syncnode_client_net_tx_bytes_total",
		"syncnode_client_disk_read_bytes_total",
		"syncnode_client_disk_write_bytes_total",
		"syncnode_client_open_fds",
		"syncnode_client_goroutines",
	}
	got := gatherByName(t)
	for _, n := range want {
		if _, ok := got[n]; !ok {
			t.Errorf("bench registry missing client metric %q", n)
		}
	}
}

// TestClientMetricsDelta verifies the first tick seeds counters silently and
// subsequent ticks emit deltas — this is the property that lets us mix
// since-boot /proc counters with a "during bench" Prometheus counter.
func TestClientMetricsDelta(t *testing.T) {
	s := &fakeSampler{
		cpu:  0,
		rss:  0,
		rx:   1_000_000,
		tx:   2_000_000,
		disk: map[string]diskIO{"nvme0n1": {ReadBytes: 5_000_000, WriteBytes: 7_000_000}},
		fds:  10,
	}

	// Read baseline rx/tx counter values BEFORE running the sampler so other
	// tests that already advanced the registry don't pollute the comparison.
	baseRx := readCounterValue(t, "syncnode_client_net_rx_bytes_total", "", "")
	baseTx := readCounterValue(t, "syncnode_client_net_tx_bytes_total", "", "")
	baseRead := readCounterValue(t, "syncnode_client_disk_read_bytes_total", "device", "nvme0n1")
	baseWrite := readCounterValue(t, "syncnode_client_disk_write_bytes_total", "device", "nvme0n1")

	var prevRx, prevTx uint64
	diskPrev := map[string]diskIO{}
	haveSeed := false

	// Tick 1: seed only. Net counters must not move because the first sample
	// is the baseline (we don't want to dump cumulative since-boot into our
	// Prometheus counter on first scrape).
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)

	if got := readCounterValue(t, "syncnode_client_net_rx_bytes_total", "", ""); got != baseRx {
		t.Errorf("rx counter moved on seed tick: base=%v got=%v", baseRx, got)
	}

	// Tick 2: drive a measurable delta.
	s.rx = 1_000_500
	s.tx = 2_000_700
	s.disk = map[string]diskIO{"nvme0n1": {ReadBytes: 5_000_400, WriteBytes: 7_000_900}}
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)

	if got := readCounterValue(t, "syncnode_client_net_rx_bytes_total", "", ""); got-baseRx != 500 {
		t.Errorf("rx delta want=500 got=%v", got-baseRx)
	}
	if got := readCounterValue(t, "syncnode_client_net_tx_bytes_total", "", ""); got-baseTx != 700 {
		t.Errorf("tx delta want=700 got=%v", got-baseTx)
	}
	if got := readCounterValue(t, "syncnode_client_disk_read_bytes_total", "device", "nvme0n1"); got-baseRead != 400 {
		t.Errorf("disk read delta want=400 got=%v", got-baseRead)
	}
	if got := readCounterValue(t, "syncnode_client_disk_write_bytes_total", "device", "nvme0n1"); got-baseWrite != 900 {
		t.Errorf("disk write delta want=900 got=%v", got-baseWrite)
	}
}

// TestClientMetricsCounterResetSafe verifies a counter rollback (e.g. kernel
// reset) does not panic Counter.Add with a negative value — we simply skip
// the negative delta and re-baseline.
func TestClientMetricsCounterResetSafe(t *testing.T) {
	s := &fakeSampler{rx: 1000, tx: 1000, disk: map[string]diskIO{}}
	var prevRx, prevTx uint64
	diskPrev := map[string]diskIO{}
	haveSeed := false
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed) // seed
	s.rx = 500                                           // simulate reset
	s.tx = 500
	// Must not panic — exercised via deferred recover.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("sampleOnce panicked on counter reset: %v", r)
		}
	}()
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)
}

// TestClientMetricsCPURatio confirms the CPU gauge is normalised to 0..1 (a
// 4-core machine at 200% gopsutil reading should report 0.5, regardless of
// how many cores the test host has — we feed pct so the math is testable).
func TestClientMetricsCPURatio(t *testing.T) {
	// gopsutil reports "% of one core × cores busy", so 200 on a 4-core box
	// means 2/4 = 0.5. We can't change runtime.NumCPU(), so we just verify
	// the gauge is in [0,1] for a plausible reading. A stricter check would
	// require injecting NumCPU which isn't worth the ceremony.
	s := &fakeSampler{cpu: 75, disk: map[string]diskIO{}}
	var prevRx, prevTx uint64
	diskPrev := map[string]diskIO{}
	haveSeed := false
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)
	got := readGaugeValue(t, "syncnode_client_cpu_usage_ratio")
	if got < 0 || got > 1 {
		t.Errorf("cpu ratio out of [0,1]: %v", got)
	}
}

// TestClientMetricsRSSAndFDs covers the simple gauge paths.
func TestClientMetricsRSSAndFDs(t *testing.T) {
	s := &fakeSampler{rss: 12345678, fds: 77, disk: map[string]diskIO{}}
	var prevRx, prevTx uint64
	diskPrev := map[string]diskIO{}
	haveSeed := false
	sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)
	if got := readGaugeValue(t, "syncnode_client_mem_rss_bytes"); got != 12345678 {
		t.Errorf("rss want=12345678 got=%v", got)
	}
	if got := readGaugeValue(t, "syncnode_client_open_fds"); got != 77 {
		t.Errorf("fds want=77 got=%v", got)
	}
	// Goroutines is sourced from runtime; just check it's positive.
	if got := readGaugeValue(t, "syncnode_client_goroutines"); got <= 0 {
		t.Errorf("goroutines should be > 0, got %v", got)
	}
}

// TestClientMetricsNICFilter spot-checks the loopback/virtual NIC filter.
func TestClientMetricsNICFilter(t *testing.T) {
	cases := map[string]bool{
		"lo":            true,
		"eth0":          false,
		"ens33":         false,
		"docker0":       true,
		"veth1234abcd":  true,
		"br-cafebabe01": true,
		"flannel.1":     true,
		"cni0":          true,
		"tun0":          true,
		"":              true,
	}
	for name, want := range cases {
		if got := isLoopbackOrVirtualNIC(name); got != want {
			t.Errorf("isLoopbackOrVirtualNIC(%q)=%v want=%v", name, got, want)
		}
	}
}

// TestClientMetricsBlockDeviceFilter spot-checks the partition/loop/dm
// rejection. Particularly important: nvme0n1 (disk) kept, nvme0n1p1
// (partition) rejected; sda kept, sda1 rejected.
func TestClientMetricsBlockDeviceFilter(t *testing.T) {
	cases := map[string]bool{
		"sda":         true,
		"sda1":        false,
		"nvme0n1":     true,
		"nvme0n1p1":   false,
		"vda":         true,
		"vda1":        false,
		"hda":         true,
		"loop0":       false,
		"ram1":        false,
		"dm-0":        false,
		"md0":         false,
		"sr0":         false,
		"zram0":       false,
		"":            false,
	}
	for name, want := range cases {
		if got := isPhysicalBlockDevice(name); got != want {
			t.Errorf("isPhysicalBlockDevice(%q)=%v want=%v", name, got, want)
		}
	}
}

// readGaugeValue extracts a single gauge's value from the bench registry.
func readGaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	mfs := gatherByName(t)
	mf, ok := mfs[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	if len(mf.GetMetric()) == 0 {
		t.Fatalf("metric %q has no samples", name)
	}
	return mf.GetMetric()[0].GetGauge().GetValue()
}

// readCounterValue extracts a single counter's value. labelName / labelValue
// can be empty to match the no-label counter (NewCounter not NewCounterVec).
func readCounterValue(t *testing.T, name, labelName, labelValue string) float64 {
	t.Helper()
	mfs := gatherByName(t)
	mf, ok := mfs[name]
	if !ok {
		return 0 // counter may not yet exist for a CounterVec that has never been touched
	}
	if labelName == "" {
		if len(mf.GetMetric()) == 0 {
			return 0
		}
		return mf.GetMetric()[0].GetCounter().GetValue()
	}
	if m := findByLabel(mf, labelName, labelValue); m != nil {
		return m.GetCounter().GetValue()
	}
	return 0
}
