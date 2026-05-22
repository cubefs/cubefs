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
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
	gopsdisk "github.com/shirou/gopsutil/disk"
	gopsnet "github.com/shirou/gopsutil/net"
	gopsprocess "github.com/shirou/gopsutil/process"
)

// Client-side resource metrics for the bench workload. Sampled by a 1 Hz
// goroutine and registered on the SAME isolated registry as the bench op
// metrics (see metrics.go) so they share /metrics/bench and stay out of the
// node-level /metrics endpoint.
//
// Why these metrics together:
//   - syncnode bench is a CLIENT workload (it drives S3 / POSIX backends from
//     the syncnode pod). Server-side cubefs metrics (master/datanode/metanode)
//     never explain client-side bottlenecks (CPU pegged, fd exhaustion, NIC
//     saturation). Pairing client resources with op throughput/latency on the
//     same time axis is the only way to attribute a slowdown.
//   - Network/disk are read at HOST level (syncnode runs with hostNetwork, so
//     the host NIC counters reflect the pod's traffic). Process-level CPU /
//     RSS / fd / goroutines are read off the syncnode PID.
//
// Counters (net_*_total, disk_*_total) are exposed as cumulative since-boot
// values straight from /proc; Prometheus's rate() handles the rest.
var (
	clientCPUUsageRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "syncnode_client_cpu_usage_ratio",
		Help: "Syncnode process CPU usage ratio (0..1, normalised across all cores).",
	})
	clientMemRSSBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "syncnode_client_mem_rss_bytes",
		Help: "Syncnode process resident set size in bytes.",
	})
	clientNetRxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "syncnode_client_net_rx_bytes_total",
		Help: "Host NIC cumulative bytes received (sum across non-loopback interfaces, since boot).",
	})
	clientNetTxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "syncnode_client_net_tx_bytes_total",
		Help: "Host NIC cumulative bytes sent (sum across non-loopback interfaces, since boot).",
	})
	clientDiskReadBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_client_disk_read_bytes_total",
		Help: "Host block device cumulative bytes read, per device (since boot).",
	}, []string{"device"})
	clientDiskWriteBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_client_disk_write_bytes_total",
		Help: "Host block device cumulative bytes written, per device (since boot).",
	}, []string{"device"})
	clientOpenFDs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "syncnode_client_open_fds",
		Help: "Syncnode process open file descriptors.",
	})
	clientGoroutines = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "syncnode_client_goroutines",
		Help: "Syncnode process live goroutines.",
	})
)

func init() {
	benchRegistry.MustRegister(
		clientCPUUsageRatio,
		clientMemRSSBytes,
		clientNetRxBytes,
		clientNetTxBytes,
		clientDiskReadBytes,
		clientDiskWriteBytes,
		clientOpenFDs,
		clientGoroutines,
	)
}

// clientSampler is the indirection used by the sampling loop so tests can
// drive deterministic values without touching /proc. The production
// implementation (gopsutilSampler) wraps gopsutil; tests construct their own.
type clientSampler interface {
	// CPUPercent returns process CPU usage as a percentage (0..100 * NumCPU).
	// Implementations should return a fresh delta-based reading per call.
	CPUPercent() (float64, error)
	// RSSBytes returns process resident set size.
	RSSBytes() (uint64, error)
	// NetCounters returns host-level cumulative rx/tx bytes summed across
	// non-loopback NICs.
	NetCounters() (rx uint64, tx uint64, err error)
	// DiskCounters returns per-device cumulative read/write bytes, keyed by
	// device name (e.g. "sda", "nvme0n1"). Partition entries and loop devices
	// are filtered by the implementation.
	DiskCounters() (map[string]diskIO, error)
	// NumFDs returns the process open fd count.
	NumFDs() (int32, error)
}

type diskIO struct {
	ReadBytes  uint64
	WriteBytes uint64
}

// gopsutilSampler is the production sampler: gopsutil for process, host NIC,
// host disk, and runtime.NumGoroutine for goroutines (the Go runtime knows
// best and gopsutil's count is laggy / wrong here).
type gopsutilSampler struct {
	proc *gopsprocess.Process
}

func newGopsutilSampler() (*gopsutilSampler, error) {
	p, err := gopsprocess.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	return &gopsutilSampler{proc: p}, nil
}

func (g *gopsutilSampler) CPUPercent() (float64, error) {
	// gopsutil returns 0..100*NumCPU (a 4-core machine fully busy reads ~400).
	// Caller normalises to a 0..1 ratio.
	return g.proc.CPUPercent()
}

func (g *gopsutilSampler) RSSBytes() (uint64, error) {
	m, err := g.proc.MemoryInfo()
	if err != nil {
		return 0, err
	}
	if m == nil {
		return 0, nil
	}
	return m.RSS, nil
}

func (g *gopsutilSampler) NetCounters() (uint64, uint64, error) {
	// pernic=true so we can drop loopback; the "all" aggregate from gopsutil
	// already includes lo and we want to exclude it.
	stats, err := gopsnet.IOCounters(true)
	if err != nil {
		return 0, 0, err
	}
	var rx, tx uint64
	for _, s := range stats {
		if isLoopbackOrVirtualNIC(s.Name) {
			continue
		}
		rx += s.BytesRecv
		tx += s.BytesSent
	}
	return rx, tx, nil
}

func (g *gopsutilSampler) DiskCounters() (map[string]diskIO, error) {
	raw, err := gopsdisk.IOCounters()
	if err != nil {
		return nil, err
	}
	out := make(map[string]diskIO, len(raw))
	for name, s := range raw {
		if !isPhysicalBlockDevice(name) {
			continue
		}
		out[name] = diskIO{ReadBytes: s.ReadBytes, WriteBytes: s.WriteBytes}
	}
	return out, nil
}

func (g *gopsutilSampler) NumFDs() (int32, error) { return g.proc.NumFDs() }

// isLoopbackOrVirtualNIC filters interfaces that would inflate "node NIC"
// throughput numbers: loopback, docker/k8s bridge taps, veth pairs, tun/tap.
// We intentionally err on the side of dropping known-noise devices rather
// than enumerating every physical NIC name.
func isLoopbackOrVirtualNIC(name string) bool {
	if name == "" || name == "lo" {
		return true
	}
	prefixes := []string{"veth", "docker", "br-", "cni", "flannel", "calico", "weave", "kube", "tun", "tap", "virbr"}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// isPhysicalBlockDevice keeps whole-disk names (sda, nvme0n1, vda, hda) and
// drops partitions (sda1, nvme0n1p2), loop / ram / dm / md aggregates, and
// device-mapper meta. Per-partition stats double-count against the whole
// disk in /proc/diskstats; per-disk values are what we want.
func isPhysicalBlockDevice(name string) bool {
	if name == "" {
		return false
	}
	// Reject obvious non-physical.
	rejectPrefixes := []string{"loop", "ram", "dm-", "md", "sr", "fd", "zram"}
	for _, p := range rejectPrefixes {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	// Detect partitions. Whole disks end on a letter; partitions end on a
	// digit. Special-case NVMe where the disk itself ends on a digit
	// ("nvme0n1") and partitions add "p<n>" ("nvme0n1p1").
	if strings.HasPrefix(name, "nvme") {
		return !strings.Contains(name, "p")
	}
	if len(name) == 0 {
		return false
	}
	last := name[len(name)-1]
	return last < '0' || last > '9'
}

// StartClientMetricsSampler launches a 1 Hz goroutine that refreshes the
// client-side resource gauges and counters on the bench registry. It exits
// when stopC is closed. Sampling failures are logged at WARN level and the
// loop continues; we never want a transient /proc hiccup to take down the
// syncnode.
//
// Returns immediately if the sampler cannot be constructed (e.g. unsupported
// OS); the metrics still appear on /metrics/bench but stay at their zero
// values, which is the right behaviour for a non-Linux dev workstation.
func StartClientMetricsSampler(stopC <-chan struct{}) {
	s, err := newGopsutilSampler()
	if err != nil {
		log.LogWarnf("client_metrics: gopsutil sampler init failed (metrics will stay at zero): %v", err)
		return
	}
	runClientMetricsSampler(stopC, s, time.Second)
}

// runClientMetricsSampler is the test-friendly core: it accepts an injected
// sampler + tick interval so unit tests can drive the loop deterministically.
func runClientMetricsSampler(stopC <-chan struct{}, s clientSampler, interval time.Duration) {
	go func() {
		// Counter state for monotonic-increase enforcement. /proc counters
		// are already monotonic since boot, but if the kernel resets them
		// (very rare) Add() with a negative delta would panic; we guard by
		// tracking the previously-seen value and emitting Add(0) for resets.
		var (
			prevRx, prevTx uint64
			diskPrev       = map[string]diskIO{}
			haveSeed       bool
		)
		t := time.NewTicker(interval)
		defer t.Stop()
		// Seed the counter baselines with the first reading so the very
		// first published value reflects an actual delta rather than the
		// since-boot cumulative.
		for {
			select {
			case <-stopC:
				return
			case <-t.C:
				sampleOnce(s, &prevRx, &prevTx, diskPrev, &haveSeed)
			}
		}
	}()
}

// sampleOnce is the body of one tick. Split out so tests can call it
// directly without spinning a real ticker.
func sampleOnce(s clientSampler, prevRx, prevTx *uint64, diskPrev map[string]diskIO, haveSeed *bool) {
	if pct, err := s.CPUPercent(); err == nil {
		// Normalise to 0..1 ratio across all cores. NumCPU() never returns 0
		// on a real system; guard for completeness.
		n := runtime.NumCPU()
		if n <= 0 {
			n = 1
		}
		ratio := pct / 100.0 / float64(n)
		if ratio < 0 {
			ratio = 0
		}
		clientCPUUsageRatio.Set(ratio)
	} else {
		log.LogWarnf("client_metrics: CPUPercent: %v", err)
	}

	if rss, err := s.RSSBytes(); err == nil {
		clientMemRSSBytes.Set(float64(rss))
	} else {
		log.LogWarnf("client_metrics: RSSBytes: %v", err)
	}

	if fds, err := s.NumFDs(); err == nil {
		clientOpenFDs.Set(float64(fds))
	} else {
		log.LogWarnf("client_metrics: NumFDs: %v", err)
	}

	clientGoroutines.Set(float64(runtime.NumGoroutine()))

	if rx, tx, err := s.NetCounters(); err == nil {
		if !*haveSeed {
			*prevRx, *prevTx = rx, tx
		} else {
			if rx >= *prevRx {
				clientNetRxBytes.Add(float64(rx - *prevRx))
			}
			if tx >= *prevTx {
				clientNetTxBytes.Add(float64(tx - *prevTx))
			}
			*prevRx, *prevTx = rx, tx
		}
	} else {
		log.LogWarnf("client_metrics: NetCounters: %v", err)
	}

	if cur, err := s.DiskCounters(); err == nil {
		if !*haveSeed {
			for k, v := range cur {
				diskPrev[k] = v
			}
		} else {
			for dev, now := range cur {
				prev, ok := diskPrev[dev]
				if !ok {
					// First time we see this device: seed without emitting,
					// so we don't dump the entire since-boot value into our
					// "during bench" counter on the first scrape.
					diskPrev[dev] = now
					continue
				}
				if now.ReadBytes >= prev.ReadBytes {
					clientDiskReadBytes.WithLabelValues(dev).Add(float64(now.ReadBytes - prev.ReadBytes))
				}
				if now.WriteBytes >= prev.WriteBytes {
					clientDiskWriteBytes.WithLabelValues(dev).Add(float64(now.WriteBytes - prev.WriteBytes))
				}
				diskPrev[dev] = now
			}
		}
	} else {
		log.LogWarnf("client_metrics: DiskCounters: %v", err)
	}

	*haveSeed = true
}
