package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/util/rdma"
)

// initRDMAFromConfig wires the SDK-side RDMA pool from cliConfig.
// Returns nil silently when RDMA is disabled — existing cfs-sync
// deployments need no config change to keep their TCP-only behaviour.
//
// Init failure logs to stderr and returns nil; the SDK's data path
// tolerates a nil rdmaConnPool and falls back to TCP.
//
// Prints a single line to stderr on enable so operators can confirm
// from logs that the pool came up. Metrics emitted under
// cubefs_rdma_* (Prometheus default registry); cfs-sync exposes
// them on the `--metrics-port` flag (see runtime helper) when set,
// otherwise the pool is silently observable via runtime/pprof.
func initRDMAFromConfig(cfg *cliConfig) {
	if cfg == nil || !cfg.RDMAEnable {
		return
	}

	// Defaults mirror FUSE / ObjectNode for one mental model.
	num := cfg.RDMANumSlots
	if num <= 0 {
		num = 256
	}
	size := cfg.RDMASlotSize
	if size <= 0 {
		size = 135168
	}
	mc := cfg.RDMAMaxConns
	if mc <= 0 {
		mc = 4
	}
	shift := cfg.RDMAPortShift
	if shift == 0 {
		shift = 40
	}
	minp := cfg.RDMAMinPayloadBytes
	if minp == 0 {
		minp = 4096
	}
	busy := cfg.RDMABusySpinCount
	if busy <= 0 {
		busy = 200
	}
	yield := cfg.RDMAYieldCount
	if yield <= 0 {
		yield = 1000
	}
	sleepUs := cfg.RDMASleepThresholdUs
	if sleepUs <= 0 {
		sleepUs = 50
	}

	pc := rdma.RDMAPoolConfig{
		NumSlots:        int(num),
		SlotSize:        int(size),
		MaxConns:        int(mc),
		Role:            rdma.RoleClient,
		MinPayloadBytes: int(minp),
		RDMAPortShift:   int(shift),
		Poll: rdma.PollConfig{
			BusySpinCount:    int(busy),
			YieldCount:       int(yield),
			SleepThresholdUs: time.Duration(sleepUs) * time.Microsecond,
		},
	}
	if err := stream.InitRDMAConnPool(pc); err != nil {
		fmt.Fprintf(os.Stderr, "RDMA: init failed, falling back to TCP: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr,
		"RDMA: cfs-sync client pool initialized (numSlots=%d slotSize=%d maxConns=%d "+
			"portShift=%d minPayload=%d busy=%d yield=%d sleep=%dus)\n"+
			"      Metrics on /metrics: cubefs_rdma_requests_total, cubefs_rdma_fallback_total, "+
			"cubefs_rdma_latency_seconds\n",
		num, size, mc, shift, minp, busy, yield, sleepUs)
}
