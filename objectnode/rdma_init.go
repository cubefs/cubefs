package objectnode

import (
	"time"

	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/rdma"
)

// initRDMAClientPool wires the SDK-side RDMA pool from the ObjectNode
// JSON config. Process-global: must be called once before any
// per-volume ExtentClient is constructed (handleStart calls it after
// loadConfig). When rdmaEnable is absent or false, the function is a
// no-op and the SDK falls back to its TCP path verbatim — the
// guarantee for backward compatibility on existing deployments.
//
// Returns nil on success or when RDMA is disabled. Init failure is
// logged at WARN and the SDK continues with TCP; the caller does NOT
// need to surface that as a fatal error because the data path tolerates
// nil rdmaConnPool.
func initRDMAClientPool(cfg *config.Config) error {
	if !cfg.GetBool(configRDMAEnable) {
		log.LogInfof("RDMA: disabled (set %q=true to enable)", configRDMAEnable)
		return nil
	}

	// Defaults mirror FUSE mount-option defaults so operators see one
	// mental model across client surfaces.
	numSlots := cfg.GetInt64(configRDMANumSlots)
	if numSlots <= 0 {
		numSlots = 256
	}
	slotSize := cfg.GetInt64(configRDMASlotSize)
	if slotSize <= 0 {
		slotSize = 135168 // BlockSize (128 KB) + PageSize (4 KB)
	}
	maxConns := cfg.GetInt64(configRDMAMaxConns)
	if maxConns <= 0 {
		maxConns = 4
	}
	portShift := cfg.GetInt64(configRDMAPortShift)
	if portShift == 0 {
		portShift = 40 // datanode default rdmaPort=17350 vs listen=17310
	}
	minPayload := cfg.GetInt64(configRDMAMinPayloadBytes)
	if minPayload == 0 {
		minPayload = 4096
	}
	busy := cfg.GetInt64(configRDMABusySpinCount)
	if busy <= 0 {
		busy = 200
	}
	yield := cfg.GetInt64(configRDMAYieldCount)
	if yield <= 0 {
		yield = 1000
	}
	sleepUs := cfg.GetInt64(configRDMASleepThresholdUs)
	if sleepUs <= 0 {
		sleepUs = 50
	}
	// Phase A: cfg=0 → util/rdma defaults (64 × 4 MiB). disabled
	// defaults false → Phase A active.
	readSlotCount := cfg.GetInt64(configRDMAReadSlotCount)
	readSlotSize := cfg.GetInt64(configRDMAReadSlotSize)
	oneSidedDisabled := cfg.GetBool(configRDMAOneSidedReadDisabled)
	readTimeoutMs := cfg.GetInt64(configRDMAReadTimeoutMs)

	poolCfg := rdma.RDMAPoolConfig{
		NumSlots:             int(numSlots),
		SlotSize:             int(slotSize),
		MaxConns:             int(maxConns),
		Role:                 rdma.RoleClient,
		MinPayloadBytes:      int(minPayload),
		RDMAPortShift:        int(portShift),
		ReadSlotCount:        int(readSlotCount),
		ReadSlotSize:         int(readSlotSize),
		OneSidedReadDisabled: oneSidedDisabled,
		ReadTimeoutMs:        int(readTimeoutMs),
		Poll: rdma.PollConfig{
			BusySpinCount:    int(busy),
			YieldCount:       int(yield),
			SleepThresholdUs: time.Duration(sleepUs) * time.Microsecond,
		},
	}
	if err := stream.InitRDMAConnPool(poolCfg); err != nil {
		log.LogWarnf("RDMA: init failed, falling back to TCP: %v", err)
		return nil
	}
	rdma.StartStatsLogger("ObjectNode")
	stream.StartPhaseAStatsLogger("ObjectNode")
	log.LogInfof("RDMA: ObjectNode client pool initialized "+
		"(numSlots=%d slotSize=%d maxConns=%d portShift=%d minPayload=%d busy=%d yield=%d sleep=%dus). "+
		"Metrics: cubefs_rdma_requests_total / cubefs_rdma_fallback_total / cubefs_rdma_latency_seconds. "+
		"Periodic stats logged every 60s — grep 'RDMA stats[ObjectNode]'.",
		numSlots, slotSize, maxConns, portShift, minPayload, busy, yield, sleepUs)
	return nil
}
