package metanode

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

// Ensure basic SetLimiter/Wait/RmLimiter/IsOpNameValid behaviors
func TestOpLimiter_Basic(t *testing.T) {
	ol := newOpLimiter()

	// invalid op name
	err, _, _ := ol.IsOpNameValid("invalid-op-name")
	require.Error(t, err)

	// valid op name
	name := "metalookup"
	err = ol.SetLimiter(name, 1, 0)
	require.NoError(t, err)

	// consume burst tokens then expect rate limit
	code := proto.GOpInfo[name]
	for i := 0; i < defaultOpLimitBurst; i++ {
		require.NoError(t, ol.Wait(code))
	}
	require.Error(t, ol.Wait(code))

	// remove and ensure Wait passes (no limiter present)
	require.NoError(t, ol.RmLimiter(name))
	require.NoError(t, ol.Wait(code))
}

// Ensure timeout>0 branch in Wait works (token replenishment)
func TestOpLimiter_TimeoutBranch(t *testing.T) {
	ol := newOpLimiter()
	name := "metalookup"
	// 1 QPS, timeout 1s
	require.NoError(t, ol.SetLimiter(name, 1, 1))
	code := proto.GOpInfo[name]
	// consume burst first
	for i := 0; i < defaultOpLimitBurst; i++ {
		require.NoError(t, ol.Wait(code))
	}
	start := time.Now()
	// next will wait up to 1s until token available, then succeed
	require.NoError(t, ol.Wait(code))
	// should take at least ~1s to pass (allow some jitter)
	require.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond)
}

// Ensure getOpListHandler result includes mapping items from proto.GOpInfo (sanity for names)
func TestGOpInfo_JSONMarshable(t *testing.T) {
	// quick sanity that mapping is JSON-marshable and contains expected key
	data, err := json.Marshal(proto.GOpInfo)
	require.NoError(t, err)
	require.Contains(t, string(data), fmt.Sprintf("\"%s\"", "metalookup"))
}

func TestGOpInfo_AsyncExtentsListRegistered(t *testing.T) {
	require.Equal(t, proto.OpMetaAsyncExtentsList, proto.GOpInfo["metaasyncextentslist"])
	require.Equal(t, proto.OpMetaExtentsList, proto.GOpInfo["metaextentslist"])
	require.NotEqual(t, proto.GOpInfo["metaextentslist"], proto.GOpInfo["metaasyncextentslist"])
	require.Equal(t, "OpMetaAsyncExtentsList", (&proto.Packet{Opcode: proto.OpMetaAsyncExtentsList}).GetOpMsg())
}

func TestOpLimiter_AsyncExtentsListDefaultLimiter(t *testing.T) {
	ol := newOpLimiter()

	err, name, code := ol.IsOpNameValid("metaasyncextentslist")
	require.NoError(t, err)
	require.Equal(t, "metaasyncextentslist", name)
	require.Equal(t, proto.OpMetaAsyncExtentsList, code)

	ol.m.RLock()
	_, exists := ol.limiterInfos[proto.OpMetaAsyncExtentsList]
	ol.m.RUnlock()
	require.True(t, exists, "default async limiter should be registered for metaasyncextentslist")
}
