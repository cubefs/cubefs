// Copyright 2026 The CFS Authors.
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

package flashgroupmanager

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestFlashNodeTopology_DeleteRemoteCacheFlowsForVol(t *testing.T) {
	t.Run("emptyVolNameNoOp", func(t *testing.T) {
		topo := NewFlashNodeTopology("t", proto.DefaultRegion, 1, proto.TopoStatusNormal)
		topo.SetRemoteCacheReadFlow("v1", 10)
		topo.SetRemoteCacheWriteFlow("v1", 20)
		require.False(t, topo.DeleteRemoteCacheFlowsForVol(""))
		require.Contains(t, topo.GetRemoteCacheReadFlowMap(), "v1")
		require.Contains(t, topo.GetRemoteCacheWriteFlowMap(), "v1")
	})

	t.Run("removesReadOnly", func(t *testing.T) {
		topo := NewFlashNodeTopology("t", proto.DefaultRegion, 1, proto.TopoStatusNormal)
		topo.SetRemoteCacheReadFlow("onlyRead", 100)
		require.True(t, topo.DeleteRemoteCacheFlowsForVol("onlyRead"))
		require.Empty(t, topo.GetRemoteCacheReadFlowMap())
		require.Empty(t, topo.GetRemoteCacheWriteFlowMap())
	})

	t.Run("removesWriteOnly", func(t *testing.T) {
		topo := NewFlashNodeTopology("t", proto.DefaultRegion, 1, proto.TopoStatusNormal)
		topo.SetRemoteCacheWriteFlow("onlyWrite", 200)
		require.True(t, topo.DeleteRemoteCacheFlowsForVol("onlyWrite"))
		require.Empty(t, topo.GetRemoteCacheReadFlowMap())
		require.Empty(t, topo.GetRemoteCacheWriteFlowMap())
	})

	t.Run("removesBothMaps", func(t *testing.T) {
		topo := NewFlashNodeTopology("t", proto.DefaultRegion, 1, proto.TopoStatusNormal)
		topo.SetRemoteCacheReadFlow("x", 1)
		topo.SetRemoteCacheWriteFlow("x", 2)
		require.True(t, topo.DeleteRemoteCacheFlowsForVol("x"))
		_, okR := topo.GetRemoteCacheReadFlowMap()["x"]
		_, okW := topo.GetRemoteCacheWriteFlowMap()["x"]
		require.False(t, okR)
		require.False(t, okW)
	})

	t.Run("unknownVolReturnsFalse", func(t *testing.T) {
		topo := NewFlashNodeTopology("t", proto.DefaultRegion, 1, proto.TopoStatusNormal)
		topo.SetRemoteCacheReadFlow("keep", 5)
		require.False(t, topo.DeleteRemoteCacheFlowsForVol("nosuch"))
		require.EqualValues(t, 5, topo.GetRemoteCacheReadFlowMap()["keep"])
	})

	t.Run("nilMapsSafe", func(t *testing.T) {
		topo := &FlashNodeTopology{}
		topo.RemoteCacheReadFlowMap = nil
		topo.RemoteCacheWriteFlowMap = nil
		require.False(t, topo.DeleteRemoteCacheFlowsForVol("any"))
	})
}
