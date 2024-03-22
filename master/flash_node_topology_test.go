// Copyright 2023 The CFS Authors.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlash(t *testing.T) {
	t.Run("Node", testFlashNode)
	t.Run("Group", testFlashGroup)
	t.Run("Topology", testFlashTopology)
}

func testFlashTopology(t *testing.T) {
	t.Run("Clear", testFlashTopologyClear)
	t.Run("Load", testFlashTopologyLoad)
}

func testFlashTopologyClear(t *testing.T) {
	groups := createFlashGroups(t)
	_, err := mc.AdminAPI().FlashGroupAddFlashNode(groups[0].ID, 1, testZone1, mfs1Addr)
	require.NoError(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(groups[2].ID, 2, testZone2, "")
	require.NoError(t, err)
	server.cluster.fsm.store.Flush()
	server.cluster.flashNodeTopo.clear()
}

func testFlashTopologyLoad(t *testing.T) {
	server.cluster.loadFlashNodes()
	server.cluster.loadFlashGroups()
	server.cluster.loadFlashTopology()
}
