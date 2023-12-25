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

func testFlashNode(t *testing.T) {
	t.Run("Set", testFlashNodeSet)
	t.Run("Remove", testFlashNodeRemove)
	t.Run("Get", testFlashNodeGet)
	t.Run("List", testFlashNodeList)
}

func testFlashNodeSet(t *testing.T) {
	fnView, err := mc.NodeAPI().GetFlashNode(mfs1Addr)
	require.NoError(t, err)
	require.True(t, fnView.IsEnable)

	require.NoError(t, mc.NodeAPI().SetFlashNode(mfs1Addr, "false"))
	fnView, err = mc.NodeAPI().GetFlashNode(mfs1Addr)
	require.NoError(t, err)
	require.False(t, fnView.IsEnable)
	require.NoError(t, mc.NodeAPI().SetFlashNode(mfs1Addr, "true"))
}

func testFlashNodeRemove(t *testing.T) {
	addr := mfs8Addr
	_, err := mc.NodeAPI().GetFlashNode(addr)
	require.Error(t, err)
	flashServer := addFlashServer(addr, testZone1)
	defer flashServer.Stop()

	_, err = mc.NodeAPI().AddFlashNode("not-addr", testZone1, "")
	require.Error(t, err)
	id, err := mc.NodeAPI().AddFlashNode(addr, testZone1, "")
	require.NoError(t, err)
	fnView, err := mc.NodeAPI().GetFlashNode(addr)
	require.NoError(t, err)
	require.Equal(t, id, fnView.ID)

	_, err = mc.NodeAPI().RemoveFlashNode(addr)
	require.NoError(t, err)
	_, err = mc.NodeAPI().RemoveFlashNode("not-addr")
	require.Error(t, err)
	_, err = mc.NodeAPI().RemoveFlashNode(addr)
	require.Error(t, err)
	_, err = mc.NodeAPI().GetFlashNode(addr)
	require.Error(t, err)
}

func testFlashNodeGet(t *testing.T) {
	_, err := mc.NodeAPI().GetFlashNode("not-addr")
	require.Error(t, err)
	testCases := []struct {
		NodeAddr string
		ZoneName string
	}{
		{mfs1Addr, testZone1},
		{mfs2Addr, testZone1},
		{mfs3Addr, testZone2},
		{mfs4Addr, testZone2},
		{mfs5Addr, testZone3},
		{mfs6Addr, testZone3},
		{mfs7Addr, testZone3},
	}
	for _, testCase := range testCases {
		fnView, err := mc.NodeAPI().GetFlashNode(testCase.NodeAddr)
		require.NoError(t, err)
		require.Equal(t, testCase.NodeAddr, fnView.Addr)
	}
}

func testFlashNodeList(t *testing.T) {
	zoneNodes, err := mc.NodeAPI().ListFlashNodes(true)
	require.NoError(t, err)
	require.Equal(t, 3, len(zoneNodes))
	require.Equal(t, 2, len(zoneNodes[testZone1]))
	require.Equal(t, 2, len(zoneNodes[testZone2]))
	require.Equal(t, 3, len(zoneNodes[testZone3]))

	require.NoError(t, mc.NodeAPI().SetFlashNode(mfs7Addr, "0"))
	defer func() {
		require.NoError(t, mc.NodeAPI().SetFlashNode(mfs7Addr, "1"))
	}()

	zoneNodes, err = mc.NodeAPI().ListFlashNodes(false)
	require.NoError(t, err)
	require.Equal(t, 3, len(zoneNodes))
	require.Equal(t, 2, len(zoneNodes[testZone1]))
	require.Equal(t, 2, len(zoneNodes[testZone2]))
	require.Equal(t, 2, len(zoneNodes[testZone3]))
}
