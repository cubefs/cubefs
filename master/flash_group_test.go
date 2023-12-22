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
	"math"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func createFlashGroups(t *testing.T) (groups []proto.FlashGroupAdminView) {
	const n int = 10
	for idx := range [n]struct{}{} {
		var slots string
		if idx == 7 {
			slots = "create,by,default"
		}
		if idx == 3 {
			slots = "3000,3333"
		}
		if idx%2 == 0 {
			slots = "2222222"
		}
		fgView, err := mc.AdminAPI().CreateFlashGroup(slots)
		require.NoError(t, err)
		require.Equal(t, proto.FlashGroupStatus_Inactive, fgView.Status)
		groups = append(groups, fgView)
	}
	require.Equal(t, n, len(groups))
	return
}

func removeFlashGroups(t *testing.T, groups []proto.FlashGroupAdminView) {
	for _, g := range groups {
		_, err := mc.AdminAPI().RemoveFlashGroup(g.ID)
		require.NoError(t, err)
	}
}

func TestFlashGroupCreate(t *testing.T) {
	groups := createFlashGroups(t)
	defer removeFlashGroups(t, groups)
	_, err := mc.AdminAPI().SetFlashGroup(math.MaxUint64, true)
	require.Error(t, err)
	_, err = mc.AdminAPI().RemoveFlashGroup(math.MaxUint64)
	require.Error(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(math.MaxUint64, 1, "", mfs8Addr)
	require.Error(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(math.MaxUint64, 1, testZone1, mfs1Addr)
	require.Error(t, err)
	_, err = mc.AdminAPI().FlashGroupRemoveFlashNode(math.MaxUint64, 1, "", mfs8Addr)
	require.Error(t, err)
	_, err = mc.AdminAPI().FlashGroupRemoveFlashNode(math.MaxUint64, 1, testZone1, mfs1Addr)
	require.Error(t, err)
}

func TestFlashGroupSet(t *testing.T) {
	groups := createFlashGroups(t)
	defer removeFlashGroups(t, groups)
	g := groups[0]
	g, err := mc.AdminAPI().SetFlashGroup(g.ID, true)
	require.NoError(t, err)
	require.True(t, g.Status.IsActive())
	g, err = mc.AdminAPI().SetFlashGroup(g.ID, true)
	require.NoError(t, err)
	require.True(t, g.Status.IsActive())
	g, err = mc.AdminAPI().SetFlashGroup(g.ID, false)
	require.NoError(t, err)
	require.False(t, g.Status.IsActive())
}

func TestFlashGroupRemove(t *testing.T) {
	groups := createFlashGroups(t)
	removeFlashGroups(t, groups)
}

func TestFlashGroupNode(t *testing.T) {
	groups := createFlashGroups(t)
	defer removeFlashGroups(t, groups)
	g := groups[0]
	_, err := mc.AdminAPI().FlashGroupAddFlashNode(g.ID, 1, testZone3, mfs8Addr)
	require.Error(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(g.ID, 1, testZone3, mfs1Addr)
	require.NoError(t, err)
	_, err = mc.AdminAPI().FlashGroupRemoveFlashNode(g.ID, 1, testZone3, mfs1Addr)
	require.NoError(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(g.ID, 2, testZone1, "")
	require.NoError(t, err)
	_, err = mc.AdminAPI().FlashGroupRemoveFlashNode(g.ID, 2, testZone1, "")
	require.NoError(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(g.ID, 100, testZone2, "")
	require.Error(t, err)
	_, err = mc.AdminAPI().FlashGroupRemoveFlashNode(g.ID, 100, testZone2, "")
	require.Error(t, err)
}

func TestFlashGroupGet(t *testing.T) {
	groups := createFlashGroups(t)
	defer removeFlashGroups(t, groups)
	for _, g := range groups {
		_, err := mc.AdminAPI().GetFlashGroup(g.ID)
		require.NoError(t, err)
	}
}

func TestFlashGroupList(t *testing.T) {
	groups := createFlashGroups(t)
	defer removeFlashGroups(t, groups)
	g := groups[0]

	fgViews, err := mc.AdminAPI().ListFlashGroups()
	require.NoError(t, err)
	require.Equal(t, len(groups), len(fgViews.FlashGroups))

	fg, err := mc.AdminAPI().SetFlashGroup(g.ID, true)
	require.NoError(t, err)
	require.True(t, fg.Status.IsActive())

	fgViews, err = mc.AdminAPI().ListFlashGroup(true)
	require.NoError(t, err)
	require.Equal(t, 1, len(fgViews.FlashGroups))
	require.Equal(t, g.ID, fgViews.FlashGroups[0].ID)
}

func TestFlashGroupClient(t *testing.T) {
	groups := createFlashGroups(t)
	defer removeFlashGroups(t, groups)
	g := groups[0]

	_, err := mc.AdminAPI().FlashGroupAddFlashNode(g.ID, 0, "", mfs1Addr)
	require.NoError(t, err)
	_, err = mc.AdminAPI().FlashGroupAddFlashNode(groups[2].ID, 2, testZone3, "")
	require.NoError(t, err)

	fgs, err := mc.AdminAPI().ClientFlashGroups()
	require.NoError(t, err)
	t.Logf("%+v", fgs)
}
