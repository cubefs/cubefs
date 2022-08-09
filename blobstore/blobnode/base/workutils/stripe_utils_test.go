// Copyright 2022 The CubeFS Authors.
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

package workutils

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

var testModes = []codemode.CodeMode{
	codemode.EC15P12,
	codemode.EC6P6,
	codemode.EC16P20L2,
	codemode.EC6P10L2,
	codemode.EC6P3L3,
	codemode.EC4P4L2,
}

func testWithAllMode(t *testing.T, testFunc func(t *testing.T, mode codemode.CodeMode)) {
	for _, mode := range testModes {
		testFunc(t, mode)
	}
}

type stripeLayoutTest struct {
	N [][]int
	M [][]int
	L [][]int
}

func (stripe *stripeLayoutTest) globalStripe() []int {
	var globalStripeIdxs []int
	for _, idxs := range stripe.N {
		globalStripeIdxs = append(globalStripeIdxs, idxs...)
	}

	for _, idxs := range stripe.M {
		globalStripeIdxs = append(globalStripeIdxs, idxs...)
	}
	return globalStripeIdxs
}

var EC15p12 = stripeLayoutTest{
	N: [][]int{{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}, {10, 11, 12, 13, 14}},
	M: [][]int{{15, 16, 17, 18}, {19, 20, 21, 22}, {23, 24, 25, 26}},
	L: [][]int{{}},
}

var EC6p6 = stripeLayoutTest{
	N: [][]int{{0, 1}, {2, 3}, {4, 5}},
	M: [][]int{{6, 7}, {8, 9}, {10, 11}},
	L: [][]int{{}},
}

var EC16P20L2 = stripeLayoutTest{
	N: [][]int{{0, 1, 2, 3, 4, 5, 6, 7}, {8, 9, 10, 11, 12, 13, 14, 15}},
	M: [][]int{{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}, {26, 27, 28, 29, 30, 31, 32, 33, 34, 35}},
	L: [][]int{{36}, {37}},
}

var EC6P10L2 = stripeLayoutTest{
	N: [][]int{{0, 1, 2}, {3, 4, 5}},
	M: [][]int{{6, 7, 8, 9, 10}, {11, 12, 13, 14, 15}},
	L: [][]int{{16}, {17}},
}

var EC6P3L3 = stripeLayoutTest{
	N: [][]int{{0, 1}, {2, 3}, {4, 5}},
	M: [][]int{{6}, {7}, {8}},
	L: [][]int{{9}, {10}, {11}},
}

var EC4P4L2 = stripeLayoutTest{
	N: [][]int{{0, 1}, {2, 3}},
	M: [][]int{{4, 5}, {6, 7}},
	L: [][]int{{8}, {9}},
}

var allModeStripe = map[codemode.CodeMode]stripeLayoutTest{
	codemode.EC15P12:   EC15p12,
	codemode.EC6P6:     EC6p6,
	codemode.EC16P20L2: EC16P20L2,
	codemode.EC6P10L2:  EC6P10L2,
	codemode.EC6P3L3:   EC6P3L3,
	codemode.EC4P4L2:   EC4P4L2,
}

func testGetIdcLocalStripe(mode codemode.CodeMode, idcIdx int) (local []int, n, m int) {
	stripe := allModeStripe[mode]
	local = append(local, stripe.N[idcIdx]...)
	local = append(local, stripe.M[idcIdx]...)
	local = append(local, stripe.L[idcIdx]...)
	return local, len(stripe.N[idcIdx]) + len(stripe.M[idcIdx]), len(stripe.L[idcIdx])
}

func testGetIdcIdx(mode codemode.CodeMode, idx int) (idcIdx int) {
	codeInfo := mode.Tactic()
	stripe := allModeStripe[mode]
	for idcIdx = 0; idcIdx < codeInfo.AZCount; idcIdx++ {
		var idcLocalStripeIdxs [][]int
		idcLocalStripeIdxs = append(idcLocalStripeIdxs, stripe.N[idcIdx])
		if containIntTest(idx, idcLocalStripeIdxs) {
			return idcIdx
		}
	}
	return
}

func containIntTest(i int, lists [][]int) bool {
	for _, l := range lists {
		for _, val := range l {
			if val == i {
				return true
			}
		}
	}
	return false
}

func listEqualTest(l1, l2 []int) bool {
	if len(l1) != len(l2) {
		return false
	}
	m := make(map[int]bool)
	for _, e := range l1 {
		m[e] = true
	}
	for _, e := range l2 {
		if _, ok := m[e]; !ok {
			return false
		}
	}
	return true
}

func uin8ListTointListTest(l []uint8) []int {
	var ret []int
	for _, e := range l {
		ret = append(ret, int(e))
	}
	return ret
}

func TestIsLocalStripeUint(t *testing.T) {
	testWithAllMode(t, testIsLocalStripeUint)
}

func testIsLocalStripeUint(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()

	n := codeInfo.N
	m := codeInfo.M
	l := codeInfo.L
	for idx := 0; idx < n+m+l; idx++ {
		flag := IsLocalStripeUint(idx, mode)
		isLocal := false
		if containIntTest(idx, allModeStripe[mode].L) {
			isLocal = true
		}
		require.Equal(t, isLocal, flag)
	}
}

func TestIdxSplitByIdc(t *testing.T) {
	testWithAllMode(t, testIdxSplitByIdc)
}

func testIdxSplitByIdc(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()
	stripe := allModeStripe[mode]
	var allIdxs []uint8
	for idx := 0; idx < codeInfo.N+codeInfo.M+codeInfo.L; idx++ {
		allIdxs = append(allIdxs, uint8(idx))
	}

	splitIdxs := IdxSplitByLocalStripe(allIdxs, mode)
	for _, idxs := range splitIdxs {
		idcIdx := testGetIdcIdx(mode, int(idxs[0]))
		var compareStripe []int
		compareStripe = append(compareStripe, stripe.N[idcIdx]...)
		compareStripe = append(compareStripe, stripe.M[idcIdx]...)
		compareStripe = append(compareStripe, stripe.L[idcIdx]...)
		require.Equal(t, true, listEqualTest(uin8ListTointListTest(idxs), compareStripe))
	}
}

func TestLocalStripe(t *testing.T) {
	testWithAllMode(t, testLocalStripe)
}

func testLocalStripe(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()
	if codeInfo.L == 0 {
		return
	}

	stripe := allModeStripe[mode]
	for i := 0; i < codeInfo.AZCount; i++ {
		var localStripe []int
		localStripe = append(localStripe, stripe.N[i]...)
		localStripe = append(localStripe, stripe.M[i]...)
		localStripe = append(localStripe, stripe.L[i]...)
		localN := len(stripe.N[i]) + len(stripe.M[i])
		localM := len(stripe.L[i])
		for _, idx := range localStripe {
			localIdxs, n, m := LocalStripe(idx, mode)
			require.Equal(t, localN, n)
			require.Equal(t, localM, m)
			require.Equal(t, true, listEqualTest(localIdxs, localStripe))
		}
	}
}

func TestGlobalStripe(t *testing.T) {
	testWithAllMode(t, testGlobalStripe)
}

func testGlobalStripe(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()
	stripe := allModeStripe[mode]
	var compareGlobal []int
	for i := 0; i < codeInfo.AZCount; i++ {
		compareGlobal = append(compareGlobal, stripe.N[i]...)
		compareGlobal = append(compareGlobal, stripe.M[i]...)
	}

	global, n, m := GlobalStripe(mode)
	require.Equal(t, n, codeInfo.N)
	require.Equal(t, m, codeInfo.M)
	require.Equal(t, true, listEqualTest(global, compareGlobal))
}

func TestGetAllLocalStripe(t *testing.T) {
	testWithAllMode(t, testGetAllLocalStripe)
}

func testGetAllLocalStripe(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()
	if codeInfo.L == 0 {
		localStripes, n, m := AllLocalStripe(mode)
		require.Equal(t, 0, len(localStripes))
		require.Equal(t, 0, n)
		require.Equal(t, 0, m)
		return
	}

	localStripes, n, m := AllLocalStripe(mode)
	require.Equal(t, codeInfo.AZCount, len(localStripes))
	idcMap := make(map[int]bool)
	for _, local := range localStripes {
		idcIdx := testGetIdcIdx(mode, local[0])
		idcMap[idcIdx] = true
		compareLocal, compareN, compareM := testGetIdcLocalStripe(mode, idcIdx)
		require.Equal(t, true, listEqualTest(local, compareLocal))
		require.Equal(t, compareN, n)
		require.Equal(t, compareM, m)
	}
	require.Equal(t, codeInfo.AZCount, len(idcMap))
}

func TestModeFunc(t *testing.T) {
	codeMode := codemode.EC16P20L2
	mode := codeMode.Tactic()
	N := mode.N
	M := mode.M
	L := mode.L
	PutQuorum := mode.PutQuorum

	require.Equal(t, N+L+M, AllReplCnt(codeMode))
	require.Equal(t, N+M-PutQuorum, AllowFailCnt(codeMode))
}

func TestCanRecover(t *testing.T) {
	testWithAllMode(t, testCanRecover)
}

func testCanRecover(t *testing.T, mode codemode.CodeMode) {
	stripeLayout := allModeStripe[mode]
	status := NewBidExistStatus(mode)
	for _, idxs := range stripeLayout.N {
		for _, idx := range idxs {
			status.Exist(uint8(idx))
		}
	}
	require.Equal(t, true, status.CanRecover())

	for _, idxs := range stripeLayout.M {
		for _, idx := range idxs {
			status.Exist(uint8(idx))
		}
	}
	require.Equal(t, true, status.CanRecover())

	// can not recover
	status = NewBidExistStatus(mode)
LoopTest:
	for i := 0; i < mode.Tactic().AZCount; i++ {
		for _, idx := range stripeLayout.N[i] {
			status.Exist(uint8(idx))
			if status.existCnt == ModeN(mode)-1 {
				break LoopTest
			}
		}

		for _, idx := range stripeLayout.M[i] {
			status.Exist(uint8(idx))
			if status.existCnt == ModeN(mode)-1 {
				break LoopTest
			}
		}
	}
	require.Equal(t, false, status.CanRecover())
}

func genMockVol(vid proto.Vid, mode codemode.CodeMode) ([]proto.VunitLocation, codemode.CodeMode) {
	modeInfo := mode.Tactic()
	replicas := make([]proto.VunitLocation, modeInfo.N+modeInfo.M+modeInfo.L)
	for i := 0; i < modeInfo.N+modeInfo.M+modeInfo.L; i++ {
		vuid, _ := proto.NewVuid(vid, uint8(i), 1)
		replicas[i] = proto.VunitLocation{
			Vuid:   vuid,
			Host:   "127.0.0.1:xxxx",
			DiskID: 1,
		}
	}
	return replicas, mode
}

func TestAbstractGlobalStripeReplicas(t *testing.T) {
	testWithAllMode(t, testAbstractGlobalStripeReplicas)
}

func testAbstractGlobalStripeReplicas(t *testing.T, mode codemode.CodeMode) {
	testAbstractGlobalStripeReplicasWithBads(t, mode, []uint8{})
	testAbstractGlobalStripeReplicasWithBads(t, mode, []uint8{0})
	testAbstractGlobalStripeReplicasWithBads(t, mode, []uint8{0, 1})
}

func testAbstractGlobalStripeReplicasWithBads(t *testing.T, mode codemode.CodeMode, badIdxs []uint8) {
	replicas, _ := genMockVol(1, mode)
	globalStripe, err := AbstractGlobalStripeReplicas(replicas, mode, badIdxs)
	require.NoError(t, err)

	stripeLayout := allModeStripe[mode]
	expectStripe := stripeLayout.globalStripe()
	require.Equal(t, len(expectStripe), len(globalStripe)+len(badIdxs))

	m := make(map[int]struct{})
	for _, replicas := range globalStripe {
		m[int(replicas.Vuid.Index())] = struct{}{}
	}

	for _, idx := range badIdxs {
		m[int(idx)] = struct{}{}
	}

	for _, idx := range expectStripe {
		_, ok := m[idx]
		require.Equal(t, true, ok)
	}
}
