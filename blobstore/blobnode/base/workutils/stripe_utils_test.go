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
)

func testWithAllMode(t *testing.T, testFunc func(t *testing.T, mode codemode.CodeMode)) {
	for mode := range allModeStripe {
		testFunc(t, mode)
	}
}

type stripeLayoutTest struct {
	N [][]int
	M [][]int
	L [][]int
}

var EC15P12 = stripeLayoutTest{
	N: [][]int{{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}, {10, 11, 12, 13, 14}},
	M: [][]int{{15, 16, 17, 18}, {19, 20, 21, 22}, {23, 24, 25, 26}},
	L: [][]int{{}},
}

var EC6P6 = stripeLayoutTest{
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
	codemode.EC15P12:   EC15P12,
	codemode.EC6P6:     EC6P6,
	codemode.EC16P20L2: EC16P20L2,
	codemode.EC6P10L2:  EC6P10L2,
	codemode.EC6P3L3:   EC6P3L3,
	codemode.EC4P4L2:   EC4P4L2,
}

func testGetIdcIdx(mode codemode.CodeMode, idx int) (idcIdx int) {
	codeInfo := mode.Tactic()
	stripe := allModeStripe[mode]
	for idcIdx = 0; idcIdx < codeInfo.AZCount; idcIdx++ {
		var idcLocalStripeIdxs [][]int
		idcLocalStripeIdxs = append(idcLocalStripeIdxs, stripe.N[idcIdx])
		if contains(idcLocalStripeIdxs, idx) {
			return idcIdx
		}
	}
	return
}

func contains(lists [][]int, i int) bool {
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
			if status.existCnt == mode.Tactic().N-1 {
				break LoopTest
			}
		}

		for _, idx := range stripeLayout.M[i] {
			status.Exist(uint8(idx))
			if status.existCnt == mode.Tactic().N-1 {
				break LoopTest
			}
		}
	}
	require.Equal(t, false, status.CanRecover())
}
