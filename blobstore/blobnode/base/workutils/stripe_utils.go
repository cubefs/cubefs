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
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// BidExistStatus blob id exist status
type BidExistStatus struct {
	mode     codemode.CodeMode
	existCnt int
	exist    []bool
}

// NewBidExistStatus returns bid exist status
func NewBidExistStatus(mode codemode.CodeMode) BidExistStatus {
	return BidExistStatus{
		mode:     mode,
		existCnt: 0,
		exist:    make([]bool, AllReplCnt(mode)),
	}
}

// Exist returns true if vuid is exist
func (s *BidExistStatus) Exist(vuidIdx uint8) {
	s.existCnt++
	s.exist[vuidIdx] = true
}

// ExistCnt returns exist count
func (s *BidExistStatus) ExistCnt() int {
	return s.existCnt
}

// CanRecover returns if data can be recover
func (s *BidExistStatus) CanRecover() bool {
	if len(s.exist) != AllReplCnt(s.mode) {
		panic("unexpect:len(existStatus.exist) != AllReplCnt(mode)")
	}

	globalStripe, _, _ := GlobalStripe(s.mode)
	existInGlobalStripe := 0
	for _, idx := range globalStripe {
		if s.exist[idx] {
			existInGlobalStripe++
		}
	}
	return existInGlobalStripe >= ModeN(s.mode)
}

// ModeN returns the count of N
func ModeN(mode codemode.CodeMode) int {
	modeInfo := mode.Tactic()
	return modeInfo.N
}

// IsLocalStripeUint returns true if stripe is local unit
func IsLocalStripeUint(idx int, mode codemode.CodeMode) bool {
	localStripe, n, m := LocalStripe(idx, mode)
	localStripeUnits := localStripe[n : n+m]
	for i := range localStripeUnits {
		if localStripeUnits[i] == idx {
			return true
		}
	}
	return false
}

// IdxSplitByLocalStripe returns local stripe idx
func IdxSplitByLocalStripe(idxs []uint8, mode codemode.CodeMode) [][]uint8 {
	splitMap := make(map[int][]uint8)
	for _, idx := range idxs {
		stripeIdxs, _, _ := LocalStripe(int(idx), mode)
		if len(stripeIdxs) == 0 {
			continue
		}
		splitMap[stripeIdxs[0]] = append(splitMap[stripeIdxs[0]], idx)
	}

	ret := [][]uint8{}
	for _, val := range splitMap {
		ret = append(ret, val)
	}
	return ret
}

// LocalStripe returns local stripe message
func LocalStripe(vuidIdx int, mode codemode.CodeMode) (locatIdxs []int, n, m int) {
	modeInfo := mode.Tactic()
	return modeInfo.LocalStripe(vuidIdx)
}

// GlobalStripe returns global stripe message
func GlobalStripe(mode codemode.CodeMode) (idxs []int, n, m int) {
	modeInfo := mode.Tactic()
	return modeInfo.GlobalStripe()
}

// AllLocalStripe returns all local stripe message
func AllLocalStripe(mode codemode.CodeMode) (stripes [][]int, n, m int) {
	modeInfo := mode.Tactic()
	return modeInfo.AllLocalStripe()
}

// AllReplCnt returns replica count
func AllReplCnt(mode codemode.CodeMode) int {
	modeInfo := mode.Tactic()
	return modeInfo.N + modeInfo.M + modeInfo.L
}

// AllowFailCnt returns allow fail count
func AllowFailCnt(mode codemode.CodeMode) int {
	modeInfo := mode.Tactic()
	return modeInfo.N + modeInfo.M - modeInfo.PutQuorum
}

// AbstractGlobalStripeReplicas returns abstract global stripe replicas
func AbstractGlobalStripeReplicas(
	replicas []proto.VunitLocation,
	mode codemode.CodeMode,
	badIdxs []uint8) []proto.VunitLocation {
	globalStripeIdxs, _, _ := GlobalStripe(mode)

	idxs := filterOut(globalStripeIdxs, badIdxs)

	return AbstractReplicas(replicas, idxs)
}

func filterOut(idxs []int, badIdxs []uint8) []int {
	badIdxsMap := make(map[uint8]struct{})
	for _, idx := range badIdxs {
		badIdxsMap[idx] = struct{}{}
	}

	var filterIdxs []int
	for _, idx := range idxs {
		if _, ok := badIdxsMap[uint8(idx)]; ok {
			continue
		}
		filterIdxs = append(filterIdxs, idx)
	}
	return filterIdxs
}

// AbstractReplicas returns abstract replicas
func AbstractReplicas(replicas []proto.VunitLocation, idxs []int) []proto.VunitLocation {
	abstract := make([]proto.VunitLocation, len(idxs))
	for i, idx := range idxs {
		if uint8(idx) != replicas[idx].Vuid.Index() {
			log.Panicf("unexpect replicas: idx[%d], replica idx[%d]", idx, replicas[idx].Vuid.Index())
		}
		abstract[i] = replicas[idx]
	}
	return abstract
}
