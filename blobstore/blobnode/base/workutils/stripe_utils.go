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
		exist:    make([]bool, mode.GetShardNum()),
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
	if len(s.exist) != s.mode.GetShardNum() {
		return false
	}

	globalStripe, _, _ := s.mode.T().GlobalStripe()
	existInGlobalStripe := 0
	for _, idx := range globalStripe {
		if s.exist[idx] {
			existInGlobalStripe++
		}
	}
	return existInGlobalStripe >= s.mode.T().N
}

// IsLocalStripeIndex returns true if index is local unit.
func IsLocalStripeIndex(mode codemode.CodeMode, idx int) bool {
	localStripe, n, m := mode.T().LocalStripe(idx)
	for _, localIdx := range localStripe[n : n+m] {
		if localIdx == idx {
			return true
		}
	}
	return false
}

// IdxSplitByLocalStripe returns local stripe idx
func IdxSplitByLocalStripe(idxs []uint8, mode codemode.CodeMode) [][]uint8 {
	splitMap := make(map[int][]uint8)
	tactic := mode.Tactic()
	for _, idx := range idxs {
		stripeIdxs, _, _ := tactic.LocalStripe(int(idx))
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
