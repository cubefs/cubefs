// Copyright 2018 The Chubao Authors.
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
	"sync"
)

func getAvailCarryEcNodeTab(maxTotal uint64, excludeHosts []string, ecNodes *sync.Map) (nodeTabs SortedWeightedNodes, availCount int) {
	nodeTabs = make(SortedWeightedNodes, 0)
	ecNodes.Range(func(key, value interface{}) bool {
		ecNode := value.(*ECNode)
		if contains(excludeHosts, ecNode.Addr) == true {
			return true
		}
		if ecNode.isWriteAble() == false {
			return true
		}
		if ecNode.isAvailCarryNode() == true {
			availCount++
		}
		nt := new(weightedNode)
		nt.Carry = ecNode.Carry
		if ecNode.AvailableSpace < 0 {
			nt.Weight = 0.0
		} else {
			nt.Weight = float64(ecNode.AvailableSpace) / float64(maxTotal)
		}
		nt.Ptr = ecNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}
