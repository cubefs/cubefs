// Copyright 2018 The ChuBao Authors.
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

package blob

import (
	"container/list"
	"fmt"
	"strings"
	"sync"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/log"
)

type DataPartition struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

func (dp *DataPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v)", dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts)
}

func (dp *DataPartition) GetFollowAddrs() string {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type PartitionCache struct {
	sync.RWMutex
	parts  map[uint64]*list.Element
	rwlist *list.List
	rolist *list.List
}

func NewPartitionCache() *PartitionCache {
	return &PartitionCache{
		parts:  make(map[uint64]*list.Element),
		rwlist: list.New(),
		rolist: list.New(),
	}
}

func (pc *PartitionCache) Put(dp *DataPartition) {
	pc.Lock()
	defer pc.Unlock()

	if dp.Status != proto.ReadWrite && dp.Status != proto.ReadOnly {
		log.LogErrorf("PartitionCache Put: Invalid dp(%v)", dp)
		return
	}

	if old, ok := pc.parts[dp.PartitionID]; ok {
		pc.remove(old)
		delete(pc.parts, dp.PartitionID)
	}

	element := pc.add(dp)
	pc.parts[dp.PartitionID] = element
}

func (pc *PartitionCache) Get(pid uint64) *DataPartition {
	pc.RLock()
	defer pc.RUnlock()

	element, ok := pc.parts[pid]
	if !ok {
		return nil
	}
	dp := element.Value.(*DataPartition)
	return dp
}

func (pc *PartitionCache) GetWritePartition() *DataPartition {
	pc.Lock()
	defer pc.Unlock()

	element := pc.rwlist.Front()
	if element == nil {
		return nil
	}
	pc.rwlist.MoveToBack(element)
	dp := element.Value.(*DataPartition)
	return dp
}

func (pc *PartitionCache) GetWritePartitionLen() int {
	pc.RLock()
	defer pc.RUnlock()
	return pc.rwlist.Len()
}

func (pc *PartitionCache) List() []*DataPartition {
	pc.RLock()
	defer pc.RUnlock()
	dplist := make([]*DataPartition, 0, len(pc.parts))
	for _, e := range pc.parts {
		dp := e.Value.(*DataPartition)
		dplist = append(dplist, dp)
	}
	return dplist
}

//Caller should make sure dp is valid and pc is protected by lock
func (pc *PartitionCache) add(dp *DataPartition) *list.Element {
	var element *list.Element
	switch dp.Status {
	case proto.ReadWrite:
		element = pc.rwlist.PushBack(dp)
	case proto.ReadOnly:
		element = pc.rolist.PushBack(dp)
	default:
		panic(fmt.Sprintf("Invalid dp(%v)", dp))
	}
	return element
}

func (pc *PartitionCache) remove(element *list.Element) {
	dp := element.Value.(*DataPartition)
	switch dp.Status {
	case proto.ReadWrite:
		pc.rwlist.Remove(element)
	case proto.ReadOnly:
		pc.rolist.Remove(element)
	}
}
