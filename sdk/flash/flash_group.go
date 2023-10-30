// Copyright 2018 The CubeFS Authors.
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

package flash

import (
	"encoding/binary"
	"fmt"
	"sync"

	"go.uber.org/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/fastcrc32"
	"github.com/cubefs/cubefs/util/log"
)

type FlashGroup struct {
	*proto.FlashGroupInfo
	rankedHost map[ZoneRankType][]string
	hostLock   sync.RWMutex
	epoch      atomic.Uint64
}

func (fg *FlashGroup) String() string {
	if fg == nil {
		return ""
	}
	return fmt.Sprintf("flashGroup[fgId(%v) Hosts(%v)]", fg.ID, fg.Hosts)
}

func NewFlashGroup(flashGroupInfo *proto.FlashGroupInfo, rankedHost map[ZoneRankType][]string) *FlashGroup {
	return &FlashGroup{
		FlashGroupInfo: flashGroupInfo,
		rankedHost:     rankedHost,
	}
}

func (fg *FlashGroup) getFlashHost() (host string) {
	fg.hostLock.RLock()
	defer fg.hostLock.RUnlock()

	epoch := fg.epoch.Load()
	fg.epoch.Add(1)

	classifyHost := fg.rankedHost
	if sameZoneHosts, ok := classifyHost[SameZoneRank]; ok {
		host = sameZoneHosts[epoch%uint64(len(sameZoneHosts))]
	} else {
		return ""
	}
	return
}

func (fg *FlashGroup) moveToUnknownRank(addr string) bool {
	fg.hostLock.Lock()
	defer fg.hostLock.Unlock()

	moved := false
	for rank := SameZoneRank; rank <= SameRegionRank; rank++ {
		hosts := fg.rankedHost[rank]
		for i, host := range hosts {
			if host == addr {
				moved = true
				hosts = append(hosts[:i], hosts[i+1:]...)
				break
			}
		}
		if moved {
			fg.rankedHost[rank] = hosts
			break
		}
	}

	unknowns := fg.rankedHost[UnknownZoneRank]
	unknowns = append(unknowns, addr)
	fg.rankedHost[UnknownZoneRank] = unknowns

	log.LogWarnf("moveToUnknownRank: fgID(%v) host: %v", fg.ID, addr)
	return moved
}

type SlotItem struct {
	slot       uint32
	FlashGroup *FlashGroup
}

func (this *SlotItem) Less(than btree.Item) bool {
	that := than.(*SlotItem)
	return this.slot < that.slot
}

func (this *SlotItem) Copy() btree.Item {
	return this
}

func ComputeCacheBlockSlot(volume string, inode, fixedFileOffset uint64) uint32 {
	volLen := len(volume)
	buf := make([]byte, volLen+16)
	copy(buf[:volLen], volume)
	binary.BigEndian.PutUint64(buf[volLen:volLen+8], inode)
	binary.BigEndian.PutUint64(buf[volLen+8:volLen+16], fixedFileOffset)
	return fastcrc32.Checksum(buf)
}
