// Copyright 2025 The CubeFS Authors.
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

package proto

import (
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	DeleteBlobMsgFieldID = iota + 1
	SliceRepairMsgFieldID
)

/*
- s: space data(blob, item)
- da: delete message tier normal
- dp: delete message tier punish
- ra: repair message tier single idx
- rb: repair message tier multi idx
- rp: repair message tier punish
*/
var (
	SpaceDataPrefix = []byte{'s'}
	DeleteMsgPrefix = []byte{'d'}
	RepairMsgPrefix = []byte{'r'}

	// tier prefix
	TierSingleIdxPrefix = []byte{'a'}
	TierMultiIdxPrefix  = []byte{'b'}
	TierPunishPrefix    = []byte{'p'}
)

const (
	ShardNodeBlobDeleteTask  = "shard_node_blob_delete"
	ShardNodeSliceRepairTask = "shard_node_slice_repair"
)

type VolumeInfoSimple struct {
	Vid            proto.Vid
	CodeMode       codemode.CodeMode
	Status         proto.VolumeStatus
	VunitLocations []proto.VunitLocation `json:"vunit_locations"`
}

// EqualWith returns whether equal with another.
func (vol *VolumeInfoSimple) EqualWith(volInfo *VolumeInfoSimple) bool {
	if len(vol.VunitLocations) != len(volInfo.VunitLocations) {
		return false
	}
	if vol.Vid != volInfo.Vid ||
		vol.CodeMode != volInfo.CodeMode ||
		vol.Status != volInfo.Status {
		return false
	}
	for i := range vol.VunitLocations {
		if vol.VunitLocations[i] != volInfo.VunitLocations[i] {
			return false
		}
	}
	return true
}

// IsIdle returns true if volume is idle
func (vol *VolumeInfoSimple) IsIdle() bool {
	return vol.Status == proto.VolumeStatusIdle
}

// IsActive returns true if volume is active
func (vol *VolumeInfoSimple) IsActive() bool {
	return vol.Status == proto.VolumeStatusActive
}

func (vol *VolumeInfoSimple) Set(info *cmapi.VolumeInfo) {
	vol.Vid = info.Vid
	vol.CodeMode = info.CodeMode
	vol.Status = info.Status
	vol.VunitLocations = make([]proto.VunitLocation, len(info.Units))

	// check volume info
	codeModeInfo := info.CodeMode.Tactic()
	vunitCnt := codeModeInfo.N + codeModeInfo.M + codeModeInfo.L
	if len(info.Units) != vunitCnt {
		log.Panicf("volume %d info unexpect", info.Vid)
	}

	diskIDMap := make(map[proto.DiskID]struct{}, vunitCnt)
	for _, repl := range info.Units {
		if _, ok := diskIDMap[repl.DiskID]; ok {
			log.Panicf("vid %d many chunks on same disk", info.Vid)
		}
		diskIDMap[repl.DiskID] = struct{}{}
	}

	for i := 0; i < len(info.Units); i++ {
		vol.VunitLocations[i] = proto.VunitLocation{
			Vuid:   info.Units[i].Vuid,
			Host:   info.Units[i].Host,
			DiskID: info.Units[i].DiskID,
		}
	}
}

type MessageType int

const (
	MessageTypeDelete MessageType = iota
	MessageTypeRepair
)

type MessageTier int

const (
	TierSingleIdx MessageTier = iota // for normal delete messages and repair messages with len(badIdx) <= 1
	TierMultiIdx                     // for repair messages with len(badIdx) > 1
	TierPunish                       // for both delete and repair messages need to be punished
)

type MessageExt interface {
	IsProtected(protectDuration time.Duration) bool
	GetVid() proto.Vid
	GetBid() proto.BlobID
	GetSuid() proto.Suid
	GetMsgKey() []byte
	GetMsgType() MessageType
	GetTier(maxRetryTimes int) MessageTier
	SetTime(ts int64)
	GetTime() int64
	GetReqId() string
	AddRetry()
	GetRetry() int
	GetBidNum() uint64
	String() string
	Marshal() ([]byte, error)
}
