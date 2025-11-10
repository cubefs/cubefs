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

package message

import (
	"fmt"
	"sync"
	"time"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// delMsgExt is a message extension for DeleteMsg
type delMsgExt struct {
	msg    snproto.DeleteMsg
	suid   proto.Suid
	msgKey []byte
	l      sync.RWMutex
}

type deleteStage uint32

const (
	InitStage deleteStage = iota
	DeleteStageMarkDelete
	DeleteStageDelete
)

func (ext *delMsgExt) IsProtected(protectDuration time.Duration) bool {
	ts := time.Unix(ext.msg.Time, 0)
	return time.Now().Before(ts.Add(protectDuration))
}

func (ext *delMsgExt) GetVid() proto.Vid {
	return ext.msg.Slice.Vid
}

func (ext *delMsgExt) GetBid() proto.BlobID {
	return ext.msg.Slice.MinSliceID
}

func (ext *delMsgExt) GetSuid() proto.Suid {
	return ext.suid
}

func (ext *delMsgExt) GetMsgKey() []byte {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msgKey
}

func (ext *delMsgExt) GetMsgType() snproto.MessageType {
	return snproto.MessageTypeDelete
}

func (ext *delMsgExt) GetTier(maxRetryTimes int) snproto.MessageTier {
	ext.l.RLock()
	defer ext.l.RUnlock()
	if ext.msg.Retry >= uint32(maxRetryTimes) {
		return snproto.TierPunish
	}
	return snproto.TierSingleIdx
}

func (ext *delMsgExt) SetTime(ts int64) {
	ext.l.Lock()
	defer ext.l.Unlock()
	ext.msg.Time = ts
}

func (ext *delMsgExt) GetTime() int64 {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msg.Time
}

func (ext *delMsgExt) GetReqId() string {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msg.ReqId
}

func (ext *delMsgExt) GetRetry() int {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return int(ext.msg.Retry)
}

func (ext *delMsgExt) AddRetry() {
	ext.l.Lock()
	defer ext.l.Unlock()
	ext.msg.Retry++
}

func (ext *delMsgExt) GetBidNum() uint64 {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return uint64(ext.msg.Slice.Count)
}

func (ext *delMsgExt) setSliceUnitDelStage(bid proto.BlobID, vuid proto.Vuid, stage deleteStage) {
	ext.l.Lock()
	defer ext.l.Unlock()

	if ext.msg.MsgDelStage == nil {
		ext.msg.MsgDelStage = make(map[uint64]snproto.BlobDeleteStage)
	}

	_, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		ext.msg.MsgDelStage[uint64(bid)] = snproto.BlobDeleteStage{
			Stage: make(map[uint32]uint32),
		}
	}
	ext.msg.MsgDelStage[uint64(bid)].Stage[uint32(vuid.Index())] = uint32(stage)
}

func (ext *delMsgExt) hasSliceUnitMarkDel(bid proto.BlobID, vuid proto.Vuid) bool {
	ext.l.RLock()
	defer ext.l.RUnlock()
	stg, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		return false
	}
	unitStg, ok := stg.Stage[uint32(vuid.Index())]
	if !ok {
		return false
	}
	return deleteStage(unitStg) == DeleteStageMarkDelete
}

func (ext *delMsgExt) hasSliceUnitDelete(bid proto.BlobID, vuid proto.Vuid) bool {
	ext.l.RLock()
	defer ext.l.RUnlock()
	stg, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		return false
	}
	unitStg, ok := stg.Stage[uint32(vuid.Index())]
	if !ok {
		return false
	}
	return deleteStage(unitStg) == DeleteStageDelete
}

func (ext *delMsgExt) hasMarkDel(bid proto.BlobID) bool {
	ext.l.RLock()
	defer ext.l.RUnlock()
	stg, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		return false
	}
	if stg.Stage == nil {
		return false
	}
	for _, stage := range stg.Stage {
		if deleteStage(stage) < DeleteStageMarkDelete {
			return false
		}
	}
	return true
}

func (ext *delMsgExt) hasDelete(bid proto.BlobID) bool {
	ext.l.RLock()
	defer ext.l.RUnlock()
	stg, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		return false
	}
	if stg.Stage == nil {
		return false
	}
	for _, stage := range stg.Stage {
		if deleteStage(stage) < DeleteStageDelete {
			return false
		}
	}
	return true
}

func (ext *delMsgExt) String() string {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return fmt.Sprintf("msg: %+v, key: %+v, suid: %d", ext.msg, ext.msgKey, ext.suid)
}

func (ext *delMsgExt) Marshal() ([]byte, error) {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msg.Marshal()
}

// repairMsgExt is a message extension for repair messages
type repairMsgExt struct {
	msg    snproto.SliceRepairMsg
	suid   proto.Suid
	msgKey []byte
	l      sync.RWMutex
}

func (ext *repairMsgExt) IsProtected(protectDuration time.Duration) bool {
	ts := time.Unix(ext.msg.Time, 0)
	return time.Now().Before(ts.Add(protectDuration))
}

func (ext *repairMsgExt) GetVid() proto.Vid {
	return ext.msg.Vid
}

func (ext *repairMsgExt) GetBid() proto.BlobID {
	return ext.msg.Bid
}

func (ext *repairMsgExt) GetSuid() proto.Suid {
	return ext.suid
}

func (ext *repairMsgExt) GetMsgKey() []byte {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msgKey
}

func (ext *repairMsgExt) GetMsgType() snproto.MessageType {
	return snproto.MessageTypeRepair
}

func (ext *repairMsgExt) GetTier(maxRetryTimes int) snproto.MessageTier {
	ext.l.RLock()
	defer ext.l.RUnlock()
	if ext.msg.Retry >= uint32(maxRetryTimes) {
		return snproto.TierPunish
	}
	if len(ext.msg.BadIdx) > 1 {
		return snproto.TierMultiIdx
	}
	return snproto.TierSingleIdx
}

func (ext *repairMsgExt) SetTime(ts int64) {
	ext.l.Lock()
	defer ext.l.Unlock()
	ext.msg.Time = ts
}

func (ext *repairMsgExt) GetTime() int64 {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msg.Time
}

func (ext *repairMsgExt) GetReqId() string {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msg.ReqId
}

func (ext *repairMsgExt) AddRetry() {
	ext.l.Lock()
	defer ext.l.Unlock()
	ext.msg.Retry++
}

func (ext *repairMsgExt) GetRetry() int {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return int(ext.msg.Retry)
}

func (ext *repairMsgExt) GetBidNum() uint64 {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return uint64(1)
}

func (ext *repairMsgExt) String() string {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return fmt.Sprintf("repair msg: %+v, key: %+v, suid: %d", ext.msg, ext.msgKey, ext.suid)
}

func (ext *repairMsgExt) Marshal() ([]byte, error) {
	ext.l.RLock()
	defer ext.l.RUnlock()
	return ext.msg.Marshal()
}

func messageExtToItem(key []byte, msg snproto.MessageExt) (itm snapi.Item, err error) {
	var (
		msgRaw  []byte
		fieldID uint32
	)
	msgType := msg.GetMsgType()
	switch msgType {
	case snproto.MessageTypeDelete:
		msgRaw, err = msg.Marshal()
		if err != nil {
			return
		}
		fieldID = uint32(snproto.DeleteBlobMsgFieldID)
	case snproto.MessageTypeRepair:
		msgRaw, err = msg.Marshal()
		if err != nil {
			return
		}
		fieldID = uint32(snproto.SliceRepairMsgFieldID)
	default:
		return snapi.Item{}, errors.New(fmt.Sprintf("unknown message type: %d", msgType))
	}
	itm = snapi.Item{
		ID: string(key),
		Fields: []snapi.Field{
			{
				ID:    proto.FieldID(fieldID),
				Value: msgRaw,
			},
		},
	}
	return
}
