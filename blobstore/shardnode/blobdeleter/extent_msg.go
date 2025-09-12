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

package blobdeleter

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
)

type deleteStage uint32

const (
	InitStage deleteStage = iota
	DeleteStageMarkDelete
	DeleteStageDelete
)

type delMsgExt struct {
	msg snproto.DeleteMsg

	// use to delete and update msg in kvstore
	suid   proto.Suid
	msgKey []byte
	l      sync.RWMutex
}

func (ext *delMsgExt) isProtected(protectDuration time.Duration) bool {
	ts := time.Unix(ext.msg.Time, 0)
	return time.Now().Before(ts.Add(protectDuration))
}

func (ext *delMsgExt) setShardDelStage(bid proto.BlobID, vuid proto.Vuid, stage deleteStage) {
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

func (ext *delMsgExt) hasShardMarkDel(bid proto.BlobID, vuid proto.Vuid) bool {
	ext.l.RLock()
	defer ext.l.RUnlock()
	stg, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		return false
	}
	shardStg, ok := stg.Stage[uint32(vuid.Index())]
	if !ok {
		return false
	}
	return deleteStage(shardStg) == DeleteStageMarkDelete
}

func (ext *delMsgExt) hasShardDelete(bid proto.BlobID, vuid proto.Vuid) bool {
	ext.l.RLock()
	defer ext.l.RUnlock()
	stg, ok := ext.msg.MsgDelStage[uint64(bid)]
	if !ok {
		return false
	}
	shardStg, ok := stg.Stage[uint32(vuid.Index())]
	if !ok {
		return false
	}
	return deleteStage(shardStg) == DeleteStageDelete
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
