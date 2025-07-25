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

	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
)

type deleteStageMgr struct {
	l         sync.RWMutex
	delStages map[uint64]snproto.BlobDeleteStage
}

type deleteStage uint32

const (
	InitStage deleteStage = iota
	DeleteStageMarkDelete
	DeleteStageDelete
)

func newDeleteStageMgr() *deleteStageMgr {
	return &deleteStageMgr{
		delStages: make(map[uint64]snproto.BlobDeleteStage),
	}
}

func (dsm *deleteStageMgr) setMsgDelStage(stage map[uint64]snproto.BlobDeleteStage) {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	stageCopy := make(map[uint64]snproto.BlobDeleteStage)
	for k, v := range stage {
		stageCopy[k] = v
	}
	dsm.delStages = stageCopy
}

func (dsm *deleteStageMgr) getMsgDelStage() map[uint64]snproto.BlobDeleteStage {
	dsm.l.RLock()
	defer dsm.l.RUnlock()
	stageCopy := make(map[uint64]snproto.BlobDeleteStage)
	for k, v := range dsm.delStages {
		stageCopy[k] = v
	}
	return stageCopy
}

func (dsm *deleteStageMgr) setShardDelStage(bid proto.BlobID, vuid proto.Vuid, stage deleteStage) {
	dsm.l.Lock()
	defer dsm.l.Unlock()

	_, ok := dsm.delStages[uint64(bid)]
	if !ok {
		dsm.delStages[uint64(bid)] = snproto.BlobDeleteStage{
			Stage: make(map[uint32]uint32),
		}
	}

	dsm.delStages[uint64(bid)].Stage[uint32(vuid.Index())] = uint32(stage)
}

func (dsm *deleteStageMgr) hasShardMarkDel(bid proto.BlobID, vuid proto.Vuid) bool {
	dsm.l.RLock()
	defer dsm.l.RUnlock()
	stg, ok := dsm.delStages[uint64(bid)]
	if !ok {
		return false
	}
	shardStg, ok := stg.Stage[uint32(vuid.Index())]
	if !ok {
		return false
	}
	return deleteStage(shardStg) == DeleteStageMarkDelete
}

func (dsm *deleteStageMgr) hasShardDelete(bid proto.BlobID, vuid proto.Vuid) bool {
	dsm.l.RLock()
	defer dsm.l.RUnlock()
	stg, ok := dsm.delStages[uint64(bid)]
	if !ok {
		return false
	}
	shardStg, ok := stg.Stage[uint32(vuid.Index())]
	if !ok {
		return false
	}
	return deleteStage(shardStg) == DeleteStageDelete
}

func (dsm *deleteStageMgr) hasMarkDel(bid proto.BlobID) bool {
	dsm.l.RLock()
	defer dsm.l.RUnlock()
	stg, ok := dsm.delStages[uint64(bid)]
	if !ok {
		return false
	}
	for _, stage := range stg.Stage {
		if deleteStage(stage) < DeleteStageMarkDelete {
			return false
		}
	}
	return true
}

func (dsm *deleteStageMgr) hasDelete(bid proto.BlobID) bool {
	dsm.l.RLock()
	defer dsm.l.RUnlock()
	stg, ok := dsm.delStages[uint64(bid)]
	if !ok {
		return false
	}
	for _, stage := range stg.Stage {
		if deleteStage(stage) < DeleteStageDelete {
			return false
		}
	}
	return true
}
