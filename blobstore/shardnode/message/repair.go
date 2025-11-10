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
	"context"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// SliceRepairMgr handles repair messages
type SliceRepairMgr struct {
	*messageMgr
}

func NewShardRepairMgr(cfg *MessageMgrConfig) (*SliceRepairMgr, error) {
	if cfg.messageType == 0 {
		cfg.messageType = snproto.MessageTypeRepair
	}

	repairMgr := &SliceRepairMgr{}
	cfg.executor = repairMgr
	msgMgr, err := newMessageMgr(cfg)
	if err != nil {
		return nil, err
	}

	repairMgr.messageMgr = msgMgr
	repairMgr.Start()
	return repairMgr, nil
}

func (m *SliceRepairMgr) Start() {
	m.messageMgr.run()
}

func (m *SliceRepairMgr) ItemToMessageExt(item interface{}) (snproto.MessageExt, error) {
	msgItem, ok := item.(messageItem)
	if !ok {
		return nil, errors.New("invalid item")
	}
	msg, err := itemToRepairMsg(msgItem.item)
	if err != nil {
		return nil, err
	}
	return &repairMsgExt{
		msg:    msg,
		suid:   msgItem.suid,
		msgKey: []byte(msgItem.item.ID),
	}, nil
}

// ExecuteWithCheckVolConsistency implements MessageExecutor interface for SliceRepairMgr
// TODO: implement repair logic
func (m *SliceRepairMgr) ExecuteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, ret interface{}) error {
	// TODO: implement repair logic
	return errors.New("repair logic not implemented yet")
}

func itemToRepairMsg(itm snapi.Item) (msg snproto.SliceRepairMsg, err error) {
	var msgRaw []byte
	for i := range itm.Fields {
		if itm.Fields[i].ID == snproto.SliceRepairMsgFieldID {
			msgRaw = itm.Fields[i].Value
			break
		}
	}
	if len(msgRaw) < 1 {
		return msg, errors.New("empty repair message data in item")
	}

	msg = snproto.SliceRepairMsg{}
	if err = msg.Unmarshal(msgRaw); err != nil {
		return
	}
	return
}
