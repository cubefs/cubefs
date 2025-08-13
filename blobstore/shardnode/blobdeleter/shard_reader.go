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
	"context"
	"errors"
	"time"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type shardListReader struct {
	storage.ShardHandler

	messages          []*delMsgExt
	nextMarker        []byte
	protectedTimeUnix int64
}

func newShardListReader(handler storage.ShardHandler) *shardListReader {
	return &shardListReader{
		ShardHandler: handler,
		messages:     make([]*delMsgExt, 0),
	}
}

func (r *shardListReader) listMessage(ctx context.Context, protectDuration time.Duration, count int) ([]*delMsgExt, error) {
	if len(r.messages) > 0 {
		return r.listFromCache(protectDuration)
	}
	return r.listFromStorage(ctx, protectDuration, count)
}

func (r *shardListReader) listFromCache(protectDuration time.Duration) ([]*delMsgExt, error) {
	protectedIndex := r.findProtectedIndex(protectDuration)

	// has cached message but all protected
	if protectedIndex == 0 {
		return nil, nil
	}

	if protectedIndex == len(r.messages) {
		ret := r.messages
		r.messages = r.messages[:0]
		return ret, nil
	}

	ret := r.messages[:protectedIndex]
	r.messages = r.messages[protectedIndex:]
	return ret, nil
}

func (r *shardListReader) findProtectedIndex(protectDuration time.Duration) int {
	for i := range r.messages {
		if r.messages[i].isProtected(protectDuration) {
			return i
		}
	}
	// all messages are not protected
	return len(r.messages)
}

func (r *shardListReader) listFromStorage(ctx context.Context, protectDuration time.Duration, count int) ([]*delMsgExt, error) {
	span := trace.SpanFromContextSafe(ctx)
	if r.isProtected(protectDuration) {
		return nil, nil
	}

	h := storage.OpHeader{
		RouteVersion: r.GetRouteVersion(),
	}

	items, nextMarker, err := r.ListItem(ctx, h, snproto.DeleteMsgPrefix, r.nextMarker, uint64(count))
	if err != nil {
		return nil, err
	}

	suid := r.GetSuid()
	ret := make([]*delMsgExt, 0, len(items))
	for i := 0; i < len(items); i++ {
		msg, err := itemToDelMsg(items[i])
		if err != nil {
			return nil, err
		}
		me := &delMsgExt{
			msg:    msg,
			suid:   suid,
			msgKey: items[i].ID,
		}
		span.Debugf("shard[%d] load key: %+v, msg: %+v", suid, items[i].ID, msg)
		if !me.isProtected(protectDuration) {
			ret = append(ret, me)
			continue
		}
		r.messages = append(r.messages, me)
	}

	r.nextMarker = nextMarker
	if len(nextMarker) < 1 {
		// no more message in shard storage, new message after now should be protected
		r.setProtected(time.Now().Unix())
		return ret, nil
	}

	// if nextMarker's timeUnix if protected, set protected
	ts, _, _, _, err := base.DecodeDelMsgKey(r.nextMarker, r.ShardingSubRangeCount())
	if err != nil {
		span.Errorf("decode nextMarker[%+v] failed, err: %v", r.nextMarker, err)
		return nil, err
	}
	nextMarkerTimeUnix := ts.TimeUnix()
	if time.Now().Before(time.Unix(nextMarkerTimeUnix, 0).Add(protectDuration)) {
		r.setProtected(nextMarkerTimeUnix)
	}
	return ret, nil
}

func (r *shardListReader) init() {
	r.messages = r.messages[:0]
	r.nextMarker = nil
}

func (r *shardListReader) setProtected(timeUnix int64) {
	r.protectedTimeUnix = timeUnix
	log.Debugf("shard[%d] set protected, timestamp: %d", r.GetSuid(), r.protectedTimeUnix)
}

func (r *shardListReader) isProtected(protectDuration time.Duration) bool {
	return time.Now().Before(time.Unix(r.protectedTimeUnix, 0).Add(protectDuration))
}

func delMsgToItem(key []byte, msg snproto.DeleteMsg) (itm snapi.Item, err error) {
	raw, err := msg.Marshal()
	if err != nil {
		return
	}
	itm = snapi.Item{
		ID: key,
		Fields: []snapi.Field{
			{
				ID:    snproto.DeleteBlobMsgFieldID,
				Value: raw,
			},
		},
	}
	return
}

func itemToDelMsg(itm snapi.Item) (msg snproto.DeleteMsg, err error) {
	var msgRaw []byte
	for i := range itm.Fields {
		if itm.Fields[i].ID == snproto.DeleteBlobMsgFieldID {
			msgRaw = itm.Fields[i].Value
			break
		}
	}
	if len(msgRaw) < 1 {
		return msg, errors.New("empty message data in item")
	}

	msg = snproto.DeleteMsg{}
	if err = msg.Unmarshal(msgRaw); err != nil {
		return
	}
	return
}
