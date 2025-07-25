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
	"encoding/binary"
	"errors"
	"time"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
)

type shardListReader struct {
	storage.ShardHandler

	messages     []*delMsgExt
	nextMarkerTs int64
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
	if r.isProtected(protectDuration) {
		return nil, nil
	}

	h := storage.OpHeader{
		RouteVersion: r.GetRouteVersion(),
	}

	marker := convertTimeUnixToMarker(r.nextMarkerTs)
	items, nextMarker, err := r.ListItem(ctx, h, snproto.DeleteMsgPrefix, marker, uint64(count))
	if err != nil {
		return nil, err
	}

	ret := make([]*delMsgExt, 0)
	for i := 0; i < len(items); i++ {
		msg, err := itemToDelMsg(items[i])
		if err != nil {
			return nil, err
		}
		me := &delMsgExt{
			msg:       msg,
			suid:      r.GetSuid(),
			shardKeys: snapi.ParseShardKeys(items[i].ID, r.ShardingSubRangeCount()),
		}
		if !me.isProtected(protectDuration) {
			ret = append(ret, me)
			continue
		}
		r.messages = append(r.messages, me)
	}

	// set next timestamp to start list
	r.nextMarkerTs = convertMarkerTimeUnix(nextMarker)

	if len(items) < 1 || len(nextMarker) < 1 {
		// no more message in shard storage, new message after now should be protected
		r.setProtected()
	}
	return ret, nil
}

func (r *shardListReader) init() {
	r.messages = r.messages[:0]
	r.nextMarkerTs = 0
}

func (r *shardListReader) setProtected() {
	r.nextMarkerTs = time.Now().Unix()
}

func (r *shardListReader) isProtected(protectDuration time.Duration) bool {
	return time.Now().Before(time.Unix(r.nextMarkerTs, 0).Add(protectDuration))
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

func convertTimeUnixToMarker(ts int64) []byte {
	buf := make([]byte, 1+8)
	copy(buf, snproto.DeleteMsgPrefix)
	binary.BigEndian.PutUint64(buf[1:], uint64(ts<<32))
	return buf
}

func convertMarkerTimeUnix(marker []byte) int64 {
	if len(marker) < 1 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(marker[1:9]) >> 32)
}

type delMsgExt struct {
	msg snproto.DeleteMsg

	// use to delete and update msg in kvstore
	suid      proto.Suid
	msgKey    []byte
	shardKeys [][]byte
}

func (ext *delMsgExt) isProtected(protectDuration time.Duration) bool {
	ts := time.Unix(ext.msg.Time, 0)
	return time.Now().Before(ts.Add(protectDuration))
}
