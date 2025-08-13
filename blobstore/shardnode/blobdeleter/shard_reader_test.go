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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockshardnode"
)

var (
	ctr = gomock.NewController
	any = gomock.Any()
)

func TestNewShardListReader(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	reader := newShardListReader(handler)

	require.NotNil(t, reader)
	require.Equal(t, handler, reader.ShardHandler)
	require.Empty(t, reader.messages)
	require.Equal(t, 0, len(reader.nextMarker))
}

func TestShardListReader_ListFromCache_EmptyCache(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	reader := newShardListReader(handler)

	result, err := reader.listFromCache(time.Hour)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestShardListReader_ListFromCache_AllProtected(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	reader := newShardListReader(handler)

	// add protected messages to cache, timestamp is now
	now := time.Now().Unix()
	msg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: now,
		},
	}
	reader.messages = append(reader.messages, msg)

	result, err := reader.listFromCache(time.Hour)

	require.NoError(t, err)
	require.Nil(t, result)
	require.Len(t, reader.messages, 1) // message still in cache
}

func TestShardListReader_ListFromCache_AllUnprotected(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	reader := newShardListReader(handler)

	// add unprotected messages to cache, timestamp is 2 hours ago
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	msg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: oldTime,
		},
	}
	reader.messages = append(reader.messages, msg)

	result, err := reader.listFromCache(time.Hour)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Empty(t, reader.messages) // cache must be cleared
}

func TestShardListReader_ListFromCache_PartialProtected(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	reader := newShardListReader(handler)

	// add unprotected messages to cache, timestamp is 2 hours ago
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	unprotectedMsg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: oldTime,
		},
	}

	// add protected message to cache, timestamp is now
	now := time.Now().Unix()
	protectedMsg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: now,
		},
	}

	reader.messages = append(reader.messages, unprotectedMsg, protectedMsg)

	result, err := reader.listFromCache(time.Hour)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, unprotectedMsg, result[0])
	require.Len(t, reader.messages, 1) // protected message still in cache
	require.Equal(t, protectedMsg, reader.messages[0])
}

func TestShardListReader_ListFromStorage_Protected(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	handler.EXPECT().GetSuid().Return(proto.Suid(123))
	reader := newShardListReader(handler)

	// set protected flag
	reader.setProtected(time.Now().Unix())

	result, err := reader.listFromStorage(context.Background(), time.Hour, 10)

	require.NoError(t, err)
	require.Nil(t, result)
}

func TestShardListReader_ListFromStorage_Success(t *testing.T) {
	g := base.NewTsGenerator(0)
	tagNum := 2
	ts := g.GenerateTs()
	id, _ := base.EncodeRawDelMsgKey(ts, proto.Vid(1), proto.BlobID(100), tagNum)
	// mock item
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	item := snapi.Item{
		ID: id,
		Fields: []snapi.Field{
			{
				ID: snproto.DeleteBlobMsgFieldID,
				Value: marshalDeleteMsg(snproto.DeleteMsg{
					Time: oldTime,
				}),
			},
		},
	}

	handler := mock.NewMockSpaceShardHandler(ctr(t))
	handler.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1))
	handler.EXPECT().GetSuid().Return(proto.Suid(123)).Times(2)
	handler.EXPECT().ListItem(any, any, any, any, any).Return([]snapi.Item{item}, nil, nil)

	reader := newShardListReader(handler)

	result, err := reader.listFromStorage(context.Background(), time.Hour, 10)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, oldTime, result[0].msg.Time)
	require.Equal(t, proto.Suid(123), result[0].suid)
	require.Equal(t, item.ID, result[0].msgKey)
	require.True(t, reader.isProtected(time.Hour))
}

func TestShardListReader_ListFromStorage_WithProtectedMessages(t *testing.T) {
	g := base.NewTsGenerator(0)
	tagNum := 2
	ts1 := g.GenerateTs()
	id1, _ := base.EncodeRawDelMsgKey(ts1, proto.Vid(1), proto.BlobID(100), tagNum)
	ts2 := g.GenerateTs()
	id2, _ := base.EncodeRawDelMsgKey(ts2, proto.Vid(1), proto.BlobID(101), tagNum)

	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	now := time.Now().Unix()

	unprotectedItem := snapi.Item{
		ID: id1,
		Fields: []snapi.Field{
			{
				ID: snproto.DeleteBlobMsgFieldID,
				Value: marshalDeleteMsg(snproto.DeleteMsg{
					Time: oldTime,
				}),
			},
		},
	}

	protectedItem := snapi.Item{
		ID: id2,
		Fields: []snapi.Field{
			{
				ID: snproto.DeleteBlobMsgFieldID,
				Value: marshalDeleteMsg(snproto.DeleteMsg{
					Time: now,
				}),
			},
		},
	}

	ts := base.NewTs(time.Now().Unix())
	protectedNextMarker, _ := base.EncodeRawDelMsgKey(ts, proto.Vid(1), proto.BlobID(102), tagNum)

	handler := mock.NewMockSpaceShardHandler(ctr(t))
	handler.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1))
	handler.EXPECT().GetSuid().Return(proto.Suid(123)).Times(2)
	handler.EXPECT().ListItem(any, any, any, any, any).Return([]snapi.Item{unprotectedItem, protectedItem}, protectedNextMarker, nil)
	handler.EXPECT().ShardingSubRangeCount().Return(tagNum)

	reader := newShardListReader(handler)
	result, err := reader.listFromStorage(context.Background(), time.Hour, 10)
	require.NoError(t, err)
	require.Len(t, result, 1) // only return unprotected msg
	require.Equal(t, oldTime, result[0].msg.Time)
	require.Len(t, reader.messages, 1) // protected msg is cached

	// next msg in storage is protected, list reader should be protected
	require.True(t, reader.isProtected(time.Hour))
}

func TestShardListReader_Init(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	reader := newShardListReader(handler)

	reader.messages = append(reader.messages, &delMsgExt{})
	reader.nextMarker = []byte("marker")
	reader.init()

	require.Empty(t, reader.messages)
	require.Equal(t, 0, len(reader.nextMarker))
}

func TestShardListReader_SetProtected(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	handler.EXPECT().GetSuid().Return(proto.Suid(123))
	reader := newShardListReader(handler)

	reader.setProtected(time.Now().Unix())
	now := time.Now().Unix()
	require.True(t, reader.protectedTimeUnix >= now-1 && reader.protectedTimeUnix <= now+1)
}

func TestShardListReader_IsProtected(t *testing.T) {
	handler := mock.NewMockSpaceShardHandler(ctr(t))
	handler.EXPECT().GetSuid().Return(proto.Suid(123)).AnyTimes()
	reader := newShardListReader(handler)

	vid := proto.Vid(1)
	bid := proto.BlobID(100)
	tagNum := 2

	// nextMarker > 0, and is unprotected
	ts := base.NewTs(time.Now().Add(-2 * time.Hour).Unix())
	key, _ := base.EncodeRawDelMsgKey(ts, vid, bid, tagNum)
	_ts, _, _, _, err := base.DecodeDelMsgKey(key, tagNum)
	require.NoError(t, err)

	reader.setProtected(_ts.TimeUnix())
	require.False(t, reader.isProtected(time.Hour))

	// nextMarker > 0, and is protected
	ts = base.NewTs(time.Now().Unix())
	key, _ = base.EncodeRawDelMsgKey(ts, vid, bid, tagNum)
	_ts, _, _, _, err = base.DecodeDelMsgKey(key, tagNum)
	require.NoError(t, err)

	reader.setProtected(_ts.TimeUnix())
	require.True(t, reader.isProtected(time.Hour))

	// nextMarker == 0 (storage list to end), reader is set protected, but passed
	reader.protectedTimeUnix = time.Now().Add(-time.Hour).Unix()
	require.False(t, reader.isProtected(time.Hour))

	// nextMarker == 0 (storage list to end), reader is set protected), but passed
	reader.setProtected(time.Now().Unix())
	require.True(t, reader.isProtected(time.Hour))
}

func TestDelMsgExt_IsProtected(t *testing.T) {
	// test msg unprotected
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	unprotectedMsg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: oldTime,
		},
	}
	require.False(t, unprotectedMsg.isProtected(time.Hour))

	// test msg protected
	now := time.Now().Unix()
	protectedMsg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: now,
		},
	}
	require.True(t, protectedMsg.isProtected(time.Hour))
}

func TestItemToDelMsg(t *testing.T) {
	// test valid item
	msg := snproto.DeleteMsg{
		Time: time.Now().Unix(),
	}
	item := snapi.Item{
		ID: []byte("test_key"),
		Fields: []snapi.Field{
			{
				ID:    snproto.DeleteBlobMsgFieldID,
				Value: marshalDeleteMsg(msg),
			},
		},
	}

	result, err := itemToDelMsg(item)
	require.NoError(t, err)
	require.Equal(t, msg.Time, result.Time)

	// test invalid item
	invalidItem := snapi.Item{
		ID: []byte("invalid_key"),
		Fields: []snapi.Field{
			{
				ID:    snproto.DeleteBlobMsgFieldID,
				Value: []byte("invalid_data"),
			},
		},
	}

	_, err = itemToDelMsg(invalidItem)
	require.Error(t, err)
}

// marshal DeleteMsg to bytes
func marshalDeleteMsg(msg snproto.DeleteMsg) []byte {
	data, _ := msg.Marshal()
	return data
}
