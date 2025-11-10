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
	"time"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	defaultMaxListMessageNum = 64
	defaultTaskPoolSize      = 64
	defaultMsgChannelSize    = 1 << 10
)

// TierConfig manages configuration for message processing in different tiers
type TierConfig struct {
	messageType  snproto.MessageType   `json:"-"`
	EnabledTiers []snproto.MessageTier `json:"enabled_tiers"`

	// SafeMessageTimeout is the protect duration for non-punish tiers
	SafeMessageTimeout util.Duration `json:"safe_message_timeout"`
	// PunishTimeout is the protect duration for punish tiers
	PunishTimeout util.Duration `json:"punish_timeout"`

	// concurrency config for each tier
	TierSingleIdxMaxListMessageNum int `json:"tier_single_idx_max_list_message_num"`
	TierSingleIdxTaskPoolSize      int `json:"tier_single_idx_task_pool_size"`
	TierSingleIdxMsgChannelSize    int `json:"tier_single_idx_msg_channel_size"`
	TierMultiIdxMaxListMessageNum  int `json:"tier_multi_idx_max_list_message_num"`
	TierMultiIdxTaskPoolSize       int `json:"tier_multi_idx_task_pool_size"`
	TierMultiIdxMsgChannelSize     int `json:"tier_multi_idx_msg_channel_size"`
	TierPunishMaxListMessageNum    int `json:"tier_punish_max_list_message_num"`
	TierPunishTaskPoolSize         int `json:"tier_punish_task_pool_size"`
	TierPunishMsgChannelSize       int `json:"tier_punish_msg_channel_size"`

	tierArgs map[snproto.MessageTier]tierArgs `json:"-"`
}

// tierArgs contains arguments of a tier for listing messages
type tierArgs struct {
	protectDuration time.Duration // punish message ---> cfg.PunishTimeout; other message ---> cfg.SafeMessageTimeout
	count           int
	prefix          []byte
	taskPoolSize    int
	channelSize     int
}

// build builds the internal runtime structures from configuration
func (tc *TierConfig) build(messageType snproto.MessageType) {
	defaulter.LessOrEqual(&tc.TierSingleIdxMaxListMessageNum, defaultMaxListMessageNum)
	defaulter.LessOrEqual(&tc.TierSingleIdxTaskPoolSize, defaultTaskPoolSize)
	defaulter.LessOrEqual(&tc.TierSingleIdxMsgChannelSize, defaultMsgChannelSize)
	defaulter.LessOrEqual(&tc.TierMultiIdxMaxListMessageNum, defaultMaxListMessageNum)
	defaulter.LessOrEqual(&tc.TierMultiIdxTaskPoolSize, defaultTaskPoolSize)
	defaulter.LessOrEqual(&tc.TierMultiIdxMsgChannelSize, defaultMsgChannelSize)
	defaulter.LessOrEqual(&tc.TierPunishMaxListMessageNum, defaultMaxListMessageNum)
	defaulter.LessOrEqual(&tc.TierPunishTaskPoolSize, defaultTaskPoolSize)
	defaulter.LessOrEqual(&tc.TierPunishMsgChannelSize, defaultMsgChannelSize)

	tc.messageType = messageType
	tc.tierArgs = make(map[snproto.MessageTier]tierArgs)
	for _, t := range tc.EnabledTiers {
		switch t {
		case snproto.TierSingleIdx:
			tc.tierArgs[t] = tierArgs{
				protectDuration: tc.SafeMessageTimeout.Duration,
				count:           tc.TierSingleIdxMaxListMessageNum,
				prefix:          getMessagePrefix(tc.messageType, t),
				taskPoolSize:    tc.TierSingleIdxTaskPoolSize,
				channelSize:     tc.TierSingleIdxMsgChannelSize,
			}
		case snproto.TierMultiIdx:
			tc.tierArgs[t] = tierArgs{
				protectDuration: tc.SafeMessageTimeout.Duration,
				count:           tc.TierMultiIdxMaxListMessageNum,
				prefix:          getMessagePrefix(tc.messageType, t),
				taskPoolSize:    tc.TierMultiIdxTaskPoolSize,
				channelSize:     tc.TierMultiIdxMsgChannelSize,
			}
		case snproto.TierPunish:
			tc.tierArgs[t] = tierArgs{
				protectDuration: tc.PunishTimeout.Duration,
				count:           tc.TierPunishMaxListMessageNum,
				prefix:          getMessagePrefix(tc.messageType, t),
				taskPoolSize:    tc.TierPunishTaskPoolSize,
				channelSize:     tc.TierPunishMsgChannelSize,
			}
		default:
			log.Warnf("unknown tier: %d, ignore", t)
		}
	}
}

// tierShardListReader manages multiple shardListReader instances for different tiers
type tierShardListReader struct {
	storage.ShardHandler

	// readers map stores shardListReader for each tier
	readers map[snproto.MessageTier]*shardListReader
}

func newTierShardListReader(handler storage.ShardHandler, cfg *TierConfig) *tierShardListReader {
	readers := make(map[snproto.MessageTier]*shardListReader)
	for t := range cfg.tierArgs {
		readers[t] = newShardListReader(handler)
	}
	return &tierShardListReader{
		ShardHandler: handler,
		readers:      readers,
	}
}

func (r *tierShardListReader) listMessageByTier(ctx context.Context, cfg *TierConfig) (map[snproto.MessageTier][]messageItem, error) {
	span := trace.SpanFromContextSafe(ctx)
	tierMap := make(map[snproto.MessageTier][]messageItem)
	suid := r.GetSuid()

	for t, reader := range r.readers {
		args, ok := cfg.tierArgs[t]
		if !ok {
			continue
		}

		msgs, err := reader.listMessage(ctx, args.prefix, args.protectDuration, args.count)
		if err != nil {
			span.Errorf("shard[%d] list message for tier[%d] failed, err: %s", suid, t, err)
			continue
		}

		span.Debugf("shard[%d] list messages by tier[%d]: %d", suid, t, len(msgs))
		tierMap[t] = msgs
	}

	return tierMap, nil
}

func (r *tierShardListReader) init() {
	for _, reader := range r.readers {
		reader.init()
	}
}

type messageItem struct {
	item snapi.Item
	time int64
	suid proto.Suid
}

func (i *messageItem) isProtected(protectDuration time.Duration) bool {
	return time.Now().Before(time.Unix(i.time, 0).Add(protectDuration))
}

type shardListReader struct {
	storage.ShardHandler

	messages          []messageItem
	nextMarker        []byte
	protectedTimeUnix int64
}

func newShardListReader(handler storage.ShardHandler) *shardListReader {
	return &shardListReader{
		ShardHandler: handler,
		messages:     make([]messageItem, 0),
	}
}

func (r *shardListReader) listMessage(ctx context.Context, prefix []byte, protectDuration time.Duration, count int) ([]messageItem, error) {
	if len(r.messages) > 0 {
		return r.listFromCache(protectDuration)
	}
	return r.listFromStorage(ctx, prefix, protectDuration, count)
}

func (r *shardListReader) listFromCache(protectDuration time.Duration) ([]messageItem, error) {
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

func (r *shardListReader) listFromStorage(ctx context.Context, prefix []byte, protectDuration time.Duration, count int) ([]messageItem, error) {
	span := trace.SpanFromContextSafe(ctx)
	if r.isProtected(protectDuration) {
		return nil, nil
	}

	h := storage.OpHeader{
		RouteVersion: r.GetRouteVersion(),
	}

	items, nextMarker, err := r.ListItem(ctx, h, prefix, r.nextMarker, uint64(count))
	if err != nil {
		return nil, err
	}

	suid := r.GetSuid()
	rc := r.ShardingSubRangeCount()
	ret := make([]messageItem, 0, len(items))
	mk := newMsgKey()
	defer mk.release()

	for i := 0; i < len(items); i++ {
		key := []byte(items[i].ID)
		mk.setKey(key)
		err := mk.decode(rc)
		if err != nil {
			return nil, err
		}
		span.Debugf("shard[%d] load key: %+v", suid, []byte(items[i].ID))

		itm := messageItem{
			item: items[i],
			time: mk.ts.TimeUnix(),
			suid: suid,
		}

		// if msg is not protected, add to return list
		if !itm.isProtected(protectDuration) {
			ret = append(ret, itm)
			continue
		}

		// add to cache
		r.messages = append(r.messages, itm)
	}

	r.nextMarker = nextMarker
	if len(nextMarker) < 1 {
		// no more message in shard storage, new message after now should be protected
		r.setProtected(time.Now().Unix())
		// set nextMarker to last msgKey to avoid list from start
		if len(items) > 0 {
			r.nextMarker = []byte(items[len(items)-1].ID)
			span.Debugf("shard[%d] list to end, set last msgKey:[%+v] as nextMarker", suid, r.nextMarker)
		}
		return ret, nil
	}

	// if nextMarker's timeUnix if protected, set protected
	mk.setKey(r.nextMarker)
	err = mk.decode(rc)
	if err != nil {
		span.Errorf("decode nextMarker[%+v] failed, err: %v", r.nextMarker, err)
		return nil, err
	}
	nextMarkerTimeUnix := mk.ts.TimeUnix()
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
