// Copyright 2022 The CubeFS Authors.
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

package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// ShardRepairHandler stream http handler
type ShardRepairHandler interface {
	SendShardRepairMsg(ctx context.Context, info *proxy.ShardRepairArgs) error
}

// ShardRepairConfig is shard repair config
type ShardRepairConfig struct {
	Topic         string            `json:"topic"`
	PriorityTopic string            `json:"priority_topic"`
	MsgSenderCfg  kafka.ProducerCfg `json:"msg_sender_cfg"`
}

// NewShardRepairMgr returns shard repair manager
func NewShardRepairMgr(cfg ShardRepairConfig) (*ShardRepairMgr, error) {
	shardRepairMsgSender, err := kafka.NewProducer(&cfg.MsgSenderCfg)
	if err != nil {
		return nil, err
	}

	shardRepairMgr := ShardRepairMgr{
		topic:                cfg.Topic,
		priorityTopic:        cfg.PriorityTopic,
		topicSelector:        defaultTopicSelector,
		shardRepairMsgSender: shardRepairMsgSender,
	}

	return &shardRepairMgr, nil
}

// ShardRepairMgr is shard repair manager
type ShardRepairMgr struct {
	priorityTopic        string
	topic                string
	topicSelector        func(info *proxy.ShardRepairArgs, topic, priorityTopic string) string
	shardRepairMsgSender kafka.MsgProducer
}

// SendShardRepairMsg sends shard repair msg to mq
func (s *ShardRepairMgr) SendShardRepairMsg(ctx context.Context, info *proxy.ShardRepairArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	topic := s.topicSelector(info, s.topic, s.priorityTopic)
	msg := proto.ShardRepairMsg{
		ClusterID: info.ClusterID,
		Bid:       info.Bid,
		Vid:       info.Vid,
		BadIdx:    info.BadIdxes,
		Reason:    info.Reason,
		ReqId:     span.TraceID(),
	}

	msgByte, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshl message: msg[%+v], err:[%w]", msg, err)
	}

	now := time.Now()
	err = s.shardRepairMsgSender.SendMessage(topic, msgByte)
	if err != nil {
		return fmt.Errorf("send repair message: topic[%s], msg[%+v], err:[%w]", topic, msg, err)
	}

	span.Debugf("send shard repair message success: topic[%s], msg[%+v], spend[%+v(100ns)]", topic, msg, int64(time.Since(now)/100))
	return nil
}

func defaultTopicSelector(info *proxy.ShardRepairArgs, topic, priorityTopic string) string {
	if len(info.BadIdxes) > 1 {
		return priorityTopic
	}
	return topic
}
