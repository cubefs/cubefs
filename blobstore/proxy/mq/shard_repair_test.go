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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
)

func TestShardRepairMgr_sendShardRepairMsg(t *testing.T) {
	mockProducer := newProducer(t)

	mgr := &shardRepairMgr{
		priorityTopic:        "priority",
		topic:                "normal",
		topicSelector:        defaultTopicSelector,
		shardRepairMsgSender: mockProducer,
	}
	testCases := []struct {
		args *proxy.ShardRepairArgs
		err  error
	}{
		{
			args: &proxy.ShardRepairArgs{
				ClusterID: 0,
				Bid:       1000,
				Vid:       1,
				BadIdxes:  []uint8{1},
				Reason:    "",
			},
			err: nil,
		},
		{
			args: &proxy.ShardRepairArgs{
				ClusterID: 0,
				Bid:       1000,
				Vid:       100,
				BadIdxes:  []uint8{1, 2, 3, 4},
				Reason:    "",
			},
			err: ErrSendMessage,
		},
	}
	for _, tc := range testCases {
		err := mgr.SendShardRepairMsg(context.Background(), tc.args)
		require.Equal(t, true, errors.Is(err, tc.err))
	}
}

func TestNewShardRepairMgr(t *testing.T) {
	_, err := NewShardRepairMgr(ShardRepairConfig{
		Topic:        "",
		MsgSenderCfg: kafka.ProducerCfg{},
	})
	require.Error(t, err)

	seedBroker, leader := NewBrokers(t)

	mgr, err := NewShardRepairMgr(ShardRepairConfig{
		Topic:         "my_topic",
		PriorityTopic: "my_topic",
		MsgSenderCfg:  kafka.ProducerCfg{BrokerList: []string{seedBroker.Addr()}},
	})
	require.NoError(t, err)

	info := &proxy.ShardRepairArgs{
		ClusterID: 0,
		Bid:       1000,
		Vid:       1,
		BadIdxes:  []uint8{1, 2},
		Reason:    "",
	}

	for i := 0; i < 10; i++ {
		err := mgr.SendShardRepairMsg(context.Background(), info)
		require.NoError(t, err)
	}

	leader.Close()
	seedBroker.Close()
}
