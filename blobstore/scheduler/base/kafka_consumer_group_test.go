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

package base

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func newMockBroker(t *testing.T) *sarama.MockBroker {
	broker := sarama.NewMockBroker(t, 2)
	group := fmt.Sprintf("%s-%s", proto.ServiceNameScheduler, testTopic)

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(testTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(testTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(testTopic, 0, sarama.OffsetNewest, 1),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker),
		"HeartbeatRequest": sarama.NewMockHeartbeatResponse(t),
		"JoinGroupRequest": sarama.NewMockSequence(
			sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName),
		),
		"SyncGroupRequest": sarama.NewMockSequence(
			sarama.NewMockSyncGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockSyncGroupResponse(t).SetMemberAssignment(
				&sarama.ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						testTopic: {0},
					},
				}),
		),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset(
			group, testTopic, 0, 0, "", sarama.ErrNoError,
		).SetError(sarama.ErrNoError),
		"FetchRequest": sarama.NewMockSequence(
			sarama.NewMockFetchResponse(t, 1).
				SetMessage(testTopic, 0, 0, sarama.StringEncoder("foo")).
				SetMessage(testTopic, 0, 1, sarama.StringEncoder("bar")),
			sarama.NewMockFetchResponse(t, 1),
		),
	})
	return broker
}

func TestConsumer(t *testing.T) {
	{
		broker := newMockBroker(t)
		defer broker.Close()

		var wg sync.WaitGroup
		wg.Add(1)

		cli := NewKafkaConsumer([]string{broker.Addr()}, time.Second, newMockAccess(nil))
		consumer, err := cli.StartKafkaConsumer(proto.TaskTypeShardRepair, testTopic,
			func(msg *sarama.ConsumerMessage, consumerPause ConsumerPause) bool {
				wg.Done()
				return true
			})
		require.NoError(t, err)
		wg.Wait()
		consumer.Stop()
	}
	{
		broker := newMockBroker(t)
		defer broker.Close()

		var wg sync.WaitGroup
		wg.Add(1)

		cli := NewKafkaConsumer([]string{broker.Addr()}, time.Second, newMockAccess(nil))
		consumer, err := cli.StartKafkaConsumer(proto.TaskTypeBlobDelete, testTopic,
			func(msg *sarama.ConsumerMessage, consumerPause ConsumerPause) bool {
				wg.Done()
				return false
			})
		require.NoError(t, err)
		wg.Wait()
		consumer.Stop()
	}
}
