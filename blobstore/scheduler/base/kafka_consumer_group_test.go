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

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const maxMsgCnt = 100

func newMockBroker(t *testing.T) *sarama.MockBroker {
	broker := sarama.NewMockBroker(t, 2)
	group := fmt.Sprintf("%s-%s", proto.ServiceNameScheduler, testTopic)

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	msg := sarama.StringEncoder("foo")
	for i := 0; i < maxMsgCnt; i++ {
		mockFetchResponse.SetMessage(testTopic, 0, int64(i), msg)
	}

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(testTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(testTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(testTopic, 0, sarama.OffsetNewest, maxMsgCnt),
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
		"FetchRequest": mockFetchResponse,
	})
	return broker
}

func TestConsumer(t *testing.T) {
	batchNum := 2
	{
		broker := newMockBroker(t)
		defer broker.Close()

		var wg sync.WaitGroup
		wg.Add(batchNum)
		offset := int64(0)

		cli := NewKafkaConsumer([]string{broker.Addr()}, newMockAccess(nil))
		consumer, err := cli.StartKafkaConsumer(KafkaConsumerCfg{
			TaskType:     proto.TaskTypeShardRepair,
			Topic:        testTopic,
			MaxBatchSize: maxMsgCnt / batchNum,
			MaxWaitTimeS: 4,
		}, func(msg []*sarama.ConsumerMessage, consumerPause ConsumerPause) bool {
			require.NotEqual(t, 0, len(msg))
			require.Equal(t, offset, msg[0].Offset)
			t.Logf("task:%s, msgs len=%d, msgs offset start=%v", proto.TaskTypeShardRepair, len(msg), msg[0].Offset) // offset:0, 50
			offset += int64(maxMsgCnt / batchNum)
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
		wg.Add(batchNum)

		cli := NewKafkaConsumer([]string{broker.Addr()}, newMockAccess(nil))
		consumer, err := cli.StartKafkaConsumer(KafkaConsumerCfg{
			TaskType:     proto.TaskTypeBlobDelete,
			Topic:        testTopic,
			MaxBatchSize: 8,
			MaxWaitTimeS: 4,
		}, func(msg []*sarama.ConsumerMessage, consumerPause ConsumerPause) bool {
			require.NotEqual(t, 0, len(msg))
			require.Equal(t, int64(0), msg[0].Offset)
			t.Logf("task:%s, msgs len=%d, msgs offset start=%v", proto.TaskTypeBlobDelete, len(msg), msg[0].Offset) // offset:0
			wg.Done()
			return false
		})
		require.NoError(t, err)
		wg.Wait()
		consumer.Stop()
	}
}
