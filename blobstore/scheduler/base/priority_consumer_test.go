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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPriorityConsumer(t *testing.T) {
	broker := newBroker(t)
	defer broker.Close()

	cfgs := []PriorityConsumerConfig{
		{
			KafkaConfig: KafkaConfig{
				Topic:      testTopic,
				BrokerList: []string{broker.Addr()},
				Partitions: []int32{0},
			},
			Priority: 1,
		},
	}

	mockAcc := newMockAccess(nil)
	priorityConsumer, err := NewPriorityConsumer(cfgs, mockAcc)
	require.NoError(t, err)

	// Then: messages starting from offset 0 are consumed.
	for i := 0; i < 10; i++ {
		msgs := priorityConsumer.ConsumeMessages(context.Background(), 100)
		if len(msgs) > 0 {
			require.Equal(t, "FOO", string(msgs[0].Value))
			err = priorityConsumer.CommitOffset(context.Background())
			require.NoError(t, err)
		}
	}

	mockAcc.err = errMock
	err = priorityConsumer.CommitOffset(context.Background())
	require.Error(t, err)

	// NewPriorityConsumer with empty BrokerList
	cfgs = []PriorityConsumerConfig{
		{
			KafkaConfig: KafkaConfig{
				Topic:      testTopic,
				BrokerList: nil,
				Partitions: []int32{0},
			},
			Priority: 1,
		},
	}
	_, err = NewPriorityConsumer(cfgs, mockAcc)
	require.Error(t, err)
}
