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
	"errors"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

//go:generate mockgen -destination=./utils_mock_test.go -package=base -mock_names IAllocVunit=MockAllocVunit github.com/cubefs/cubefs/blobstore/scheduler/base IAllocVunit

const testTopic = "test_topic"

var errMock = errors.New("mock error")

func init() {
	log.SetOutputLevel(log.Lfatal)
}

func newBroker(t *testing.T) *sarama.MockBroker {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	mockFetchResponse.SetVersion(1)
	var msg sarama.ByteEncoder = []byte("FOO")
	for i := 0; i < 1000; i++ {
		mockFetchResponse.SetMessage(testTopic, 0, int64(i), msg)
	}

	broker := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:0")
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(testTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(testTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(testTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})

	return broker
}

type mockAccess struct {
	offsets map[string]int64
	err     error
}

func newMockAccess(err error) *mockAccess {
	return &mockAccess{
		offsets: make(map[string]int64),
		err:     err,
	}
}

func (m *mockAccess) SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) error {
	key := fmt.Sprintf("%s_%d", topic, partition)
	m.offsets[key] = offset
	return m.err
}

func (m *mockAccess) GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (int64, error) {
	key := fmt.Sprintf("%s_%d", topic, partition)
	return m.offsets[key], m.err
}
