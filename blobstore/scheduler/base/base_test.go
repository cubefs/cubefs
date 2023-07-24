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
	"testing"

	"github.com/Shopify/sarama"

	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

//go:generate mockgen -destination=./utils_mock_test.go -package=base -mock_names IAllocVunit=MockAllocVunit github.com/cubefs/cubefs/blobstore/scheduler/base IAllocVunit

const testTopic = "test_topic"

func newBroker(t *testing.T) *sarama.MockBroker {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
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
