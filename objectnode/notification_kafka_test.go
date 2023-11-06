// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/require"
)

const (
	kafkaNotifierUnitTestBroker = "127.0.0.1:9095"
	kafkaNotifierUnitTestTopic  = "kafka-notifier-unit-test"
)

func TestNewKafkaNotifier(t *testing.T) {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	broker := sarama.NewMockBrokerAddr(t, 0, kafkaNotifierUnitTestBroker)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(kafkaNotifierUnitTestTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(kafkaNotifierUnitTestTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(kafkaNotifierUnitTestTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	defer broker.Close()

	config := KafkaNotifierConfig{IsTest: true}
	_, err := NewKafkaNotifier("cubefs", config)
	require.Error(t, err)

	config.Brokers = broker.Addr()
	_, err = NewKafkaNotifier("cubefs", config)
	require.Error(t, err)

	config.Topic = kafkaNotifierUnitTestTopic
	config.Version = "invalid version"
	_, err = NewKafkaNotifier("cubefs", config)
	require.Error(t, err)

	config.Version = sarama.V2_1_0_0.String()
	notifier, err := NewKafkaNotifier("cubefs", config)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())
}

func TestKafkaNotifier(t *testing.T) {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	broker := sarama.NewMockBrokerAddr(t, 0, kafkaNotifierUnitTestBroker)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(kafkaNotifierUnitTestTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(kafkaNotifierUnitTestTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(kafkaNotifierUnitTestTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	defer broker.Close()

	mockProducer := mocks.NewSyncProducer(t, nil)
	config := KafkaNotifierConfig{IsTest: true}
	config.Topic = kafkaNotifierUnitTestTopic
	config.Brokers = broker.Addr()
	notifier, err := NewKafkaNotifier("cubefs", config)
	require.NoError(t, err)
	notifier.producer = mockProducer

	require.Equal(t, "cubefs:kafka", notifier.Name())
	require.Equal(t, NotifierID{ID: "cubefs", Name: "kafka"}, notifier.ID())

	mockProducer.ExpectSendMessageAndSucceed()
	require.NoError(t, notifier.sendTest())
	require.NoError(t, notifier.Close())
}
