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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/proxy/mock"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func newProducer() Producer {
	ctr := gomock.NewController(&testing.T{})
	producer := mock.NewMockProducer(ctr)
	producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(topic string, msg []byte) (err error) {
		if topic == "priority" {
			return ErrSendMessage
		}
		return nil
	})

	producer.EXPECT().SendMessages(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(topic string, msgs [][]byte) (err error) {
		if len(msgs) == 2 {
			return ErrSendMessage
		}
		return nil
	})
	return producer
}

func NewBrokers(t *testing.T) (*sarama.MockBroker, *sarama.MockBroker) {
	kafka.DefaultKafkaVersion = sarama.V0_9_0_1

	seedBroker := sarama.NewMockBrokerAddr(t, 1, "127.0.0.1:0")
	leader := sarama.NewMockBrokerAddr(t, 2, "127.0.0.1:0")

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, 0)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, 0)
	for i := 0; i < 10; i++ {
		leader.Returns(prodSuccess)
	}

	return seedBroker, leader
}
