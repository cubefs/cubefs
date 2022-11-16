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

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

var testTopic = "test"

func TestSendMessage(t *testing.T) {
	DefaultKafkaVersion = sarama.V0_9_0_0
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(testTopic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	seedBroker.Returns(metadataResponse)

	for i := 0; i < 3; i++ {
		prodSuccess := new(sarama.ProduceResponse)
		prodSuccess.AddTopicPartition(testTopic, 0, sarama.ErrNoError)
		leader.Returns(prodSuccess)
	}

	cli, err := NewProducer(&ProducerCfg{BrokerList: []string{seedBroker.Addr()}, Topic: testTopic})
	require.NoError(t, err)

	msg := "After all, tomorrow is another day!"
	err = cli.SendMessage(testTopic, []byte(msg))
	require.NoError(t, err)

	err = cli.SendMessages(testTopic, [][]byte{[]byte(msg), []byte(msg)})
	require.NoError(t, err)
}
