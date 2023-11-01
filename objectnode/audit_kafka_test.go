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
	"encoding/json"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/require"
)

const (
	auditKafkaUnitTestBroker = "127.0.0.1:9095"
	auditKafkaUnitTestTopic  = "audit-kafka-unit-test"
)

func TestNewKafkaAudit(t *testing.T) {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	broker := sarama.NewMockBrokerAddr(t, 0, auditKafkaUnitTestBroker)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(auditKafkaUnitTestTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(auditKafkaUnitTestTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(auditKafkaUnitTestTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	defer broker.Close()

	conf := KafkaAuditConfig{IsTest: true}
	_, err := NewKafkaAudit("cubefs", conf)
	require.Error(t, err)

	conf.Brokers = broker.Addr()
	_, err = NewKafkaAudit("cubefs", conf)
	require.Error(t, err)

	conf.Topic = auditKafkaUnitTestTopic
	conf.Version = "invalid version"
	_, err = NewKafkaAudit("cubefs", conf)
	require.Error(t, err)

	conf.Version = sarama.V2_1_0_0.String()
	audit, err := NewKafkaAudit("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, audit.Close())
}

func TestKafkaAudit(t *testing.T) {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	broker := sarama.NewMockBrokerAddr(t, 0, auditKafkaUnitTestBroker)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(auditKafkaUnitTestTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(auditKafkaUnitTestTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(auditKafkaUnitTestTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	defer broker.Close()

	conf := KafkaAuditConfig{IsTest: true}
	conf.Topic = auditKafkaUnitTestTopic
	conf.Brokers = broker.Addr()
	audit, err := NewKafkaAudit("cubefs", conf)
	require.NoError(t, err)
	require.Equal(t, "kafka-audit-cubefs", audit.Name())

	mockProducer := mocks.NewSyncProducer(t, nil)
	audit.producer = mockProducer

	// send test
	mockProducer.ExpectSendMessageAndSucceed()
	require.NoError(t, audit.sendTest())

	// send data
	entry := &AuditEntry{}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	mockProducer.ExpectSendMessageAndSucceed()
	require.NoError(t, audit.Send(data))

	// audit is closed
	require.NoError(t, audit.Close())
}
