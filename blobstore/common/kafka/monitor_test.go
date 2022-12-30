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
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type MockKafkaClient struct{}

func (c *MockKafkaClient) RefreshController() (broker *sarama.Broker, err error) {
	return
}

func (c *MockKafkaClient) Broker(brokerID int32) (broker *sarama.Broker, err error) {
	return
}

func (c *MockKafkaClient) RefreshBrokers(addrs []string) error {
	return nil
}

func (c *MockKafkaClient) Config() *sarama.Config {
	return nil
}

func (c *MockKafkaClient) Controller() (*sarama.Broker, error) {
	return nil, nil
}

func (c *MockKafkaClient) Brokers() []*sarama.Broker {
	return nil
}

func (c *MockKafkaClient) Topics() ([]string, error) {
	return nil, nil
}

func (c *MockKafkaClient) Partitions(topic string) ([]int32, error) {
	return nil, nil
}

func (c *MockKafkaClient) WritablePartitions(topic string) ([]int32, error) {
	return nil, nil
}

func (c *MockKafkaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	return nil, nil
}

func (c *MockKafkaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (c *MockKafkaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (c *MockKafkaClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (c *MockKafkaClient) RefreshMetadata(topics ...string) error {
	return nil
}

func (c *MockKafkaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	if time == sarama.OffsetNewest {
		return 100, nil
	}
	return 1, nil
}

func (c *MockKafkaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	return nil, nil
}

func (c *MockKafkaClient) RefreshCoordinator(consumerGroup string) error {
	return nil
}

func (c *MockKafkaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	return nil, nil
}

func (c *MockKafkaClient) Close() error {
	return nil
}

func (c *MockKafkaClient) Closed() bool {
	return true
}

func TestSetConsumeOffset(t *testing.T) {
	brokens := []string{"127.0.01:9092"}
	mockTestKafkaClient = &MockKafkaClient{}
	monitor, _ := NewKafkaMonitor(proto.ClusterID(1), "", brokens, "Test_monitor", []int32{0, 1, 2}, 1)
	monitor.SetConsumeOffset(1, 1)
	err := monitor.update(0)
	require.NoError(t, err)
	err = monitor.update(1)
	require.NoError(t, err)
	err = monitor.update(2)
	require.NoError(t, err)
	monitor.report()
	time.Sleep(time.Second + 10*time.Microsecond)
	monitor.Close()
}
