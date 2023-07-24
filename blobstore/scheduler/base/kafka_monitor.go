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

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

// KafkaTopicMonitor kafka monitor
type KafkaTopicMonitor struct {
	closer.Closer
	taskType proto.TaskType
	topic    string
	monitor  *kafka.Monitor
}

// NewKafkaTopicMonitor returns kafka topic monitor
func NewKafkaTopicMonitor(taskType proto.TaskType, clusterID proto.ClusterID, cfg *KafkaConfig) (*KafkaTopicMonitor, error) {
	consumer, err := sarama.NewConsumer(cfg.BrokerList, defaultKafkaCfg())
	if err != nil {
		return nil, err
	}

	partitions, err := consumer.Partitions(cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("get partitions: err[%w]", err)
	}

	// create kafka monitor
	monitor, err := kafka.NewKafkaMonitor(clusterID, proto.ServiceNameScheduler, cfg.BrokerList, cfg.Topic, partitions, kafka.DefaultIntervalSecs)
	if err != nil {
		return nil, fmt.Errorf("new kafka monitor: broker list[%v], topic[%v], parts[%v], error[%w]", cfg.BrokerList, cfg.Topic, partitions, err)
	}

	return &KafkaTopicMonitor{
		Closer:   closer.New(),
		taskType: taskType,
		topic:    cfg.Topic,
		monitor:  monitor,
	}, nil
}

func (m *KafkaTopicMonitor) Close() {
	m.Closer.Close()
	m.monitor.Close()
}

func defaultKafkaCfg() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = kafka.DefaultKafkaVersion
	cfg.Consumer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionSnappy
	return cfg
}
