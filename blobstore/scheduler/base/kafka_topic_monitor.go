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
	"net/http"
	"time"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// KafkaTopicMonitor kafka monitor
type KafkaTopicMonitor struct {
	closer.Closer
	taskType       proto.TaskType
	topic          string
	partitions     []int32
	offsetAccessor IConsumerOffset
	monitor        *kafka.Monitor
	interval       time.Duration
}

// NewKafkaTopicMonitor returns kafka topic monitor
func NewKafkaTopicMonitor(taskType proto.TaskType, clusterID proto.ClusterID, cfg *KafkaConfig, offsetAccessor IConsumerOffset, monitorIntervalS int) (*KafkaTopicMonitor, error) {
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
		return nil, fmt.Errorf("new kafka monitor: broker list[%v], topic[%v], parts[%v], error[%w]",
			cfg.BrokerList, cfg.Topic, partitions, err)
	}

	interval := time.Second * time.Duration(monitorIntervalS)
	if interval <= 0 {
		interval = time.Millisecond
	}
	return &KafkaTopicMonitor{
		Closer:         closer.New(),
		taskType:       taskType,
		topic:          cfg.Topic,
		partitions:     partitions,
		offsetAccessor: offsetAccessor,
		monitor:        monitor,
		interval:       interval,
	}, nil
}

// Run run kafka monitor
func (m *KafkaTopicMonitor) Run() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, partition := range m.partitions {
				off, err := m.offsetAccessor.GetConsumeOffset(m.taskType, m.topic, partition)
				if err != nil {
					if rpc.DetectStatusCode(err) != http.StatusNotFound {
						log.Errorf("get consume offset failed: err[%v]", err)
					}
					continue
				}
				m.monitor.SetConsumeOffset(off, partition)
			}
		case <-m.Closer.Done():
			return
		}
	}
}

func (m *KafkaTopicMonitor) Close() {
	m.Closer.Close()
	m.monitor.Close()
}
