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
	"fmt"
	"sort"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type topicPriority struct {
	topic    string
	priority int // The larger the value, the higher the priority
}

// PriorityConsumerConfig priority consumer
type PriorityConsumerConfig struct {
	KafkaConfig
	Priority int `json:"priority"`
}

type priorityConsumer struct {
	topicConsumers      map[string]IConsumer
	sortedTopicPriority []topicPriority // Sort from largest to smallest
}

// NewPriorityConsumer return priority consumer
func NewPriorityConsumer(taskType proto.TaskType, cfgs []PriorityConsumerConfig, offsetAccessor IConsumerOffset) (IConsumer, error) {
	multiConsumer := priorityConsumer{}
	multiConsumer.topicConsumers = make(map[string]IConsumer, len(cfgs))
	multiConsumer.sortedTopicPriority = make([]topicPriority, 0)
	for _, cfg := range cfgs {
		cs, err := NewTopicConsumer(taskType, &cfg.KafkaConfig, offsetAccessor)
		if err != nil {
			return nil, fmt.Errorf("new topic consumer: cfg[%+v], err[%w]", cfg.KafkaConfig, err)
		}

		multiConsumer.topicConsumers[cfg.KafkaConfig.Topic] = cs
		multiConsumer.sortedTopicPriority = append(multiConsumer.sortedTopicPriority, topicPriority{
			topic: cfg.KafkaConfig.Topic, priority: cfg.Priority,
		})
	}

	sort.SliceStable(multiConsumer.sortedTopicPriority, func(i, j int) bool {
		return multiConsumer.sortedTopicPriority[i].priority > multiConsumer.sortedTopicPriority[j].priority
	})

	return &multiConsumer, nil
}

// ConsumeMessages consume messages
func (m *priorityConsumer) ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
	remainCnt := msgCnt
	for _, pair := range m.sortedTopicPriority {
		consumer := m.topicConsumers[pair.topic]
		ms := consumer.ConsumeMessages(ctx, remainCnt)
		msgs = append(msgs, ms...)
		if len(msgs) >= msgCnt {
			return msgs
		}
		remainCnt = msgCnt - len(msgs)
	}
	return
}

// CommitOffset commit offset
func (m *priorityConsumer) CommitOffset(ctx context.Context) error {
	for _, c := range m.topicConsumers {
		if err := c.CommitOffset(ctx); err != nil {
			return err
		}
	}
	return nil
}
