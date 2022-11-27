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
	"net/http"
	"time"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const minConsumeWaitTime = time.Millisecond * 500

// IConsumer define the interface of consumer for message consume
type IConsumer interface {
	ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage)
	CommitOffset(ctx context.Context) error
}

// IConsumerOffset records consume offset
type IConsumerOffset interface {
	GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (offset int64, err error)
	SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) (err error)
}

// KafkaConfig kafka config
type KafkaConfig struct {
	Topic      string
	Partitions []int32
	BrokerList []string
}

// ConsumeInfo consume info
type ConsumeInfo struct {
	Offset int64
	Commit int64
}

// TopicConsumer rotate consume msg among partition consumers
type TopicConsumer struct {
	partitionsConsumers []IConsumer

	curIdx int
}

// NewTopicConsumer returns topic round-robin partition consumer
func NewTopicConsumer(taskType proto.TaskType, cfg *KafkaConfig, offsetAccessor IConsumerOffset) (IConsumer, error) {
	consumers, err := NewKafkaPartitionConsumers(taskType, cfg, offsetAccessor)
	if err != nil {
		return nil, err
	}
	topicConsumer := &TopicConsumer{
		partitionsConsumers: consumers,
	}
	return topicConsumer, err
}

// ConsumeMessages consumer messages
func (c *TopicConsumer) ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
	msgs = c.partitionsConsumers[c.curIdx].ConsumeMessages(ctx, msgCnt)
	c.curIdx = (c.curIdx + 1) % len(c.partitionsConsumers)
	return
}

// CommitOffset commit offset
func (c *TopicConsumer) CommitOffset(ctx context.Context) error {
	for _, pc := range c.partitionsConsumers {
		if err := pc.CommitOffset(ctx); err != nil {
			return err
		}
	}
	return nil
}

// PartitionConsumer partition consumer
type PartitionConsumer struct {
	taskType       proto.TaskType
	topic          string
	partition      int32
	consumer       sarama.PartitionConsumer
	consumeInfo    ConsumeInfo
	offsetAccessor IConsumerOffset // consume offset persistence
}

// NewKafkaPartitionConsumers returns kafka partition consumers
func NewKafkaPartitionConsumers(taskType proto.TaskType, cfg *KafkaConfig, offsetAccessor IConsumerOffset) ([]IConsumer, error) {
	var consumers []IConsumer
	consumer, err := sarama.NewConsumer(cfg.BrokerList, defaultKafkaCfg())
	if err != nil {
		return nil, fmt.Errorf("new consumer: err[%w]", err)
	}
	if len(cfg.Partitions) == 0 {
		partitions, err := consumer.Partitions(cfg.Topic)
		if err != nil {
			return nil, err
		}
		cfg.Partitions = partitions
	}
	for _, partition := range cfg.Partitions {
		partitionConsumer, err := newKafkaPartitionConsumer(taskType, consumer, cfg.Topic, partition, offsetAccessor)
		if err != nil {
			return nil, fmt.Errorf("new kafka partition consumer: err[%w]", err)
		}
		consumers = append(consumers, partitionConsumer)
	}

	return consumers, nil
}

func newKafkaPartitionConsumer(taskType proto.TaskType, consumer sarama.Consumer, topic string, partition int32, offsetAccessor IConsumerOffset) (*PartitionConsumer, error) {
	kafkaConsumer := PartitionConsumer{
		taskType:       taskType,
		topic:          topic,
		offsetAccessor: offsetAccessor,
	}

	partConsumeInfo, err := kafkaConsumer.loadConsumeInfo(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("loadConsumeInfo: topic[%s], err[%w]", topic, err)
	}

	pc, err := consumer.ConsumePartition(topic, partition, partConsumeInfo.Commit)
	if err != nil {
		return nil, fmt.Errorf("consume partition: topic[%s], partition[%d], partConsumeInfo[%+v], err[%w]", topic, partition, partConsumeInfo, err)
	}

	kafkaConsumer.partition = partition
	kafkaConsumer.consumer = pc
	kafkaConsumer.consumeInfo = partConsumeInfo

	return &kafkaConsumer, nil
}

// ConsumeMessages consume messages
func (c *PartitionConsumer) ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
	span := trace.SpanFromContextSafe(ctx)

	d := time.Millisecond / 2 * time.Duration(msgCnt) // assume each message cost 0.5 ms
	if d < minConsumeWaitTime {
		d = minConsumeWaitTime
	}

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	start := time.Now()
	for {
		var err error
		var msg *sarama.ConsumerMessage
		select {
		case msg = <-c.consumer.Messages():
		case err = <-c.consumer.Errors():
		case <-ticker.C:
		}
		if err != nil {
			span.Errorf("acquire msg failed: topic[%s], partition[%d], err[%+v]", c.topic, c.partition, err)
			break
		}

		if msg == nil {
			span.Debugf("no message for consume and return")
			break // consume finish,return
		}

		c.consumeInfo.Offset = msg.Offset
		msgs = append(msgs, msg)
		if len(msgs) >= msgCnt {
			break
		}
	}

	span.Debugf("consume info: topic[%s], partition[%d], time cost[%+v], consumer msg numbers[%d], offset[%d], batch msg cnt[%d]",
		c.topic, c.partition, time.Since(start), len(msgs), c.consumeInfo.Offset, msgCnt)
	return
}

// CommitOffset commit offset
func (c *PartitionConsumer) CommitOffset(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	offset := c.consumeInfo.Offset
	span.Debugf("start commit offset: offset[%d], topic[%s], partition[%d]", offset, c.topic, c.partition)
	err := c.offsetAccessor.SetConsumeOffset(c.taskType, c.topic, c.partition, offset)
	if err != nil {
		span.Errorf("commit offset failed: [%+v]", err)
		return err
	}
	c.consumeInfo.Commit = offset
	return nil
}

func (c *PartitionConsumer) loadConsumeInfo(topic string, pt int32) (consumeInfo ConsumeInfo, err error) {
	commitOffset, err := c.offsetAccessor.GetConsumeOffset(c.taskType, topic, pt)
	if err != nil {
		if rpc.DetectStatusCode(err) == http.StatusNotFound {
			return ConsumeInfo{Commit: sarama.OffsetOldest, Offset: sarama.OffsetOldest}, nil
		}
		return
	}

	return ConsumeInfo{Commit: commitOffset + 1, Offset: commitOffset}, err
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
