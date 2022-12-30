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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

type IConsumerOffset interface {
	GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (offset int64, err error)
	SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) (err error)
}

// KafkaConfig kafka config
type KafkaConfig struct {
	Topic      string
	BrokerList []string
}

type PartitionOffset struct {
	Topic     string
	Partition int32

	lock            sync.RWMutex
	consumedOffset  int64
	committedOffset int64
}

func (po *PartitionOffset) ConsumedOffset() int64 {
	po.lock.RLock()
	offset := po.consumedOffset
	po.lock.RUnlock()
	return offset
}

func (po *PartitionOffset) MarkConsume(offset int64) {
	po.lock.Lock()
	po.consumedOffset = offset
	po.lock.Unlock()
}

func (po *PartitionOffset) MarkCommit(offset int64) {
	po.lock.Lock()
	po.committedOffset = offset
	po.lock.Unlock()
}

func (po *PartitionOffset) NeedCommit() (ok bool) {
	po.lock.RLock()
	ok = po.consumedOffset != po.committedOffset
	po.lock.RUnlock()
	return
}

type ConsumerOffsetManager struct {
	offsets map[int32]*PartitionOffset
	locker  sync.RWMutex
}

func newConsumerOffsetManager() *ConsumerOffsetManager {
	return &ConsumerOffsetManager{
		offsets: make(map[int32]*PartitionOffset),
	}
}

func (c *ConsumerOffsetManager) List() []*PartitionOffset {
	c.locker.RLock()
	pOffsets := make([]*PartitionOffset, 0)
	for _, offset := range c.offsets {
		pOffsets = append(pOffsets, offset)
	}
	c.locker.RUnlock()
	return pOffsets
}

func (c *ConsumerOffsetManager) MarkConsumerOffset(partition int32, offset int64) {
	c.locker.Lock()
	c.offsets[partition].MarkConsume(offset)
	c.locker.Unlock()
}

func (c *ConsumerOffsetManager) AddPartition(pOffset *PartitionOffset) {
	c.locker.Lock()
	c.offsets[pOffset.Partition] = pOffset
	c.locker.Unlock()
}

type ConsumerPause interface {
	Done() <-chan struct{}
}

type Consumer struct {
	taskType proto.TaskType
	topic    string

	ConsumeFn      func(msg *sarama.ConsumerMessage, consumerPause ConsumerPause) bool
	offsetGetter   IConsumerOffset
	offsetManager  *ConsumerOffsetManager
	commitInterval time.Duration
	closer.Closer
	consumerPause closer.Closer
}

func newConsumer(taskType proto.TaskType, commitInterval time.Duration, offsetGetter IConsumerOffset, topic string,
	consumerFn func(msg *sarama.ConsumerMessage, consumerPause ConsumerPause) bool) *Consumer {
	consumer := &Consumer{
		taskType:       taskType,
		topic:          topic,
		ConsumeFn:      consumerFn,
		offsetGetter:   offsetGetter,
		commitInterval: commitInterval,
		Closer:         closer.New(),
	}
	go consumer.run()
	return consumer
}

func (consumer *Consumer) run() {
	t := time.NewTicker(consumer.commitInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			consumer.Commit()
		case <-consumer.Done():
			return
		}
	}
}

func (consumer *Consumer) Commit() {
	if consumer.offsetManager == nil {
		return
	}
	for _, pOffset := range consumer.offsetManager.List() {
		if pOffset.NeedCommit() {
			offset := pOffset.ConsumedOffset()
			InsistOn(context.Background(), "set consumer offset failed", func() error {
				return consumer.offsetGetter.SetConsumeOffset(consumer.taskType, pOffset.Topic, pOffset.Partition,
					offset)
			})
			pOffset.MarkCommit(offset)
		}
	}
}

func (consumer *Consumer) markMessage(message *sarama.ConsumerMessage) {
	consumer.offsetManager.MarkConsumerOffset(message.Partition, message.Offset)
}

func (consumer *Consumer) addPartitionOffset(partition int32, offset int64) {
	consumer.offsetManager.AddPartition(&PartitionOffset{
		Topic:           consumer.topic,
		Partition:       partition,
		committedOffset: offset,
		consumedOffset:  offset,
	})
}

func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	span := trace.SpanFromContextSafe(session.Context())
	if len(session.Claims()) != 1 {
		span.Errorf("un support multiple topics: size[%+d]", len(session.Claims()))
		return errors.New("un support multiple topics")
	}
	partitions, ok := session.Claims()[consumer.topic]
	if !ok {
		span.Errorf("not expect topic: expect[%s]", consumer.topic)
		return errors.New("topic not exist")
	}
	consumer.offsetManager = newConsumerOffsetManager()
	for _, partition := range partitions {
		var (
			offset int64
			exist  = true
		)
		InsistOn(context.Background(), "get consumer offset failed", func() error {
			var err error
			offset, err = consumer.offsetGetter.GetConsumeOffset(consumer.taskType, consumer.topic, partition)
			if rpc.DetectStatusCode(err) == http.StatusNotFound {
				exist = false
				err = nil
			}
			return err
		})
		if exist {
			session.MarkOffset(consumer.topic, partition, offset+1, "")
		}
		consumer.addPartitionOffset(partition, offset)
	}
	consumer.consumerPause = closer.New()
	span.Infof("setup consumer: [%+v]", session.Claims())
	return nil
}

func (consumer *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	span := trace.SpanFromContextSafe(session.Context())
	consumer.Commit()
	consumer.offsetManager = nil
	span.Infof("cleanup consumer: [%+v]", session.Claims())
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	span := trace.SpanFromContextSafe(session.Context())

	// should close consuming process when `session.Context()` is done
	go func() {
		<-session.Context().Done()
		consumer.consumerPause.Close()
	}()

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				span.Warnf("no message for consume and continue")
				continue
			}
			// when session is done the message may not consumed
			if success := consumer.ConsumeFn(message, consumer.consumerPause); !success {
				span.Warnf("message not consume and return: topic[%s], partition[%d], offset[%d]",
					message.Topic, message.Partition, message.Offset)
				return nil
			}
			session.MarkMessage(message, "")
			consumer.markMessage(message)
		case <-session.Context().Done():
			return nil
		}
	}
}

type KafkaConsumer interface {
	StartKafkaConsumer(taskType proto.TaskType, topic string, fn func(msg *sarama.ConsumerMessage, consumerPause ConsumerPause) bool) (GroupConsumer, error)
}

type kafkaClient struct {
	brokers        []string
	commitInterval time.Duration
	offsetGetter   IConsumerOffset
}

type GroupConsumer interface {
	Stop()
}

type KafkaConsumerGroup struct {
	group    string
	client   sarama.ConsumerGroup
	span     trace.Span
	consumer *Consumer
}

func (cg *KafkaConsumerGroup) Stop() {
	cg.consumer.Close()
	if err := cg.client.Close(); err != nil {
		cg.span.Errorf("stop kafka consumer failed: err[%+v]", err)
		return
	}
	cg.span.Infof("stop kafka consumer: group[%s]", cg.group)
}

func NewKafkaConsumer(brokers []string, commitInterval time.Duration, offsetGetter IConsumerOffset) KafkaConsumer {
	return &kafkaClient{
		brokers:        brokers,
		commitInterval: commitInterval,
		offsetGetter:   offsetGetter,
	}
}

func (cli *kafkaClient) StartKafkaConsumer(taskType proto.TaskType, topic string, fn func(msg *sarama.ConsumerMessage,
	consumerPause ConsumerPause) bool) (GroupConsumer, error) {
	config := sarama.NewConfig()
	config.Version = kafka.DefaultKafkaVersion
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.Retry.Max = 10

	consumer := newConsumer(taskType, cli.commitInterval, cli.offsetGetter, topic, fn)
	group := fmt.Sprintf("%s-%s", proto.ServiceNameScheduler, topic)

	span, ctx := trace.StartSpanFromContext(context.Background(), group)

	client, err := sarama.NewConsumerGroup(cli.brokers, group, config)
	if err != nil {
		span.Errorf("creating consumer group client failed: err[%+v]", err)
		return nil, err
	}

	go func() {
		for {
			if err := client.Consume(ctx, []string{topic}, consumer); err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				span.Errorf("consumer failed and try again: err[%+v]", err)
			}
		}
	}()
	groupConsumer := &KafkaConsumerGroup{
		group:    group,
		client:   client,
		span:     span,
		consumer: consumer,
	}
	span.Infof("start kafka consumer: group[%s]", group)
	return groupConsumer, nil
}
