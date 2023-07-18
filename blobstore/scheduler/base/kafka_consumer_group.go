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
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

type IConsumerOffset interface {
	GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (offset int64, err error)
	SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) (err error)
}

type kafkaOffsetMgr struct {
	offsetMap map[string]int64
	lock      sync.RWMutex
}

func NewKafkaOffsetMgr() IConsumerOffset {
	return &kafkaOffsetMgr{
		offsetMap: make(map[string]int64),
	}
}

func (m *kafkaOffsetMgr) genConsumerOffsetKey(taskType proto.TaskType, topic string, partition int32) string {
	return fmt.Sprintf("%s-%s-%d", taskType, topic, partition)
}

func (m *kafkaOffsetMgr) GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (offset int64, err error) {
	key := m.genConsumerOffsetKey(taskType, topic, partition)

	m.lock.RLock()
	offset = m.offsetMap[key]
	m.lock.RUnlock()
	return
}

func (m *kafkaOffsetMgr) SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) (err error) {
	key := m.genConsumerOffsetKey(taskType, topic, partition)

	m.lock.Lock()
	m.offsetMap[key] = offset
	m.lock.Unlock()
	return nil
}

// KafkaConfig kafka config
type KafkaConfig struct {
	Topic      string
	BrokerList []string
}

type ConsumerPause interface {
	Done() <-chan struct{}
}

type Consumer struct {
	taskType     proto.TaskType
	topic        string
	maxBatchSize int
	maxWaitTimeS int

	ConsumeFn func(msg []*sarama.ConsumerMessage, consumerPause ConsumerPause) bool
	offsetMgr IConsumerOffset
	closer.Closer
	consumerPause closer.Closer
}

func newConsumer(cfg KafkaConsumerCfg, offsetMgr IConsumerOffset,
	consumerFn func(msg []*sarama.ConsumerMessage, consumerPause ConsumerPause) bool) *Consumer {
	consumer := &Consumer{
		taskType:     cfg.TaskType,
		topic:        cfg.Topic,
		maxBatchSize: cfg.MaxBatchSize,
		maxWaitTimeS: cfg.MaxWaitTimeS,
		ConsumeFn:    consumerFn,
		offsetMgr:    offsetMgr,
		Closer:       closer.New(),
	}
	return consumer
}

func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	span := trace.SpanFromContextSafe(session.Context())
	if len(session.Claims()) != 1 {
		span.Errorf("un support multiple topics: size[%+d]", len(session.Claims()))
		return errors.New("un support multiple topics")
	}
	_, ok := session.Claims()[consumer.topic]
	if !ok {
		span.Errorf("not expect topic: expect[%s]", consumer.topic)
		return errors.New("topic not exist")
	}
	consumer.consumerPause = closer.New()
	span.Infof("setup consumer: [%+v]", session.Claims())
	return nil
}

func (consumer *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	span := trace.SpanFromContextSafe(session.Context())
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

	tk := time.NewTicker(time.Second * time.Duration(consumer.maxWaitTimeS))
	defer tk.Stop()
	msgs := make([]*sarama.ConsumerMessage, 0, consumer.maxBatchSize)
	for {
		var message *sarama.ConsumerMessage
		select {
		case message = <-claim.Messages():
			if message == nil {
				span.Warnf("no message for consume and continue")
				continue
			}
			msgs = append(msgs, message)
			if len(msgs) < consumer.maxBatchSize {
				continue
			}

		case <-tk.C:
			if len(msgs) == 0 {
				continue
			}

		case <-session.Context().Done():
			return nil
		}

		// the batch is full, or the time come; when session is done the message may not consumed
		lastMsg := msgs[len(msgs)-1]
		if success := consumer.ConsumeFn(msgs, consumer.consumerPause); !success {
			span.Warnf("message not consume and return: topic[%s], partition[%d], offset[%d]", lastMsg.Topic, lastMsg.Partition, lastMsg.Offset)
			return nil
		}
		session.MarkMessage(lastMsg, "")
		session.Commit()
		consumer.updateLocalOffsetMgr(msgs)

		// reset batch msgs and ticker
		msgs = msgs[:0]
		tk.Reset(time.Second * time.Duration(consumer.maxWaitTimeS))
	}
}

func (consumer *Consumer) updateLocalOffsetMgr(msgs []*sarama.ConsumerMessage) {
	// map: partition -> offset
	offsetMap := make(map[int32]int64)

	// The offset in the same partition is sequential, and record the last offset in the same partition
	for _, msg := range msgs {
		offsetMap[msg.Partition] = msg.Offset
	}

	for partition, offset := range offsetMap {
		consumer.offsetMgr.SetConsumeOffset(consumer.taskType, consumer.topic, partition, offset)
	}
}

type KafkaConsumerCfg struct {
	TaskType     proto.TaskType
	Topic        string
	MaxBatchSize int
	MaxWaitTimeS int
}

type KafkaConsumer interface {
	StartKafkaConsumer(cfg KafkaConsumerCfg, fn func(msg []*sarama.ConsumerMessage, consumerPause ConsumerPause) bool) (GroupConsumer, error)
}

type kafkaClient struct {
	brokers   []string
	offsetMgr IConsumerOffset
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

func NewKafkaConsumer(brokers []string, offsetMgr IConsumerOffset) KafkaConsumer {
	return &kafkaClient{
		brokers:   brokers,
		offsetMgr: offsetMgr,
	}
}

func (cli *kafkaClient) StartKafkaConsumer(cfg KafkaConsumerCfg, fn func(msg []*sarama.ConsumerMessage,
	consumerPause ConsumerPause) bool) (GroupConsumer, error) {
	config := sarama.NewConfig()
	config.Version = kafka.DefaultKafkaVersion
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.Retry.Max = 10

	consumer := newConsumer(cfg, cli.offsetMgr, fn)
	group := fmt.Sprintf("%s-%s", proto.ServiceNameScheduler, cfg.Topic)

	span, ctx := trace.StartSpanFromContext(context.Background(), group)

	client, err := sarama.NewConsumerGroup(cli.brokers, group, config)
	if err != nil {
		span.Errorf("creating consumer group client failed: err[%+v]", err)
		return nil, err
	}

	go func() {
		for {
			if err := client.Consume(ctx, []string{cfg.Topic}, consumer); err != nil {
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
