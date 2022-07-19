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
	"sync/atomic"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

var (
	_ sarama.PartitionConsumer = &mockPartitionConsumer{}
	_ sarama.Consumer          = &mockConsumer{}
)

type mockPartitionConsumer struct {
	topic  string
	pid    int32
	offset int64

	msgCh chan *sarama.ConsumerMessage
	errCh chan *sarama.ConsumerError
}

func newMockPartitionConsumer(topic string, pid int32) *mockPartitionConsumer {
	return &mockPartitionConsumer{
		topic:  topic,
		pid:    pid,
		offset: 0,
		msgCh:  make(chan *sarama.ConsumerMessage),
		errCh:  make(chan *sarama.ConsumerError),
	}
}

func (m *mockPartitionConsumer) sendMsg(key, val string) {
	m.msgCh <- &sarama.ConsumerMessage{
		Key:       []byte(key),
		Value:     []byte(val),
		Topic:     m.topic,
		Partition: m.pid,
		Offset:    atomic.LoadInt64(&m.offset),
	}
	atomic.AddInt64(&m.offset, 1)
}

func (m *mockPartitionConsumer) sendErr(err error) {
	m.errCh <- &sarama.ConsumerError{
		Topic:     m.topic,
		Partition: m.pid,
		Err:       err,
	}
}

func (m *mockPartitionConsumer) AsyncClose()                              {}
func (m *mockPartitionConsumer) Close() error                             { return nil }
func (m *mockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage { return m.msgCh }
func (m *mockPartitionConsumer) Errors() <-chan *sarama.ConsumerError     { return m.errCh }
func (m *mockPartitionConsumer) HighWaterMarkOffset() int64               { return 0 }

type mockConsumer struct {
	topics map[string][]*mockPartitionConsumer
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{
		topics: map[string][]*mockPartitionConsumer{
			"topic1": newBatchpc("topic1", []int32{1, 2, 3, 4}),
			"topic2": newBatchpc("topic2", []int32{1, 2, 3, 4}),
		},
	}
}

func newBatchpc(topic string, pids []int32) (ret []*mockPartitionConsumer) {
	for _, pid := range pids {
		pc := newMockPartitionConsumer(topic, pid)
		ret = append(ret, pc)
	}
	return ret
}

func (m *mockConsumer) Run(msgCnt int) {
	for _, pcs := range m.topics {
		for _, pc := range pcs {
			tmpPc := pc
			go func() {
				for i := 1; i <= msgCnt; i++ {
					tmpPc.sendMsg(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
				}
			}()
		}
	}
}

func (m *mockConsumer) getPc(topic string, pid int32) *mockPartitionConsumer {
	pcs := m.topics[topic]
	for _, pc := range pcs {
		if pc.pid == pid {
			return pc
		}
	}
	return nil
}

func (m *mockConsumer) Topics() ([]string, error) {
	var ret []string
	for topic := range m.topics {
		ret = append(ret, topic)
	}
	return ret, nil
}

func (m *mockConsumer) Partitions(topic string) ([]int32, error) {
	var pids []int32
	for _, pc := range m.topics[topic] {
		pids = append(pids, pc.pid)
	}
	return pids, nil
}

func (m *mockConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	for _, pc := range m.topics[topic] {
		if pc.pid == partition {
			return pc, nil
		}
	}
	return nil, errors.New("not found")
}

func (m *mockConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func (m *mockConsumer) Close() error {
	return nil
}

func TestPartitionConsumer(t *testing.T) {
	mockConsume := newMockConsumer()
	access := newMockAccess(nil)
	pc, err := newKafkaPartitionConsumer(proto.TaskTypeBlobDelete, mockConsume, "topic1", 1, access)
	require.NoError(t, err)

	mockConsume.Run(100)
	msgs := pc.ConsumeMessages(context.Background(), 5)
	require.Equal(t, 5, len(msgs))

	err = pc.CommitOffset(context.Background())
	require.NoError(t, err)
}

func TestTopicConsume(t *testing.T) {
	const topic = "topic1"
	var cs []IConsumer
	mockConsume := newMockConsumer()
	access := newMockAccess(nil)
	for _, pid := range []int32{1, 2, 3} {
		pc, _ := newKafkaPartitionConsumer(proto.TaskTypeBlobDelete, mockConsume, topic, pid, access)
		cs = append(cs, pc)
	}
	mockConsume.Run(100)

	topicConsumer := &TopicConsumer{
		partitionsConsumers: cs,
	}
	for _, pid := range []int32{1, 2, 3} {
		topicConsumer.ConsumeMessages(context.Background(), 1)
		err := topicConsumer.CommitOffset(context.Background())
		require.NoError(t, err)
		off, err := access.GetConsumeOffset(proto.TaskTypeBlobDelete, topic, pid)
		require.NoError(t, err)
		require.Equal(t, int64(0), off)
	}

	topicConsumer.ConsumeMessages(context.Background(), 1)
	err := topicConsumer.CommitOffset(context.Background())
	require.NoError(t, err)
	off, err := access.GetConsumeOffset(proto.TaskTypeBlobDelete, topic, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), off)
}

func TestConsumerError(t *testing.T) {
	mockConsume := newMockConsumer()
	access := newMockAccess(nil)
	pc, err := newKafkaPartitionConsumer(proto.TaskTypeBlobDelete, mockConsume, "topic1", 1, access)
	require.NoError(t, err)
	pcTmp := mockConsume.getPc("topic1", 1)
	go func() { pcTmp.sendErr(errors.New("fake error")) }()

	msgs := pc.ConsumeMessages(context.Background(), 1)
	require.Equal(t, 0, len(msgs))
}

func TestLoadConsumeInfo(t *testing.T) {
	mockConsume := newMockConsumer()
	access := newMockAccess(errcode.ErrNotFound)
	pc, _ := newKafkaPartitionConsumer(proto.TaskTypeBlobDelete, mockConsume, "topic1", 1, access)
	off, _ := pc.loadConsumeInfo("topic1", 1)
	require.Equal(t, sarama.OffsetOldest, off.Offset)
}

func TestNewTopicConsumer(t *testing.T) {
	broker := newBroker(t)
	defer broker.Close()
	cfg := &KafkaConfig{
		Topic:      testTopic,
		BrokerList: []string{broker.Addr()},
		Partitions: []int32{0},
	}

	access := newMockAccess(nil)
	consumer, err := NewTopicConsumer(proto.TaskTypeBlobDelete, cfg, access)
	require.NoError(t, err)

	msgs := consumer.ConsumeMessages(context.Background(), 1)
	require.Equal(t, 1, len(msgs))

	access.err = errMock
	err = consumer.CommitOffset(context.Background())
	require.Error(t, err)

	cfg.BrokerList = []string{}
	_, err = NewTopicConsumer(proto.TaskTypeBlobDelete, cfg, access)
	require.Error(t, err)

	cfg.Partitions = nil
	cfg.BrokerList = []string{broker.Addr()}
	_, err = NewTopicConsumer(proto.TaskTypeBlobDelete, cfg, access)
	require.Error(t, err)
}
