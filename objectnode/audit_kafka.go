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
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/util/log"
)

type KafkaAuditConfig struct {
	Enable     bool `json:"enable"`
	BufSize    int  `json:"buf_size"`
	MaxWorkers int  `json:"max_workers"`
	IsTest     bool `json:"-"`

	KafkaConfig
}

func (c *KafkaAuditConfig) FixConfig() error {
	if err := c.KafkaConfig.FixConfig(); err != nil {
		return err
	}

	if c.BufSize <= 0 {
		c.BufSize = minAuditBufSize
	}
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = 1
	}

	return nil
}

type KafkaAudit struct {
	name     string
	workers  int
	bufCh    chan interface{}
	producer sarama.SyncProducer

	wg sync.WaitGroup

	sync.RWMutex
	KafkaAuditConfig
}

func NewKafkaAudit(id string, conf KafkaAuditConfig) (*KafkaAudit, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	producer, err := conf.BuildSyncProducer()
	if err != nil {
		return nil, err
	}

	a := &KafkaAudit{
		KafkaAuditConfig: conf,
		name:             "kafka-audit-" + id,
		producer:         producer,
		workers:          1,
		bufCh:            make(chan interface{}, conf.BufSize),
	}

	if !conf.IsTest {
		if err = a.sendTest(); err != nil {
			a.producer.Close()
			return nil, err
		}
		go a.Start()
	}

	return a, nil
}

func (k *KafkaAudit) sendTest() error {
	msg := &sarama.ProducerMessage{
		Topic: k.Topic,
		Value: sarama.ByteEncoder([]byte{}),
	}
	_, _, err := k.producer.SendMessage(msg)
	return err
}

func (k *KafkaAudit) Name() string {
	return k.name
}

func (k *KafkaAudit) Send(ctx context.Context, entry interface{}) error {
	k.RLock()
	defer k.RUnlock()
	if k.bufCh == nil || k.producer == nil {
		return errors.New("kafka audit has been closed")
	}

	select {
	case k.bufCh <- entry:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		if k.workers < k.MaxWorkers {
			go k.Start()
			k.workers++
		}
		select {
		case k.bufCh <- entry:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return errors.New("kafka audit buffer full")
		}
	}
}

func (k *KafkaAudit) Close() error {
	k.Lock()
	if k.bufCh != nil {
		close(k.bufCh)
		k.bufCh = nil
	}
	k.Unlock()

	k.wg.Wait()
	var err error
	k.Lock()
	if k.producer != nil {
		err = k.producer.Close()
		k.producer = nil
	}
	k.Unlock()

	return err
}

func (k *KafkaAudit) Start() {
	k.RLock()
	bufCh, producer := k.bufCh, k.producer
	k.RUnlock()
	if bufCh == nil || producer == nil {
		return
	}

	k.wg.Add(1)
	defer k.wg.Done()

	buf := make([]byte, 0, 256)
	for entry := range bufCh {
		buf = buf[:0]
		data, err := json.Marshal(&entry)
		if err != nil {
			log.LogErrorf("json marshal '%+v' failed: %v", entry, err)
			continue
		}
		buf = append(buf, data...)

		if _, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: k.Topic,
			Value: sarama.ByteEncoder(buf),
		}); err != nil {
			log.LogErrorf("kafka audit producer send msg failed: %v", err)
		}
	}
}
