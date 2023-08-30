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
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/util/log"
)

type AuditKafkaConfig struct {
	BufSize    int   `json:"buf_size"`
	MaxWorkers int32 `json:"max_workers"`

	Brokers  string `json:"brokers"`
	Topic    string `json:"topic"`
	Version  string `json:"version"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c *AuditKafkaConfig) Validate() error {
	if c.Brokers == "" || len(strings.Split(c.Brokers, ";")) <= 0 {
		return errors.New("no broker found")
	}
	if c.Topic == "" {
		return errors.New("no topic found")
	}
	if c.Version != "" {
		if _, err := sarama.ParseKafkaVersion(c.Version); err != nil {
			return err
		}
	}

	if c.BufSize < minAuditBufSize {
		c.BufSize = minAuditBufSize
	}
	if c.MaxWorkers > maxAuditWorkers {
		c.MaxWorkers = maxAuditWorkers
	}

	return nil
}

func (c *AuditKafkaConfig) buildProducer() (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	if c.Version != "" {
		version, err := sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return nil, err
		}
		cfg.Version = version
	}
	cfg.Metadata.RefreshFrequency = 120 * time.Second
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	if c.Username != "" && c.Password != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = c.Username
		cfg.Net.SASL.Password = c.Password
	}
	cfg.Net.KeepAlive = 60 * time.Second

	producer, err := sarama.NewSyncProducer(strings.Split(c.Brokers, ";"), cfg)
	if err != nil {
		return nil, err
	}

	if _, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: c.Topic,
		Value: sarama.ByteEncoder([]byte{}),
	}); err != nil {
		producer.Close()
		return nil, err
	}

	return producer, nil
}

type AuditKafka struct {
	id       string
	workers  int32
	bufCh    chan interface{}
	producer sarama.SyncProducer

	wg sync.WaitGroup

	sync.RWMutex
	AuditKafkaConfig
}

func NewAuditKafka(id string, conf AuditKafkaConfig) (AuditLogger, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	producer, err := conf.buildProducer()
	if err != nil {
		return nil, err
	}

	a := &AuditKafka{
		AuditKafkaConfig: conf,
		id:               id,
		producer:         producer,
		workers:          1,
		bufCh:            make(chan interface{}, conf.BufSize),
	}

	go a.Start()

	return a, nil
}

func (a *AuditKafka) Name() string {
	return "audit-kafka-" + a.id
}

func (a *AuditKafka) Send(ctx context.Context, entry interface{}) error {
	a.RLock()
	defer a.RUnlock()
	if a.bufCh == nil || a.producer == nil {
		return errors.New("audit has been closed")
	}

	select {
	case a.bufCh <- entry:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		if a.workers < a.MaxWorkers {
			go a.Start()
			a.workers++
		}
		select {
		case a.bufCh <- entry:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return errors.New("audit buffer full")
		}
	}
}

func (a *AuditKafka) Close() error {
	a.Lock()
	if a.bufCh != nil {
		close(a.bufCh)
		a.bufCh = nil
	}
	a.Unlock()

	a.wg.Wait()
	var err error
	a.Lock()
	if a.producer != nil {
		err = a.producer.Close()
		a.producer = nil
	}
	a.Unlock()

	return err
}

func (a *AuditKafka) Start() {
	a.RLock()
	ch := a.bufCh
	producer := a.producer
	a.RUnlock()
	if ch == nil || producer == nil {
		return
	}
	a.wg.Add(1)
	defer a.wg.Done()

	buf := make([]byte, 0, 256)
	for e := range ch {
		buf = buf[:0]
		data, err := json.Marshal(&e)
		if err != nil {
			log.LogErrorf("json marshal '%+v' failed: %v", e, err)
			continue
		}
		buf = append(buf, data...)
		msg := &sarama.ProducerMessage{
			Topic: a.Topic,
			Value: sarama.ByteEncoder(buf),
		}

		if err = retry.ExponentialBackoff(5, 100).On(func() error {
			_, _, err = producer.SendMessage(msg)
			return err
		}); err != nil {
			log.LogErrorf("producer send msg failed: %v", err)
		}
		msg.Value = nil
	}
}
