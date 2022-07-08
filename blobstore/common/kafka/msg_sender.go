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
	"time"

	"github.com/Shopify/sarama"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

var DefaultKafkaVersion = sarama.V0_10_0_0

type MsgProducer interface {
	SendMessage(topic string, msg []byte) (err error)
	SendMessages(topic string, msgs [][]byte) (err error)
}

type ProducerCfg struct {
	BrokerList []string `json:"broker_list"`
	Topic      string   `json:"topic"`
	TimeoutMs  int64    `json:"timeout_ms"`
}

type Producer struct {
	sarama.SyncProducer
}

func defaultCfg() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = DefaultKafkaVersion
	return config
}

func NewProducer(cfg *ProducerCfg) (*Producer, error) {
	config := defaultCfg()
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 1000
	}
	config.Producer.Timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond

	producer, err := sarama.NewSyncProducer(cfg.BrokerList, config)
	if err != nil {
		log.Error("sarama.NewClient:", err)
		return nil, err
	}
	return &Producer{producer}, err
}

func (p *Producer) SendMessage(topic string, msg []byte) (err error) {
	m := &sarama.ProducerMessage{
		Topic:     topic,
		Timestamp: time.Now(),
		Value:     sarama.ByteEncoder(msg),
	}
	_, _, err = p.SyncProducer.SendMessage(m)
	return err
}

func (p *Producer) SendMessages(topic string, msgs [][]byte) (err error) {
	sendMsgs := make([]*sarama.ProducerMessage, len(msgs))
	for idx, msg := range msgs {
		sendMsgs[idx] = &sarama.ProducerMessage{
			Topic:     topic,
			Timestamp: time.Now(),
			Value:     sarama.ByteEncoder(msg),
		}
	}
	return p.SyncProducer.SendMessages(sendMsgs)
}
