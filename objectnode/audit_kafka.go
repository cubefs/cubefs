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
	"github.com/Shopify/sarama"
)

type KafkaAuditConfig struct {
	Enable bool `json:"enable"`
	IsTest bool `json:"-"`

	KafkaConfig
}

type KafkaAudit struct {
	name     string
	producer sarama.SyncProducer

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
		name:             "kafka-audit-" + id,
		producer:         producer,
		KafkaAuditConfig: conf,
	}
	if !conf.IsTest {
		if err = a.sendTest(); err != nil {
			a.producer.Close()
			return nil, err
		}
	}

	return a, nil
}

func (k *KafkaAudit) sendTest() error {
	return k.Send([]byte("{}"))
}

func (k *KafkaAudit) Name() string {
	return k.name
}

func (k *KafkaAudit) Send(data []byte) error {
	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: k.Topic,
		Value: sarama.ByteEncoder(data),
	})

	return err
}

func (k *KafkaAudit) Close() error {
	var err error
	if k.producer != nil {
		err = k.producer.Close()
	}

	return err
}
