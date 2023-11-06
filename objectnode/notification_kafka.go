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

type KafkaNotifierConfig struct {
	Enable bool `json:"enable"`
	IsTest bool `json:"-"`

	KafkaConfig
}

type KafkaNotifier struct {
	id       NotifierID
	producer sarama.SyncProducer

	KafkaNotifierConfig
}

func NewKafkaNotifier(id string, conf KafkaNotifierConfig) (*KafkaNotifier, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	producer, err := conf.BuildSyncProducer()
	if err != nil {
		return nil, err
	}

	k := &KafkaNotifier{
		id:                  NotifierID{ID: id, Name: "kafka"},
		producer:            producer,
		KafkaNotifierConfig: conf,
	}
	if !conf.IsTest {
		if err = k.sendTest(); err != nil {
			return nil, err
		}
	}

	return k, nil
}

func (k *KafkaNotifier) sendTest() error {
	return k.Send([]byte("{}"))
}

func (k *KafkaNotifier) ID() NotifierID {
	return k.id
}

func (k *KafkaNotifier) Name() string {
	return k.id.String()
}

func (k *KafkaNotifier) Send(data []byte) error {
	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: k.Topic,
		Value: sarama.ByteEncoder(data),
	})

	return err
}

func (k *KafkaNotifier) Close() error {
	var err error
	if k.producer != nil {
		err = k.producer.Close()
	}

	return err
}
