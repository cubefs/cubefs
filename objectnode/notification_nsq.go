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
	"errors"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/nsqio/go-nsq"
)

type NsqConfig struct {
	Topic    string `json:"topic"`
	NsqdAddr string `json:"nsqd_addr"`
}

// FixConfig validates and fixes the configuration.
func (c *NsqConfig) FixConfig() error {
	if c.Topic == "" {
		return errors.New("nsq: no topic found")
	}
	if c.NsqdAddr == "" {
		return errors.New("nsq: no nsqd addr found")
	}

	return nil
}

// BuildProducer creates a NSQ producer.
// Make sure to call FixConfig before calling it to validate and fix the configuration.
func (c *NsqConfig) BuildProducer() (*nsq.Producer, error) {
	cfg := nsq.NewConfig()

	producer, err := nsq.NewProducer(c.NsqdAddr, cfg)
	if err != nil {
		return nil, err
	}

	if err = producer.Ping(); err != nil {
		producer.Stop()
		return nil, err
	}

	return producer, nil
}

type NsqNotifierConfig struct {
	Enable bool `json:"enable"`

	NsqConfig
}

type NsqNotifier struct {
	id       NotifierID
	producer *nsq.Producer
	conf     *NsqNotifierConfig

	sync.RWMutex
}

func NewNsqNotifier(id string, conf NsqNotifierConfig) (*NsqNotifier, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	producer, err := conf.BuildProducer()
	if err != nil {
		return nil, err
	}

	return &NsqNotifier{
		id:       NotifierID{ID: id, Name: "nsq"},
		producer: producer,
		conf:     &conf,
	}, nil
}

func (n *NsqNotifier) ID() NotifierID {
	return n.id
}

func (n *NsqNotifier) Name() string {
	return n.id.String()
}

func (n *NsqNotifier) getProducer() *nsq.Producer {
	n.RLock()
	defer n.RUnlock()

	return n.producer
}

func (n *NsqNotifier) Send(data []byte) error {
	producer := n.getProducer()
	if producer == nil {
		return errors.New("nsq notifier has been closed")
	}

	return retry.ExponentialBackoff(3, 100).On(func() error {
		var err error
		if err = producer.Publish(n.conf.Topic, data); err == nil {
			return nil
		}

		switch {
		case strings.Contains(err.Error(), "connection refused"):
			var newProducer *nsq.Producer
			n.Lock()
			if newProducer, err = n.conf.BuildProducer(); err == nil {
				producer.Stop()
				producer = newProducer
				n.producer = newProducer
			}
			n.Unlock()
		case errors.Is(err, nsq.ErrClosing), errors.Is(err, nsq.ErrStopped):
			producer = n.getProducer()
		}

		return err
	})
}

func (n *NsqNotifier) Close() error {
	n.Lock()
	defer n.Unlock()
	if n.producer != nil {
		n.producer.Stop()
		n.producer = nil
	}

	return nil
}
