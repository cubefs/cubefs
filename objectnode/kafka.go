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
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type KafkaConfig struct {
	Brokers  string `json:"brokers"`
	Topic    string `json:"topic"`
	Version  string `json:"version"`
	Username string `json:"username"`
	Password string `json:"password"`
	TLS      struct {
		Enable     bool   `json:"enable"`
		SkipVerify bool   `json:"skip_verify"`
		ClientAuth int    `json:"client_auth"`
		ClientKey  string `json:"client_key"`
		ClientCert string `json:"client_cert"`
		RootCAFile string `json:"root_ca_file"`
	} `json:"tls"`
	TimeoutMs int64 `json:"timeout_ms"`
}

// FixConfig validates and fixes the configuration.
func (c *KafkaConfig) FixConfig() error {
	if c.Brokers == "" || len(strings.Split(c.Brokers, ",")) <= 0 {
		return errors.New("kafka: no broker found")
	}
	if c.Topic == "" {
		return errors.New("kafka: no topic found")
	}
	if c.Version != "" {
		if _, err := sarama.ParseKafkaVersion(c.Version); err != nil {
			return err
		}
	}
	if c.Username != "" && c.Password == "" || c.Username == "" && c.Password != "" {
		return errors.New("kafka: username and password must be a pair")
	}
	if c.TLS.ClientKey != "" && c.TLS.ClientCert == "" || c.TLS.ClientKey == "" && c.TLS.ClientCert != "" {
		return errors.New("kafka: client_cert and client_key must be a pair")
	}
	if c.TimeoutMs <= 0 {
		c.TimeoutMs = 5000
	}

	return nil
}

// newSaramaConfig creates a new Sarama configuration.
func (c *KafkaConfig) newSaramaConfig() (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	if c.Version != "" {
		version, err := sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return nil, err
		}
		cfg.Version = version
	}
	cfg.Metadata.Retry.Max = 2
	cfg.Metadata.RefreshFrequency = 120 * time.Second

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Timeout = time.Duration(c.TimeoutMs) * time.Millisecond

	if c.Username != "" && c.Password != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = c.Username
		cfg.Net.SASL.Password = c.Password
	}
	cfg.Net.KeepAlive = 60 * time.Second

	tslConfig, err := NewTLSConfig(c.TLS.ClientCert, c.TLS.ClientKey)
	if err != nil {
		return nil, err
	}
	cfg.Net.TLS.Enable = c.TLS.Enable
	cfg.Net.TLS.Config = tslConfig
	cfg.Net.TLS.Config.InsecureSkipVerify = c.TLS.SkipVerify
	cfg.Net.TLS.Config.ClientAuth = tls.ClientAuthType(c.TLS.ClientAuth)
	if c.TLS.RootCAFile != "" {
		if cfg.Net.TLS.Config.RootCAs, err = GetRootCAs(c.TLS.RootCAFile); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

// BuildSyncProducer creates a synchronous Kafka producer.
// Make sure to call FixConfig before calling it to validate and fix the configuration.
func (c *KafkaConfig) BuildSyncProducer() (sarama.SyncProducer, error) {
	cfg, err := c.newSaramaConfig()
	if err != nil {
		return nil, err
	}

	return sarama.NewSyncProducer(strings.Split(c.Brokers, ","), cfg)
}
