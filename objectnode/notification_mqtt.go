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
	"fmt"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttConfig struct {
	Broker               string `json:"broker"`
	Topic                string `json:"topic"`
	QoS                  byte   `json:"qos"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	RootCAFile           string `json:"root_ca_file"`
	ClientCert           string `json:"client_cert"`
	ClientKey            string `json:"client_key"`
	KeepAlive            int    `json:"keep_alive_s"`
	MaxReconnectInterval int    `json:"max_reconnect_interval_s"`
	ConnectTimeout       int    `json:"connect_timeout_s"`
}

// FixConfig validates and fixes the configuration.
func (c *MqttConfig) FixConfig() error {
	u, err := url.Parse(c.Broker)
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "ws", "wss", "ssl", "tls", "tcp":
	default:
		return errors.New("mqtt: unknown protocol in broker address")
	}

	if c.Topic == "" {
		return errors.New("mqtt: no topic found")
	}
	if c.Username != "" && c.Password == "" || c.Username == "" && c.Password != "" {
		return errors.New("mqtt: username and password must be a pair")
	}
	if c.ClientCert != "" && c.ClientKey == "" || c.ClientCert == "" && c.ClientKey != "" {
		return errors.New("mqtt: client cert and client key must be a pair")
	}

	if c.KeepAlive <= 0 {
		c.KeepAlive = 30
	}
	if c.MaxReconnectInterval <= 0 {
		c.MaxReconnectInterval = 60
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 30
	}

	return nil
}

// BuildClient creates a new MQTT client.
// Make sure to call FixConfig before calling it to validate and fix the configuration.
func (c *MqttConfig) BuildClient() (mqtt.Client, error) {
	options := mqtt.NewClientOptions()
	options.AddBroker(c.Broker)
	options.SetClientID(fmt.Sprintf("c-%x-fs", time.Now().UnixNano()))
	options.SetUsername(c.Username)
	options.SetPassword(c.Password)
	options.SetKeepAlive(time.Duration(c.KeepAlive) * time.Second)
	options.SetAutoReconnect(true)
	options.SetConnectTimeout(time.Duration(c.ConnectTimeout) * time.Second)
	options.SetMaxReconnectInterval(time.Duration(c.MaxReconnectInterval) * time.Second)

	tlsConfig, err := NewTLSConfig(c.ClientCert, c.ClientKey)
	if err != nil {
		return nil, err
	}
	if c.RootCAFile != "" {
		if tlsConfig.RootCAs, err = GetRootCAs(c.RootCAFile); err != nil {
			return nil, err
		}
	}
	options.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(options)
	token := client.Connect()
	if !token.WaitTimeout(5 * time.Second) {
		return nil, errors.New("connect timeout")
	}

	return client, token.Error()
}

type MqttNotifierConfig struct {
	Enable bool `json:"enable"`

	MqttConfig
}

type MqttNotifier struct {
	id     NotifierID
	client mqtt.Client

	MqttNotifierConfig
}

func NewMqttNotifier(id string, conf MqttNotifierConfig) (*MqttNotifier, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	client, err := conf.BuildClient()
	if err != nil {
		return nil, err
	}

	return &MqttNotifier{
		id:                 NotifierID{ID: id, Name: "mqtt"},
		client:             client,
		MqttNotifierConfig: conf,
	}, nil
}

func (m *MqttNotifier) ID() NotifierID {
	return m.id
}

func (m *MqttNotifier) Name() string {
	return m.id.String()
}

func (m *MqttNotifier) Send(data []byte) error {
	token := m.client.Publish(m.Topic, m.QoS, false, string(data))
	if !token.WaitTimeout(5 * time.Second) {
		return errors.New("mqtt publish timeout")
	}

	return token.Error()
}

func (m *MqttNotifier) Close() error {
	if m.client != nil {
		m.client.Disconnect(250)
	}

	return nil
}
