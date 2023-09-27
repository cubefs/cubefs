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

	"github.com/nats-io/nats.go"
)

type NatsConfig struct {
	Address         string `json:"address"`
	Subject         string `json:"subject"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	Token           string `json:"token"`
	RootCAFile      string `json:"root_ca_file"`
	ClientCert      string `json:"client_cert"`
	ClientKey       string `json:"client_key"`
	JetStreamEnable bool   `json:"jetstream_enable"`
}

// FixConfig validates and fixes the configuration.
func (c *NatsConfig) FixConfig() error {
	if c.Address == "" {
		return errors.New("nats: no address found")
	}
	if c.Subject == "" {
		return errors.New("nats: no subject found")
	}
	if c.Username != "" && c.Password == "" || c.Username == "" && c.Password != "" {
		return errors.New("nats: username and password must be a pair")
	}
	if c.ClientCert != "" && c.ClientKey == "" || c.ClientCert == "" && c.ClientKey != "" {
		return errors.New("nats: client cert and client key must be a pair")
	}

	return nil
}

// BuildConnect creates a NATS connection.
// Make sure to call FixConfig before calling it to validate and fix the configuration.
func (c *NatsConfig) BuildConnect() (*nats.Conn, error) {
	options := []nats.Option{
		nats.Name("CubeFS ObjectNotifier"),
		nats.MaxReconnects(-1),
	}
	if c.Username != "" && c.Password != "" {
		options = append(options, nats.UserInfo(c.Username, c.Password))
	}
	if c.Token != "" {
		options = append(options, nats.Token(c.Token))
	}
	if c.RootCAFile != "" {
		options = append(options, nats.RootCAs(c.RootCAFile))
	}
	if c.ClientCert != "" && c.ClientKey != "" {
		options = append(options, nats.ClientCert(c.ClientCert, c.ClientKey))
	}

	conn, err := nats.Connect(c.Address, options...)
	if err != nil {
		return nil, err
	}

	if !conn.IsConnected() {
		conn.Close()
		return nil, errors.New("nats: connect failed")
	}

	return conn, nil
}

type NatsNotifierConfig struct {
	Enable bool `json:"enable"`

	NatsConfig
}

type NatsNotifier struct {
	id        NotifierID
	conn      *nats.Conn
	jetStream nats.JetStream

	NatsNotifierConfig
}

func NewNatsNotifier(id string, conf NatsNotifierConfig) (*NatsNotifier, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	conn, err := conf.BuildConnect()
	if err != nil {
		return nil, err
	}

	var jetStream nats.JetStream
	if conf.JetStreamEnable {
		if jetStream, err = conn.JetStream(); err != nil {
			return nil, err
		}
	}

	return &NatsNotifier{
		id:                 NotifierID{ID: id, Name: "nats"},
		conn:               conn,
		jetStream:          jetStream,
		NatsNotifierConfig: conf,
	}, nil
}

func (n *NatsNotifier) ID() NotifierID {
	return n.id
}

func (n *NatsNotifier) Name() string {
	return n.id.String()
}

func (n *NatsNotifier) Send(data []byte) error {
	var err error
	if n.jetStream != nil {
		_, err = n.jetStream.Publish(n.Subject, data)
	} else {
		err = n.conn.Publish(n.Subject, data)
	}

	return err
}

func (n *NatsNotifier) Close() error {
	if n.conn != nil {
		n.conn.Close()
	}

	return nil
}
