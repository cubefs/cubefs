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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMqttNotifier(t *testing.T) {
	conf := MqttNotifierConfig{}
	_, err := NewMqttNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Broker = "127.0.0.1:1883"
	_, err = NewMqttNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Broker = "ftp://127.0.0.1:1883"
	_, err = NewMqttNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Broker = "tcp://127.0.0.1:1883"
	_, err = NewMqttNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Broker = "tcp://127.0.0.1:11883"
	conf.Topic = "mqtt-notifier-unit-test"
	_, err = NewMqttNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Broker = "tcp://broker.emqx.io:1883"
	notifier, err := NewMqttNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())
}

func TestMqttNotifier(t *testing.T) {
	conf := MqttNotifierConfig{}
	conf.Topic = "mqtt-notifier-unit-test"
	conf.Broker = "tcp://broker.emqx.io:1883"
	notifier, err := NewMqttNotifier("cubefs", conf)
	require.NoError(t, err)

	require.Equal(t, "cubefs:mqtt", notifier.Name())
	require.Equal(t, NotifierID{ID: "cubefs", Name: "mqtt"}, notifier.ID())

	require.NoError(t, notifier.Send([]byte("test")))
	require.NoError(t, notifier.Close())
	require.ErrorContains(t, notifier.Send([]byte("test")), "closed")
}
