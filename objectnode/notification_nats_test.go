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

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

func TestNewNatsNotifier(t *testing.T) {
	conf := NatsNotifierConfig{}

	_, err := NewNatsNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Address = "127.0.0.1:14222"
	_, err = NewNatsNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Subject = "nats-notifier-unit-test"
	_, err = NewNatsNotifier("cubefs", conf)
	require.Error(t, err)

	opts := natsserver.DefaultTestOptions
	opts.Port = 14222
	server := natsserver.RunServer(&opts)
	defer server.Shutdown()

	conf.Address = server.Addr().String()
	notifier, err := NewNatsNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())

	conf.JetStreamEnable = true
	notifier, err = NewNatsNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())
}

func TestNatsNotifier(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = 14222
	server := natsserver.RunServer(&opts)
	defer server.Shutdown()

	conf := NatsNotifierConfig{}
	conf.Subject = "nats-notifier-unit-test"
	conf.Address = server.Addr().String()
	notifier, err := NewNatsNotifier("cubefs", conf)
	require.NoError(t, err)

	require.Equal(t, "cubefs:nats", notifier.Name())
	require.Equal(t, NotifierID{ID: "cubefs", Name: "nats"}, notifier.ID())

	require.NoError(t, notifier.Send([]byte("test")))
	require.NoError(t, notifier.Close())
}
