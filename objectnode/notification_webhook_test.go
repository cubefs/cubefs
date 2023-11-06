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

func TestNewWebhookNotifier(t *testing.T) {
	conf := WebhookNotifierConfig{}
	_, err := NewWebhookNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Endpoint = "127.0.0.1:8080"
	_, err = NewWebhookNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Endpoint = "tcp://127.0.0.1:80801"
	_, err = NewWebhookNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Endpoint = "http://127.0.0.1:80801"
	_, err = NewWebhookNotifier("cubefs", conf)
	require.Error(t, err)

	server := CreateWebhookTestServer()
	defer server.Close()

	conf.Endpoint = server.URL
	notifier, err := NewWebhookNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())
}

func TestWebhookNotifier(t *testing.T) {
	server := CreateWebhookTestServer()
	defer server.Close()

	conf := WebhookNotifierConfig{}
	conf.Endpoint = server.URL
	notifier, err := NewWebhookNotifier("cubefs", conf)
	require.NoError(t, err)

	require.Equal(t, "cubefs:webhook", notifier.Name())
	require.Equal(t, NotifierID{ID: "cubefs", Name: "webhook"}, notifier.ID())

	require.NoError(t, notifier.sendTest())
	require.NoError(t, notifier.Close())
}
