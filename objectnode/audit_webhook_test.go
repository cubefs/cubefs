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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewWebhookAudit(t *testing.T) {
	config := WebhookAuditConfig{IsTest: true}
	_, err := NewWebhookAudit("cubefs", config)
	require.Error(t, err)

	config.Endpoint = "127.0.0.1:80808"
	_, err = NewWebhookAudit("cubefs", config)
	require.Error(t, err)

	config.Endpoint = "tcp://127.0.0.1:80808"
	_, err = NewWebhookAudit("cubefs", config)
	require.Error(t, err)

	config.Endpoint = "http://127.0.0.1:80808"
	_, err = NewWebhookAudit("cubefs", config)
	require.Error(t, err)

	server := CreateWebhookTestServer()
	defer server.Close()
	config.Endpoint = server.URL
	webhook, err := NewWebhookAudit("cubefs", config)
	require.NoError(t, err)
	require.NoError(t, webhook.Close())
}

func CreateWebhookTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func TestWebhookAudit(t *testing.T) {
	server := CreateWebhookTestServer()
	defer server.Close()

	conf := WebhookAuditConfig{
		BufSize:    5,
		MaxWorkers: 1,
		IsTest:     true,
	}
	conf.Endpoint = server.URL
	audit, err := NewWebhookAudit("cubefs", conf)
	require.NoError(t, err)
	require.Equal(t, "webhook-audit-cubefs", audit.Name())

	// send test
	require.NoError(t, audit.sendTest())

	// send to full
	for i := 0; i < conf.BufSize; i++ {
		entry := &AuditEntry{}
		require.NoError(t, audit.Send(context.Background(), entry))
	}

	// audit is full
	entry := &AuditEntry{}
	require.ErrorContains(t, audit.Send(context.Background(), entry), "full")

	// start to consume
	go audit.Start()
	time.Sleep(time.Second)

	// send again
	require.NoError(t, audit.Send(context.Background(), entry))

	// audit is closed
	require.NoError(t, audit.Close())
	require.ErrorContains(t, audit.Send(context.Background(), entry), "closed")
}
