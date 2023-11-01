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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewWebhookAudit(t *testing.T) {
	config := WebhookAuditConfig{}
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

	conf := WebhookAuditConfig{}
	conf.Endpoint = server.URL
	audit, err := NewWebhookAudit("cubefs", conf)
	require.NoError(t, err)
	require.Equal(t, "webhook-audit-cubefs", audit.Name())

	// send test
	require.NoError(t, audit.sendTest())

	// send data
	entry := &AuditEntry{}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, audit.Send(data))

	// audit is closed
	require.NoError(t, audit.Close())
}
