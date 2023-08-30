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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var AuditWebhookUserAgent = "Golang cubefs/objectnode audit webhook"

type WebhookAuditConfig struct {
	Enable bool `json:"enable"`

	WebhookConfig
}

type WebhookAudit struct {
	name   string
	client *http.Client

	WebhookAuditConfig
}

func NewWebhookAudit(id string, conf WebhookAuditConfig) (*WebhookAudit, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	client, err := conf.BuildClient()
	if err != nil {
		return nil, err
	}

	a := &WebhookAudit{
		name:               "webhook-audit-" + id,
		client:             client,
		WebhookAuditConfig: conf,
	}
	if err = a.sendTest(); err != nil {
		return nil, fmt.Errorf("webhook: send test data to '%s' failed: %v", a.Endpoint, err)
	}

	return a, nil
}

func (w *WebhookAudit) sendTest() error {
	return w.send([]byte("{}"))
}

func (w *WebhookAudit) send(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, w.Endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set(ContentType, ValueContentTypeJSON)
	req.Header.Set(UserAgent, AuditWebhookUserAgent)
	if w.Authorization != "" {
		req.Header.Set(Authorization, w.Authorization)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := w.client.Do(req.WithContext(ctx))
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err != nil || resp.StatusCode/100 == 2 {
		return err
	}

	return fmt.Errorf("%s returns '%s' statuscode", w.Endpoint, resp.Status)
}

func (w *WebhookAudit) Name() string {
	return w.name
}

func (w *WebhookAudit) Send(data []byte) error {
	return retry.ExponentialBackoff(5, 100).On(func() error {
		return w.send(data)
	})
}

func (w *WebhookAudit) Close() error {
	return nil
}
