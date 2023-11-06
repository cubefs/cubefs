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
)

var NotifierUserAgent = "Golang cubefs/objectnode event notification"

type WebhookNotifierConfig struct {
	Enable bool `json:"enable"`

	WebhookConfig
}

type WebhookNotifier struct {
	id     NotifierID
	client *http.Client

	WebhookNotifierConfig
}

func NewWebhookNotifier(id string, conf WebhookNotifierConfig) (*WebhookNotifier, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	client, err := conf.BuildClient()
	if err != nil {
		return nil, err
	}

	w := &WebhookNotifier{
		id:                    NotifierID{ID: id, Name: "webhook"},
		client:                client,
		WebhookNotifierConfig: conf,
	}
	if err = w.sendTest(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WebhookNotifier) sendTest() error {
	return w.Send([]byte("{}"))
}

func (w *WebhookNotifier) ID() NotifierID {
	return w.id
}

func (w *WebhookNotifier) Name() string {
	return w.id.String()
}

func (w *WebhookNotifier) Send(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, w.Endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set(ContentType, ValueContentTypeJSON)
	req.Header.Set(UserAgent, NotifierUserAgent)
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

func (w *WebhookNotifier) Close() error {
	return nil
}
