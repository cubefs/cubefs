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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/util/log"
)

var AuditWebhookUserAgent = "Golang CubeFS/ObjectNode audit-webhook"

type WebhookAuditConfig struct {
	Enable     bool `json:"enable"`
	BufSize    int  `json:"buf_size"`
	MaxWorkers int  `json:"max_workers"`
	IsTest     bool `json:"-"`

	WebhookConfig
}

func (c *WebhookAuditConfig) FixConfig() error {
	if err := c.WebhookConfig.FixConfig(); err != nil {
		return err
	}

	if c.BufSize <= 0 {
		c.BufSize = minAuditBufSize
	}
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = 1
	}

	return nil
}

type WebhookAudit struct {
	name    string
	workers int
	bufCh   chan interface{}
	client  *http.Client

	wg sync.WaitGroup

	sync.RWMutex
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
		WebhookAuditConfig: conf,
		name:               "webhook-audit-" + id,
		workers:            1,
		client:             client,
		bufCh:              make(chan interface{}, conf.BufSize),
	}

	if err = a.sendTest(); err != nil {
		return nil, fmt.Errorf("webhook: send test data to '%s' failed: %v", a.Endpoint, err)
	}

	if !conf.IsTest {
		go a.Start()
	}

	return a, nil
}

func (w *WebhookAudit) sendTest() error {
	return w.send([]byte("{}"))
}

func (w *WebhookAudit) Name() string {
	return w.name
}

func (w *WebhookAudit) Send(ctx context.Context, entry interface{}) error {
	w.RLock()
	defer w.RUnlock()
	if w.bufCh == nil || w.client == nil {
		return errors.New("webhook audit has been closed")
	}

	select {
	case w.bufCh <- entry:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		if w.workers < w.MaxWorkers {
			go w.Start()
			w.workers++
		}
		select {
		case w.bufCh <- entry:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return errors.New("webhook audit buffer full")
		}
	}
}

func (w *WebhookAudit) Close() error {
	w.Lock()
	if w.bufCh != nil {
		close(w.bufCh)
		w.bufCh = nil
	}
	w.Unlock()

	w.wg.Wait()
	return nil
}

func (w *WebhookAudit) Start() {
	w.RLock()
	bufCh, client := w.bufCh, w.client
	w.RUnlock()
	if bufCh == nil || client == nil {
		return
	}

	w.wg.Add(1)
	defer w.wg.Done()

	buf := make([]byte, 0, 256)
	for entry := range bufCh {
		buf = buf[:0]
		data, err := json.Marshal(&entry)
		if err != nil {
			log.LogErrorf("json marshal '%+v' failed: %v", entry, err)
			continue
		}
		buf = append(buf, data...)

		if err = retry.ExponentialBackoff(5, 100).On(func() error {
			return w.send(buf)
		}); err != nil {
			log.LogErrorf("webhook audit send data to '%s' failed: %v", w.Endpoint, err)
		}
	}
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
