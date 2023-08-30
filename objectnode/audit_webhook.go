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
	"net/url"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/util/log"
)

var AuditWebhookUserAgent = "Golang CubeFS/ObjectNode audit-webhook"

type AuditWebhookConfig struct {
	BufSize    int   `json:"buf_size"`
	MaxWorkers int32 `json:"max_workers"`

	Endpoint      string          `json:"endpoint"`
	Authorization string          `json:"authorization"`
	Proxy         string          `json:"proxy"`
	Transport     TransportConfig `json:"transport"`
}

func (c *AuditWebhookConfig) Validate() error {
	if c.Endpoint == "" {
		return errors.New("no endpoint found")
	}
	if _, err := url.Parse(c.Endpoint); err != nil {
		return fmt.Errorf("invalid endpoint: %s", c.Endpoint)
	}

	if c.BufSize < minAuditBufSize {
		c.BufSize = minAuditBufSize
	}
	if c.MaxWorkers > maxAuditWorkers {
		c.MaxWorkers = maxAuditWorkers
	}
	c.Transport.CheckAndFix()

	return nil
}

func (c *AuditWebhookConfig) buildClient() *http.Client {
	transport := c.Transport.BuildTransport()
	if c.Proxy != "" {
		proxyURL, _ := url.Parse(c.Proxy)
		if proxyURL != nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}

	return &http.Client{Transport: transport}
}

type AuditWebhook struct {
	id      string
	workers int32
	bufCh   chan interface{}
	client  *http.Client

	wg sync.WaitGroup

	sync.RWMutex
	AuditWebhookConfig
}

func NewAuditWebhook(id string, conf AuditWebhookConfig) (AuditLogger, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	a := &AuditWebhook{
		AuditWebhookConfig: conf,
		id:                 id,
		workers:            1,
		client:             conf.buildClient(),
		bufCh:              make(chan interface{}, conf.BufSize),
	}

	if err := a.send([]byte("{}")); err != nil {
		return nil, fmt.Errorf("send test data to '%s' failed: %v", a.Endpoint, err)
	}

	go a.Start()

	return a, nil
}

func (a *AuditWebhook) Name() string {
	return "audit-webhook-" + a.id
}

func (a *AuditWebhook) Send(ctx context.Context, entry interface{}) error {
	a.RLock()
	defer a.RUnlock()
	if a.bufCh == nil || a.client == nil {
		return errors.New("audit has been closed")
	}

	select {
	case a.bufCh <- entry:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		if a.workers < a.MaxWorkers {
			go a.Start()
			a.workers++
		}
		select {
		case a.bufCh <- entry:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return errors.New("audit buffer full")
		}
	}
}

func (a *AuditWebhook) Close() error {
	a.Lock()
	if a.bufCh != nil {
		close(a.bufCh)
		a.bufCh = nil
	}
	a.Unlock()

	a.wg.Wait()
	return nil
}

func (a *AuditWebhook) Start() {
	a.RLock()
	ch := a.bufCh
	cli := a.client
	a.RUnlock()
	if ch == nil || cli == nil {
		return
	}
	a.wg.Add(1)
	defer a.wg.Done()

	buf := make([]byte, 0, 256)
	for e := range ch {
		buf = buf[:0]
		data, err := json.Marshal(&e)
		if err != nil {
			log.LogErrorf("json marshal '%+v' failed: %v", e, err)
			continue
		}
		buf = append(buf, data...)

		if err = retry.ExponentialBackoff(5, 100).On(func() error {
			return a.send(buf)
		}); err != nil {
			log.LogErrorf("send data to '%s' failed: %v", a.Endpoint, err)
		}
	}
}

func (a *AuditWebhook) send(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, a.Endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set(ContentType, ValueContentTypeJSON)
	req.Header.Set(UserAgent, AuditWebhookUserAgent)
	if a.Authorization != "" {
		req.Header.Set(Authorization, a.Authorization)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := a.client.Do(req.WithContext(ctx))
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err != nil || resp.StatusCode/100 == 2 {
		return err
	}

	return fmt.Errorf("%s returns '%s' statuscode", a.Endpoint, resp.Status)
}
