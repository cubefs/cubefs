// Copyright 2022 The CubeFS Authors.
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

package rpc

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	urllib "net/url"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var errNoHost = errors.New("no host available")

// LbConfig load balance config
type LbConfig struct {
	// hosts
	Hosts []string `json:"hosts"`
	// backup hosts
	BackupHosts []string `json:"backup_hosts"`
	// HostTryTimes Number of host failure retries, HostTryTimes < RequestTryTimes,
	// Avoid requesting the unavailable host all the time
	HostTryTimes int `json:"host_try_times"`
	// Failure retry interval, default value is -1, if FailRetryIntervalS < 0,
	// remove failed hosts will not work.
	FailRetryIntervalS int `json:"fail_retry_interval_s"`
	// Within MaxFailsPeriodS, if the number of failures is greater than or equal
	// to MaxFails, the host is considered disconnected.
	MaxFailsPeriodS int `json:"max_fails_period_s"`

	// RequestTryTimes The maximum number of attempts for a request hosts.
	RequestTryTimes int `json:"try_times"`

	// should retry function
	ShouldRetry func(code int, err error) bool `json:"-"`

	// config for simple client
	Config
}

type lbClient struct {
	requestTryTimes int
	// host for simple client
	clientMap map[string]Client

	sel Selector
	cfg *LbConfig
}

var _ Client = (*lbClient)(nil)

// NewLbClient returns a lb client
func NewLbClient(cfg *LbConfig, sel Selector) Client {
	if cfg.HostTryTimes == 0 {
		cfg.HostTryTimes = (len(cfg.Hosts) + len(cfg.BackupHosts)) * 2
	}
	if cfg.MaxFailsPeriodS == 0 {
		cfg.MaxFailsPeriodS = 1
	}
	if cfg.RequestTryTimes == 0 {
		cfg.RequestTryTimes = cfg.HostTryTimes + 1
	}
	if cfg.ShouldRetry == nil {
		cfg.ShouldRetry = defaultShouldRetry
	}
	if cfg.HostTryTimes > cfg.RequestTryTimes {
		cfg.HostTryTimes = cfg.RequestTryTimes - 1
	}
	if cfg.FailRetryIntervalS == 0 {
		cfg.FailRetryIntervalS = -1
	}
	if sel == nil {
		sel = newSelector(cfg)
	}
	cl := &lbClient{sel: sel, cfg: cfg}
	cl.clientMap = make(map[string]Client)
	for _, host := range cfg.Hosts {
		cl.clientMap[host] = NewClient(&cfg.Config)
	}
	for _, host := range cfg.BackupHosts {
		cl.clientMap[host] = NewClient(&cfg.Config)
	}

	cl.requestTryTimes = cfg.RequestTryTimes
	return cl
}

var defaultShouldRetry = func(code int, err error) bool {
	if err != nil || (code/100 != 4 && code/100 != 2) {
		return true
	}
	return false
}

func (c *lbClient) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := c.doCtx(ctx, req)
	if err != nil {
		return resp, err
	}
	return resp, err
}

func (c *lbClient) Form(ctx context.Context, method, url string, form map[string][]string) (resp *http.Response, err error) {
	body := urllib.Values(form).Encode()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) Put(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
	data, ct, err := marshalObj(params)
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return
	}
	request.Header.Set(HeaderContentType, ct)
	return c.Do(ctx, request)
}

func (c *lbClient) Post(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
	data, ct, err := marshalObj(params)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	request.Header.Set(HeaderContentType, ct)
	return c.Do(ctx, request)
}

func (c *lbClient) DoWith(ctx context.Context, req *http.Request, ret interface{}, opts ...Option) error {
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	err = serverCrcEncodeCheck(ctx, req, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) GetWith(ctx context.Context, url string, ret interface{}) error {
	resp, err := c.Get(ctx, url)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) PutWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) (err error) {
	data, ct, err := marshalObj(params)
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return
	}
	request.Header.Set(HeaderContentType, ct)
	for _, opt := range opts {
		opt(request)
	}
	resp, err := c.Do(ctx, request)
	if err != nil {
		return
	}
	err = serverCrcEncodeCheck(ctx, request, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) PostWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) error {
	data, ct, err := marshalObj(params)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set(HeaderContentType, ct)

	for _, opt := range opts {
		opt(request)
	}
	resp, err := c.Do(ctx, request)
	if err != nil {
		return err
	}

	// set Header and log errors
	err = serverCrcEncodeCheck(ctx, request, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) Head(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) Delete(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) doCtx(ctx context.Context, r *http.Request) (resp *http.Response, err error) {
	reqURI := r.URL.RequestURI()
	span := trace.SpanFromContextSafe(ctx)
	span.Debug("lb.doCtx: start", reqURI)
	var (
		hosts    []string
		tryTimes = c.requestTryTimes
		index    = 0
	)

	for i := 0; i < tryTimes; i++ {
		// get the available hosts
		if index == len(hosts) || hosts == nil {
			hosts = c.sel.GetAvailableHosts()
			if len(hosts) < 1 {
				err = errNoHost
				span.Errorf("lb.doCtx: get host failed: %s", err.Error())
				return
			}
			index = 0
		}
		host := hosts[index]
		// get the real url
		r.URL, err = urllib.Parse(host + reqURI)
		if err != nil {
			span.Errorf("lb.doCtx: parse %s error", host+reqURI)
			return
		}
		r.Host = r.URL.Host
		resp, err = c.clientMap[host].Do(ctx, r)
		if i == tryTimes-1 {
			span.Warnf("lb.doCtx: the last host of request, try times: %d, err: %v, host: %s",
				i+1, err, host)
			return
		}
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		if c.cfg.ShouldRetry(code, err) {
			span.Infof("lb.doCtx: retry host, try times: %d, code: %d, err: %v, host: %s",
				i+1, code, err, host)
			index++
			c.sel.SetFail(host)
			if r.Body == nil {
				continue
			}
			if r.GetBody != nil {
				r.Body, err = r.GetBody()
				if err != nil {
					span.Warnf("lb.doCtx: retry failed, try times: %d, code: %d, err: %v, host: %s",
						i+1, code, err, host)
					return
				}
				continue
			}
			span.Warnf("lb.doCtx: request not support retry, try times: %d, code: %d, err: %v, host: %s",
				i+1, code, err, host)
			return
		}
		span.Debugf("lb.doCtx: the last host of request, try times: %d, code: %d, err: %v, host: %s",
			i+1, code, err, host)
		return
	}
	return
}

func (c *lbClient) Close() {
	c.sel.Close()
}
