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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	urllib "net/url"
	"strings"
	"time"

	"github.com/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/errors"
)

// Config simple client config
type Config struct {
	// the whole request and response timeout
	ClientTimeoutMs int64 `json:"client_timeout_ms"`
	// bandwidthBPMs for read body
	BodyBandwidthMBPs float64 `json:"body_bandwidth_mbps"`

	// base timeout for read body
	BodyBaseTimeoutMs int64 `json:"body_base_timeout_ms"`
	// transport config
	Tc TransportConfig `json:"transport_config"`
}

// ErrBodyReadTimeout timeout error
var ErrBodyReadTimeout = errors.New("read body timeout")

// Option client options
type Option func(req *http.Request)

// WithCrcEncode request with crc32 encode
func WithCrcEncode() Option {
	return func(req *http.Request) {
		req.Header.Set(HeaderCrcEncoded, "1")
		// util not support reader = nil
		if req.ContentLength > 0 && req.Body != nil {
			encoder := crc32block.NewBodyEncoder(req.Body)
			req.Body = encoder
			req.ContentLength = encoder.CodeSize(req.ContentLength)
		}
	}
}

// Client implements the rpc client with http
type Client interface {
	// Method*** handle response by yourself
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
	Head(ctx context.Context, url string) (*http.Response, error)
	Get(ctx context.Context, url string) (*http.Response, error)
	Delete(ctx context.Context, url string) (*http.Response, error)
	Form(ctx context.Context, method, url string, form map[string][]string) (*http.Response, error)
	Put(ctx context.Context, url string, params interface{}) (*http.Response, error)
	Post(ctx context.Context, url string, params interface{}) (*http.Response, error)

	// ***With means parse result in client
	DoWith(ctx context.Context, req *http.Request, ret interface{}, opts ...Option) error
	GetWith(ctx context.Context, url string, ret interface{}) error
	PutWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) error
	PostWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) error

	// Close background goroutines in lb client
	Close()
}

type client struct {
	client            *http.Client
	bandwidthBPMs     int64 // using for reading body
	bodyBaseTimeoutMs int64 // base time read body
}

// NewClient returns a rpc client
func NewClient(cfg *Config) Client {
	if cfg.BodyBaseTimeoutMs == 0 {
		cfg.BodyBaseTimeoutMs = 30 * 1e3
	}
	return &client{
		client: &http.Client{
			Transport: NewTransport(&cfg.Tc),
			Timeout:   time.Duration(cfg.ClientTimeoutMs) * time.Millisecond,
		},
		bandwidthBPMs:     int64(cfg.BodyBandwidthMBPs * (1 << 20) / 1e3),
		bodyBaseTimeoutMs: cfg.BodyBaseTimeoutMs,
	}
}

func (c *client) Form(ctx context.Context, method, url string, form map[string][]string) (resp *http.Response, err error) {
	body := urllib.Values(form).Encode()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return
	}
	return c.Do(ctx, request)
}

func (c *client) Put(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
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

func (c *client) Post(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
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

func (c *client) DoWith(ctx context.Context, req *http.Request, ret interface{}, opts ...Option) error {
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

func (c *client) GetWith(ctx context.Context, url string, ret interface{}) error {
	resp, err := c.Get(ctx, url)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *client) PutWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) (err error) {
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

func (c *client) PostWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) error {
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

	err = serverCrcEncodeCheck(ctx, request, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *client) Head(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *client) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *client) Delete(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if req.Header.Get(HeaderUA) == "" {
		req.Header.Set(HeaderUA, UserAgent)
	}
	span := trace.SpanFromContextSafe(ctx)
	err := trace.InjectWithHTTPHeader(ctx, req)
	if err != nil {
		span.Errorf("inject failed, %v", err)
	}
	resp, err := c.doWithCtx(ctx, req)
	if err != nil {
		return resp, err
	}

	header := resp.Header
	traceLog := header[HeaderTraceLog]
	if len(traceLog) > 0 {
		span.AppendRPCTrackLog([]string{strings.Join(traceLog, ";")})
	}
	return resp, err
}

func (c *client) Close() {
}

func (c *client) doWithCtx(ctx context.Context, req *http.Request) (resp *http.Response, err error) {
	span := trace.SpanFromContextSafe(ctx)
	ctxReq, cancelReq := context.WithCancel(ctx)
	req = req.WithContext(ctxReq)

	if c.bandwidthBPMs > 0 && req.Body != nil {
		t := req.ContentLength/c.bandwidthBPMs + c.bodyBaseTimeoutMs
		req.Body = &timeoutReadCloser{timeoutMs: t, body: req.Body}
	}

	// waiting the result of response
	waitingCtx, waitingCancel := context.WithCancel(context.TODO())
	errChan := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			span.Infof("request %s is canceled", req.URL)
			errChan <- ctx.Err()
			// note the context in request
			cancelReq()
		case <-waitingCtx.Done():
		}
	}()

	// context canceled before do request
	select {
	case e := <-errChan:
		waitingCancel()
		return nil, e
	default:
	}
	resp, err = c.client.Do(req)
	if err != nil {
		waitingCancel()
		span.Warnf("do request to %s failed: %s", req.URL, err.Error())
		return
	}
	if c.bandwidthBPMs > 0 {
		t := resp.ContentLength/c.bandwidthBPMs + c.bodyBaseTimeoutMs
		resp.Body = &timeoutReadCloser{timeoutMs: t, body: resp.Body}
	}
	resp.Body = &cancelBody{cancelFunc: waitingCancel, rc: resp.Body, isCancel: false}
	return
}

// ParseData parse response with data
func ParseData(resp *http.Response, data interface{}) (err error) {
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode/100 == 2 {
		if data != nil && resp.ContentLength != 0 {
			if err := json.NewDecoder(resp.Body).Decode(data); err != nil {
				return NewError(resp.StatusCode, "JSONDecode", err)
			}
		}
		if resp.StatusCode == 200 {
			return nil
		}
		return NewError(resp.StatusCode, "", err)
	}

	return ParseResponseErr(resp)
}

// ParseResponseErr parse error of response
func ParseResponseErr(resp *http.Response) (err error) {
	// wrap the error with HttpError for StatusCode is not 2XX
	if resp.StatusCode > 299 && resp.ContentLength != 0 {
		errR := &errorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(errR); err != nil {
			return NewError(resp.StatusCode, resp.Status, nil)
		}
		err = NewError(resp.StatusCode, errR.Code, errors.New(errR.Error))
		return
	}
	return NewError(resp.StatusCode, resp.Status, nil)
}

type cancelBody struct {
	rc         io.ReadCloser
	cancelFunc context.CancelFunc
	isCancel   bool
}

func (cb *cancelBody) Read(p []byte) (n int, err error) {
	n, err = cb.rc.Read(p)
	if err == nil || err == io.EOF {
		return
	}
	cb.cancel()
	return
}

func (cb *cancelBody) Close() (err error) {
	err = cb.rc.Close()
	cb.cancel()
	return
}

func (cb *cancelBody) cancel() {
	if !cb.isCancel {
		cb.cancelFunc()
		cb.isCancel = true
	}
}

type timeoutReadCloser struct {
	body      io.ReadCloser
	timeoutMs int64
}

func (tr *timeoutReadCloser) Close() (err error) {
	return tr.body.Close()
}

func (tr *timeoutReadCloser) Read(p []byte) (n int, err error) {
	readOk := make(chan int)
	if tr.timeoutMs > 0 {
		startTime := time.Now().UnixNano() / 1e6
		after := time.After(time.Millisecond * time.Duration(tr.timeoutMs))
		go func() {
			n, err = tr.body.Read(p)
			close(readOk)
		}()
		select {
		case <-readOk:
			// really cost time
			tr.timeoutMs = tr.timeoutMs - (time.Now().UnixNano()/1e6 - startTime)
			return
		case <-after:
			tr.body.Close()
			return 0, ErrBodyReadTimeout
		}
	}
	tr.body.Close()
	return 0, ErrBodyReadTimeout
}

func serverCrcEncodeCheck(ctx context.Context, request *http.Request, resp *http.Response) (err error) {
	// set Header and log errors
	if request.Header.Get(HeaderCrcEncoded) != "" && resp.Header.Get(HeaderAckCrcEncoded) == "" {
		msg := fmt.Sprintf("server do not ack that body has been crc encoded, url:%v", request.URL)
		trace.SpanFromContextSafe(ctx).Error(msg)
		return NewError(http.StatusNotImplemented, "resp.Status", errors.New(msg))
	}
	return nil
}
