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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	urllib "net/url"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const decodeStatus = 598

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
			if bodyGetter := req.GetBody; bodyGetter != nil {
				req.GetBody = func() (io.ReadCloser, error) {
					body, err := bodyGetter()
					return crc32block.NewBodyEncoder(body), err
				}
			}
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
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.Tc = cfg.Tc.Default()
	if cfg.Tc.DialTimeoutMs <= 0 {
		cfg.Tc.DialTimeoutMs = 200
	}
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
	body, err := marshalObj(params)
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPut, url, body.Body)
	if err != nil {
		return
	}
	request.Header.Set(HeaderContentType, body.ContentType)
	return c.Do(ctx, request)
}

func (c *client) Post(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
	body, err := marshalObj(params)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, url, body.Body)
	if err != nil {
		return nil, err
	}
	request.Header.Set(HeaderContentType, body.ContentType)
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
	defer resp.Body.Close()

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
	return parseData(resp, ret)
}

func (c *client) PutWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) (err error) {
	body, err := marshalObj(params)
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPut, url, body.Body)
	if err != nil {
		return
	}
	request.Header.Set(HeaderContentType, body.ContentType)
	for _, opt := range opts {
		opt(request)
	}
	resp, err := c.Do(ctx, request)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	err = serverCrcEncodeCheck(ctx, request, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *client) PostWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) error {
	body, err := marshalObj(params)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, url, body.Body)
	if err != nil {
		return err
	}
	request.Header.Set(HeaderContentType, body.ContentType)

	for _, opt := range opts {
		opt(request)
	}
	resp, err := c.Do(ctx, request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

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
	// Do nothing to close.
}

func (c *client) doWithCtx(ctx context.Context, req *http.Request) (resp *http.Response, err error) {
	req = req.WithContext(ctx)
	resp, err = c.client.Do(req)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Warnf("do request to %s failed, error: %s", req.URL, err.Error())
		return
	}
	if c.bandwidthBPMs > 0 {
		timeout := time.Millisecond * time.Duration(resp.ContentLength/c.bandwidthBPMs+c.bodyBaseTimeoutMs)
		timer := time.NewTimer(time.Hour)
		resp.Body = &timeoutReadCloser{body: resp.Body, timer: timer, timeout: timeout}
	}
	return
}

// parseData close response body in this package.
func parseData(resp *http.Response, data interface{}) (err error) {
	defer resp.Body.Close()
	return ParseData(resp, data)
}

// ParseData parse response with data, close response body by yourself.
func ParseData(resp *http.Response, data interface{}) (err error) {
	if code := resp.StatusCode; code/100 == 2 {
		size := resp.ContentLength
		if data != nil && size != 0 {
			if d, ok := data.(UnmarshalerFrom); ok {
				return d.UnmarshalFrom(io.LimitReader(resp.Body, size))
			}

			if d, ok := data.(Unmarshaler); ok {
				buf := bytespool.Alloc(int(size))
				defer bytespool.Free(buf)
				if _, err = io.ReadFull(resp.Body, buf); err != nil {
					return NewError(decodeStatus, "ReadResponse",
						fmt.Errorf("%d response read %s", code, err.Error()))
				}
				return d.Unmarshal(buf)
			}

			if err = json.NewDecoder(resp.Body).Decode(data); err != nil {
				return NewError(decodeStatus, "JSONDecode",
					fmt.Errorf("%d response decode %s", code, err.Error()))
			}
		}
		if code == 200 {
			return nil
		}
		return NewError(code, "NotStatusOK", fmt.Errorf("%d response", code))
	}
	return ParseResponseErr(resp)
}

// ParseResponseErr parse error of response
func ParseResponseErr(resp *http.Response) (err error) {
	// wrap the error with HttpError for StatusCode is not 2XX
	if resp.StatusCode > 299 && resp.ContentLength != 0 {
		errR := &errorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(errR); err != nil {
			return NewError(decodeStatus, "JSONDecode",
				fmt.Errorf("%d response decode %s", resp.StatusCode, err.Error()))
		}
		err = NewError(resp.StatusCode, errR.Code, errors.New(errR.Error))
		return
	}
	return NewError(resp.StatusCode, resp.Status, fmt.Errorf("%d response", resp.StatusCode))
}

// http request body will not closed by net/http, it should close by caller.
// but response body must be closed after Client.Do.
type timeoutReadCloser struct {
	body      io.ReadCloser
	timer     *time.Timer
	timeout   time.Duration
	closeOnce sync.Once
	hasRead   bool
}

func (tr *timeoutReadCloser) Close() (err error) {
	tr.closeOnce.Do(func() {
		err = tr.body.Close()
		tr.timer.Stop()
	})
	return
}

func (tr *timeoutReadCloser) Read(p []byte) (int, error) {
	if tr.timeout <= 0 {
		return 0, ErrBodyReadTimeout
	}

	if tr.hasRead {
		select {
		case <-tr.timer.C:
		default:
		}
	}
	tr.hasRead = true
	tr.timer.Reset(tr.timeout)

	start := time.Now()

	var n int
	var err error
	readOk := make(chan struct{})
	go func() {
		n, err = tr.body.Read(p)
		close(readOk)
	}()

	select {
	case <-readOk:
		tr.timeout -= time.Since(start)
		return n, err
	case <-tr.timer.C:
		tr.timeout = 0
		tr.Close() // trigger to break Read
		<-readOk
		return 0, ErrBodyReadTimeout
	}
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
