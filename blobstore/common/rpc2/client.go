// Copyright 2024 The CubeFS Authors.
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

package rpc2

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/retry"
)

type Client struct {
	connector       Connector
	ConnectorConfig ConnectorConfig

	Retry   int
	RetryOn func(error) bool
	// | Request | Response Header |   Response Body  |
	// |      Request Timeout      | Response Timeout |
	// |                 Timeout                      |
	Timeout         time.Duration
	RequestTimeout  time.Duration
	ResponseTimeout time.Duration
}

func (c *Client) DoWith(req *Request, ret Unmarshaler) error {
	resp, err := c.Do(req, ret)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func (c *Client) Do(req *Request, ret Unmarshaler) (resp *Response, err error) {
	if c.Retry <= 0 {
		c.Retry = 3
	}
	afterBody := req.AfterBody
	err = retry.Timed(c.Retry, 1).RuptOn(func() (bool, error) {
		resp, err = c.do(req, ret)
		if err != nil {
			if c.RetryOn != nil && !c.RetryOn(err) {
				return true, err
			}
			if req.Body == nil || req.GetBody == nil {
				return true, err
			}
			body, errBody := req.GetBody()
			if errBody != nil {
				return true, err
			}
			req.Body = clientNopBody(body)
			req.AfterBody = afterBody
			return false, err
		}
		return true, nil
	})
	return
}

func (c *Client) Close() error {
	if c.connector == nil {
		return nil
	}
	return c.connector.Close()
}

func (c *Client) do(req *Request, ret Unmarshaler) (*Response, error) {
	if c.connector == nil {
		c.connector = defaultConnector(c.ConnectorConfig)
	}
	for _, opt := range req.opts {
		opt(req)
	}
	req.Header.SetStable()
	req.Trailer.SetStable()

	conn, err := c.connector.Get(req.Context(), req.RemoteAddr)
	if err != nil {
		return nil, err
	}
	req.client = c
	req.conn = conn

	resp, err := req.request(c.requestDeadline(req.Context()))
	if err != nil {
		c.connector.Put(req.Context(), req.conn, true)
		return nil, err
	}
	if err = resp.ParseResult(ret); err != nil {
		resp.Body.Close()
		return nil, err
	}
	req.conn.SetReadDeadline(c.responseDeadline(req.Context()))
	return resp, nil
}

func (c *Client) requestDeadline(ctx context.Context) time.Time {
	var timeout, reqTimeout time.Time
	if c.Timeout > 0 {
		timeout = time.Now().Add(c.Timeout)
	}
	if c.RequestTimeout > 0 {
		reqTimeout = time.Now().Add(c.RequestTimeout)
	}
	return beforeContextDeadline(ctx, latestTime(timeout, reqTimeout))
}

func (c *Client) responseDeadline(ctx context.Context) time.Time {
	var timeout, respTimeout time.Time
	if c.Timeout > 0 {
		timeout = time.Now().Add(c.Timeout)
	}
	if c.ResponseTimeout > 0 {
		respTimeout = time.Now().Add(c.ResponseTimeout)
	}
	return beforeContextDeadline(ctx, latestTime(timeout, respTimeout))
}

func NewRequest(ctx context.Context, addr, path string, para Marshaler, body io.Reader) (*Request, error) {
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = io.NopCloser(body)
	}
	if para == nil {
		para = NoParameter
	}
	paraData, err := para.Marshal()
	if err != nil {
		return nil, err
	}
	req := &Request{
		RemoteAddr: addr,
		RequestHeader: RequestHeader{
			Version:    Version,
			Magic:      Magic,
			RemotePath: path,
			TraceID:    getSpan(ctx).TraceID(),
			Parameter:  paraData,
		},
		ctx:       ctx,
		Body:      clientNopBody(rc),
		AfterBody: func() error { return nil },
	}
	if body != nil {
		switch v := body.(type) {
		case *bytes.Buffer:
			req.ContentLength = int64(v.Len())
			buf := v.Bytes()
			req.GetBody = func() (io.ReadCloser, error) {
				r := bytes.NewReader(buf)
				return io.NopCloser(r), nil
			}
		case *bytes.Reader:
			req.ContentLength = int64(v.Len())
			snapshot := *v
			req.GetBody = func() (io.ReadCloser, error) {
				r := snapshot
				return io.NopCloser(&r), nil
			}
		case *strings.Reader:
			req.ContentLength = int64(v.Len())
			snapshot := *v
			req.GetBody = func() (io.ReadCloser, error) {
				r := snapshot
				return io.NopCloser(&r), nil
			}
		default:
		}
		if req.ContentLength == 0 {
			if sized, ok := body.(interface{ Size() int }); ok {
				req.ContentLength = int64(sized.Size())
			}
		}
		if req.GetBody != nil && req.ContentLength == 0 {
			req.Body = NoBody
			req.GetBody = func() (io.ReadCloser, error) { return NoBody, nil }
		}
	}
	return req, nil
}

type StreamClient[Req any, Res any] struct {
	Client *Client
}

func (sc *StreamClient[Req, Res]) Streaming(req *Request, ret Unmarshaler) (StreamingClient[Req, Res], error) {
	resp, err := sc.Client.Do(req, ret)
	if err != nil {
		return nil, err
	}
	cs := &clientStream{
		req:     req,
		header:  resp.Header,
		trailer: resp.Trailer.ToHeader(),
	}
	return &GenericClientStream[Req, Res]{ClientStream: cs}, nil
}

func NewStreamRequest(ctx context.Context, addr, path string, para Marshaler) (*Request, error) {
	if para == nil {
		para = NoParameter
	}
	req, err := NewRequest(ctx, addr, path, nil, Codec2Reader(para))
	if err != nil {
		return nil, err
	}
	req.StreamCmd = StreamCmd_SYN
	req.ContentLength = int64(para.Size())
	return req, nil
}
