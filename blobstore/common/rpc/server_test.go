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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockResponseWriter struct {
	headers http.Header
	status  int
	body    []byte
}

var _ http.ResponseWriter = (*mockResponseWriter)(nil)

func (m *mockResponseWriter) Header() (h http.Header) {
	if m.headers == nil {
		m.headers = http.Header{}
	}
	return m.headers
}

func (m *mockResponseWriter) Write(p []byte) (n int, err error) {
	m.body = p
	return len(p), nil
}

func (m *mockResponseWriter) WriteHeader(status int) {
	m.status = status
}

func (m *mockResponseWriter) ToResponse() *http.Response {
	return &http.Response{
		StatusCode:    m.status,
		ContentLength: int64(len(m.body)),
		Body:          io.NopCloser(bytes.NewReader(m.body)),
	}
}

func TestServerRouterBase(t *testing.T) {
	router := New()

	type A struct {
		Bar string
	}
	args := new(A)
	str := ""
	router.Use(func(c *Context) {
		str += "1"
		c.Next()
		str += "2"
	})
	router.Use(func(c *Context) {
		str += "3"
		c.Next()
		str += "4"
	})
	router.Handle(http.MethodGet, "/foo/:bar", func(c *Context) {
		if err := c.ParseArgs(args); err != nil {
			panic(err)
		}
		str += args.Bar
	}, OptArgsURI())

	w := new(mockResponseWriter)
	req, _ := http.NewRequest(http.MethodGet, "/foo/you", nil)
	router.Router.ServeHTTP(w, req)

	require.Equal(t, "you", args.Bar)
	require.Equal(t, "13you42", str)
}

func TestServerAbort(t *testing.T) {
	router := New()
	str := ""
	router.Use(func(c *Context) {
		str += "1"
		c.Next()

		require.True(t, c.IsAborted())
		val, ok := c.Get("Abort")
		require.True(t, ok)
		require.Equal(t, int(1), val.(int))

		str += "2"
	})
	router.Use(func(c *Context) {
		str += "3"
		c.Set("Abort", 1)
		c.Abort()
		c.Next() // aborted,
		str += "4"
	})
	router.Handle(http.MethodGet, "/foo/:bar", func(c *Context) {
		str += "5"
	}, OptArgsURI())

	w := new(mockResponseWriter)
	req, _ := http.NewRequest(http.MethodGet, "/foo/you", nil)
	router.Router.ServeHTTP(w, req)

	require.Equal(t, "1342", str)
}

func TestServerAbortStatus(t *testing.T) {
	resp := func(r *Router) *mockResponseWriter {
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		r.Router.ServeHTTP(w, req)
		return w
	}

	{
		router := New()
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) { c.Respond() })
		w := resp(router)
		require.Equal(t, 200, w.status)
		require.Equal(t, "0", w.Header().Get(HeaderContentLength))
	}
	{
		router := New()
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatus(200) })
		w := resp(router)
		require.Equal(t, 200, w.status)
		require.Equal(t, "", w.Header().Get(HeaderContentLength))
	}
	{
		router := New()
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) {})
		w := resp(router)
		require.Equal(t, 200, w.status)
		require.Equal(t, "", w.Header().Get(HeaderContentLength))
	}
	{
		router := New()
		router.Use(func(c *Context) { c.Abort() })
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatus(204) })
		w := resp(router)
		require.Equal(t, 200, w.status)
	}
	{
		router := New()
		router.Use(func(c *Context) { c.AbortWithStatus(400) })
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatus(204) })
		w := resp(router)
		require.Equal(t, 400, w.status)
	}
	{
		router := New()
		router.Use(func(c *Context) { c.AbortWithStatusJSON(399, struct{ I int }{10}) })
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatus(204) })
		w := resp(router)
		require.Equal(t, 399, w.status)
	}
	{
		router := New()
		router.Use(func(c *Context) { c.AbortWithError(&Error{Status: 412}) })
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatus(204) })
		w := resp(router)
		require.Equal(t, 412, w.status)
	}
}

func TestServerOptArgs(t *testing.T) {
	type A struct {
		Bar string
	}
	body := "{\"bar\": \"bodybar\"}"
	{
		router := New()
		args := new(A)
		router.Handle(http.MethodGet, "/:bar", func(c *Context) {
			if err := c.ArgsBody(args); err != nil {
				panic(err)
			}
		})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/uribar?bar=querybar", bytes.NewBufferString(body))
		req.Header.Set(HeaderContentLength, strconv.Itoa(len(body)))
		router.Router.ServeHTTP(w, req)

		require.Equal(t, "bodybar", args.Bar)
	}
	{
		router := New()
		args := new(A)
		router.Handle(http.MethodGet, "/:bar", func(c *Context) {
			if err := c.ArgsURI(args); err != nil {
				panic(err)
			}
		})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/uribar?bar=querybar", bytes.NewBufferString(body))
		req.Header.Set(HeaderContentLength, strconv.Itoa(len(body)))
		router.Router.ServeHTTP(w, req)

		require.Equal(t, "uribar", args.Bar)
	}
	{
		router := New()
		args := new(A)
		router.Handle(http.MethodGet, "/:bar", func(c *Context) {
			if err := c.ArgsQuery(args); err != nil {
				panic(err)
			}
		})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/uribar?bar=querybar", bytes.NewBufferString(body))
		req.Header.Set(HeaderContentLength, strconv.Itoa(len(body)))
		router.Router.ServeHTTP(w, req)

		require.Equal(t, "querybar", args.Bar)
	}
	{
		router := New()
		args := new(A)
		router.Handle(http.MethodPost, "/:bar", func(c *Context) {
			if err := c.ArgsForm(args); err != nil {
				panic(err)
			}
		})

		form := url.Values{}
		form.Set("bar", "formbar")
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodPost, "/uribar?bar=querybar", nil)
		req.Form = form
		router.Router.ServeHTTP(w, req)

		require.Equal(t, "formbar", args.Bar)
	}
	{
		router := New()
		args := new(A)
		router.Handle(http.MethodPost, "/:bar", func(c *Context) {
			if err := c.ArgsPostForm(args); err != nil {
				panic(err)
			}
		})

		form := url.Values{}
		form.Set("bar", "formbar")
		postForm := url.Values{}
		postForm.Set("bar", "postformbar")
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodPost, "/uribar?bar=querybar", nil)
		req.Form = form
		req.PostForm = postForm
		router.Router.ServeHTTP(w, req)

		require.Equal(t, "postformbar", args.Bar)
	}
}

func TestServerOptMetaCapacity(t *testing.T) {
	router := New()
	router.Handle(http.MethodGet, "/none", func(c *Context) {
		if c.opts.metaCapacity != 0 {
			panic(c.opts.metaCapacity)
		}
	}, OptMetaCapacity(-199))
	router.Handle(http.MethodGet, "/1024", func(c *Context) {
		if c.opts.metaCapacity != 1024 {
			panic(c.opts.metaCapacity)
		}
	}, OptMetaCapacity(1024))

	{
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/none", nil)
		router.ServeHTTP(w, req)
	}
	{
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/1024", nil)
		router.ServeHTTP(w, req)
	}
}

type marshalData struct {
	I int
	S string
}

func (d *marshalData) Marshal() ([]byte, string, error) {
	if d.I > 0 {
		if d.I == 100 {
			return nil, "", &Error{Status: 204, Err: errors.New("none")}
		}
		return []byte(d.S), MIMEStream, nil
	}
	return nil, "", &Error{Status: 400, Err: errors.New("fake error")}
}

func (d *marshalData) Unmarshal(buff []byte) error {
	for i, j := 0, len(buff)-1; i < j; i, j = i+1, j-1 {
		buff[i], buff[j] = buff[j], buff[i]
	}
	d.S = string(buff)
	return nil
}

type marshalToData struct {
	I int
	S string
}

func (d *marshalToData) MarshalTo(w io.Writer) (string, error) {
	if d.I > 0 {
		w.Write([]byte(d.S))
		return MIMEPlain, nil
	}
	return "", &Error{Status: 400, Err: errors.New("fake error")}
}

func (d *marshalToData) UnmarshalFrom(r io.Reader) error {
	buff, _ := io.ReadAll(r)
	if string(buff) == "error" {
		return errors.New("unmarshaler from error")
	}
	for i, j := 0, len(buff)-1; i < j; i, j = i+1, j-1 {
		buff[i], buff[j] = buff[j], buff[i]
	}
	d.S = string(buff)
	return nil
}

func TestServerResponseWith(t *testing.T) {
	resp := func(r *Router) *mockResponseWriter {
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		r.Router.ServeHTTP(w, req)
		return w
	}

	{
		router := New()
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) { c.Respond() })
		w := resp(router)
		require.Equal(t, 0, len(w.body))
	}
	{
		router := New()
		router.Use(func(c *Context) { c.AbortWithStatusJSON(400, "bad") })
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatus(204) })
		w := resp(router)
		require.Equal(t, "\"bad\"", string(w.body))
	}
	{
		router := New()
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) { c.RespondStatusData(255, nil) })
		w := resp(router)
		require.Equal(t, 255, w.status)
		require.Equal(t, "null", string(w.body))
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalData{I: 11, S: "foo bar"})
		})
		w := resp(router)
		require.Equal(t, 200, w.status)
		require.Equal(t, "foo bar", string(w.body))
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalData{I: -11, S: "foo bar"})
		})
		w := resp(router)
		require.Equal(t, 400, w.status)

		ret := new(errorResponse)
		err := json.Unmarshal(w.body, ret)
		require.NoError(t, err)
		require.Equal(t, "", ret.Code)
		require.Equal(t, "fake error", ret.Error)
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(marshalData{I: 11, S: "foo bar"})
		})
		w := resp(router)
		require.Equal(t, 200, w.status)

		ret := new(marshalData)
		err := json.Unmarshal(w.body, ret)
		require.NoError(t, err)
		require.Equal(t, 11, ret.I)
		require.Equal(t, "foo bar", ret.S)
	}

	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalToData{I: 11, S: "bar foo"})
		})
		w := resp(router)
		require.Equal(t, 200, w.status)
		require.Equal(t, MIMEPlain, w.headers.Get(HeaderContentType))
		require.Equal(t, "bar foo", string(w.body))
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalToData{I: -11, S: "bar foo"})
		})
		w := resp(router)
		require.Equal(t, 400, w.status)

		ret := new(errorResponse)
		err := json.Unmarshal(w.body, ret)
		require.NoError(t, err)
		require.Equal(t, "", ret.Code)
		require.Equal(t, "fake error", ret.Error)
	}

	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalData{I: 100, S: "foo bar"})
		})
		resp := resp(router).ToResponse()
		require.Equal(t, 204, resp.StatusCode)

		err := ParseData(resp, nil)
		require.Error(t, err)
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalData{I: 11, S: "foo bar"})
		})
		resp := resp(router).ToResponse()
		require.Equal(t, 200, resp.StatusCode)

		var r marshalData
		err := ParseData(resp, &r)
		require.NoError(t, err)
		require.Equal(t, "rab oof", r.S)

		resp.Body = io.NopCloser(bytes.NewBuffer(nil))
		err = ParseData(resp, &r)
		require.Error(t, err)
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalToData{I: 1, S: "bar foo"})
		})
		resp := resp(router).ToResponse()
		require.Equal(t, 200, resp.StatusCode)

		var r marshalToData
		err := ParseData(resp, &r)
		require.NoError(t, err)
		require.Equal(t, "oof rab", r.S)
	}
	{
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) {
			c.RespondJSON(&marshalToData{I: 1, S: "error"})
		})
		resp := resp(router).ToResponse()
		require.Equal(t, 200, resp.StatusCode)

		var r marshalToData
		err := ParseData(resp, &r)
		require.Error(t, err)
	}
}

func BenchmarkServerBase(b *testing.B) {
	router := New()
	router.Use(func(c *Context) { c.Next() })
	router.Use(func(c *Context) { c.Next() })
	router.Handle(http.MethodGet, "/", func(c *Context) { c.Respond() })

	b.Helper()
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		router.ServeHTTP(w, req)
	}
}
