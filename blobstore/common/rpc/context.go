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
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/julienschmidt/httprouter"
)

const (
	abortIndex int8 = math.MaxInt8 >> 1
)

var jsonNull = [4]byte{'n', 'u', 'l', 'l'}

// Context handler context with http variables
type Context struct {
	opts  *serverOptions
	Param httprouter.Params

	Request *http.Request
	Writer  http.ResponseWriter

	// pass key/value in whole request
	mu   sync.RWMutex
	Meta map[string]interface{}

	wroteHeader bool

	// interceptors control
	index    int8
	handlers []HandlerFunc
}

// ArgsBody args in body
func (c *Context) ArgsBody(args interface{}) error {
	return c.ParseArgs(args, OptArgsBody())
}

// ArgsURI args in uri
func (c *Context) ArgsURI(args interface{}) error {
	return c.ParseArgs(args, OptArgsURI())
}

// ArgsQuery args in query
func (c *Context) ArgsQuery(args interface{}) error {
	return c.ParseArgs(args, OptArgsQuery())
}

// ArgsForm args in form
func (c *Context) ArgsForm(args interface{}) error {
	return c.ParseArgs(args, OptArgsForm())
}

// ArgsPostForm args in post form
func (c *Context) ArgsPostForm(args interface{}) error {
	return c.ParseArgs(args, OptArgsPostForm())
}

// ParseArgs reflect param to args
func (c *Context) ParseArgs(args interface{}, opts ...ServerOption) error {
	if err := parseArgs(c, args, opts...); err != nil {
		return NewError(http.StatusBadRequest, "Argument", err)
	}
	return nil
}

// RequestLength read request body length
func (c *Context) RequestLength() (int, error) {
	cl := c.Request.ContentLength
	if cl < 0 {
		return 0, fmt.Errorf("Unknown content length in request")
	}
	return int(cl), nil
}

// Next should be used only inside interceptor.
// It executes the pending handlers inside the calling handler.
func (c *Context) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		c.handlers[c.index](c)
		c.index++
	}
}

// IsAborted return aborted or not
func (c *Context) IsAborted() bool {
	return c.index >= abortIndex
}

// Abort the next handlers
func (c *Context) Abort() {
	c.index = abortIndex
}

// AbortWithStatus abort with status
func (c *Context) AbortWithStatus(statusCode int) {
	c.RespondStatus(statusCode)
	c.Abort()
}

// AbortWithStatusJSON abort with status and response data
func (c *Context) AbortWithStatusJSON(statusCode int, obj interface{}) {
	c.RespondStatusData(statusCode, obj)
	c.Abort()
}

// AbortWithError abort with error
func (c *Context) AbortWithError(err error) {
	c.RespondError(err)
	c.Abort()
}

// Respond response 200, and Content-Length: 0
func (c *Context) Respond() {
	c.Writer.Header().Set(HeaderContentLength, "0")
	c.RespondStatus(http.StatusOK)
}

// RespondStatus response status code
func (c *Context) RespondStatus(statusCode int) {
	c.Writer.WriteHeader(statusCode)
	c.wroteHeader = true
}

// RespondError response error
func (c *Context) RespondError(err error) {
	httpErr := Error2HTTPError(err)
	if httpErr == nil {
		c.Respond()
		return
	}
	c.RespondStatusData(httpErr.StatusCode(), errorResponse{
		Error: httpErr.Error(),
		Code:  httpErr.ErrorCode(),
	})
}

// RespondJSON response json
func (c *Context) RespondJSON(obj interface{}) {
	c.RespondStatusData(http.StatusOK, obj)
}

// RespondStatusData response data with code
func (c *Context) RespondStatusData(statusCode int, obj interface{}) {
	body, err := marshalObj(obj)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondWithReader(statusCode, body.ContentLength, body.ContentType, body.Body, nil)
}

// RespondWith response with code, content-type, bytes
func (c *Context) RespondWith(statusCode int, contentType string, body []byte) {
	c.Writer.Header().Set(HeaderContentType, contentType)
	c.Writer.Header().Set(HeaderContentLength, strconv.Itoa(len(body)))

	c.Writer.WriteHeader(statusCode)
	c.wroteHeader = true
	c.Writer.Write(body)
}

// RespondWithReader response with code, content-length, content-type, an io.Reader and extra headers
func (c *Context) RespondWithReader(statusCode int, contentLength int, contentType string,
	body io.Reader, extraHeaders map[string]string) {
	c.Writer.Header().Set(HeaderContentType, contentType)
	c.Writer.Header().Set(HeaderContentLength, strconv.Itoa(contentLength))
	for key, val := range extraHeaders {
		c.Writer.Header().Set(key, val)
	}

	c.Writer.WriteHeader(statusCode)
	c.wroteHeader = true
	io.CopyN(c.Writer, body, int64(contentLength))
}

// Stream sends a streaming response and returns a boolean
// indicates "Is client disconnected in middle of stream"
func (c *Context) Stream(step func(w io.Writer) bool) bool {
	w := c.Writer
	clientGone := c.Request.Context().Done()
	for {
		select {
		case <-clientGone:
			return true
		default:
			keepOpen := step(w)
			c.Flush()
			if !keepOpen {
				return false
			}
		}
	}
}

// Set is used to store a new key/value pair exclusively for this context.
func (c *Context) Set(key string, val interface{}) {
	c.mu.Lock()
	if c.Meta == nil {
		c.Meta = make(map[string]interface{})
	}
	c.Meta[key] = val
	c.mu.Unlock()
}

// Get returns the value for the given key,
// If the value does not exists it returns (nil, false).
func (c *Context) Get(key string) (val interface{}, exists bool) {
	c.mu.RLock()
	val, exists = c.Meta[key]
	c.mu.RUnlock()
	return
}

// RemoteIP parses the IP from Request.RemoteAddr, returns the net.IP (without the port).
func (c *Context) RemoteIP() (net.IP, bool) {
	ip, _, err := net.SplitHostPort(strings.TrimSpace(c.Request.RemoteAddr))
	if err != nil {
		return nil, false
	}
	remoteIP := net.ParseIP(ip)
	if remoteIP == nil {
		return nil, false
	}
	return remoteIP, true
}

// Hijack implements the http.Hijacker interface.
func (c *Context) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c.wroteHeader = true
	return c.Writer.(http.Hijacker).Hijack()
}

// Flush implements the http.Flush interface.
func (c *Context) Flush() {
	c.Writer.(http.Flusher).Flush()
}

// Pusher implements the http.Pusher interface.
func (c *Context) Pusher() http.Pusher {
	if pusher, ok := c.Writer.(http.Pusher); ok {
		return pusher
	}
	return nil
}
