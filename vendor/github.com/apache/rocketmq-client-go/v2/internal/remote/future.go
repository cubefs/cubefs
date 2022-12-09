/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package remote

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"sync"
)

// ResponseFuture
type ResponseFuture struct {
	ResponseCommand *RemotingCommand
	Err             error
	Opaque          int32
	callback        func(*ResponseFuture)
	Done            chan bool
	callbackOnce    sync.Once
	ctx             context.Context
}

// NewResponseFuture create ResponseFuture with opaque, timeout and callback
func NewResponseFuture(ctx context.Context, opaque int32, callback func(*ResponseFuture)) *ResponseFuture {
	return &ResponseFuture{
		Opaque:   opaque,
		Done:     make(chan bool),
		callback: callback,
		ctx:      ctx,
	}
}

func (r *ResponseFuture) executeInvokeCallback() {
	r.callbackOnce.Do(func() {
		if r.callback != nil {
			r.callback(r)
		}
	})
}

func (r *ResponseFuture) waitResponse() (*RemotingCommand, error) {
	var (
		cmd *RemotingCommand
		err error
	)
	select {
	case <-r.Done:
		cmd, err = r.ResponseCommand, r.Err
	case <-r.ctx.Done():
		err = errors.ErrRequestTimeout
		r.Err = err
	}
	return cmd, err
}
