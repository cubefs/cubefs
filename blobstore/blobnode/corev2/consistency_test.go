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

package core

import (
	"container/list"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestNewConsistencyController(t *testing.T) {
	cc := NewConsistencyController()
	elem := cc.Begin(1)
	require.NotNil(t, elem)

	cc.End(elem)
	cc.Synchronize()
}

func TestConsistencyController_Begin(t *testing.T) {
	cc := NewConsistencyController()

	// =================
	wg := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			elem := cc.Begin(proto.BlobID(i))
			time.Sleep(time.Millisecond * 10)
			cc.End(elem)
		}(i)
	}

	wg.Add(1)
	var curTime uint64
	go func() {
		defer wg.Done()
		curTime = cc.Synchronize()
	}()

	for i := 50; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			elem := cc.Begin(proto.BlobID(i))
			time.Sleep(time.Millisecond * 10)
			cc.End(elem)
		}(i)
	}
	wg.Wait()

	for e := cc.reqs.Front(); e != nil; e = e.Next() {
		req := e.Value.(Request)
		require.Equal(t, true, req.timestamp >= curTime)
	}
}

func TestConsistencyController_Synchronize(t *testing.T) {
	ctx := context.TODO()
	span := trace.SpanFromContextSafe(ctx)

	cc := NewConsistencyController()

	// =================
	elems := make([]*list.Element, 0)
	for i := 0; i < 10; i++ {
		elem := cc.Begin(proto.BlobID(i))
		elems = append(elems, elem)
	}

	done := make(chan struct{}, 1)
	go func() {
		cc.synchronize(5)
		done <- struct{}{}
	}()

	// ========== wait Sync ==============
	select {
	case <-done:
		require.Fail(t, "unexpected, not timeout")
	case <-time.After(time.Duration(time.Second * 2)):
		span.Infof("expected, timeout")
	}
	require.Equal(t, 10, cc.reqs.Len())

	// =========== Synchronized successfully ===========
	// End <=5 previous requests
	for i := 0; i < 6; i++ {
		cc.End(elems[i])
	}
	// Synchronized successfully
	select {
	case <-done:
		span.Infof("sync success")
		require.Equal(t, 4, cc.reqs.Len())
	case <-time.After(time.Duration(time.Second * 2)):
		require.Fail(t, "unexpected, timeout")
	}

	// all > 5
	for e := cc.reqs.Front(); e != nil; e = e.Next() {
		req := e.Value.(Request)
		span.Infof("req: %v", req)
		require.Equal(t, true, req.timestamp > 5)
	}
	for i := 6; i < 10; i++ {
		cc.End(elems[i])
	}
	require.Equal(t, 10, int(cc.CurrentTime()))

	go func() {
		cc.Synchronize()
		done <- struct{}{}
	}()

	// Synchronized successfully
	select {
	case <-done:
		span.Infof("sync success")
		require.Equal(t, 0, cc.reqs.Len())
	case <-time.After(time.Duration(time.Second * 2)):
		require.Fail(t, "unexpected, timeout")
	}
}
