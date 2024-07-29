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

package raft

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdGenerator(t *testing.T) {
	generator := newIDGenerator(1, time.Now())

	id1 := generator.Next()
	id2 := generator.Next()
	require.Equal(t, id1+1, id2)
}

func TestNotify(t *testing.T) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	notifyChan := newNotify(timeoutCtx)
	retVal := proposalResult{reply: "test data"}
	notifyChan.Notify(retVal)

	select {
	case val := <-notifyChan.ch:
		// ensure the received value matches
		if !reflect.DeepEqual(val, retVal) {
			t.Errorf("Expected %v, got %v", retVal, val)
		}
	default:
		t.Errorf("Expected to receive notification from channel")
	}
}

func TestWait(t *testing.T) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	notifyChan := newNotify(timeoutCtx)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// test context cancellation
	cancel()
	_, err := notifyChan.Wait(ctx)
	if err == nil {
		t.Errorf("Expected error when waiting on context cancellation")
	}

	// test receiving notification
	retVal := proposalResult{reply: "test data"}
	go func() {
		time.Sleep(time.Millisecond * 100) // simulate some delay
		notifyChan.Notify(retVal)
	}()
	result, err := notifyChan.Wait(context.Background())
	if err != nil {
		t.Errorf("Expected value but got error: %v", err)
	}
	// ensure the received value matches
	if !reflect.DeepEqual(result, retVal) {
		t.Errorf("Expected %v, got %v", retVal, result)
	}
}
