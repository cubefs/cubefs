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
