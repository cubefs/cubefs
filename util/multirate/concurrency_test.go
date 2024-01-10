package multirate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBaseConcurrency(t *testing.T) {
	mc := NewMultiConcurrency()
	opread := 1
	ctx := context.Background()
	mc.addRule(opread, 1, 2*time.Millisecond)
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk2"))
	assert.NotNil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	go func() {
		time.Sleep(time.Millisecond)
		mc.Done(opread, "/disk1")
	}()
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	assert.NotNil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))

	mc.addRule(opread, 2, time.Millisecond)
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	go func() {
		time.Sleep(1500 * time.Microsecond)
		mc.Done(opread, "/disk1")
	}()
	assert.NotNil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
}

func TestCancelConcurrency(t *testing.T) {
	mc := NewMultiConcurrency()
	opread := 1
	mc.addRule(opread, 1, 0)
	ctx, cancel := context.WithCancel(context.Background())

	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")
		assert.Contains(t, err.Error(), "canceled")
		wg.Done()
	}()
	cancel()
	wg.Wait()
}

func TestResetConcurrency(t *testing.T) {
	count := uint64(100)
	c := newConcurrency(count, time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(int(count) * 2)
	for i := uint64(0); i < count; i++ {
		go func() {
			c.done()
			wg.Done()
		}()
	}
	for ; count > 0; count-- {
		go func() {
			c.reset(count)
			wg.Done()
		}()
	}
	wg.Wait()
}
