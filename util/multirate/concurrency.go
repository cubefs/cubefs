package multirate

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	concurrencySeparator = ","

	controlGetConcurrency = "/multirate/getCon"
)

type concurrency struct {
	count   uint64
	quota   chan bool
	timeout time.Duration
	lock    sync.RWMutex
}

func newConcurrency(count uint64, timeout time.Duration) *concurrency {
	c := &concurrency{count: count, quota: make(chan bool, count), timeout: timeout}
	for i := uint64(0); i < count; i++ {
		c.quota <- true
	}
	return c
}

func (c *concurrency) wait(ctx context.Context) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeout := c.timeout
		if timeout == 0 {
			timeout = defaultTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	select {
	case q := <-c.quota:
		if q {
			return nil
		} else {
			return fmt.Errorf("quota has been reset, try again")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *concurrency) done() {
	c.lock.RLock()
	defer c.lock.RUnlock()
	select {
	case c.quota <- true:
	default:
	}
}

func (c *concurrency) reset(count uint64) {
	if count == c.count {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	close(c.quota)
	c.count = count
	c.quota = make(chan bool, count)
	for i := uint64(0); i < count; i++ {
		c.quota <- true
	}
}

type concurrencyRule struct {
	count   uint64
	timeout time.Duration
}

type MultiConcurrency struct {
	rules sync.Map // map[op]*concurrencyRule
	cons  sync.Map // map[key]*concurrency
}

func NewMultiConcurrency() *MultiConcurrency {
	return new(MultiConcurrency)
}

func NewMultiConcurrencyWithHandler() *MultiConcurrency {
	mc := NewMultiConcurrency()
	http.HandleFunc(controlGetConcurrency, mc.handlerGetConcurrency)
	return mc
}

func (mc *MultiConcurrency) WaitUseDefaultTimeout(ctx context.Context, op int, disk string) error {
	key := getConcurrencyKey(op, disk)
	cVal, ok := mc.cons.Load(key)
	var c *concurrency
	if ok {
		c = cVal.(*concurrency)
		return c.wait(ctx)
	}

	rVal, ok := mc.rules.Load(op)
	if !ok {
		// if op dosn't have a rule, just return success
		return nil
	}
	rule := rVal.(*concurrencyRule)
	c = newConcurrency(rule.count, rule.timeout)
	mc.cons.Store(key, c)
	return c.wait(ctx)
}

func (mc *MultiConcurrency) Done(op int, disk string) {
	key := getConcurrencyKey(op, disk)
	val, ok := mc.cons.Load(key)
	if ok {
		val.(*concurrency).done()
	}
}

func (mc *MultiConcurrency) Count(op int) (count uint64) {
	val, ok := mc.rules.Load(op)
	if ok {
		count = val.(*concurrencyRule).count
	}
	return
}

func (mc *MultiConcurrency) addRule(op int, count uint64, timeout time.Duration) {
	val, ok := mc.rules.Load(op)
	if !ok {
		if count > 0 {
			mc.rules.Store(op, &concurrencyRule{count: count, timeout: timeout})
		}
		return
	}
	rule := val.(*concurrencyRule)
	oldCount := rule.count
	oldTimeout := rule.timeout
	if count == 0 {
		mc.rules.Delete(op)
	} else {
		rule.count = count
		rule.timeout = timeout
	}
	if oldCount != count || oldTimeout != timeout {
		mc.updateConcurrency(op, count, timeout)
	}
}

func (mc *MultiConcurrency) updateConcurrency(op int, count uint64, timeout time.Duration) {
	prefix := strconv.Itoa(op) + concurrencySeparator
	mc.cons.Range(func(k, v interface{}) bool {
		key := k.(string)
		if !strings.HasPrefix(key, prefix) {
			return true
		}
		if count == 0 {
			mc.cons.Delete(key)
			return true
		}
		c := v.(*concurrency)
		c.reset(count)
		c.timeout = timeout
		return true
	})
}

func (mc *MultiConcurrency) getRulesDesc() string {
	var builder strings.Builder
	mc.rules.Range(func(k, v interface{}) bool {
		op := k.(int)
		rule := v.(*concurrencyRule)
		builder.WriteString(fmt.Sprintf("op:%v, count:%v, timeout:%v", op, rule.count, rule.timeout))
		builder.WriteString("\n")
		return true
	})
	return builder.String()
}

func getConcurrencyKey(op int, disk string) string {
	var sb strings.Builder
	sb.WriteString(strconv.Itoa(op))
	sb.WriteString(concurrencySeparator)
	sb.WriteString(disk)
	return sb.String()
}

func (mc *MultiConcurrency) handlerGetConcurrency(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(mc.getRulesDesc()))
}
