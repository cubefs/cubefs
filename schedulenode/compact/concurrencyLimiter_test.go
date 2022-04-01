package compact

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrencyLimiter(t *testing.T) {
	limiter := NewConcurrencyLimiter(10)
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			limiter.Add()
			defer func() {
				limiter.Done()
				wg.Done()
			}()
			t.Logf("index:%v", i)
			time.Sleep(time.Millisecond * 10)
		}(i)
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		time.Sleep(3 * time.Second)
		var expectLimit int32 = 5
		limiter.Reset(expectLimit)
		limit := limiter.Limit()
		if limit != expectLimit {
			t.Errorf("limit mismatch, expect:%v, actual:%v", expectLimit, limit)
			return
		}
	}()
	wg.Wait()
}
