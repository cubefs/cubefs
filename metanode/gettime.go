package metanode

import (
	"sync"
	"time"
)

// Now
var Now *NowTime = NewNowTime()

type NowTime struct {
	sync.RWMutex
	now time.Time
}

func (t *NowTime) GetCurrentTime() (now time.Time) {
	t.RLock()
	now = t.now
	t.RUnlock()
	return
}

func (t *NowTime) UpdateTime(now time.Time) {
	t.Lock()
	t.now = now
	t.Unlock()
}

func NewNowTime() *NowTime {
	return &NowTime{
		now: time.Now(),
	}
}

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			now := time.Now()
			Now.UpdateTime(now)
		}
	}()
}
