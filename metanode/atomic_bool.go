package metanode

import (
	"sync"
)

type AtomicBool struct {
	sync.RWMutex
	applyID uint64
	status  bool
}

func NewAtomicBool() *AtomicBool {
	return &AtomicBool{}
}

func (b *AtomicBool) SetTrue(appID uint64) {
	b.Lock()
	b.applyID = appID
	b.status = true
	b.Unlock()
}

func (b *AtomicBool) Bool() bool {
	b.RLock()
	ok := b.status
	b.RUnlock()
	return ok
}

func (b *AtomicBool) SetFalse(appID uint64) {
	b.Lock()
	if b.applyID <= appID {
		b.status = false
	}
	b.Unlock()
}
