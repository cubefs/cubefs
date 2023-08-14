package holder

import (
	"container/list"
	"context"
	"sync"
)

type Action interface {
	Overlap(o Action) bool
}

type __entry struct {
	index  uint64
	act    Action
	waitCh chan struct{}
}

func (h *__entry) overlap(o Action) bool {
	return h != nil && h.act != nil && h.act.Overlap(o)
}

func (h *__entry) release() {
	close(h.waitCh)
}

func (h *__entry) wait(ctx context.Context) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-h.waitCh:
	}
	return
}

type waitFunc func(ctx context.Context) error

type ActionHolder struct {
	lst *list.List
	mu  sync.RWMutex
}

func (h *ActionHolder) Wait(ctx context.Context, act Action) (err error) {
	for {
		if wait := h.findWait(act); wait != nil {
			if err = wait(ctx); err != nil {
				return
			}
			continue
		}
		break
	}
	return
}

func (h *ActionHolder) findWait(act Action) (wait waitFunc) {
	h.mu.RLock()
	for e := h.lst.Back(); e != nil; e = e.Prev() {
		if entry := e.Value.(*__entry); entry.overlap(act) {
			wait = entry.wait
			break
		}
	}
	h.mu.RUnlock()
	return
}

func (h *ActionHolder) Register(index uint64, act Action) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		if e := h.lst.Back(); e != nil {
			if entry := e.Value.(*__entry); entry.index >= index {
				h.lst.Remove(e)
				entry.release()
				continue
			}
		}
		h.lst.PushBack(&__entry{
			index:  index,
			act:    act,
			waitCh: make(chan struct{}),
		})
		break
	}
}

func (h *ActionHolder) Unregister(index uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		if e := h.lst.Front(); e != nil {
			if entry := e.Value.(*__entry); entry.index <= index {
				h.lst.Remove(e)
				entry.release()
				continue
			}
		}
		break
	}
}

func NewActionHolder() *ActionHolder {
	return &ActionHolder{
		lst: list.New(),
	}
}
