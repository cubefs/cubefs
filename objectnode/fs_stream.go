package objectnode

import (
	"context"
	"io"
	"sync"
)

type Stream struct {
	mu     *sync.Mutex
	cond   *sync.Cond
	buffer []byte
	closed bool
	ctx    context.Context
}

func (s *Stream) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}
	if s.closed {
		return 0, io.EOF
	}
	s.buffer = append(s.buffer, p...)
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()
	return len(p), nil
}

func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()
	return nil
}

func (s *Stream) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}
	if len(s.buffer) == 0 && s.closed {
		return 0, io.EOF
	}
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		s.cond.L.Lock()
		s.cond.Wait()
		s.cond.L.Unlock()
		s.mu.Lock()
		if len(s.buffer) == 0 && s.closed {
			return 0, io.EOF
		}
	}
	var readN = len(p)
	if len(s.buffer) < readN {
		readN = len(s.buffer)
	}
	copy(p, s.buffer[:readN])
	s.buffer = s.buffer[readN:]
	return readN, nil
}

func NewStream(ctx context.Context) *Stream {
	stream := &Stream{}
	stream.mu = new(sync.Mutex)
	stream.cond = sync.NewCond(new(sync.Mutex))
	stream.ctx = ctx
	return stream
}
