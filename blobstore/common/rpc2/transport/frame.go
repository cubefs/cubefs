package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const ( // cmds
	// protocol version 1:
	cmdSYN byte = iota // stream open
	cmdFIN             // stream close, a.k.a EOF mark
	cmdPSH             // data push
	cmdPIN             // client send  keepalive
	cmdPON             // server reply keepalive

	// protocol version 2 extra commands
	// notify bytes consumed by remote peer-end
	cmdUPD
)

const (
	// data size of cmdUPD, format:
	// |4B data consumed(ACK)| 4B window size(WINDOW) |
	szCmdUPD = 8
)

const (
	// initial peer window guess, a slow-start
	initialPeerWindow = 262144
)

//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-------+-------+-----------------------------------------------+
// |  Ver  |  Cmd  |                   length                      |
// +-------+-------+-----------------------------------------------+
// |                         StreamID                              |
// +---------------------------------------------------------------+

const (
	sizeOfVerCmd = 1
	sizeOfLength = 3
	sizeOfSid    = 4
	headerSize   = sizeOfVerCmd + sizeOfLength + sizeOfSid
)

type rawHeader [headerSize]byte

func (h rawHeader) Version() byte {
	return h[3] >> 4 // little endian
}

func (h rawHeader) Cmd() byte {
	return h[3] & 0x0f // little endian
}

func (h rawHeader) Length() uint32 {
	return binary.LittleEndian.Uint32(h[:]) & 0xffffff
}

func (h rawHeader) StreamID() uint32 {
	return binary.LittleEndian.Uint32(h[4:])
}

func (h rawHeader) String() string {
	return fmt.Sprintf("Version:%d Cmd:%d StreamID:%d Length:%d",
		h.Version(), h.Cmd(), h.StreamID(), h.Length())
}

type updHeader [szCmdUPD]byte

func (h updHeader) Consumed() uint32 {
	return binary.LittleEndian.Uint32(h[:])
}

func (h updHeader) Window() uint32 {
	return binary.LittleEndian.Uint32(h[4:])
}

// FrameWrite frame for write
type FrameWrite struct {
	ver byte
	cmd byte
	sid uint32

	off  int
	data []byte // with frame header

	done   uint32 // 0 = new, 1 = locked, 2 == closed
	once   sync.Once
	closer interface {
		Free([]byte) error
	}

	ctx context.Context
}

func (f *FrameWrite) tryLock() bool {
	return atomic.CompareAndSwapUint32(&f.done, 0, 1)
}

func (f *FrameWrite) unlock() {
	atomic.CompareAndSwapUint32(&f.done, 1, 0)
}

func (f *FrameWrite) Write(p []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, nil
	}
	n := copy(f.data[f.off:], p)
	f.off += n
	return n, nil
}

func (f *FrameWrite) ReadFrom(r io.Reader) (int64, error) {
	if f.off >= len(f.data) {
		return 0, nil
	}
	var nn int64
	for {
		n, err := r.Read(f.data[f.off:])
		nn += int64(n)
		f.off += n
		if f.off == len(f.data) {
			return nn, nil
		}
		if n == 0 || err != nil {
			return nn, err
		}
	}
}

func (f *FrameWrite) Len() int {
	return f.off - headerSize
}

func (f *FrameWrite) Close() (err error) {
	f.once.Do(func() {
		for !atomic.CompareAndSwapUint32(&f.done, 0, 2) {
		}
		data := f.data
		f.data = nil
		f.off = 0
		err = f.closer.Free(data)
		f.closer = nil
	})
	return
}

func (f *FrameWrite) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *FrameWrite) WithContext(ctx context.Context) {
	f.ctx = ctx
}

// FrameRead frame for read
type FrameRead struct {
	off  int
	data []byte

	once   sync.Once
	closer interface {
		Free([]byte) error
	}
}

func (f *FrameRead) Read(p []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.off:])
	f.off += n
	return n, nil
}

func (f *FrameRead) WriteTo(w io.Writer) (int64, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	var nn int64
	for {
		n, err := w.Write(f.data[f.off:])
		nn += int64(n)
		f.off += n
		if f.off == len(f.data) {
			return nn, nil
		}
		if n == 0 || err != nil {
			return nn, err
		}
	}
}

func (f *FrameRead) Bytes(n int) []byte {
	b := f.data[f.off : f.off+n]
	f.off += n
	return b
}

func (f *FrameRead) Len() int {
	return len(f.data) - f.off
}

func (f *FrameRead) Close() (err error) {
	f.once.Do(func() {
		data := f.data
		f.data = nil
		f.off = 0
		err = f.closer.Free(data)
		f.closer = nil
	})
	return err
}

type ringFrame struct {
	lock   sync.Mutex
	index  int // to read the index
	next   int // next empty place, -1 means to grow
	frames []*FrameRead
}

func newRingFrame() *ringFrame {
	return &ringFrame{next: -1}
}

func (r *ringFrame) Enqueue(f *FrameRead) {
	r.lock.Lock()
	if r.next >= 0 {
		r.frames[r.next] = f
		r.next = (r.next + 1) % len(r.frames)
		if r.frames[r.next] != nil {
			r.next = -1
		}
		r.lock.Unlock()
		return
	}

	frames := append(r.frames, f) // runtime.growslice
	n := copy(frames, r.frames[r.index:])
	copy(frames[n:], r.frames[0:r.index])
	r.frames = frames
	r.index = 0

	if len(r.frames) < cap(r.frames) {
		r.next = len(r.frames)
		r.frames = r.frames[:cap(r.frames)]
	}
	r.lock.Unlock()
}

func (r *ringFrame) Dequeue() (f *FrameRead) {
	r.lock.Lock()
	if len(r.frames) == 0 {
		r.lock.Unlock()
		return
	}
	if f = r.frames[r.index]; f != nil {
		r.frames[r.index] = nil
		if r.next == -1 {
			r.next = r.index
		}
		r.index = (r.index + 1) % len(r.frames)
	}
	r.lock.Unlock()
	return
}

func (r *ringFrame) Recycle() (n int) {
	r.lock.Lock()
	for idx := range r.frames {
		if f := r.frames[idx]; f != nil {
			n += f.Len()
			r.frames[idx] = nil
			f.Close()
		}
		r.next = 0
	}
	r.lock.Unlock()
	return
}

func (r *ringFrame) HasData() (has bool) {
	r.lock.Lock()
	for idx := range r.frames {
		if r.frames[idx] != nil {
			has = true
			break
		}
	}
	r.lock.Unlock()
	return
}
