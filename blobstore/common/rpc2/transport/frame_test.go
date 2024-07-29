package transport

import (
	"crypto/rand"
	"hash/crc32"
	"io"
	mrand "math/rand"
	"sync"
	"testing"
	"time"
)

func newFrameWrite(size int) *FrameWrite {
	data, err := defaultAllocator.Alloc(size + headerSize)
	if err != nil {
		panic(err)
	}
	return &FrameWrite{
		off:    headerSize,
		data:   data,
		closer: defaultAllocator,
	}
}

func newFrameRead(size int) *FrameRead {
	data, err := defaultAllocator.Alloc(size)
	if err != nil {
		panic(err)
	}
	return &FrameRead{
		data:   data,
		closer: defaultAllocator,
	}
}

type rwErr struct{}

func (rwErr) Read(p []byte) (n int, err error)  { return 0, io.EOF }
func (rwErr) Write(p []byte) (n int, err error) { return 0, io.ErrClosedPipe }

type rwZero struct{}

func (rwZero) Read(p []byte) (n int, err error)  { return }
func (rwZero) Write(p []byte) (n int, err error) { return 0, nil }

type rwOne struct{}

func (rwOne) Read(p []byte) (n int, err error) { return 1, nil }
func (rwOne) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	return 1, nil
}

func TestFrameWrite(t *testing.T) {
	var hdr rawHeader
	t.Log(hdr.String())

	{
		f := newFrameWrite(1234)
		if f.Len() != 0 {
			t.Fatal(0)
		}
		f.Write(make([]byte, 1200))
		if f.Len() != 1200 {
			t.Fatal(1200)
		}
		f.Write(make([]byte, 34))
		if f.Len() != 1234 {
			t.Fatal(1234)
		}
		f.Write(make([]byte, 34))
		if f.Len() != 1234 {
			t.Fatal(1234)
		}
		f.Close()
	}
	{
		f := newFrameWrite(1234)
		n, err := f.ReadFrom(&rwOne{})
		if n != 1234 || err != nil || f.Len() != 1234 {
			t.Fatal(1234)
		}
		n, err = f.ReadFrom(&rwOne{})
		if n != 0 || err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{
		f := newFrameWrite(1234)
		n, err := f.ReadFrom(&rwZero{})
		if n != 0 || err != nil || f.Len() != 0 {
			t.Fatal(0)
		}
		f.Close()
	}
	{
		f := newFrameWrite(1234)
		_, err := f.ReadFrom(&rwErr{})
		if err != io.EOF {
			t.Fatal(err)
		}
		f.Close()
	}
}

func TestFrameRead(t *testing.T) {
	{
		f := newFrameRead(1234)
		if f.Len() != 1234 {
			t.Fatal(1234)
		}
		f.Read(make([]byte, 1200))
		if f.Len() != 34 {
			t.Fatal(34)
		}
		f.Read(make([]byte, 30))
		if f.Len() != 4 {
			t.Fatal(4)
		}
		f.Bytes(4)
		if f.Len() != 0 {
			t.Fatal(0)
		}
		_, err := f.Read(make([]byte, 34))
		if err != io.EOF {
			t.Fatal(err)
		}
		f.Close()
	}
	{
		f := newFrameRead(1234)
		n, err := f.WriteTo(&rwOne{})
		if n != 1234 || err != nil || f.Len() != 0 {
			t.Fatal(1234)
		}
		_, err = f.WriteTo(&rwOne{})
		if err != io.EOF {
			t.Fatal(err)
		}
		f.Close()
	}
	{
		f := newFrameRead(1234)
		n, err := f.WriteTo(&rwZero{})
		if n != 0 || err != nil || f.Len() != 1234 {
			t.Fatal(0)
		}
		f.Close()
	}
	{
		f := newFrameRead(1234)
		_, err := f.WriteTo(&rwErr{})
		if err != io.ErrClosedPipe {
			t.Fatal(err)
		}
		f.Close()
	}
}

func TestRingFrameBase(t *testing.T) {
	ring := newRingFrame()

	if f := ring.Dequeue(); f != nil {
		t.Fatal("empty dequeue")
	}
	if has := ring.HasData(); has {
		t.Fatal("empty has data")
	}

	getFrame := func() *FrameRead {
		data, _ := defaultAllocator.Alloc(64)
		return &FrameRead{
			off:    0,
			data:   data,
			closer: defaultAllocator,
		}
	}
	for range [4]struct{}{} {
		ring.Enqueue(getFrame())
	}

	f := ring.Dequeue()
	buf := make([]byte, 32)
	if n, _ := f.Read(buf); n != 32 {
		t.Fatal("read half")
	}
	if n, _ := f.Read(buf); n != 32 {
		t.Fatal("read half")
	}
	if f.Len() != 0 {
		t.Fatal("has read")
	}
	f.Close()

	f = ring.Dequeue()
	if n, _ := f.Read(make([]byte, 64)); n != 64 {
		t.Fatal("read once")
	}
	f.Close()

	if has := ring.HasData(); !has {
		t.Fatal("has data before recycle")
	}
	if n := ring.Recycle(); n != 64*2 {
		t.Fatal("recycle")
	}
	if has := ring.HasData(); has {
		t.Fatal("has data after recycle")
	}
}

func TestRingFrameData(t *testing.T) {
	ring := newRingFrame()
	newBuffer := func() []byte {
		return make([]byte, mrand.Intn(32<<10)+(32<<10))
	}

	run := func() {
		var wg sync.WaitGroup
		wg.Add(1)

		wcrc := crc32.NewIEEE()
		rcrc := crc32.NewIEEE()
		size := mrand.Intn(4<<20) + (2 << 20)

		go func(rsize int) {
			defer wg.Done()
			for rsize > 0 {
				if f := ring.Dequeue(); f != nil {
					n, _ := f.WriteTo(rcrc)
					rsize -= int(n)
				} else {
					time.Sleep(time.Millisecond)
				}
			}
		}(size)

		for size > 0 {
			b := newBuffer()
			if size < len(b) {
				b = b[:size]
			}
			rand.Read(b)
			ring.Enqueue(&FrameRead{data: b[:]})
			wcrc.Write(b)
			size -= len(b)
		}

		wg.Wait()
		if wcrc.Sum32() != rcrc.Sum32() {
			t.Fatal("write read data mismatch")
		}
	}
	for range [100]struct{}{} {
		run()
	}
}

func BenchmarkBufferRing(b *testing.B) {
	r := make([]byte, 32)
	w := make([]byte, 64)
	f := &FrameRead{data: w[:]}
	var ff *FrameRead
	ring := newRingFrame()
	const n = 128
	b.SetBytes(64 * n)
	for i := 0; i < b.N; i++ {
		for range [n]struct{}{} {
			ring.Enqueue(f)
			ff = ring.Dequeue()
			ff.Read(r)
			ff.Read(r)
		}
	}
}
