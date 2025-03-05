package transport

import (
	"math/rand"
	"testing"
)

func TestAllocatorAlloc(t *testing.T) {
	alloc := NewAllocator()
	if _, err := alloc.Alloc(-1); err != ErrAllocOversize {
		t.Fatal(-1)
	}
	add := func(x int) int { return x + headerSize }
	if b, _ := alloc.Alloc(0); len(b.Bytes()) != add(0) || cap(b.Bytes()) != add(2) {
		t.Fatal(0, len(b.Bytes()), cap(b.Bytes()))
	}
	if b, _ := alloc.Alloc(1); len(b.Bytes()) != add(1) {
		t.Fatal(1)
	}
	if b, _ := alloc.Alloc(3); len(b.Bytes()) != add(3) || cap(b.Bytes()) != add(4) {
		t.Fatal(3)
	}
	if b, _ := alloc.Alloc(1023); len(b.Bytes()) != add(1023) || cap(b.Bytes()) != add(1024) {
		t.Fatal(1023)
	}
	if b, _ := alloc.Alloc(maxsize); len(b.Bytes()) != add(maxsize) {
		t.Fatal(maxsize)
	}
	if _, err := alloc.Alloc(maxsize + 1); err != ErrAllocOversize {
		t.Fatal(maxsize + 1)
	}
	ab, _ := alloc.Alloc(1024)
	ab.Free()
	if ab.(*assignedBuffer).buffer != nil {
		t.Fatal(0)
	}
}

func TestAllocatorFree(t *testing.T) {
	a := NewAllocator()
	alloc := a.(*allocator)
	buf := make([]byte, 3)
	if err := alloc.Free(&buf); err == nil {
		t.Fatal("put elem:3 []bytes misbehavior")
	}
	buff := alignedBufferWithHeader(4 + 1)
	if err := alloc.Free(buff); err == nil {
		t.Fatal("put elem:4 []bytes misbehavior")
	}
	buf = (*buff)[1:]
	if err := alloc.Free(&buf); err != ErrAllocAddress {
		t.Fatal("put elem:4 []bytes misbehavior")
	}
	buff = alignedBufferWithHeader(4)
	if err := alloc.Free(buff); err != nil {
		t.Fatal("put elem:4 []bytes misbehavior")
	}
	buff = alignedBufferWithHeader(1024)
	buf = (*buff)[:1023]
	if err := alloc.Free(&buf); err != nil {
		t.Fatal("put elem:1024 []bytes misbehavior")
	}
	buff = alignedBufferWithHeader(65536)
	if err := alloc.Free(buff); err != nil {
		t.Fatal("put elem:65536 []bytes misbehavior")
	}
	buf = make([]byte, 65537)
	if err := alloc.Free(&buf); err == nil {
		t.Fatal("put elem:65537 []bytes misbehavior")
	}
}

func TestAllocatorAllocThenFree(t *testing.T) {
	alloc := NewAllocator()
	buffer, _ := alloc.Alloc(4)
	data := buffer.Bytes()
	buffer.Written(headerSize)
	buffer.Written(4)
	buffer.Len()
	buffer.Free()
	newData, _ := alloc.Alloc(4)
	if cap(data) != cap(newData.Bytes()) {
		t.Fatal("different cap while alloc.Alloc()")
	}
}

func TestAllocatorAlignment(t *testing.T) {
	alloc := NewAllocator()
	for range [100]struct{}{} {
		size := rand.Int31n(maxsize)
		b, err := alloc.Alloc(int(size))
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Free(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkMSB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msb(rand.Int())
	}
}

func BenchmarkAllocator(b *testing.B) {
	alloc := NewAllocator()
	for i := 0; i < b.N; i++ {
		size := (i % maxsize) + 1
		b, _ := alloc.Alloc(size)
		_ = b.Bytes()
		b.Free()
	}
}
