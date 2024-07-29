package transport

import (
	"math/rand"
	"testing"
)

func TestAllocatorAlloc(t *testing.T) {
	alloc := NewAllocator()
	if _, err := alloc.Alloc(0); err != ErrAllocOversize {
		t.Fatal(0)
	}
	if b, _ := alloc.Alloc(1); len(b) != 1 {
		t.Fatal(1)
	}
	if b, _ := alloc.Alloc(3); len(b) != 3 || cap(b) != 4 {
		t.Fatal(3)
	}
	if b, _ := alloc.Alloc(1023); len(b) != 1023 || cap(b) != 1024 {
		t.Fatal(1023)
	}
	if b, _ := alloc.Alloc(maxsize); len(b) != maxsize {
		t.Fatal(maxsize)
	}
	if _, err := alloc.Alloc(maxsize + 1); err != ErrAllocOversize {
		t.Fatal(maxsize + 1)
	}
}

func TestAllocatorFree(t *testing.T) {
	alloc := NewAllocator()
	if err := alloc.Free(nil); err == nil {
		t.Fatal("put nil misbehavior")
	}
	if err := alloc.Free(make([]byte, 3)); err == nil {
		t.Fatal("put elem:3 []bytes misbehavior")
	}
	if err := alloc.Free(make([]byte, 4)); err != nil {
		t.Fatal("put elem:4 []bytes misbehavior")
	}
	if err := alloc.Free(make([]byte, 1023, 1024)); err != nil {
		t.Fatal("put elem:1024 []bytes misbehavior")
	}
	if err := alloc.Free(make([]byte, 65536)); err != nil {
		t.Fatal("put elem:65536 []bytes misbehavior")
	}
	if err := alloc.Free(make([]byte, 65537)); err == nil {
		t.Fatal("put elem:65537 []bytes misbehavior")
	}
}

func TestAllocatorAllocThenFree(t *testing.T) {
	alloc := NewAllocator()
	data, _ := alloc.Alloc(4)
	alloc.Free(data)
	newData, _ := alloc.Alloc(4)
	if cap(data) != cap(newData) {
		t.Fatal("different cap while alloc.Alloc()")
	}
}

func BenchmarkMSB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msb(rand.Int())
	}
}
