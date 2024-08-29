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
	if b, _ := alloc.Alloc(1); len(b.Bytes()) != 1 {
		t.Fatal(1)
	}
	if b, _ := alloc.Alloc(3); len(b.Bytes()) != 3 || cap(b.Bytes()) != 4 {
		t.Fatal(3)
	}
	if b, _ := alloc.Alloc(1023); len(b.Bytes()) != 1023 || cap(b.Bytes()) != 1024 {
		t.Fatal(1023)
	}
	if b, _ := alloc.Alloc(maxsize); len(b.Bytes()) != maxsize {
		t.Fatal(maxsize)
	}
	if _, err := alloc.Alloc(maxsize + 1); err != ErrAllocOversize {
		t.Fatal(maxsize + 1)
	}
	ab, _ := alloc.Alloc(1024)
	ab.Free()
	ab.Free()
	if len(ab.Bytes()) != 0 {
		t.Fatal(0)
	}
}

func TestAllocatorFree(t *testing.T) {
	a := NewAllocator()
	alloc := a.(*allocator)
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
	buffer, _ := alloc.Alloc(4)
	data := buffer.Bytes()
	buffer.Written(4)
	buffer.Len()
	buffer.Free()
	newData, _ := alloc.Alloc(4)
	if cap(data) != cap(newData.Bytes()) {
		t.Fatal("different cap while alloc.Alloc()")
	}
}

func BenchmarkMSB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msb(rand.Int())
	}
}
