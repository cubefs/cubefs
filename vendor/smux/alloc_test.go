package smux

import (
	"math/rand"
	"testing"
)

func TestAllocGet(t *testing.T) {
	alloc := NewAllocator()
	if alloc.Get(0) != nil {
		t.Fatal(0)
	}
	if len(alloc.Get(1)) != 1 {
		t.Fatal(1)
	}
	if len(alloc.Get(2)) != 2 {
		t.Fatal(2)
	}
	if len(alloc.Get(3)) != 3 || cap(alloc.Get(3)) != 4 {
		t.Fatal(3)
	}
	if len(alloc.Get(4)) != 4 {
		t.Fatal(4)
	}
	if len(alloc.Get(1023)) != 1023 || cap(alloc.Get(1023)) != 1024 {
		t.Fatal(1023)
	}
	if len(alloc.Get(1024)) != 1024 {
		t.Fatal(1024)
	}
	if len(alloc.Get(65536)) != 65536 {
		t.Fatal(65536)
	}
	if alloc.Get(65537) != nil {
		t.Fatal(65537)
	}
}

func TestAllocPut(t *testing.T) {
	alloc := NewAllocator()
	if err := alloc.Put(nil); err == nil {
		t.Fatal("put nil misbehavior")
	}
	if err := alloc.Put(make([]byte, 3, 3)); err == nil {
		t.Fatal("put elem:3 []bytes misbehavior")
	}
	if err := alloc.Put(make([]byte, 4, 4)); err != nil {
		t.Fatal("put elem:4 []bytes misbehavior")
	}
	if err := alloc.Put(make([]byte, 1023, 1024)); err != nil {
		t.Fatal("put elem:1024 []bytes misbehavior")
	}
	if err := alloc.Put(make([]byte, 65536, 65536)); err != nil {
		t.Fatal("put elem:65536 []bytes misbehavior")
	}
	if err := alloc.Put(make([]byte, 65537, 65537)); err == nil {
		t.Fatal("put elem:65537 []bytes misbehavior")
	}
}

func TestAllocPutThenGet(t *testing.T) {
	alloc := NewAllocator()
	data := alloc.Get(4)
	alloc.Put(data)
	newData := alloc.Get(4)
	if cap(data) != cap(newData) {
		t.Fatal("different cap while alloc.Get()")
	}
}

func BenchmarkMSB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msb(rand.Int())
	}
}
