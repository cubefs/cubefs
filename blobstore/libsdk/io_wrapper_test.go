package main

import (
	"bytes"
	"io"
	"testing"
)

// Tests reading from a buffer in read-only mode
func TestReadOnlyRead(t *testing.T) {
	ptr := allocTestBuffer(5)
	defer freeTestBuffer(ptr)

	// Write test data to C memory
	wWrite, _ := NewIOWrapper(ptr, 5, 0, WriteOnly)
	_, _ = wWrite.Write([]byte{1, 2, 3, 4, 5})

	// Read and verify data
	wRead, _ := NewIOWrapper(ptr, 5, 0, ReadOnly)
	buf := make([]byte, 3)
	_, err := wRead.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{1, 2, 3}) {
		t.Errorf("Expected %v, got %v", []byte{1, 2, 3}, buf)
	}
}

// Tests writing to a buffer in write-only mode
func TestWriteOnlyWrite(t *testing.T) {
	ptr := allocTestBuffer(5)
	defer freeTestBuffer(ptr)

	w, _ := NewIOWrapper(ptr, 5, 0, WriteOnly)
	buf := []byte{1, 2, 3}
	_, err := w.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Verify written data
	wRead, _ := NewIOWrapper(ptr, 5, 0, ReadOnly)
	readBuf := make([]byte, 3)
	nRead, _ := wRead.Read(readBuf)
	if nRead != 3 || !bytes.Equal(readBuf, buf) {
		t.Errorf("Expected %v, got %v", buf, readBuf)
	}
}

// Tests writing to a read-only ioWrapper (should return error)
func TestReadOnlyWrite(t *testing.T) {
	ptr := allocTestBuffer(5)
	defer freeTestBuffer(ptr)

	w, _ := NewIOWrapper(ptr, 5, 0, ReadOnly)
	_, err := w.Write([]byte{1, 2})
	if err == nil || err.Error() != "ioWrapper is read-only, cannot write" {
		t.Errorf("Expected error, got %v", err)
	}
}

// Tests reading from a write-only ioWrapper (should return error)
func TestWriteOnlyRead(t *testing.T) {
	ptr := allocTestBuffer(5)
	defer freeTestBuffer(ptr)

	w, _ := NewIOWrapper(ptr, 5, 0, WriteOnly)
	_, err := w.Read(make([]byte, 2))
	if err == nil || err.Error() != "ioWrapper is write-only, cannot read" {
		t.Errorf("Expected error, got %v", err)
	}
}

// Tests reading beyond buffer size (out-of-bounds read)
func TestReadBeyondSize(t *testing.T) {
	ptr := allocTestBuffer(5)
	defer freeTestBuffer(ptr)

	// Write test data
	wWrite, _ := NewIOWrapper(ptr, 5, 0, WriteOnly)
	_, _ = wWrite.Write([]byte{1, 2, 3, 4, 5})

	// Read and verify EOF
	wRead, _ := NewIOWrapper(ptr, 5, 0, ReadOnly)
	buf := make([]byte, 10)
	n, err := wRead.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("Expected 5 bytes read, got %d", n)
	}
}

// Tests writing beyond buffer size (out-of-bounds write)
func TestWriteBeyondSize(t *testing.T) {
	ptr := allocTestBuffer(5)
	defer freeTestBuffer(ptr)

	w, _ := NewIOWrapper(ptr, 5, 0, WriteOnly)
	_, err := w.Write(make([]byte, 10))
	if err == nil || err != io.ErrShortWrite {
		t.Errorf("Expected ErrShortWrite, got %v", err)
	}
}

// Tests cumulative position tracking after multiple writes
func TestPositionCumulative(t *testing.T) {
	ptr := allocTestBuffer(10)
	defer freeTestBuffer(ptr)

	w, _ := NewIOWrapper(ptr, 10, 0, WriteOnly)
	_, _ = w.Write([]byte{1, 2, 3})
	_, _ = w.Write([]byte{4, 5})

	// Verify written data
	wRead, _ := NewIOWrapper(ptr, 10, 0, ReadOnly)
	readBuf := make([]byte, 5)
	n, _ := wRead.Read(readBuf)
	expected := []byte{1, 2, 3, 4, 5}
	if n != 5 || !bytes.Equal(readBuf, expected) {
		t.Errorf("Expected %v, got %v", expected, readBuf)
	}
}

func BenchmarkReadSmall(b *testing.B) {
	ptr := allocTestBuffer(4 * 1024)
	defer freeTestBuffer(ptr)

	wWrite, _ := NewIOWrapper(ptr, 4*1024, 0, WriteOnly)
	_, _ = wWrite.Write(make([]byte, 4*1024))

	wRead, _ := NewIOWrapper(ptr, 4*1024, 0, ReadOnly)
	buf := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wRead.Read(buf)
		wRead.pos = 0
	}
}

func BenchmarkReadLarge(b *testing.B) {
	const dataSize = 4 * 1024 * 1024 // 4MB
	ptr := allocTestBuffer(dataSize)
	defer freeTestBuffer(ptr)

	wWrite, _ := NewIOWrapper(ptr, int64(dataSize), 0, WriteOnly)
	_, _ = wWrite.Write(make([]byte, dataSize))

	wRead, _ := NewIOWrapper(ptr, int64(dataSize), 0, ReadOnly)
	buf := make([]byte, dataSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wRead.Read(buf)
		wRead.pos = 0
	}
}

func BenchmarkWriteSmall(b *testing.B) {
	ptr := allocTestBuffer(4 * 1024)
	defer freeTestBuffer(ptr)

	wWrite, _ := NewIOWrapper(ptr, 4*1024, 0, WriteOnly)
	buf := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wWrite.Write(buf)
		wWrite.pos = 0
	}
}

func BenchmarkWriteLarge(b *testing.B) {
	const dataSize = 4 * 1024 * 1024 // 4MB
	ptr := allocTestBuffer(dataSize)
	defer freeTestBuffer(ptr)

	wWrite, _ := NewIOWrapper(ptr, int64(dataSize), 0, WriteOnly)
	buf := make([]byte, dataSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wWrite.Write(buf)
		wWrite.pos = 0
	}
}
