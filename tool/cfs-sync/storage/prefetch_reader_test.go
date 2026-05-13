package storage

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// virtualFile is a deterministic in-memory "file" used as the fetch source
// for prefetchReader tests. content[i] = i % 251 (a prime, so any chunk has
// distinguishable byte values).
type virtualFile struct {
	size int64
	// fetchDelay (atomically read) lets a test slow down fetches to expose
	// ordering / race issues that fast tests would miss.
	fetchDelay atomic.Int64 // nanoseconds
	// fetchErr (atomically swapped) injects an error at the next call.
	fetchErr atomic.Value // error or nil
	calls    atomic.Int64
}

func newVirtualFile(size int64) *virtualFile {
	return &virtualFile{size: size}
}

func (vf *virtualFile) byteAt(off int64) byte {
	return byte(off % 251)
}

func (vf *virtualFile) fetch(p []byte, off int64) (int, error) {
	vf.calls.Add(1)
	if d := vf.fetchDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	if v := vf.fetchErr.Load(); v != nil {
		if err, _ := v.(error); err != nil {
			return 0, err
		}
	}
	if off >= vf.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > vf.size {
		want = vf.size - off
	}
	for i := int64(0); i < want; i++ {
		p[i] = vf.byteAt(off + i)
	}
	if off+want == vf.size {
		return int(want), io.EOF
	}
	return int(want), nil
}

// expected returns the bytes a reader over [start, start+size) should produce.
func (vf *virtualFile) expected(start, size int64) []byte {
	out := make([]byte, size)
	for i := int64(0); i < size; i++ {
		out[i] = vf.byteAt(start + i)
	}
	return out
}

func TestPrefetchReader_FullSequentialRead(t *testing.T) {
	// 8 chunks of 64 KiB. Parallelism = 4 so we exercise the in-flight
	// window with a real sequence.
	const chunk = 64 * 1024
	const total = int64(8 * chunk)
	vf := newVirtualFile(total)

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 4)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := vf.expected(0, total)
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: len got=%d want=%d, first diff at %d",
			len(got), len(want), firstDiff(got, want))
	}
}

func TestPrefetchReader_TailNotAligned(t *testing.T) {
	// Total size deliberately not a multiple of chunk size.
	const chunk = 4096
	const total = int64(chunk*3 + 137)
	vf := newVirtualFile(total)

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 3)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if int64(len(got)) != total {
		t.Fatalf("read %d bytes, want %d", len(got), total)
	}
	if !bytes.Equal(got, vf.expected(0, total)) {
		t.Fatal("content mismatch on unaligned tail")
	}
}

func TestPrefetchReader_PartialReads(t *testing.T) {
	// Force the caller to receive bytes in small slices that don't align
	// with chunk boundaries — exercises the "current chunk has bytes left"
	// fast path repeatedly.
	const chunk = 8192
	const total = int64(64 * 1024)
	vf := newVirtualFile(total)

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 4)
	defer pr.Close()

	want := vf.expected(0, total)
	got := make([]byte, 0, total)
	buf := make([]byte, 333) // intentionally weird, non-aligned slice
	for {
		n, err := pr.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: first diff at %d", firstDiff(got, want))
	}
}

func TestPrefetchReader_WithStartOffset(t *testing.T) {
	// Reader exposes a sub-range of the source.
	const chunk = 1024
	const start = int64(5000)
	const size = int64(20000)
	vf := newVirtualFile(int64(1 << 20)) // 1 MiB underlying

	pr := newPrefetchReader(vf.fetch, start, size, chunk, 4)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, vf.expected(start, size)) {
		t.Fatalf("content mismatch with startOff=%d size=%d", start, size)
	}
}

func TestPrefetchReader_EmptyFile(t *testing.T) {
	vf := newVirtualFile(0)
	pr := newPrefetchReader(vf.fetch, 0, 0, 4096, 4)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll on empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d bytes from empty file, want 0", len(got))
	}
}

func TestPrefetchReader_FetchErrorPropagates(t *testing.T) {
	sentinel := errors.New("fetch boom")
	const chunk = 1024
	const total = int64(16 * chunk)
	vf := newVirtualFile(total)
	// Fail every fetch — first chunk's worker observes it and the consumer
	// must see the same sentinel.
	vf.fetchErr.Store(sentinel)

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 4)
	defer pr.Close()

	buf := make([]byte, 4096)
	n, err := pr.Read(buf)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Read err=%v want=%v (n=%d)", err, sentinel, n)
	}
	// Sticky: subsequent reads return the same error.
	if _, err2 := pr.Read(buf); !errors.Is(err2, sentinel) {
		t.Fatalf("second Read err=%v, want sticky %v", err2, sentinel)
	}
}

func TestPrefetchReader_ConcurrentClose(t *testing.T) {
	// Slow fetches keep workers in the middle of fetch() when Close fires.
	const chunk = 64 * 1024
	const total = int64(64 * chunk) // 4 MiB total
	vf := newVirtualFile(total)
	vf.fetchDelay.Store(int64(20 * time.Millisecond))

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 4)

	// Read a few bytes to start the pipeline.
	buf := make([]byte, 1024)
	if _, err := pr.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}

	done := make(chan struct{})
	go func() {
		// Close from another goroutine while workers are slow-fetching.
		// (We don't read concurrently — io.Reader contract is single
		// reader — but Close from anywhere is allowed.)
		time.Sleep(10 * time.Millisecond)
		_ = pr.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return — workers stuck?")
	}

	// Close is idempotent.
	if err := pr.Close(); err != nil {
		t.Fatalf("second Close returned %v", err)
	}
}

func TestPrefetchReader_OutOfOrderArrival(t *testing.T) {
	// Each worker gets a random delay so chunks arrive at the consumer
	// out of dispatch order. Reordering buffer must serve in seq order.
	const chunk = 1024
	const total = int64(64 * chunk)
	vf := newVirtualFile(total)
	// 0–5 ms jitter per fetch — enough to randomize completion order
	// without making the test slow.
	delays := []time.Duration{
		5 * time.Millisecond, 1 * time.Millisecond, 4 * time.Millisecond,
		2 * time.Millisecond, 3 * time.Millisecond,
	}
	var calls atomic.Int64
	fetch := func(p []byte, off int64) (int, error) {
		i := int(calls.Add(1)-1) % len(delays)
		time.Sleep(delays[i])
		return vf.fetch(p, off)
	}

	pr := newPrefetchReader(fetch, 0, total, chunk, 8)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, vf.expected(0, total)) {
		t.Fatalf("out-of-order arrivals broke ordering at byte %d",
			firstDiff(got, vf.expected(0, total)))
	}
}

func TestPrefetchReader_SingleChunkParallelismCapped(t *testing.T) {
	// Tiny file (1 chunk's worth) with requested parallelism = 8. The
	// reader should still produce correct output even though the in-flight
	// window is effectively 1.
	const chunk = 2048
	const total = int64(chunk - 7) // < 1 chunk
	vf := newVirtualFile(total)

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 8)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, vf.expected(0, total)) {
		t.Fatal("single-chunk file content mismatch")
	}
}

func TestPrefetchReader_ZeroLengthReadCall(t *testing.T) {
	vf := newVirtualFile(1024)
	pr := newPrefetchReader(vf.fetch, 0, 1024, 512, 2)
	defer pr.Close()

	// Read(zero-length) is a valid no-op per io.Reader.
	n, err := pr.Read(nil)
	if err != nil || n != 0 {
		t.Fatalf("Read(nil) = (%d, %v), want (0, nil)", n, err)
	}
	n, err = pr.Read(make([]byte, 0))
	if err != nil || n != 0 {
		t.Fatalf("Read(empty) = (%d, %v), want (0, nil)", n, err)
	}
	// Subsequent normal read still works.
	buf := make([]byte, 64)
	if _, err := pr.Read(buf); err != nil {
		t.Fatalf("Read after zero-length: %v", err)
	}
}

func TestPrefetchReader_CloseBeforeRead(t *testing.T) {
	// Close immediately, then Read should not panic and should return
	// a non-nil error (terminated state).
	vf := newVirtualFile(1024)
	pr := newPrefetchReader(vf.fetch, 0, 1024, 256, 4)
	if err := pr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	buf := make([]byte, 64)
	_, err := pr.Read(buf)
	if err == nil {
		t.Fatal("Read after Close returned nil err, want non-nil")
	}
}

func TestPrefetchReader_FetchShortRead(t *testing.T) {
	// A fetch that returns fewer bytes than requested with err=nil — the
	// worker must loop until the chunk is filled or the source signals
	// EOF. This guarantees no gaps in the output.
	vf := newVirtualFile(int64(16 * 1024))
	wrapped := func(p []byte, off int64) (int, error) {
		// Cap effective length to half of what the worker asked for, but
		// never less than 1 byte.
		half := len(p) / 2
		if half < 1 {
			half = len(p)
		}
		return vf.fetch(p[:half], off)
	}
	pr := newPrefetchReader(wrapped, 0, vf.size, 4096, 4)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	// With io.ReadFull-style worker loop, output must equal the full
	// source — short reads cause additional fetch calls, not gaps.
	if int64(len(got)) != vf.size {
		t.Fatalf("got %d bytes, want %d (worker should refill on short fetch)",
			len(got), vf.size)
	}
	if !bytes.Equal(got, vf.expected(0, vf.size)) {
		t.Fatalf("short-read content mismatch at byte %d",
			firstDiff(got, vf.expected(0, vf.size)))
	}
}

func TestPrefetchReader_FetchReturnsZeroWithNilErr(t *testing.T) {
	// Defensive: a buggy fetch that returns (0, nil) must not spin
	// forever. The worker should bail out with a sentinel error.
	called := int64(0)
	wrapped := func(p []byte, off int64) (int, error) {
		atomic.AddInt64(&called, 1)
		return 0, nil
	}
	pr := newPrefetchReader(wrapped, 0, 4096, 4096, 1)
	defer pr.Close()
	_, err := io.ReadAll(pr)
	if err == nil {
		t.Fatal("expected error from (0, nil) fetch, got nil")
	}
	// Sanity: didn't spin a million times before bailing.
	if c := atomic.LoadInt64(&called); c > 100 {
		t.Errorf("fetch called %d times; expected bailout much sooner", c)
	}
}

func TestPrefetchReader_LargerFile(t *testing.T) {
	// Stress with a non-trivial size: 32 MiB, 4 MiB chunks, 4 workers.
	const chunk = 4 * 1024 * 1024
	const total = int64(32 * 1024 * 1024)
	vf := newVirtualFile(total)

	pr := newPrefetchReader(vf.fetch, 0, total, chunk, 4)
	defer pr.Close()

	// Verify content with a streaming compare to avoid 64 MiB of test
	// memory.
	hashOff := int64(0)
	buf := make([]byte, 1<<17) // 128 KiB
	for {
		n, err := pr.Read(buf)
		for i := 0; i < n; i++ {
			if buf[i] != vf.byteAt(hashOff+int64(i)) {
				t.Fatalf("byte %d: got %d want %d", hashOff+int64(i),
					buf[i], vf.byteAt(hashOff+int64(i)))
			}
		}
		hashOff += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if hashOff != total {
		t.Fatalf("read %d bytes, want %d", hashOff, total)
	}
}

// firstDiff returns the index of the first differing byte, or -1.
func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}
