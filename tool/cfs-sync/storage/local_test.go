package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalStorage_List_Empty(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocal(dir)
	if err != nil {
		t.Fatal(err)
	}
	objs, errc := s.List(context.Background(), "")
	var got []string
	for o := range objs {
		got = append(got, o.Key)
	}
	if err := <-errc; err != nil {
		t.Fatalf("list error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty dir should return no objects, got %v", got)
	}
}

func TestLocalStorage_List_Files(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "c.txt", "c")
	writeFile(t, dir, "a.txt", "a")
	writeFile(t, dir, "b.txt", "b")

	s, _ := NewLocal(dir)
	objs, errc := s.List(context.Background(), "")
	var keys []string
	for o := range objs {
		if !o.IsDir {
			keys = append(keys, o.Key)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	// Must be in lexicographic order.
	want := []string{"a.txt", "b.txt", "c.txt"}
	if !equalSlices(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
}

func TestLocalStorage_List_Subdirs(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "sub/file.txt", "data")

	s, _ := NewLocal(dir)
	objs, errc := s.List(context.Background(), "")
	var files, dirs []string
	for o := range objs {
		if o.IsDir {
			dirs = append(dirs, o.Key)
		} else {
			files = append(files, o.Key)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0] != "sub/" {
		t.Errorf("dirs = %v, want [sub/]", dirs)
	}
	if len(files) != 1 || files[0] != "sub/file.txt" {
		t.Errorf("files = %v, want [sub/file.txt]", files)
	}
}

func TestLocalStorage_List_NonExistentDir(t *testing.T) {
	s, _ := NewLocal("/tmp")
	objs, errc := s.List(context.Background(), "this-path-does-not-exist-xyz")
	for range objs {
	}
	// Should not send an error for a missing directory.
	if err := <-errc; err != nil {
		t.Errorf("unexpected error for missing dir: %v", err)
	}
}

func TestLocalStorage_GetPut(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocal(dir)

	content := []byte("hello storage")
	if err := s.Put(context.Background(), "subdir/file.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// File should exist on disk.
	data, err := os.ReadFile(filepath.Join(dir, "subdir", "file.txt"))
	if err != nil {
		t.Fatalf("file not on disk: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("on-disk content = %q, want %q", data, content)
	}

	// Get should return the same content.
	rc, err := s.Get(context.Background(), "subdir/file.txt", 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, content) {
		t.Errorf("Get content = %q, want %q", got, content)
	}
}

func TestLocalStorage_Get_WithOffset(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.bin", "0123456789")
	s, _ := NewLocal(dir)

	rc, err := s.Get(context.Background(), "data.bin", 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "3456" {
		t.Errorf("Get(off=3,size=4) = %q, want %q", got, "3456")
	}
}

func TestLocalStorage_Get_FullFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "hello.txt", "full content")
	s, _ := NewLocal(dir)

	rc, err := s.Get(context.Background(), "hello.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "full content" {
		t.Errorf("got %q, want %q", got, "full content")
	}
}

func TestLocalStorage_Delete(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "todelete.txt", "bye")
	s, _ := NewLocal(dir)

	if err := s.Delete(context.Background(), "todelete.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "todelete.txt")); !os.IsNotExist(err) {
		t.Error("file should not exist after Delete")
	}
}

func TestLocalStorage_Delete_Missing(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocal(dir)

	err := s.Delete(context.Background(), "missing.txt")
	if err == nil {
		t.Error("expected error deleting non-existent file")
	}
}

func TestLocalStorage_MkdirAll(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocal(dir)

	if err := s.MkdirAll(context.Background(), "a/b/c"); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "a", "b", "c"))
	if err != nil {
		t.Fatalf("dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected a directory")
	}
}

func TestLocalStorage_String(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocal(dir)
	if s.String() == "" {
		t.Error("String() should not be empty")
	}
}

func TestLocalStorage_List_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 50; i++ {
		writeFile(t, dir, filepath.Join("sub", string(rune('a'+i%26))+".txt"), "x")
	}

	s, _ := NewLocal(dir)
	ctx, cancel := context.WithCancel(context.Background())
	objs, errc := s.List(ctx, "")

	// Read one object then cancel.
	<-objs
	cancel()

	// Drain remaining — should not block forever.
	for range objs {
	}
	<-errc
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// recordingReader wraps a bytes.Reader and remembers every Read call size.
// Used to assert PutWithMtime requests 4 MiB chunks, not the io.Copy default
// (32 KiB via *os.File.ReadFrom → genericReadFrom).
type recordingReader struct {
	r        *bytes.Reader
	callSize []int
}

func (rr *recordingReader) Read(p []byte) (int, error) {
	rr.callSize = append(rr.callSize, len(p))
	return rr.r.Read(p)
}

// largestCall returns the largest Read length recorded. EOF can shrink the
// final call, but all interior calls should be the full buffer.
func (rr *recordingReader) largestCall() int {
	max := 0
	for _, c := range rr.callSize {
		if c > max {
			max = c
		}
	}
	return max
}

func TestLocalStorage_PutWithMtime_Uses4MiBBuffer(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocal(dir)

	// 16 MiB of deterministic content — at least 4 full 4 MiB Read calls.
	content := make([]byte, 16*1024*1024)
	for i := range content {
		content[i] = byte(i)
	}

	rr := &recordingReader{r: bytes.NewReader(content)}
	if err := s.PutWithMtime(context.Background(), "big.bin", rr, int64(len(content)), time.Time{}); err != nil {
		t.Fatalf("PutWithMtime: %v", err)
	}

	// Confirm on-disk bytes match (correctness invariant on top of the
	// buffer-size optimisation — must never regress).
	got, err := os.ReadFile(filepath.Join(dir, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("on-disk content mismatch: len=%d want=%d", len(got), len(content))
	}

	// The optimisation lives in PutWithMtime's copy loop: caller-facing
	// Read should be invoked with the full buffer size (4 MiB).
	if largest := rr.largestCall(); largest != localPutBufSize {
		t.Errorf("largest Read call = %d, want %d (the 4 MiB optimisation regressed)",
			largest, localPutBufSize)
	}

	// At 16 MiB content / 4 MiB chunks we expect 4 full reads + 1 EOF read.
	// Don't pin exactly — buffer changes shouldn't break the test — but make
	// sure we didn't fall back to the old 32 KiB path which would produce
	// hundreds of calls.
	if n := len(rr.callSize); n > 16 {
		t.Errorf("too many Read calls (%d); 4 MiB buffer should keep this small", n)
	}
}

// failingReader returns sentinel error after returning n bytes.
type failingReader struct {
	data []byte
	pos  int
	fail error
	at   int
}

func (fr *failingReader) Read(p []byte) (int, error) {
	if fr.pos >= fr.at {
		return 0, fr.fail
	}
	n := copy(p, fr.data[fr.pos:fr.at])
	fr.pos += n
	return n, nil
}

// failingWriter returns sentinel error after accepting n bytes.
type failingWriter struct {
	written int
	limit   int
	fail    error
}

func (fw *failingWriter) Write(p []byte) (int, error) {
	if fw.written >= fw.limit {
		return 0, fw.fail
	}
	allow := fw.limit - fw.written
	if allow > len(p) {
		allow = len(p)
	}
	fw.written += allow
	if allow < len(p) {
		return allow, fw.fail
	}
	return allow, nil
}

func TestCopyWithBuffer_RoundTrip(t *testing.T) {
	src := bytes.Repeat([]byte("abcdefgh"), 1024*1024) // 8 MiB
	var dst bytes.Buffer
	if err := copyWithBuffer(&dst, bytes.NewReader(src), 4*1024*1024); err != nil {
		t.Fatalf("copyWithBuffer: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), src) {
		t.Fatalf("dst != src (lens %d vs %d)", dst.Len(), len(src))
	}
}

func TestCopyWithBuffer_SmallerThanBuffer(t *testing.T) {
	// Source much smaller than buffer — single read returns everything,
	// then EOF on the next read.
	src := []byte("short")
	var dst bytes.Buffer
	if err := copyWithBuffer(&dst, bytes.NewReader(src), 4*1024*1024); err != nil {
		t.Fatalf("copyWithBuffer: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), src) {
		t.Fatalf("dst %q != src %q", dst.String(), src)
	}
}

func TestCopyWithBuffer_PropagatesReadError(t *testing.T) {
	sentinel := errors.New("read boom")
	fr := &failingReader{data: []byte("12345"), at: 5, fail: sentinel}
	err := copyWithBuffer(io.Discard, fr, 1024)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
}

func TestCopyWithBuffer_PropagatesWriteError(t *testing.T) {
	sentinel := errors.New("write boom")
	src := bytes.NewReader(bytes.Repeat([]byte("x"), 10000))
	fw := &failingWriter{limit: 100, fail: sentinel}
	err := copyWithBuffer(fw, src, 4096)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
}

func TestCopyWithBuffer_ReadReturnsDataPlusEOF(t *testing.T) {
	// Some readers return (n, io.EOF) in one call. Must commit the bytes
	// before returning success.
	r := &dataPlusEOFReader{data: []byte("final-bytes")}
	var dst bytes.Buffer
	if err := copyWithBuffer(&dst, r, 4096); err != nil {
		t.Fatalf("copyWithBuffer: %v", err)
	}
	if dst.String() != "final-bytes" {
		t.Fatalf("dst = %q, want %q (bytes from a (n, io.EOF) Read were dropped)",
			dst.String(), "final-bytes")
	}
}

type dataPlusEOFReader struct {
	data []byte
	done bool
}

func (d *dataPlusEOFReader) Read(p []byte) (int, error) {
	if d.done {
		return 0, io.EOF
	}
	n := copy(p, d.data)
	d.done = true
	return n, io.EOF
}

// Smoke: PutWithMtime forwards Write errors. Use a fake source that yields
// content and write target on disk under a restricted dir — actually easier
// to just verify the success path here, since PutWithMtime delegates the
// guts to copyWithBuffer (covered above).
func TestLocalStorage_PutWithMtime_NewDirsCreated(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocal(dir)

	if err := s.PutWithMtime(context.Background(), "a/b/c/file.bin",
		bytes.NewReader([]byte("payload")), 7, time.Time{}); err != nil {
		t.Fatalf("PutWithMtime: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "a", "b", "c", "file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "payload" {
		t.Fatalf("got %q", got)
	}
}
