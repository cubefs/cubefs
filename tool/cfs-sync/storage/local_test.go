package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
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
