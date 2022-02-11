package fuse_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

////////////////////////////////////////////////////////////////////////
// minimalFS
////////////////////////////////////////////////////////////////////////

// A minimal fuseutil.FileSystem that can successfully mount but do nothing
// else.
type minimalFS struct {
	fuseutil.NotImplementedFileSystem
}

func (fs *minimalFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return
}

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func TestSuccessfulMount(t *testing.T) {
	ctx := context.Background()

	// Set up a temporary directory.
	dir, err := ioutil.TempDir("", "mount_test")
	if err != nil {
		t.Fatal("ioutil.TempDir: %v", err)
	}

	defer os.RemoveAll(dir)

	// Mount.
	fs := &minimalFS{}
	mfs, err := fuse.Mount(
		dir,
		fuseutil.NewFileSystemServer(fs),
		&fuse.MountConfig{})

	if err != nil {
		t.Fatalf("fuse.Mount: %v", err)
	}

	defer func() {
		if err := mfs.Join(ctx); err != nil {
			t.Errorf("Joining: %v", err)
		}
	}()

	defer fuse.Unmount(mfs.Dir())
}

func TestNonEmptyMountPoint(t *testing.T) {
	ctx := context.Background()

	// osxfuse appears to be happy to mount over a non-empty mount point.
	//
	// We leave this test in for Linux, because it tickles the behavior of
	// fusermount writing to stderr and exiting with an error code. We want to
	// make sure that a descriptive error makes it back to the user.
	if runtime.GOOS == "darwin" {
		return
	}

	// Set up a temporary directory.
	dir, err := ioutil.TempDir("", "mount_test")
	if err != nil {
		t.Fatal("ioutil.TempDir: %v", err)
	}

	defer os.RemoveAll(dir)

	// Add a file within it.
	err = ioutil.WriteFile(path.Join(dir, "foo"), []byte{}, 0600)
	if err != nil {
		t.Fatalf("ioutil.WriteFile: %v", err)
	}

	// Attempt to mount.
	fs := &minimalFS{}
	mfs, err := fuse.Mount(
		dir,
		fuseutil.NewFileSystemServer(fs),
		&fuse.MountConfig{})

	if err == nil {
		fuse.Unmount(mfs.Dir())
		mfs.Join(ctx)
		t.Fatal("fuse.Mount returned nil")
	}

	const want = "not empty"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Errorf("Unexpected error: %v", got)
	}
}

func TestNonexistentMountPoint(t *testing.T) {
	ctx := context.Background()

	// Set up a temporary directory.
	dir, err := ioutil.TempDir("", "mount_test")
	if err != nil {
		t.Fatal("ioutil.TempDir: %v", err)
	}

	defer os.RemoveAll(dir)

	// Attempt to mount into a sub-directory that doesn't exist.
	fs := &minimalFS{}
	mfs, err := fuse.Mount(
		path.Join(dir, "foo"),
		fuseutil.NewFileSystemServer(fs),
		&fuse.MountConfig{})

	if err == nil {
		fuse.Unmount(mfs.Dir())
		mfs.Join(ctx)
		t.Fatal("fuse.Mount returned nil")
	}

	const want = "no such file"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Errorf("Unexpected error: %v", got)
	}
}
