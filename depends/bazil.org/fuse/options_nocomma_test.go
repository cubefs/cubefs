// This file contains tests for platforms that have no escape
// mechanism for including commas in mount options.
//
//go:build darwin
// +build darwin

package fuse_test

import (
	"runtime"
	"testing"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs/fstestutil"
)

func TestMountOptionCommaError(t *testing.T) {
	t.Parallel()
	// this test is not tied to any specific option, it just needs
	// some string content
	var evil = "FuseTest,Marker"
	mnt, err := fstestutil.MountedT(t, fstestutil.SimpleFS{fstestutil.Dir{}}, nil,
		fuse.ForTestSetMountOption("fusetest", evil),
	)
	if err == nil {
		mnt.Close()
		t.Fatal("expected an error about commas")
	}
	if g, e := err.Error(), `mount options cannot contain commas on `+runtime.GOOS+`: "fusetest"="FuseTest,Marker"`; g != e {
		t.Fatalf("wrong error: %q != %q", g, e)
	}
}
