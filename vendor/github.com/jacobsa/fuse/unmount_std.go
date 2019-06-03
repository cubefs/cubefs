// +build !linux

package fuse

import (
	"os"
	"syscall"
)

func unmount(dir string) (err error) {
	err = syscall.Unmount(dir, 0)
	if err != nil {
		err = &os.PathError{Op: "unmount", Path: dir, Err: err}
		return
	}

	return
}
