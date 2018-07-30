package storage

import (
	"syscall"
)

const (
	FALLOC_FL_KEEP_SIZE  = 1
	FALLOC_FL_PUNCH_HOLE = 2
)

func (e *fsExtent) tryKeepSize(fd int, off int64, len int64) (err error) {
	err = syscall.Fallocate(fd, FALLOC_FL_KEEP_SIZE, off, len)
	return
}

func (e *fsExtent) tryPunchHole(fd int, off int64, len int64) (err error) {
	err = syscall.Fallocate(fd, FALLOC_FL_PUNCH_HOLE, off, len)
	return
}
