package fs

import (
	"syscall"
	"time"

	"github.com/tiglabs/baudstorage/fuse"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	RootInode = proto.RootIno
)

const (
	DIR_NLINK_DEFAULT     = 2
	REGULAR_NLINK_DEFAULT = 1
)

const (
	DefaultBlksize    = uint32(1) << 12
	DefaultMaxNameLen = uint32(256)
)

const (
	ModeRegular = proto.ModeRegular
	ModeDir     = proto.ModeDir
)

const (
	LookupValidDuration = 30 * time.Second
	AttrValidDuration   = 600 * time.Second
)

const (
	DefaultInodeExpiration = 120 * time.Second
	MaxInodeCache          = 10000000
)

const (
	DeleteExtentsTimeout = 600 * time.Second
)

func ParseError(err error) fuse.Errno {
	switch v := err.(type) {
	case syscall.Errno:
		return fuse.Errno(v)
	case fuse.Errno:
		return v
	default:
		return fuse.ENOSYS
	}
}

func ParseMode(mode uint32) fuse.DirentType {
	switch mode {
	case ModeDir:
		return fuse.DT_Dir
	default:
		return fuse.DT_File
	}
}
