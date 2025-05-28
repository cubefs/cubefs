package flashnode

import "github.com/cubefs/cubefs/proto"

type MetaWrapper interface {
	Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error)
	InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
	ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
	GetExtents(inode uint64, isCache, openForWrite, isMigration bool) (gen uint64, size uint64, extents []proto.ExtentKey, err error)
	Close() error
}

type ExtentApi interface {
	OpenStream(inode uint64, openForWrite, isCache bool, fullPath string) error
	CloseStream(inode uint64) error
	Read(inode uint64, data []byte, offset int, size int, storageClass uint32, isMigration bool) (read int, err error)
	Flush(inode uint64) error
	Close() error
	ForceRefreshExtentsCache(inode uint64) error
}
