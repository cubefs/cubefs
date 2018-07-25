package fs

import (
	"fmt"

	"github.com/tiglabs/baudstorage/fuse"
	"github.com/tiglabs/baudstorage/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/sdk/data/stream"
	"github.com/tiglabs/baudstorage/sdk/meta"
	"github.com/tiglabs/baudstorage/util/log"
)

type Super struct {
	cluster string
	volname string
	ic      *InodeCache
	mw      *meta.MetaWrapper
	ec      *stream.ExtentClient
}

//functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

func NewSuper(volname, master string, bufferSize uint64) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(volname, master)
	if err != nil {
		log.LogErrorf("NewMetaWrapper failed! %v", err.Error())
		return nil, err
	}

	s.ec, err = stream.NewExtentClient(volname, master, s.mw.AppendExtentKey, s.mw.GetExtents, bufferSize)
	if err != nil {
		log.LogErrorf("NewExtentClient failed! %v", err.Error())
		return nil, err
	}

	s.volname = volname
	s.cluster = s.mw.Cluster()
	s.ic = NewInodeCache(DefaultInodeExpiration, MaxInodeCache)
	log.LogInfof("NewSuper: cluster(%v) volname(%v)", s.cluster, s.volname)
	return s, nil
}

func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(RootInode)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	total, used := s.mw.Statfs()
	resp.Blocks = total / uint64(DefaultBlksize)
	resp.Bfree = (total - used) / uint64(DefaultBlksize)
	resp.Bavail = resp.Bfree
	resp.Bsize = DefaultBlksize
	resp.Namelen = DefaultMaxNameLen
	resp.Frsize = DefaultBlksize
	return nil
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%s_fuseclient_%s", s.cluster, act)
}
