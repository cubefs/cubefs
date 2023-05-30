// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package fs

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"

	"golang.org/x/net/context"
)

// Dir defines the structure of a directory
type Dir struct {
	super  *Super
	info   *proto.InodeInfo
	dcache *cache.DentryCache
}

// Functions that Dir needs to implement
var (
	_ fs.Node                   = (*Dir)(nil)
	_ fs.NodeCreater            = (*Dir)(nil)
	_ fs.NodeForgetter          = (*Dir)(nil)
	_ fs.NodeMkdirer            = (*Dir)(nil)
	_ fs.NodeMknoder            = (*Dir)(nil)
	_ fs.NodeRemover            = (*Dir)(nil)
	_ fs.NodeFsyncer            = (*Dir)(nil)
	_ fs.NodeRequestLookuper    = (*Dir)(nil)
	_ fs.HandleReadDirAller     = (*Dir)(nil)
	_ fs.HandleReadDirPlusAller = (*Dir)(nil)
	_ fs.NodeRenamer            = (*Dir)(nil)
	_ fs.NodeSetattrer          = (*Dir)(nil)
	_ fs.NodeSymlinker          = (*Dir)(nil)
	_ fs.NodeGetxattrer         = (*Dir)(nil)
	_ fs.NodeListxattrer        = (*Dir)(nil)
	_ fs.NodeSetxattrer         = (*Dir)(nil)
	_ fs.NodeRemovexattrer      = (*Dir)(nil)
)

// NewDir returns a new directory.
func NewDir(s *Super, i *proto.InodeInfo) fs.Node {
	return &Dir{
		super: s,
		info:  i,
	}
}

// Attr set the attributes of a directory.
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {

	ino := d.info.Inode
	info, err := d.super.InodeGet(ctx, ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}
	fillAttr(info, a)
	log.LogDebugf("TRACE Attr: inode(%v)", info)
	return nil
}

func (d *Dir) NodeID() uint64 {
	return d.info.Inode
}

func (d *Dir) Mode() uint32 {
	return d.info.Mode
}

// Create handles the create request.
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("filecreate")
	defer tpObject.Set(err)

	info, err := d.super.mw.Create_ll(ctx, d.info.Inode, req.Name, proto.Mode(req.Mode.Perm()), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Create: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return nil, nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewFile(d.super, info)
	d.super.ec.OpenStream(info.Inode, false, false)

	if d.super.keepCache {
		resp.Flags |= fuse.OpenKeepCache
	}
	resp.EntryValid = LookupValidDuration

	d.super.ic.Delete(ctx, d.info.Inode)

	log.LogDebugf("TRACE Create: parent(%v) req(%v) resp(%v) ino(%v) time(%v)", d.info.Inode, req, resp, info.Inode, time.Since(start))
	return child, child, nil
}

// Forget is called when the evict is invoked from the kernel.
func (d *Dir) Forget() {

	ino := d.info.Inode
	defer func() {
		log.LogDebugf("TRACE Forget: ino(%v)", ino)
	}()

	d.super.ic.Delete(nil, ino)
}

// Mkdir handles the mkdir request.
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("mkdir")
	defer tpObject.Set(err)

	info, err := d.super.mw.Create_ll(ctx, d.info.Inode, req.Name, proto.Mode(os.ModeDir|req.Mode.Perm()), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Mkdir: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewDir(d.super, info)

	d.super.ic.Delete(ctx, d.info.Inode)

	log.LogDebugf("TRACE Mkdir: parent(%v) req(%v) ino(%v) time(%v)", d.info.Inode, req, info.Inode, time.Since(start))
	return child, nil
}

// Remove handles the remove request.
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	tpObject := exporter.NewVolumeTP("Remove", d.super.volname)
	defer func() {
		tpObject.Set(err)
	}()
	tpObject1 := exporter.NewModuleTP("remove")
	defer func() {
		tpObject1.Set(err)
	}()

	if len(d.super.delProcessPath) > 0 {
		delProcPath, errStat := os.Readlink(fmt.Sprintf("/proc/%v/exe", req.Pid))
		if errStat != nil || !contains(d.super.delProcessPath, delProcPath) {
			log.LogErrorf("Remove: pid(%v) process(%v) is not permitted err(%v), parent(%v) name(%v)", req.Pid, delProcPath, errStat, d.info.Inode, req.Name)
			return fuse.EPERM
		}
		log.LogDebugf("Remove: allow process pid(%v) path(%v) to delete file, parent(%v) name(%v)", req.Pid, delProcPath, d.info.Inode, req.Name)
	}

	start := time.Now()
	d.dcache.Delete(req.Name)

	info, syserr := d.super.mw.Delete_ll(ctx, d.info.Inode, req.Name, req.Dir)
	if syserr != nil {
		log.LogErrorf("Remove: parent(%v) name(%v) err(%v)", d.info.Inode, req.Name, syserr)
		//if errors.Is(err, syscall.EIO) {
		if syserr == syscall.EIO {
			msg := fmt.Sprintf("parent(%v) name(%v) err(%v)", d.info.Inode, req.Name, syserr)
			d.super.handleError("Remove", msg)
		}
		err = ParseError(syserr)
		return
	}

	d.super.ic.Delete(ctx, d.info.Inode)

	if info != nil && info.Nlink == 0 && !proto.IsDir(info.Mode) {
		d.super.orphan.Put(info.Inode)
		log.LogDebugf("Remove: add to orphan inode list, ino(%v)", info.Inode)
	}

	log.LogDebugf("TRACE Remove: parent(%v) req(%v) inode(%v) time(%v)", d.info.Inode, req, info, time.Since(start))
	return nil
}

func (d *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

// Lookup handles the lookup request.
func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	var (
		ino uint64
		err error
	)

	log.LogDebugf("TRACE Lookup: parent(%v) req(%v)", d.info.Inode, req)

	ino, ok := d.dcache.Get(req.Name)
	if !ok {
		ino, _, err = d.super.mw.Lookup_ll(ctx, d.info.Inode, req.Name)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", d.info.Inode, req.Name, err)
			}
			return nil, ParseError(err)
		}
	}

	info, err := d.super.InodeGet(ctx, ino)
	if err != nil {
		log.LogErrorf("Lookup: parent(%v) name(%v) ino(%v) err(%v)", d.info.Inode, req.Name, ino, err)
		return nil, ParseError(err)
	}
	mode := proto.OsMode(info.Mode)

	var child fs.Node
	if mode.IsDir() {
		child = NewDir(d.super, info)
	} else {
		child = NewFile(d.super, info)
	}

	resp.EntryValid = LookupValidDuration
	return child, nil
}

// ReadDirAll gets all the dentries in a directory and puts them into the cache.
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("readdir")
	defer tpObject.Set(err)

	children, err := d.super.mw.ReadDir_ll(ctx, d.info.Inode)
	if err != nil {
		log.LogErrorf("Readdir: ino(%v) err(%v)", d.info.Inode, err)
		return make([]fuse.Dirent, 0), ParseError(err)
	}

	inodes := make([]uint64, 0, len(children))
	dirents := make([]fuse.Dirent, 0, len(children))

	var dcache *cache.DentryCache
	if !d.super.disableDcache {
		dcache = cache.NewDentryCache(DentryValidDuration, true)
	}

	for _, child := range children {
		dentry := fuse.Dirent{
			Inode: child.Inode,
			Type:  ParseType(child.Type),
			Name:  child.Name,
		}
		inodes = append(inodes, child.Inode)
		dirents = append(dirents, dentry)
		dcache.Put(child.Name, child.Inode)
	}

	// batch get inode info is only useful when using stat/fstat to all files, or in shell ls command
	if !d.super.noBatchGetInodeOnReaddir {
		infos := d.super.mw.BatchInodeGet(ctx, inodes)
		for _, info := range infos {
			d.super.ic.Put(info)
		}
	}
	d.dcache = dcache

	log.LogDebugf("TRACE ReadDir: ino(%v) children count(%v) time(%v)", d.info.Inode, len(children), time.Since(start))
	return dirents, nil
}

// ReadDirPlusAll gets all the dentries and their information in a directory and puts them into the cache.
func (d *Dir) ReadDirPlusAll(ctx context.Context, resp *fuse.ReadDirPlusResponse) ([]*fs.DirentPlus, error) {
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("readdirplus")
	defer tpObject.Set(err)

	children, err := d.super.mw.ReadDir_ll(ctx, d.info.Inode)
	if err != nil {
		log.LogErrorf("ReaddirPlus: ino(%v) err(%v)", d.info.Inode, err)
		return make([]*fs.DirentPlus, 0), ParseError(err)
	}

	inodes := make([]uint64, 0, len(children))
	for _, child := range children {
		inodes = append(inodes, child.Inode)
	}
	infos := d.super.mw.BatchInodeGet(ctx, inodes)
	infoMap := make(map[uint64]*proto.InodeInfo, len(infos))
	for _, info := range infos {
		d.super.ic.Put(info)
		infoMap[info.Inode] = info
	}

	var dcache *cache.DentryCache
	if !d.super.disableDcache {
		dcache = cache.NewDentryCache(DentryValidDuration, true)
	}
	dirents := make([]*fs.DirentPlus, 0, len(children))
	for _, child := range children {
		dentryPlus := &fs.DirentPlus{}
		dentryPlus.Dirent = fuse.Dirent{
			Inode: child.Inode,
			Type:  ParseType(child.Type),
			Name:  child.Name,
		}
		info, exist := infoMap[child.Inode]
		if exist {
			mode := proto.OsMode(info.Mode)
			if mode.IsDir() {
				dentryPlus.Node = NewDir(d.super, info)
			} else {
				dentryPlus.Node = NewFile(d.super, info)
			}
		}
		dirents = append(dirents, dentryPlus)
		dcache.Put(child.Name, child.Inode)
	}

	d.dcache = dcache
	resp.EntryValid = LookupValidDuration

	log.LogDebugf("TRACE ReadDirPlus: ino(%v) resp(%v) children count(%v) time(%v)", d.info.Inode, resp, len(dirents), time.Since(start))
	return dirents, nil
}

// Rename handles the rename request.
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*Dir)
	if !ok {
		log.LogErrorf("Rename: NOT DIR, parent(%v) req(%v)", d.info.Inode, req)
		return fuse.ENOTSUP
	}
	start := time.Now()
	d.dcache.Delete(req.OldName)

	var err error
	tpObject := exporter.NewModuleTP("rename")
	defer tpObject.Set(err)

	err = d.super.mw.Rename_ll(ctx, d.info.Inode, req.OldName, dstDir.info.Inode, req.NewName, false)
	if err != nil {
		log.LogErrorf("Rename: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return ParseError(err)
	}

	d.super.ic.Delete(ctx, d.info.Inode)
	d.super.ic.Delete(ctx, dstDir.info.Inode)

	log.LogDebugf("TRACE Rename: SrcParent(%v) OldName(%v) DstParent(%v) NewName(%v) time(%v)", d.info.Inode, req.OldName, dstDir.info.Inode, req.NewName, time.Since(start))
	return nil
}

// Setattr handles the setattr request.
func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := d.info.Inode
	start := time.Now()
	info, err := d.super.InodeGet(ctx, ino)
	if err != nil {
		log.LogErrorf("Setattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if valid := setattr(info, req); valid != 0 {
		err = d.super.mw.Setattr(ctx, ino, valid, info.Mode, info.Uid, info.Gid, info.AccessTime.Unix(),
			info.ModifyTime.Unix())
		if err != nil {
			d.super.ic.Delete(ctx, ino)
			return ParseError(err)
		}
	}

	fillAttr(info, &resp.Attr)

	log.LogDebugf("TRACE Setattr: ino(%v) req(%v) inodeSize(%v) time(%v)", ino, req, info.Size, time.Since(start))
	return nil
}

func (d *Dir) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {

	if (req.Mode&os.ModeNamedPipe == 0 && req.Mode&os.ModeSocket == 0) || req.Rdev != 0 {
		return nil, fuse.ENOSYS
	}

	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("mknod")
	defer tpObject.Set(err)

	info, err := d.super.mw.Create_ll(ctx, d.info.Inode, req.Name, proto.Mode(req.Mode), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Mknod: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewFile(d.super, info)

	log.LogDebugf("TRACE Mknod: parent(%v) req(%v) ino(%v) time(%v)", d.info.Inode, req, info.Inode, time.Since(start))
	return child, nil
}

// Symlink handles the symlink request.
func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {

	parentIno := d.info.Inode
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("symlink")
	defer tpObject.Set(err)

	info, err := d.super.mw.Create_ll(ctx, parentIno, req.NewName, proto.Mode(os.ModeSymlink|os.ModePerm), req.Uid, req.Gid, []byte(req.Target))
	if err != nil {
		log.LogErrorf("Symlink: parent(%v) NewName(%v) err(%v)", parentIno, req.NewName, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewFile(d.super, info)

	log.LogDebugf("TRACE Symlink: parent(%v) req(%v) ino(%v) time(%v)", parentIno, req, info.Inode, time.Since(start))
	return child, nil
}

// Link handles the link request.
func (d *Dir) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {

	var oldInode *proto.InodeInfo
	switch old := old.(type) {
	case *File:
		oldInode = old.info
	default:
		return nil, fuse.EPERM
	}

	if !proto.IsRegular(oldInode.Mode) {
		log.LogErrorf("Link: not regular, parent(%v) name(%v) ino(%v) mode(%v)", d.info.Inode, req.NewName, oldInode.Inode, proto.OsMode(oldInode.Mode))
		return nil, fuse.EPERM
	}

	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTP("link")
	defer tpObject.Set(err)

	info, err := d.super.mw.Link(ctx, d.info.Inode, req.NewName, oldInode.Inode)
	if err != nil {
		log.LogErrorf("Link: parent(%v) name(%v) ino(%v) err(%v)", d.info.Inode, req.NewName, oldInode.Inode, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)

	newFile := NewFile(d.super, info)

	log.LogDebugf("TRACE Link: parent(%v) name(%v) ino(%v) time(%v)", d.info.Inode, req.NewName, info.Inode, time.Since(start))
	return newFile, nil
}

// Getxattr has not been implemented yet.
func (d *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ENOSYS
}

// Listxattr has not been implemented yet.
func (d *Dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ENOSYS
}

// Setxattr has not been implemented yet.
func (d *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ENOSYS
}

// Removexattr has not been implemented yet.
func (d *Dir) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ENOSYS
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}
