// Copyright 2018 The Chubao Authors.
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
	"io"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

// used to locate the position in parent
type DirContext struct {
	Name string
}

type DirContexts struct {
	sync.RWMutex
	dirCtx map[fuse.HandleID]*DirContext
}

func NewDirContexts() (dctx *DirContexts) {
	dctx = &DirContexts{}
	dctx.dirCtx = make(map[fuse.HandleID]*DirContext, 0)
	return
}

func (dctx *DirContexts) GetCopy(handle fuse.HandleID) DirContext {
	dctx.RLock()
	dirCtx, found := dctx.dirCtx[handle]
	dctx.RUnlock()

	if found {
		return DirContext{dirCtx.Name}
	} else {
		return DirContext{}
	}
}

func (dctx *DirContexts) Put(handle fuse.HandleID, dirCtx *DirContext) {
	dctx.Lock()
	defer dctx.Unlock()

	oldCtx, found := dctx.dirCtx[handle]
	if found {
		oldCtx.Name = dirCtx.Name
		return
	}

	dctx.dirCtx[handle] = dirCtx
}

func (dctx *DirContexts) Remove(handle fuse.HandleID) {
	dctx.Lock()
	delete(dctx.dirCtx, handle)
	dctx.Unlock()
}

// Dir defines the structure of a directory
type Dir struct {
	super  *Super
	info   *proto.InodeInfo
	dcache *DentryCache
	dctx   *DirContexts
}

// Functions that Dir needs to implement
var (
	_ fs.Node                = (*Dir)(nil)
	_ fs.NodeCreater         = (*Dir)(nil)
	_ fs.NodeForgetter       = (*Dir)(nil)
	_ fs.NodeMkdirer         = (*Dir)(nil)
	_ fs.NodeMknoder         = (*Dir)(nil)
	_ fs.NodeRemover         = (*Dir)(nil)
	_ fs.NodeFsyncer         = (*Dir)(nil)
	_ fs.NodeRequestLookuper = (*Dir)(nil)
	_ fs.HandleReadDirAller  = (*Dir)(nil)
	_ fs.NodeRenamer         = (*Dir)(nil)
	_ fs.NodeSetattrer       = (*Dir)(nil)
	_ fs.NodeSymlinker       = (*Dir)(nil)
	_ fs.NodeGetxattrer      = (*Dir)(nil)
	_ fs.NodeListxattrer     = (*Dir)(nil)
	_ fs.NodeSetxattrer      = (*Dir)(nil)
	_ fs.NodeRemovexattrer   = (*Dir)(nil)
)

// NewDir returns a new directory.
func NewDir(s *Super, i *proto.InodeInfo) fs.Node {
	return &Dir{
		super: s,
		info:  i,
		dctx:  NewDirContexts(),
	}
}

// Attr set the attributes of a directory.
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := d.info.Inode
	info, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}
	fillAttr(info, a)
	log.LogDebugf("TRACE Attr: inode(%v)", info)
	return nil
}

func (d *Dir) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	d.dctx.Remove(req.Handle)
	return nil
}

// Create handles the create request.
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	start := time.Now()

	var err error
	metric := exporter.NewTPCnt("filecreate")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	info, err := d.super.mw.Create_ll(d.info.Inode, req.Name, proto.Mode(req.Mode.Perm()), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Create: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return nil, nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewFile(d.super, info, d.info.Inode)
	d.super.ec.OpenStream(info.Inode)

	d.super.fslock.Lock()
	d.super.nodeCache[info.Inode] = child
	d.super.fslock.Unlock()

	if d.super.keepCache {
		resp.Flags |= fuse.OpenKeepCache
	}
	resp.EntryValid = LookupValidDuration

	d.super.ic.Delete(d.info.Inode)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Create: parent(%v) req(%v) resp(%v) ino(%v) (%v)ns", d.info.Inode, req, resp, info.Inode, elapsed.Nanoseconds())
	return child, child, nil
}

// Forget is called when the evict is invoked from the kernel.
func (d *Dir) Forget() {
	ino := d.info.Inode
	defer func() {
		log.LogDebugf("TRACE Forget: ino(%v)", ino)
	}()

	d.super.ic.Delete(ino)

	d.super.fslock.Lock()
	delete(d.super.nodeCache, ino)
	d.super.fslock.Unlock()
}

// Mkdir handles the mkdir request.
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	start := time.Now()

	var err error
	metric := exporter.NewTPCnt("mkdir")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	info, err := d.super.mw.Create_ll(d.info.Inode, req.Name, proto.Mode(os.ModeDir|req.Mode.Perm()), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Mkdir: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewDir(d.super, info)

	d.super.fslock.Lock()
	d.super.nodeCache[info.Inode] = child
	d.super.fslock.Unlock()

	d.super.ic.Delete(d.info.Inode)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Mkdir: parent(%v) req(%v) ino(%v) (%v)ns", d.info.Inode, req, info.Inode, elapsed.Nanoseconds())
	return child, nil
}

// Remove handles the remove request.
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	start := time.Now()
	d.dcache.Delete(req.Name)

	var err error
	metric := exporter.NewTPCnt("remove")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	info, err := d.super.mw.Delete_ll(d.info.Inode, req.Name, req.Dir)
	if err != nil {
		log.LogErrorf("Remove: parent(%v) name(%v) err(%v)", d.info.Inode, req.Name, err)
		return ParseError(err)
	}

	d.super.ic.Delete(d.info.Inode)

	if info != nil && info.Nlink == 0 && !proto.IsDir(info.Mode) {
		d.super.orphan.Put(info.Inode)
		log.LogDebugf("Remove: add to orphan inode list, ino(%v)", info.Inode)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Remove: parent(%v) req(%v) inode(%v) (%v)ns", d.info.Inode, req, info, elapsed.Nanoseconds())
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
		ino, _, err = d.super.mw.Lookup_ll(d.info.Inode, req.Name)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", d.info.Inode, req.Name, err)
			}
			return nil, ParseError(err)
		}
	}

	info, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Lookup: parent(%v) name(%v) ino(%v) err(%v)", d.info.Inode, req.Name, ino, err)
		dummyInodeInfo := &proto.InodeInfo{Inode: ino}
		dummyChild := NewFile(d.super, dummyInodeInfo, d.info.Inode)
		return dummyChild, nil
	}
	mode := proto.OsMode(info.Mode)

	d.super.fslock.Lock()
	child, ok := d.super.nodeCache[ino]
	if !ok {
		if mode.IsDir() {
			child = NewDir(d.super, info)
		} else {
			child = NewFile(d.super, info, d.info.Inode)
		}
		d.super.nodeCache[ino] = child
	}
	d.super.fslock.Unlock()

	resp.EntryValid = LookupValidDuration
	return child, nil
}

func (d *Dir) ReadDir(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) ([]fuse.Dirent, error) {
	var err error
	var limit uint64 = DefaultReaddirLimit
	start := time.Now()

	dirCtx := d.dctx.GetCopy(req.Handle)
	if d.super.noReaddirLimit {
		if dirCtx.Name != "" {
			return nil, io.EOF
		}
		if subdirs, err := d.ReadDirAll(ctx); err != nil {
			return nil, err
		} else {
			// return EOF to ask fuse stop readdir more
			dirCtx.Name = subdirs[len(subdirs)-1].Name
			d.dctx.Put(req.Handle, &dirCtx)
			return subdirs, io.EOF
		}
	}

	children, err := d.super.mw.ReadDirLimit_ll(d.info.Inode, dirCtx.Name, limit)
	if err != nil {
		log.LogErrorf("readdirlimit: Readdir: ino(%v) err(%v) try ReadDirAll", d.info.Inode, err)
		if err == syscall.ENOSYS || err == syscall.EIO {
			log.LogInfof("readdirlimit: not implemented set NoReaddirLimit as true")
			d.super.noReaddirLimit = true
			// return direct, fuse will regard this as needing readdir more
			return nil, nil
		}
		return make([]fuse.Dirent, 0), ParseError(err)
	}

	// skip the first one, which is already accessed
	childrenNr := uint64(len(children))
	if childrenNr == 0 || (dirCtx.Name != "" && childrenNr == 1) {
		return make([]fuse.Dirent, 0), io.EOF
	} else if childrenNr < limit {
		err = io.EOF
	}
	if dirCtx.Name != "" {
		children = children[1:]
	}

	/* update dirCtx */
	dirCtx.Name = children[len(children)-1].Name
	d.dctx.Put(req.Handle, &dirCtx)

	inodes := make([]uint64, 0, len(children))
	dirents := make([]fuse.Dirent, 0, len(children))

	dcache := d.dcache
	if !d.super.disableDcache {
		dcache = NewDentryCache()
		d.dcache = dcache
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

	infos := d.super.mw.BatchInodeGet(inodes)
	for _, info := range infos {
		d.super.ic.Put(info)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE ReadDir: ino(%v) (%v)ns", d.info.Inode, elapsed.Nanoseconds())
	return dirents, err
}

// ReadDirAll gets all the dentries in a directory and puts them into the cache.
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	start := time.Now()

	var err error
	metric := exporter.NewTPCnt("readdir")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	children, err := d.super.mw.ReadDir_ll(d.info.Inode)
	if err != nil {
		log.LogErrorf("Readdir: ino(%v) err(%v)", d.info.Inode, err)
		return make([]fuse.Dirent, 0), ParseError(err)
	}

	inodes := make([]uint64, 0, len(children))
	dirents := make([]fuse.Dirent, 0, len(children))

	var dcache *DentryCache
	if !d.super.disableDcache {
		dcache = NewDentryCache()
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

	infos := d.super.mw.BatchInodeGet(inodes)
	for _, info := range infos {
		d.super.ic.Put(info)
	}
	d.dcache = dcache

	elapsed := time.Since(start)
	log.LogDebugf("TRACE ReadDir: ino(%v) (%v)ns", d.info.Inode, elapsed.Nanoseconds())
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
	metric := exporter.NewTPCnt("rename")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	err = d.super.mw.Rename_ll(d.info.Inode, req.OldName, dstDir.info.Inode, req.NewName)
	if err != nil {
		log.LogErrorf("Rename: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return ParseError(err)
	}

	d.super.ic.Delete(d.info.Inode)
	d.super.ic.Delete(dstDir.info.Inode)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Rename: SrcParent(%v) OldName(%v) DstParent(%v) NewName(%v) (%v)ns", d.info.Inode, req.OldName, dstDir.info.Inode, req.NewName, elapsed.Nanoseconds())
	return nil
}

// Setattr handles the setattr request.
func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := d.info.Inode
	start := time.Now()
	info, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Setattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if valid := setattr(info, req); valid != 0 {
		err = d.super.mw.Setattr(ino, valid, info.Mode, info.Uid, info.Gid, info.AccessTime.Unix(),
			info.ModifyTime.Unix())
		if err != nil {
			d.super.ic.Delete(ino)
			return ParseError(err)
		}
	}

	fillAttr(info, &resp.Attr)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Setattr: ino(%v) req(%v) inodeSize(%v) (%v)ns", ino, req, info.Size, elapsed.Nanoseconds())
	return nil
}

func (d *Dir) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
	if (req.Mode&os.ModeNamedPipe == 0 && req.Mode&os.ModeSocket == 0) || req.Rdev != 0 {
		return nil, fuse.ENOSYS
	}

	start := time.Now()

	var err error
	metric := exporter.NewTPCnt("mknod")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	info, err := d.super.mw.Create_ll(d.info.Inode, req.Name, proto.Mode(req.Mode), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Mknod: parent(%v) req(%v) err(%v)", d.info.Inode, req, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewFile(d.super, info, d.info.Inode)

	d.super.fslock.Lock()
	d.super.nodeCache[info.Inode] = child
	d.super.fslock.Unlock()

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Mknod: parent(%v) req(%v) ino(%v) (%v)ns", d.info.Inode, req, info.Inode, elapsed.Nanoseconds())
	return child, nil
}

// Symlink handles the symlink request.
func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	parentIno := d.info.Inode
	start := time.Now()

	var err error
	metric := exporter.NewTPCnt("symlink")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	info, err := d.super.mw.Create_ll(parentIno, req.NewName, proto.Mode(os.ModeSymlink|os.ModePerm), req.Uid, req.Gid, []byte(req.Target))
	if err != nil {
		log.LogErrorf("Symlink: parent(%v) NewName(%v) err(%v)", parentIno, req.NewName, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)
	child := NewFile(d.super, info, d.info.Inode)

	d.super.fslock.Lock()
	d.super.nodeCache[info.Inode] = child
	d.super.fslock.Unlock()

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Symlink: parent(%v) req(%v) ino(%v) (%v)ns", parentIno, req, info.Inode, elapsed.Nanoseconds())
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
	metric := exporter.NewTPCnt("link")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: d.super.volname})
	}()

	info, err := d.super.mw.Link(d.info.Inode, req.NewName, oldInode.Inode)
	if err != nil {
		log.LogErrorf("Link: parent(%v) name(%v) ino(%v) err(%v)", d.info.Inode, req.NewName, oldInode.Inode, err)
		return nil, ParseError(err)
	}

	d.super.ic.Put(info)

	d.super.fslock.Lock()
	newFile, ok := d.super.nodeCache[info.Inode]
	if !ok {
		newFile = NewFile(d.super, info, d.info.Inode)
		d.super.nodeCache[info.Inode] = newFile
	}
	d.super.fslock.Unlock()

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Link: parent(%v) name(%v) ino(%v) (%v)ns", d.info.Inode, req.NewName, info.Inode, elapsed.Nanoseconds())
	return newFile, nil
}

// Getxattr has not been implemented yet.
func (d *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	if !d.super.enableXattr {
		return fuse.ENOSYS
	}
	ino := d.info.Inode
	name := req.Name
	size := req.Size
	pos := req.Position

	var value []byte
	var info *proto.XAttrInfo
	var err error

	if name == proto.SummaryKey {

		var summaryInfo proto.SummaryInfo
		cacheSummaryInfo := d.super.sc.Get(ino)
		if cacheSummaryInfo != nil {
			summaryInfo = *cacheSummaryInfo
		} else {
			summaryInfo, err = d.super.mw.GetSummary_ll(ino, name, 10)
			if err != nil {
				log.LogErrorf("GetXattr: ino(%v) name(%v) err(%v)", ino, name, err)
				return ParseError(err)
			}
			d.super.sc.Put(ino, &summaryInfo)
		}

		files := summaryInfo.Files
		subdirs := summaryInfo.Subdirs
		fbytes := summaryInfo.Fbytes
		summaryStr := "Files:" + strconv.FormatInt(int64(files), 10) + "," +
			"Dirs:" + strconv.FormatInt(int64(subdirs), 10) + "," +
			"Bytes:" + strconv.FormatInt(int64(fbytes), 10)
		value = []byte(summaryStr)

	} else {
		info, err = d.super.mw.XAttrGet_ll(ino, name)
		if err != nil {
			log.LogErrorf("GetXattr: ino(%v) name(%v) err(%v)", ino, name, err)
			return ParseError(err)
		}
		value = info.Get(name)
	}

	if pos > 0 {
		value = value[pos:]
	}
	if size > 0 && size < uint32(len(value)) {
		value = value[:size]
	}
	resp.Xattr = value
	log.LogDebugf("TRACE GetXattr: ino(%v) name(%v)", ino, name)
	return nil
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
