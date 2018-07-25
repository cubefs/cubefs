package fs

import (
	"syscall"
	"time"

	"github.com/tiglabs/baudstorage/fuse"
	"github.com/tiglabs/baudstorage/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/util/log"
)

type Dir struct {
	super *Super
	inode *Inode
}

//functions that Dir needs to implement
var (
	_ fs.Node                = (*Dir)(nil)
	_ fs.NodeCreater         = (*Dir)(nil)
	_ fs.NodeForgetter       = (*Dir)(nil)
	_ fs.NodeMkdirer         = (*Dir)(nil)
	_ fs.NodeRemover         = (*Dir)(nil)
	_ fs.NodeFsyncer         = (*Dir)(nil)
	_ fs.NodeRequestLookuper = (*Dir)(nil)
	_ fs.HandleReadDirAller  = (*Dir)(nil)
	_ fs.NodeRenamer         = (*Dir)(nil)

	//TODO: NodeSymlinker
)

func NewDir(s *Super, i *Inode) *Dir {
	return &Dir{
		super: s,
		inode: i,
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	log.LogDebugf("Attr: ino(%v)", d.inode.ino)

	inode, err := d.super.InodeGet(d.inode.ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", d.inode.ino, err.Error())
		return ParseError(err)
	}
	inode.fillAttr(a)
	d.inode = inode
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.LogDebugf("Create: parent(%v) name(%v)", d.inode.ino, req.Name)

	start := time.Now()

	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeRegular)
	if err != nil {
		log.LogErrorf("Create: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return nil, nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	inode.fillAttr(&resp.Attr)
	resp.Node = fuse.NodeID(inode.ino)
	sreader, err := d.super.ec.OpenForRead(inode.ino)
	if err != nil {
		log.LogErrorf("Create: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err)
		return nil, nil, fuse.EPERM
	}

	child := NewFile(d.super, inode)
	child.stream = sreader

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Create parent(%v) ino(%v) (%v)ns", d.inode.ino, inode.ino, elapsed.Nanoseconds())
	return child, child, nil
}

func (d *Dir) Forget() {
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	log.LogDebugf("Mkdir: ino(%v) name(%v)", d.inode.ino, req.Name)

	start := time.Now()

	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeDir)
	if err != nil {
		log.LogErrorf("Mkdir: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	child := NewDir(d.super, inode)

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Mkdir parent(%v) ino(%v) (%v)ns", d.inode.ino, inode.ino, elapsed.Nanoseconds())
	return child, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	log.LogDebugf("Remove: parent(%v) name(%v)", d.inode.ino, req.Name)

	start := time.Now()

	d.inode.dcache.Delete(req.Name)
	extents, err := d.super.mw.Delete_ll(d.inode.ino, req.Name)
	if err != nil {
		log.LogErrorf("Remove: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
		return ParseError(err)
	}
	//d.inode.dcache = nil

	if extents != nil {
		//log.LogDebugf("Remove extents: %v", extents)
		d.super.ec.Delete(extents)
	}

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Remove parent(%v) name(%v) (%v)ns", d.inode.ino, req.Name, elapsed.Nanoseconds())
	return nil
}

func (d *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	var (
		ino  uint64
		mode uint32
		err  error
	)

	log.LogDebugf("Lookup: parent(%v) name(%v)", d.inode.ino, req.Name)

	ino, ok := d.inode.dcache.Get(req.Name)
	if !ok {
		// if not found in the dentry cache, issue a lookup request
		ino, mode, err = d.super.mw.Lookup_ll(d.inode.ino, req.Name)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", d.inode.ino, req.Name, err.Error())
			}
			return nil, ParseError(err)
		}
	}

	inode, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Lookup: parent(%v) name(%v) ino(%v) err(%v)", d.inode.ino, req.Name, ino, err.Error())
		return nil, ParseError(err)
	}
	inode.fillAttr(&resp.Attr)
	mode = inode.mode

	var child fs.Node
	if mode == ModeDir {
		child = NewDir(d.super, inode)
	} else {
		child = NewFile(d.super, inode)
	}

	resp.Node = fuse.NodeID(ino)
	resp.EntryValid = LookupValidDuration
	return child, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.LogDebugf("ReadDir: ino(%v)", d.inode.ino)

	start := time.Now()

	children, err := d.super.mw.ReadDir_ll(d.inode.ino)
	if err != nil {
		log.LogErrorf("Readdir: ino(%v) err(%v)", d.inode.ino, err.Error())
		return make([]fuse.Dirent, 0), ParseError(err)
	}

	inodes := make([]uint64, 0, len(children))
	dirents := make([]fuse.Dirent, 0, len(children))
	dcache := NewDentryCache()

	for _, child := range children {
		dentry := fuse.Dirent{
			Inode: child.Inode,
			Type:  ParseMode(child.Type),
			Name:  child.Name,
		}
		inodes = append(inodes, child.Inode)
		dirents = append(dirents, dentry)
		dcache.Put(child.Name, child.Inode)
	}

	infos := d.super.mw.BatchInodeGet(inodes)
	for _, info := range infos {
		d.super.ic.Put(NewInode(info))
	}
	d.inode.dcache = dcache

	elapsed := time.Since(start)
	log.LogDebugf("PERF: ReadDir (%v)ns", elapsed.Nanoseconds())
	return dirents, nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*Dir)
	if !ok {
		return fuse.ENOTSUP
	}

	log.LogDebugf("Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v)", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName)

	start := time.Now()

	d.inode.dcache.Delete(req.OldName)
	extents, err := d.super.mw.Rename_ll(d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName)
	if err != nil {
		log.LogErrorf("Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v) err(%v)", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName, err.Error())
		return ParseError(err)
	}
	//d.inode.dcache = nil

	if extents != nil {
		d.super.ec.Delete(extents)
	}

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Rename srcIno(%v) oldName(%v) dstIno(%v) newName(%v) (%v)ns", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName, elapsed.Nanoseconds())
	return nil
}
