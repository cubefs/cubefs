package fs

import (
	"syscall"
	"time"

	"github.com/chubaoio/cbfs/fuse"
	"github.com/chubaoio/cbfs/fuse/fs"
	"golang.org/x/net/context"

	"github.com/chubaoio/cbfs/util/log"
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
	_ fs.NodeSetattrer       = (*Dir)(nil)
	_ fs.NodeSymlinker       = (*Dir)(nil)
)

func NewDir(s *Super, i *Inode) *Dir {
	return &Dir{
		super: s,
		inode: i,
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	inode, err := d.super.InodeGet(d.inode.ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", d.inode.ino, err)
		return ParseError(err)
	}
	inode.fillAttr(a)
	d.inode = inode
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	start := time.Now()
	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeRegular, nil)
	if err != nil {
		log.LogErrorf("Create: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err)
		return nil, nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	inode.fillAttr(&resp.Attr)
	child := NewFile(d.super, inode)
	d.super.ec.OpenForWrite(inode.ino)
	elapsed := time.Since(start)
	log.LogDebugf("PERF: Create parent(%v) ino(%v) (%v)ns", d.inode.ino, inode.ino, elapsed.Nanoseconds())
	return child, child, nil
}

func (d *Dir) Forget() {
	//inode := d.inode
	//log.LogDebugf("Forget: ino(%v)", inode.ino)

	//	start := time.Now()
	//
	//	d.super.ic.Delete(inode.ino) //FIXME
	//
	//	elapsed := time.Since(start)
	//	log.LogDebugf("PERF: Forget ino(%v) (%v)ns", inode.ino, elapsed.Nanoseconds())
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	//log.LogDebugf("Mkdir: ino(%v) name(%v)", d.inode.ino, req.Name)

	start := time.Now()

	info, err := d.super.mw.Create_ll(d.inode.ino, req.Name, ModeDir, nil)
	if err != nil {
		log.LogErrorf("Mkdir: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err)
		return nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	child := NewDir(d.super, inode)

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Mkdir parent(%v) ino(%v) name(%v) (%v)ns", d.inode.ino, inode.ino, req.Name, elapsed.Nanoseconds())
	return child, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	start := time.Now()
	d.inode.dcache.Delete(req.Name)
	err := d.super.mw.Delete_ll(d.inode.ino, req.Name)
	if err != nil {
		log.LogErrorf("Remove: ino(%v) name(%v) err(%v)", d.inode.ino, req.Name, err)
		return ParseError(err)
	}
	//d.inode.dcache = nil

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
		ino, mode, err = d.super.mw.Lookup_ll(d.inode.ino, req.Name)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", d.inode.ino, req.Name, err)
			}
			return nil, ParseError(err)
		}
	}

	inode, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Lookup: parent(%v) name(%v) ino(%v) err(%v)", d.inode.ino, req.Name, ino, err)
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

	resp.EntryValid = LookupValidDuration
	return child, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	start := time.Now()
	children, err := d.super.mw.ReadDir_ll(d.inode.ino)
	if err != nil {
		log.LogErrorf("Readdir: ino(%v) err(%v)", d.inode.ino, err)
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
	log.LogDebugf("PERF: ReadDir ino(%v) (%v)ns", d.inode.ino, elapsed.Nanoseconds())
	return dirents, nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*Dir)
	if !ok {
		return fuse.ENOTSUP
	}
	start := time.Now()
	d.inode.dcache.Delete(req.OldName)
	err := d.super.mw.Rename_ll(d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName)
	if err != nil {
		log.LogErrorf("Rename: srcIno(%v) oldName(%v) dstIno(%v) newName(%v) err(%v)", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName, err)
		return ParseError(err)
	}
	//d.inode.dcache = nil

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Rename srcIno(%v) oldName(%v) dstIno(%v) newName(%v) (%v)ns", d.inode.ino, req.OldName, dstDir.inode.ino, req.NewName, elapsed.Nanoseconds())
	return nil
}

func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := d.inode.ino
	start := time.Now()
	inode, err := d.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Setattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Mode() {
		inode.osMode = req.Mode
	}

	inode.fillAttr(&resp.Attr)

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Setattr ino(%v) req(%v) reqSize(%v) inodeSize(%v) (%v)ns", ino, req, req.Size, inode.size, elapsed.Nanoseconds())
	return nil
}

func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	parentIno := d.inode.ino
	start := time.Now()
	info, err := d.super.mw.Create_ll(parentIno, req.NewName, ModeSymlink, []byte(req.Target))
	if err != nil {
		log.LogErrorf("Symlink: parent(%v) NewName(%v) err(%v)", parentIno, req.NewName, err)
		return nil, ParseError(err)
	}

	inode := NewInode(info)
	d.super.ic.Put(inode)
	child := NewFile(d.super, inode)

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Symlink parent(%v) NewName(%v) Target(%v) ino(%v) (%v)ns", parentIno, req.NewName, req.Target, inode.ino, elapsed.Nanoseconds())
	return child, nil
}

func (d *Dir) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	var oldInode *Inode
	switch old := old.(type) {
	case *File:
		oldInode = old.inode
	default:
		return nil, fuse.EPERM
	}

	if oldInode.mode != ModeRegular {
		log.LogErrorf("Link: not regular, parent(%v) name(%v) ino(%v) mode(%v)", d.inode.ino, req.NewName, oldInode.ino, oldInode.mode)
		return nil, fuse.EPERM
	}

	start := time.Now()

	info, err := d.super.mw.Link(d.inode.ino, req.NewName, oldInode.ino)
	if err != nil {
		log.LogErrorf("Link: parent(%v) name(%v) ino(%v) err(%v)", d.inode.ino, req.NewName, oldInode.ino, err)
		return nil, ParseError(err)
	}

	newInode := NewInode(info)
	d.super.ic.Put(newInode)
	newFile := NewFile(d.super, newInode)

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Link parent(%v) name(%v) ino(%v) (%v)ns", d.inode.ino, req.NewName, newInode.ino, elapsed.Nanoseconds())
	return newFile, nil
}
