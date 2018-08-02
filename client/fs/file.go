package fs

import (
	"io"
	"time"

	"github.com/chubaoio/cbfs/fuse"
	"github.com/chubaoio/cbfs/fuse/fs"
	"golang.org/x/net/context"

	"github.com/chubaoio/cbfs/sdk/data/stream"
	"github.com/chubaoio/cbfs/util/log"
	"sync"
)

type File struct {
	super  *Super
	inode  *Inode
	stream *stream.StreamReader
	sync.RWMutex
}

//functions that File needs to implement
var (
	_ fs.Node           = (*File)(nil)
	_ fs.Handle         = (*File)(nil)
	_ fs.NodeForgetter  = (*File)(nil)
	_ fs.NodeOpener     = (*File)(nil)
	_ fs.HandleReleaser = (*File)(nil)
	_ fs.HandleReader   = (*File)(nil)
	_ fs.HandleWriter   = (*File)(nil)
	_ fs.HandleFlusher  = (*File)(nil)
	_ fs.NodeFsyncer    = (*File)(nil)
	_ fs.NodeSetattrer  = (*File)(nil)
	_ fs.NodeReadlinker = (*File)(nil)
)

func (f *File) getReadStream() (r *stream.StreamReader) {
	f.RLock()
	defer f.RUnlock()
	return f.stream
}

func (f *File) setReadStream(stream *stream.StreamReader) {
	f.Lock()
	defer f.Unlock()
	f.stream = stream
}

func NewFile(s *Super, i *Inode) *File {
	return &File{super: s, inode: i}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := f.inode.ino
	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}
	inode.fillAttr(a)
	log.LogDebugf("TRACE Attr: inode(%v)", inode)
	return nil
}

func (f *File) Forget() {
	if f.inode.mode != ModeRegular {
		return
	}
	ino := f.inode.ino
	f.super.ic.Delete(ino)
	extents := f.super.mw.Evict(ino)
	if extents != nil {
		f.super.ec.Delete(extents) //FIXME: metanode would take over in the future
	}
	log.LogDebugf("TRACE Forget: ino(%v) extents(%v)", ino, extents)
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	ino := f.inode.ino
	start := time.Now()
	err = f.super.mw.Open_ll(ino)
	if err != nil {
		f.super.ic.Delete(ino)
		log.LogErrorf("Open: ino(%v) req(%v) err(%v)", ino, req, ParseError(err))
		return nil, ParseError(err)
	}

	if req.Flags.IsWriteOnly() || req.Flags.IsReadWrite() {
		f.super.ec.OpenForWrite(ino)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Open: ino(%v) flags(%v) (%v)ns", ino, req.Flags, elapsed.Nanoseconds())
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	ino := f.inode.ino
	start := time.Now()
	if req.Flags.IsWriteOnly() || req.Flags.IsReadWrite() {
		err = f.super.ec.CloseForWrite(ino)
		if err != nil {
			log.LogErrorf("Release: ino(%v) req(%v) err(%v)", ino, req, err)
			return fuse.EIO
		}
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Release: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	if f.getReadStream() == nil {
		stream, err := f.super.ec.OpenForRead(f.inode.ino)
		if err != nil {
			log.LogErrorf("Open for Read: ino(%v) err(%v)", f.inode.ino, err)
			return fuse.EPERM
		}
		f.setReadStream(stream)
	}
	start := time.Now()
	size, err := f.super.ec.Read(f.getReadStream(), f.inode.ino, resp.Data[fuse.OutHeaderSize:], int(req.Offset), req.Size)
	if err != nil && err != io.EOF {
		log.LogErrorf("Read: ino(%v) req(%v) err(%v) size(%v)", f.inode.ino, req, err, size)
		return fuse.EIO
	}
	if size > req.Size {
		log.LogErrorf("Read: ino(%v) req(%v) size(%v)", f.inode.ino, req, size)
		return fuse.ERANGE
	}
	if size > 0 {
		resp.Data = resp.Data[:size+fuse.OutHeaderSize]
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Read: ino(%v) req(%v) size(%v) (%v)ns", f.inode.ino, req, size, elapsed.Nanoseconds())
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	reqlen := len(req.Data)
	if uint64(req.Offset) > f.inode.size && reqlen == 1 {
		// The fuse package is probably doing truncate size up, which is not supported yet. So Just return.
		log.LogDebugf("Write: not support, ino(%v) req(%v)", f.inode.ino, req)
		return nil
	}

	defer func() {
		f.super.ic.Delete(f.inode.ino)
	}()

	start := time.Now()
	size, err := f.super.ec.Write(f.inode.ino, int(req.Offset), req.Data)
	if err != nil {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) err(%v)", f.inode.ino, req.Offset, reqlen, err)
		return fuse.EIO
	}
	resp.Size = size
	if size != reqlen {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) size(%v)", f.inode.ino, req.Offset, reqlen, size)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Write: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v) (%v)ns ",
		f.inode.ino, req.Offset, reqlen, req.Flags, req.FileFlags, elapsed.Nanoseconds())
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	start := time.Now()
	err = f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Flush: ino(%v) err(%v)", f.inode.ino, err)
		return fuse.EIO
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Flush: ino(%v) (%v)ns", f.inode.ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	start := time.Now()
	err = f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Fsync: ino(%v) err(%v)", f.inode.ino, err)
		return fuse.EIO
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Fsync: ino(%v) (%v)ns", f.inode.ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := f.inode.ino
	start := time.Now()
	if req.Valid.Size() && req.Size == 0 {
		extents, err := f.super.mw.Truncate(ino)
		if err != nil {
			log.LogErrorf("Setattr: truncate ino(%v) err(%v)", ino, err)
			return ParseError(err)
		}

		f.super.ic.Delete(ino)

		if extents != nil {
			f.super.ec.Delete(extents)
		}
	}

	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Setattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Size() {
		if req.Size != inode.size {
			log.LogWarnf("Setattr: truncate ino(%v) reqSize(%v) inodeSize(%v)", ino, req.Size, inode.size)
		}
	}

	if req.Valid.Mode() {
		inode.osMode = req.Mode
	}

	inode.fillAttr(&resp.Attr)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Setattr: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	ino := f.inode.ino
	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Readlink: ino(%v) err(%v)", ino, err)
		return "", ParseError(err)
	}
	log.LogDebugf("TRACE Readlink: ino(%v) target(%v)", ino, string(inode.target))
	return string(inode.target), nil
}
