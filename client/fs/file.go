package fs

import (
	"io"
	"time"

	"github.com/tiglabs/baudstorage/fuse"
	"github.com/tiglabs/baudstorage/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudstorage/sdk/data/stream"
	"github.com/tiglabs/baudstorage/util/log"
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

	//TODO:HandleReadAller
)

func (f *File)getReadStream()(r *stream.StreamReader) {
	f.RLock()
	defer f.RUnlock()
	return f.stream
}

func (f *File)setReadStream(stream *stream.StreamReader){
	f.Lock()
	defer f.Unlock()
	f.stream=stream
}


func NewFile(s *Super, i *Inode) *File {
	return &File{super: s, inode: i}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := f.inode.ino
	log.LogDebugf("Attr: ino(%v)", ino)
	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err.Error())
		return ParseError(err)
	}
	inode.fillAttr(a)
	return nil
}

func (f *File) Forget() {
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	ino := f.inode.ino
	log.LogDebugf("Open: ino(%v)", ino)

	defer func() {
		if r := recover(); r != nil {
			log.LogWarn("Recovered from Open: ", r)
			err = fuse.EIO
			return
		}
	}()

	start := time.Now()
	err = f.super.mw.Open_ll(ino)
	if err != nil {
		f.super.ic.Delete(ino)
		return nil, ParseError(err)
	}
	elapsed := time.Since(start)
	log.LogDebugf("PERF: Open ino(%v) (%v)ns", ino, elapsed.Nanoseconds())
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	ino := f.inode.ino
	log.LogDebugf("Close: ino(%v)", ino)

	defer func() {
		if r := recover(); r != nil {
			log.LogWarn("Recovered from Release: ", r)
			err = fuse.EIO
			return
		}
	}()

	start := time.Now()
	err = f.super.ec.Close(ino)
	if err != nil {
		log.LogErrorf("Close: ino(%v) error (%v)", ino, err)
		return fuse.EIO
	}
	f.stream = nil

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Close ino(%v) (%v)ns", ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	log.LogDebugf("Read: HandleID(%v) ino(%v) sizeof Data(%v) offset(%v) size(%v)", req.Handle, f.inode.ino, len(resp.Data), req.Offset, req.Size)
	defer func() {
		if r := recover(); r != nil {
			log.LogWarn("Recovered from Read: ", r)
			err = fuse.EIO
			return
		}
	}()
	if f.getReadStream() ==nil {
		stream, err := f.super.ec.OpenForRead(f.inode.ino)
		if err != nil {
			log.LogErrorf("Open: ino(%v) err(%v)", f.inode.ino, err)
			return fuse.EPERM
		}
		f.setReadStream(stream)
	}
	start := time.Now()
	size, err := f.super.ec.Read(f.getReadStream(), f.inode.ino, resp.Data[fuse.OutHeaderSize:], int(req.Offset), req.Size)
	if err != nil && err != io.EOF {
		log.LogErrorf("Read error: ino(%v) err(%v) size(%v)", f.inode.ino, err, size)
		return fuse.EIO
	}
	if size > req.Size {
		log.LogErrorf("Read error: ino(%v) request size(%v) read size(%v)", f.inode.ino, req.Size, size)
		return fuse.ERANGE
	}
	if size > 0 {
		resp.Data = resp.Data[:size+fuse.OutHeaderSize]
	}

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Read ino(%v) size(%v) (%v)ns", f.inode.ino, size, elapsed.Nanoseconds())
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	reqlen := len(req.Data)
	if uint64(req.Offset) > f.inode.size && reqlen == 1 {
		// Workaround: the fuse package is probably doing truncate size up
		// if we reach here, which is not supported yet. So Just return.
		log.LogDebugf("Write: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v)", f.inode.ino, req.Offset, reqlen, req.Flags, req.FileFlags)
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("Recovered from Write: ino(%v) offset(%v) len(%v) r(%v)", f.inode.ino, req.Offset, reqlen, r)
			err = fuse.EIO
			return
		}
		// Invalidate inode cache
		f.super.ic.Delete(f.inode.ino)
	}()

	log.LogDebugf("Write: ino(%v) HandleID(%v) offset(%v) len(%v) flags(%v) fileflags(%v)", f.inode.ino, req.Handle, req.Offset, reqlen, req.Flags, req.FileFlags)

	start := time.Now()
	size, err := f.super.ec.Write(f.inode.ino, int(req.Offset), req.Data)
	if err != nil {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) err(%v)", f.inode.ino, req.Offset, reqlen, err)
		return fuse.EIO
	}
	resp.Size = size
	if size > reqlen {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) size(%v)", f.inode.ino, req.Offset, reqlen, size)
	}

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Write ino(%v) size(%v) (%v)ns ", f.inode.ino, size, elapsed.Nanoseconds())
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	log.LogDebugf("Flush: ino(%v) HandleID(%v)", f.inode.ino, req.Handle)

	defer func() {
		if r := recover(); r != nil {
			log.LogWarn("Recovered from Flush: ", r)
			err = fuse.EIO
			return
		}
	}()

	start := time.Now()
	err = f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Flush error (%v)", err)
		return fuse.EIO
	}

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Flush ino(%v) (%v)ns", f.inode.ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	log.LogDebugf("Fsync: ino(%v) HandleID(%v)", f.inode.ino, req.Handle)

	start := time.Now()

	defer func() {
		if r := recover(); r != nil {
			log.LogWarn("Recovered from Fsync: ", r)
			err = fuse.EIO
			return
		}
	}()

	err = f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Fsync error (%v)", err)
		return fuse.EIO
	}
	elapsed := time.Since(start)
	log.LogDebugf("PERF: Fsync ino(%v) (%v)ns", f.inode.ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := f.inode.ino
	log.LogDebugf("Setattr: ino(%v) size(%v)", ino, req.Size)

	start := time.Now()

	if req.Size == 0 {
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

	if req.Size != inode.size {
		log.LogWarnf("Setattr: ino(%v) reqSize(%v) inodeSize(%v)", ino, req.Size, inode.size)
	}

	inode.fillAttr(&resp.Attr)

	elapsed := time.Since(start)
	log.LogDebugf("PERF: Setattr ino(%v) reqSize(%v) inodeSize(%v) (%v)ns", ino, req.Size, inode.size, elapsed.Nanoseconds())
	return nil
}
