// Copyright 2018 The Containerfs Authors.
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
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"sync"
)

type File struct {
	super *Super
	inode *Inode
	sync.RWMutex
}

//functions that File needs to implement
var (
	_ fs.Node              = (*File)(nil)
	_ fs.Handle            = (*File)(nil)
	_ fs.NodeForgetter     = (*File)(nil)
	_ fs.NodeOpener        = (*File)(nil)
	_ fs.HandleReleaser    = (*File)(nil)
	_ fs.HandleReader      = (*File)(nil)
	_ fs.HandleWriter      = (*File)(nil)
	_ fs.HandleFlusher     = (*File)(nil)
	_ fs.NodeFsyncer       = (*File)(nil)
	_ fs.NodeSetattrer     = (*File)(nil)
	_ fs.NodeReadlinker    = (*File)(nil)
	_ fs.NodeGetxattrer    = (*File)(nil)
	_ fs.NodeListxattrer   = (*File)(nil)
	_ fs.NodeSetxattrer    = (*File)(nil)
	_ fs.NodeRemovexattrer = (*File)(nil)
)

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
	fileSize, gen := f.super.ec.GetFileSize(ino)
	log.LogDebugf("Attr: ino(%v) fileSize(%v) gen(%v) inode.gen(%v)", ino, fileSize, gen, inode.gen)
	if gen >= inode.gen {
		a.Size = uint64(fileSize)
	}

	log.LogDebugf("TRACE Attr: inode(%v) attr(%v)", inode, a)
	return nil
}

func (f *File) Forget() {
	ino := f.inode.ino
	defer func() {
		log.LogDebugf("TRACE Forget: ino(%v)", ino)
	}()

	if stream := f.super.ec.GetStreamer(ino); stream != nil {
		return
	}

	if !f.super.orphan.Evict(ino) {
		return
	}

	if err := f.super.mw.Evict(ino); err != nil {
		log.LogErrorf("Forget Evict: ino(%v) err(%v)", ino, err)
	}
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	ino := f.inode.ino
	start := time.Now()
	err = f.super.ec.OpenStream(ino)
	if err != nil {
		return nil, fuse.EIO
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Open: ino(%v) req(%v) resp(%v) (%v)ns", ino, req, resp, elapsed.Nanoseconds())
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	ino := f.inode.ino
	log.LogDebugf("TRACE Release enter: ino(%v) req(%v)", ino, req)

	start := time.Now()

	log.LogDebugf("TRACE Release close stream: ino(%v) req(%v)", ino, req)
	err = f.super.ec.CloseStream(ino)
	if err != nil {
		log.LogErrorf("Release: close writer failed, ino(%v) req(%v) err(%v)", ino, req, err)
		return fuse.EIO
	}

	f.super.ic.Delete(ino)
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Release: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	log.LogDebugf("TRACE Read enter: ino(%v) offset(%v) reqsize(%v) req(%v)", f.inode.ino, req.Offset, req.Size, req)
	start := time.Now()
	size, err := f.super.ec.Read(f.inode.ino, resp.Data[fuse.OutHeaderSize:], int(req.Offset), req.Size)
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
	log.LogDebugf("TRACE Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v) (%v)ns", f.inode.ino, req.Offset, req.Size, req, size, elapsed.Nanoseconds())
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	ino := f.inode.ino
	reqlen := len(req.Data)
	filesize, _ := f.super.ec.GetFileSize(ino)

	log.LogDebugf("TRACE Write enter: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v) req(%v)", ino, req.Offset, reqlen, req.Flags, req.FileFlags, req)

	if req.FileFlags.IsWriteOnly() && req.Offset > int64(filesize) && reqlen == 1 {
		// workaround: posix_fallocate would write 1 byte if fallocate is not supported.
		err = f.super.ec.Truncate(ino, int(req.Offset)+reqlen)
		if err == nil {
			f.super.ic.Delete(ino)
			f.super.ec.RefreshExtentsCache(ino)
			resp.Size = reqlen
		}

		//f.super.ec.SetFileSize(ino, int(req.ExtentOffset)+reqlen)
		//resp.Size = reqlen
		log.LogDebugf("fallocate: ino(%v) origFilesize(%v) req(%v) err(%v)", f.inode.ino, filesize, req, err)
		return
	}

	defer func() {
		f.super.ic.Delete(ino)
	}()

	start := time.Now()
	size, err := f.super.ec.Write(ino, int(req.Offset), req.Data)
	if err != nil {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) err(%v)", ino, req.Offset, reqlen, err)
		return fuse.EIO
	}
	resp.Size = size
	if size != reqlen {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) size(%v)", ino, req.Offset, reqlen, size)
	}

	if (int(req.FileFlags) & syscall.O_DIRECT) != 0 {
		if err = f.super.ec.Flush(ino); err != nil {
			log.LogErrorf("Write DirectIO: ino(%v) offset(%v) len(%v) err(%v)", ino, req.Offset, reqlen, err)
			return fuse.EIO
		}
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Write: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v) req(%v) (%v)ns ",
		ino, req.Offset, reqlen, req.Flags, req.FileFlags, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	return fuse.ENOSYS
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	log.LogDebugf("TRACE Fsync enter: ino(%v)", f.inode.ino)
	start := time.Now()
	err = f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Fsync: ino(%v) err(%v)", f.inode.ino, err)
		return fuse.EIO
	}
	f.super.ic.Delete(f.inode.ino)
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Fsync: ino(%v) (%v)ns", f.inode.ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := f.inode.ino
	start := time.Now()
	if req.Valid.Size() {
		if err := f.super.ec.Flush(ino); err != nil {
			log.LogErrorf("Setattr: truncate wait for flush ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		if err := f.super.ec.Truncate(ino, int(req.Size)); err != nil {
			log.LogErrorf("Setattr: truncate ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		f.super.ic.Delete(ino)
		f.super.ec.RefreshExtentsCache(ino)
	}

	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Setattr: InodeGet failed, ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Size() {
		if req.Size != inode.size {
			log.LogWarnf("Setattr: truncate ino(%v) reqSize(%v) inodeSize(%v)", ino, req.Size, inode.size)
		}
	}

	if valid := inode.setattr(req); valid != 0 {
		err = f.super.mw.Setattr(ino, valid, proto.Mode(inode.mode), inode.uid, inode.gid)
		if err != nil {
			f.super.ic.Delete(ino)
			return ParseError(err)
		}
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

func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ENOSYS
}

func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ENOSYS
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ENOSYS
}

func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ENOSYS
}
