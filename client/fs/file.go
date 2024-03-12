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
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// File defines the structure of a file.
type File struct {
	super     *Super
	info      *proto.InodeInfo
	idle      int32
	parentIno uint64
	name      string
	sync.RWMutex
	fReader *blobstore.Reader
	fWriter *blobstore.Writer
}

// Functions that File needs to implement
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

// NewFile returns a new file.
func NewFile(s *Super, i *proto.InodeInfo, flag uint32, pino uint64, filename string) fs.Node {
	if proto.IsCold(s.volType) {
		var (
			fReader    *blobstore.Reader
			fWriter    *blobstore.Writer
			clientConf blobstore.ClientConfig
		)

		clientConf = blobstore.ClientConfig{
			VolName:         s.volname,
			VolType:         s.volType,
			Ino:             i.Inode,
			BlockSize:       s.EbsBlockSize,
			Bc:              s.bc,
			Mw:              s.mw,
			Ec:              s.ec,
			Ebsc:            s.ebsc,
			EnableBcache:    s.enableBcache,
			WConcurrency:    s.writeThreads,
			ReadConcurrency: s.readThreads,
			CacheAction:     s.CacheAction,
			FileCache:       false,
			FileSize:        i.Size,
			CacheThreshold:  s.CacheThreshold,
		}
		log.Debugf("Trace NewFile:flag(%v). clientConf(%v)", flag, clientConf)

		switch flag {
		case syscall.O_RDONLY:
			fReader = blobstore.NewReader(clientConf)
		case syscall.O_WRONLY:
			fWriter = blobstore.NewWriter(clientConf)
		case syscall.O_RDWR:
			fReader = blobstore.NewReader(clientConf)
			fWriter = blobstore.NewWriter(clientConf)
		default:
			// no thing
		}
		log.Debugf("Trace NewFile:fReader(%v) fWriter(%v) ", fReader, fWriter)
		return &File{super: s, info: i, fWriter: fWriter, fReader: fReader, parentIno: pino, name: filename}
	}
	return &File{super: s, info: i, parentIno: pino, name: filename}
}

// get file parentPath
func (f *File) getParentPath() string {
	if f.parentIno == f.super.rootIno {
		return "/"
	}

	f.super.fslock.Lock()
	node, ok := f.super.nodeCache[f.parentIno]
	f.super.fslock.Unlock()
	if !ok {
		log.Errorf("Get node cache failed: ino(%v)", f.parentIno)
		return "unknown"
	}
	parentDir, ok := node.(*Dir)
	if !ok {
		log.Errorf("Type error: Can not convert node -> *Dir, ino(%v)", f.parentIno)
		return "unknown"
	}
	return parentDir.getCwd()
}

// Attr sets the attributes of a file.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Attr", err, bgTime, 1)
	}()

	ino := f.info.Inode
	info, err := f.super.InodeGet(ctx, ino)
	if err != nil {
		log.Errorf("Attr: ino(%v) err(%v)", ino, err)
		if err == fuse.ENOENT {
			a.Inode = ino
			return nil
		}
		return ParseError(err)
	}

	fillAttr(info, a)
	a.ParentIno = f.parentIno
	fileSize, gen := f.fileSizeVersion2(ctx, ino)
	log.Debugf("Attr: ino(%v) fileSize(%v) gen(%v) inode.gen(%v)", ino, fileSize, gen, info.Generation)
	if gen >= info.Generation {
		a.Size = uint64(fileSize)
	}
	if proto.IsSymlink(info.Mode) {
		a.Size = uint64(len(info.Target))
	}
	log.Debugf("TRACE Attr: inode(%v) attr(%v)", info, a)
	return nil
}

// Forget evicts the inode of the current file. This can only happen when the inode is on the orphan list.
func (f *File) Forget() {
	var err error
	bgTime := stat.BeginStat()

	ino := f.info.Inode
	defer func() {
		stat.EndStat("Forget", err, bgTime, 1)
		log.Debugf("TRACE Forget: ino(%v)", ino)
	}()

	//TODO:why cannot close fwriter
	//log.Errorf("TRACE Forget: ino(%v)", ino)
	//if f.fWriter != nil {
	//	f.fWriter.Close()
	//}

	if DisableMetaCache {
		f.super.ic.Delete(ino)
		f.super.fslock.Lock()
		delete(f.super.nodeCache, ino)
		f.super.fslock.Unlock()
		if err := f.super.ec.EvictStream(ino); err != nil {
			log.Warnf("Forget: stream not ready to evict, ino(%v) err(%v)", ino, err)
			return
		}
	}

	if !f.super.orphan.Evict(ino) {
		return
	}
	fullPath := f.getParentPath() + f.name
	if err := f.super.mw.Evict(ino, fullPath); err != nil {
		log.Warnf("Forget Evict: ino(%v) err(%v)", ino, err)
	}
}

// Open handles the open request.
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	span := proto.SpanFromContext(ctx)
	ctx = proto.ContextWithSpan(ctx, span)
	bgTime := stat.BeginStat()
	var needBCache bool

	defer func() {
		stat.EndStat("Open", err, bgTime, 1)
	}()

	ino := f.info.Inode
	span.Debugf("TRACE open ino(%v) info(%v)", ino, f.info)
	start := time.Now()

	if f.super.bcacheDir != "" && !f.filterFilesSuffix(f.super.bcacheFilterFiles) {
		parentPath := f.getParentPath()
		if parentPath != "" && !strings.HasSuffix(parentPath, "/") {
			parentPath = parentPath + "/"
		}
		span.Debugf("TRACE open ino(%v) parentPath(%v)", ino, parentPath)
		if strings.HasPrefix(parentPath, f.super.bcacheDir) {
			needBCache = true
		}
	}
	if needBCache {
		f.super.ec.OpenStreamWithCache(ctx, ino, needBCache)
	} else {
		f.super.ec.OpenStream(ctx, ino)
	}
	span.Debugf("TRACE open ino(%v) f.super.bcacheDir(%v) needBCache(%v)", ino, f.super.bcacheDir, needBCache)

	f.super.ec.RefreshExtentsCache(ctx, ino)

	if f.super.keepCache && resp != nil {
		resp.Flags |= fuse.OpenKeepCache
	}
	if proto.IsCold(f.super.volType) {
		span.Debugf("TRANCE open ino(%v) info(%v)", ino, f.info)
		fileSize, _ := f.fileSizeVersion2(ctx, ino)
		clientConf := blobstore.ClientConfig{
			VolName:         f.super.volname,
			VolType:         f.super.volType,
			BlockSize:       f.super.EbsBlockSize,
			Ino:             f.info.Inode,
			Bc:              f.super.bc,
			Mw:              f.super.mw,
			Ec:              f.super.ec,
			Ebsc:            f.super.ebsc,
			EnableBcache:    f.super.enableBcache,
			WConcurrency:    f.super.writeThreads,
			ReadConcurrency: f.super.readThreads,
			CacheAction:     f.super.CacheAction,
			FileCache:       false,
			FileSize:        uint64(fileSize),
			CacheThreshold:  f.super.CacheThreshold,
		}
		f.fWriter.FreeCache()
		switch req.Flags & 0x0f {
		case syscall.O_RDONLY:
			f.fReader = blobstore.NewReader(clientConf)
			f.fWriter = nil
		case syscall.O_WRONLY:
			f.fWriter = blobstore.NewWriter(clientConf)
			f.fReader = nil
		case syscall.O_RDWR:
			f.fReader = blobstore.NewReader(clientConf)
			f.fWriter = blobstore.NewWriter(clientConf)
		default:
			f.fWriter = blobstore.NewWriter(clientConf)
			f.fReader = nil
		}
		span.Debugf("TRACE file open,ino(%v)  req.Flags(%v) reader(%v)  writer(%v)", ino, req.Flags, f.fReader, f.fWriter)
	}

	elapsed := time.Since(start)
	span.Debugf("TRACE Open: ino(%v) req(%v) resp(%v) (%v)ns", ino, req, resp, elapsed.Nanoseconds())

	return f, nil
}

// Release handles the release request.
func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	span := proto.SpanFromContext(ctx)
	ctx = proto.ContextWithSpan(ctx, span)
	ino := f.info.Inode
	bgTime := stat.BeginStat()

	defer func() {
		stat.EndStat("Release", err, bgTime, 1)
		span.Infof("action[Release] %v", f.fWriter)
		f.fWriter.FreeCache()
		if DisableMetaCache {
			f.super.ic.Delete(ino)
		}
	}()

	span.Debugf("TRACE Release enter: ino(%v) req(%v)", ino, req)

	start := time.Now()

	//log.Errorf("TRACE Release close stream: ino(%v) req(%v)", ino, req)
	//if f.fWriter != nil {
	//	f.fWriter.Close()
	//}

	err = f.super.ec.CloseStream(ino)
	if err != nil {
		span.Errorf("Release: close writer failed, ino(%v) req(%v) err(%v)", ino, req, err)
		return ParseError(err)
	}
	elapsed := time.Since(start)
	span.Debugf("TRACE Release: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())

	return nil
}

// Read handles the read request.
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	span := proto.SpanFromContext(ctx)
	ctx = proto.ContextWithSpan(ctx, span)
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Read", err, bgTime, 1)
		stat.StatBandWidth("Read", uint32(req.Size))
	}()

	span.Debugf("TRACE Read enter: ino(%v) offset(%v) reqsize(%v) req(%v)", f.info.Inode, req.Offset, req.Size, req)

	start := time.Now()

	metric := exporter.NewTPCnt("fileread")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: f.super.volname})
	}()
	var size int
	if proto.IsHot(f.super.volType) {
		size, err = f.super.ec.Read(ctx, f.info.Inode, resp.Data[fuse.OutHeaderSize:], int(req.Offset), req.Size)
	} else {
		size, err = f.fReader.Read(ctx, resp.Data[fuse.OutHeaderSize:], int(req.Offset), req.Size)
	}
	if err != nil && err != io.EOF {
		msg := fmt.Sprintf("Read: ino(%v) req(%v) err(%v) size(%v)", f.info.Inode, req, err, size)
		f.super.handleError("Read", msg)
		errMetric := exporter.NewCounter("fileReadFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: f.super.volname, exporter.Err: "EIO"})
		return ParseError(err)
	}

	if size > req.Size {
		msg := fmt.Sprintf("Read: read size larger than request size, ino(%v) req(%v) size(%v)", f.info.Inode, req, size)
		f.super.handleError("Read", msg)
		errMetric := exporter.NewCounter("fileReadFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: f.super.volname, exporter.Err: "ERANGE"})
		return fuse.ERANGE
	}

	if size > 0 {
		resp.Data = resp.Data[:size+fuse.OutHeaderSize]
	} else if size <= 0 {
		resp.Data = resp.Data[:fuse.OutHeaderSize]
		span.Warnf("Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v)", f.info.Inode, req.Offset, req.Size, req, size)
	}

	elapsed := time.Since(start)
	span.Debugf("TRACE Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v) (%v)ns", f.info.Inode, req.Offset, req.Size, req, size, elapsed.Nanoseconds())

	return nil
}

// Write handles the write request.
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	span := proto.SpanFromContext(ctx)
	ctx = proto.ContextWithSpan(ctx, span)
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Write", err, bgTime, 1)
		stat.StatBandWidth("Write", uint32(len(req.Data)))
	}()

	ino := f.info.Inode
	reqlen := len(req.Data)
	span.Debugf("TRACE Write enter: ino(%v) offset(%v) len(%v)  flags(%v) fileflags(%v) quotaIds(%v) req(%v)",
		ino, req.Offset, reqlen, req.Flags, req.FileFlags, f.info.QuotaInfos, req)
	if proto.IsHot(f.super.volType) {
		filesize, _ := f.fileSize(ctx, ino)
		if req.Offset > int64(filesize) && reqlen == 1 && req.Data[0] == 0 {

			// workaround: posix_fallocate would write 1 byte if fallocate is not supported.
			fullPath := path.Join(f.getParentPath(), f.name)
			err = f.super.ec.Truncate(ctx, f.super.mw, f.parentIno, ino, int(req.Offset)+reqlen, fullPath)
			if err == nil {
				resp.Size = reqlen
			}
			span.Debugf("fallocate: ino(%v) origFilesize(%v) req(%v) err(%v)", f.info.Inode, filesize, req, err)
			return
		}
	}

	defer func() {
		f.super.ic.Delete(ino)
	}()

	var waitForFlush bool
	var flags int

	if isDirectIOEnabled(req.FileFlags) || (req.FileFlags&fuse.OpenSync != 0) {
		waitForFlush = true
		if f.super.enSyncWrite {
			flags |= proto.FlagsSyncWrite
		}
		if proto.IsCold(f.super.volType) {
			waitForFlush = false
			flags |= proto.FlagsSyncWrite
		}
	}

	if req.FileFlags&fuse.OpenAppend != 0 || proto.IsCold(f.super.volType) {
		flags |= proto.FlagsAppend
	}

	start := time.Now()
	metric := exporter.NewTPCnt("filewrite")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: f.super.volname})
	}()

	checkFunc := func() error {
		if !f.super.mw.EnableQuota {
			return nil
		}
		if ok := f.super.ec.UidIsLimited(ctx, req.Uid); ok {
			return ParseError(syscall.ENOSPC)
		}
		var quotaIds []uint32
		for quotaId := range f.info.QuotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}
		if limited := f.super.mw.IsQuotaLimited(quotaIds); limited {
			return ParseError(syscall.ENOSPC)
		}
		return nil
	}
	var size int
	if proto.IsHot(f.super.volType) {
		f.super.ec.GetStreamer(ctx, ino).SetParentInode(f.parentIno)
		if size, err = f.super.ec.Write(ctx, ino, int(req.Offset), req.Data, flags, checkFunc); err == ParseError(syscall.ENOSPC) {
			return
		}
	} else {
		atomic.StoreInt32(&f.idle, 0)
		size, err = f.fWriter.Write(ctx, int(req.Offset), req.Data, flags)
	}
	if err != nil {
		msg := fmt.Sprintf("Write: ino(%v) offset(%v) len(%v) err(%v)", ino, req.Offset, reqlen, err)
		f.super.handleError("Write", msg)
		errMetric := exporter.NewCounter("fileWriteFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: f.super.volname, exporter.Err: "EIO"})
		if err == syscall.EOPNOTSUPP {
			return fuse.ENOTSUP
		}
		return fuse.EIO
	}

	resp.Size = size
	if size != reqlen {
		span.Errorf("Write: ino(%v) offset(%v) len(%v) size(%v)", ino, req.Offset, reqlen, size)
	}

	// only hot volType need to wait flush
	if waitForFlush {
		err = f.super.ec.Flush(ctx, ino)
		if err != nil {
			msg := fmt.Sprintf("Write: failed to wait for flush, ino(%v) offset(%v) len(%v) err(%v) req(%v)", ino, req.Offset, reqlen, err, req)
			f.super.handleError("Wrtie", msg)
			errMetric := exporter.NewCounter("fileWriteFailed")
			errMetric.AddWithLabels(1, map[string]string{exporter.Vol: f.super.volname, exporter.Err: "EIO"})
			return ParseError(err)
		}
	}
	elapsed := time.Since(start)
	span.Debugf("TRACE Write: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v) req(%v) (%v)ns ",
		ino, req.Offset, reqlen, req.Flags, req.FileFlags, req, elapsed.Nanoseconds())
	return nil
}

// Flush only when fsyncOnClose is enabled.
func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	span := proto.SpanFromContext(ctx)
	ctx = proto.ContextWithSpan(ctx, span)
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Flush", err, bgTime, 1)
	}()

	if !f.super.fsyncOnClose {
		return fuse.ENOSYS
	}
	span.Debugf("TRACE Flush enter: ino(%v)", f.info.Inode)
	start := time.Now()

	metric := exporter.NewTPCnt("filesync")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: f.super.volname})
	}()
	if proto.IsHot(f.super.volType) {
		err = f.super.ec.Flush(ctx, f.info.Inode)
	} else {
		f.Lock()
		err = f.fWriter.Flush(f.info.Inode, ctx)
		f.Unlock()
	}
	span.Debugf("TRACE Flush: ino(%v) err(%v)", f.info.Inode, err)
	if err != nil {
		msg := fmt.Sprintf("Flush: ino(%v) err(%v)", f.info.Inode, err)
		f.super.handleError("Flush", msg)
		span.Errorf("TRACE Flush err: ino(%v) err(%v)", f.info.Inode, err)
		return ParseError(err)
	}

	if DisableMetaCache {
		f.super.ic.Delete(f.info.Inode)
	}

	elapsed := time.Since(start)
	span.Debugf("TRACE Flush: ino(%v) (%v)ns", f.info.Inode, elapsed.Nanoseconds())

	return nil
}

// Fsync hanldes the fsync request.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	span := proto.SpanFromContext(ctx)
	ctx = proto.ContextWithSpan(ctx, span)
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Fsync", err, bgTime, 1)
	}()

	span.Debugf("TRACE Fsync enter: ino(%v)", f.info.Inode)
	start := time.Now()
	if proto.IsHot(f.super.volType) {
		err = f.super.ec.Flush(ctx, f.info.Inode)
	} else {
		err = f.fWriter.Flush(f.info.Inode, ctx)
	}
	if err != nil {
		msg := fmt.Sprintf("Fsync: ino(%v) err(%v)", f.info.Inode, err)
		f.super.handleError("Fsync", msg)
		return ParseError(err)
	}
	f.super.ic.Delete(f.info.Inode)
	elapsed := time.Since(start)
	span.Debugf("TRACE Fsync: ino(%v) (%v)ns", f.info.Inode, elapsed.Nanoseconds())
	return nil
}

// Setattr handles the setattr request.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	span, ctxNew := proto.SpanWithContextPrefix(ctx, "File-Setattr-")
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Setattr", err, bgTime, 1)
	}()

	ino := f.info.Inode
	start := time.Now()
	if req.Valid.Size() && proto.IsHot(f.super.volType) {
		// when use trunc param in open request through nfs client and mount on cfs mountPoint, cfs client may not recv open message but only setAttr,
		// the streamer may not open and cause io error finally,so do a open no matter the stream be opened or not
		if err := f.super.ec.OpenStream(ctx, ino); err != nil {
			span.Errorf("Setattr: OpenStream ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		defer f.super.ec.CloseStream(ino)

		if err := f.super.ec.Flush(ctx, ino); err != nil {
			span.Errorf("Setattr: truncate wait for flush ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		fullPath := path.Join(f.getParentPath(), f.name)
		if err := f.super.ec.Truncate(ctx, f.super.mw, f.parentIno, ino, int(req.Size), fullPath); err != nil {
			span.Errorf("Setattr: truncate ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		f.super.ic.Delete(ino)
		f.super.ec.RefreshExtentsCache(ctxNew, ino)
	}

	info, err := f.super.InodeGet(ctx, ino)
	if err != nil {
		span.Errorf("Setattr: InodeGet failed, ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Size() && proto.IsHot(f.super.volType) {
		if req.Size != info.Size {
			span.Warnf("Setattr: truncate ino(%v) reqSize(%v) inodeSize(%v)", ino, req.Size, info.Size)
		}
	}

	if valid := setattr(info, req); valid != 0 {
		err = f.super.mw.Setattr(ctxNew, ino, valid, info.Mode, info.Uid, info.Gid, info.AccessTime.Unix(),
			info.ModifyTime.Unix())
		if err != nil {
			f.super.ic.Delete(ino)
			return ParseError(err)
		}
	}

	fillAttr(info, &resp.Attr)

	elapsed := time.Since(start)
	span.Debugf("TRACE Setattr: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
	return nil
}

// Readlink handles the readlink request.
func (f *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Readlink", err, bgTime, 1)
	}()

	ino := f.info.Inode
	info, err := f.super.InodeGet(ctx, ino)
	if err != nil {
		log.Errorf("Readlink: ino(%v) err(%v)", ino, err)
		return "", ParseError(err)
	}
	log.Debugf("TRACE Readlink: ino(%v) target(%v)", ino, string(info.Target))
	return string(info.Target), nil
}

// Getxattr has not been implemented yet.
func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Getxattr", err, bgTime, 1)
	}()

	if !f.super.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.info.Inode
	name := req.Name
	size := req.Size
	pos := req.Position
	info, err := f.super.mw.XAttrGet_ll(ino, name)
	if err != nil {
		log.Errorf("GetXattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	value := info.Get(name)
	if pos > 0 {
		value = value[pos:]
	}
	if size > 0 && size < uint32(len(value)) {
		value = value[:size]
	}
	resp.Xattr = value
	log.Debugf("TRACE GetXattr: ino(%v) name(%v)", ino, name)
	return nil
}

// Listxattr has not been implemented yet.
func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Listxattr", err, bgTime, 1)
	}()

	if !f.super.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.info.Inode
	_ = req.Size     // ignore currently
	_ = req.Position // ignore currently

	keys, err := f.super.mw.XAttrsList_ll(ino)
	if err != nil {
		log.Errorf("ListXattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}
	for _, key := range keys {
		resp.Append(key)
	}
	log.Debugf("TRACE Listxattr: ino(%v)", ino)
	return nil
}

// Setxattr has not been implemented yet.
func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Setxattr", err, bgTime, 1)
	}()

	if !f.super.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.info.Inode
	name := req.Name
	value := req.Xattr
	// TODO： implement flag to improve compatible (Mofei Zhang)
	if err = f.super.mw.XAttrSet_ll(ino, []byte(name), []byte(value)); err != nil {
		log.Errorf("Setxattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	log.Debugf("TRACE Setxattr: ino(%v) name(%v)", ino, name)
	return nil
}

// Removexattr has not been implemented yet.
func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Removexattr", err, bgTime, 1)
	}()

	if !f.super.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.info.Inode
	name := req.Name
	if err = f.super.mw.XAttrDel_ll(ino, name); err != nil {
		log.Errorf("Removexattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	log.Debugf("TRACE RemoveXattr: ino(%v) name(%v)", ino, name)
	return nil
}

func (f *File) fileSize(ctx context.Context, ino uint64) (size int, gen uint64) {
	span := proto.SpanFromContext(ctx)
	size, gen, valid := f.super.ec.FileSize(ctx, ino)
	if !valid {
		if info, err := f.super.InodeGet(ctx, ino); err == nil {
			size = int(info.Size)
			gen = info.Generation
		}
	}

	span.Debugf("TRANCE fileSize: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)
	return
}

func (f *File) fileSizeVersion2(ctx context.Context, ino uint64) (size int, gen uint64) {
	span := proto.SpanFromContext(ctx)
	size, gen, valid := f.super.ec.FileSize(ctx, ino)
	if proto.IsCold(f.super.volType) {
		valid = false
	}
	if !valid {
		if info, err := f.super.InodeGet(ctx, ino); err == nil {
			size = int(info.Size)
			if f.fWriter != nil {
				cacheSize := f.fWriter.CacheFileSize()
				if cacheSize > size {
					size = cacheSize
				}
			}
			gen = info.Generation
		}
	}

	span.Debugf("TRACE fileSizeVersion2: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)
	return
}

// return true mean this file will not cache in block cache
func (f *File) filterFilesSuffix(filterFiles string) bool {
	if f.name == "" {
		log.Warnf("this file inode[%v], name is nil", f.info)
		return true
	}
	if filterFiles == "" {
		return false
	}
	suffixs := strings.Split(filterFiles, ";")
	for _, suffix := range suffixs {
		//.py means one type of file
		suffix = "." + suffix
		if suffix != "." && strings.Contains(f.name, suffix) {
			log.Debugf("fileName:%s,filter:%s,suffix:%s,suffixs:%v", f.name, filterFiles, suffix, suffixs)
			return true
		}
	}
	return false
}
