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
	fullPath  string
	sync.RWMutex
	fReader *blobstore.Reader
	fWriter *blobstore.Writer
	flag    uint32
	pinos   []uint64
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
	if proto.IsCold(s.volType) || proto.IsStorageClassBlobStore(i.StorageClass) {
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
			StorageClass:    i.StorageClass,
		}
		log.LogDebugf("Trace NewFile:flag(%v). clientConf(%v)", flag, clientConf)

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
		log.LogDebugf("Trace NewFile:fReader(%v) fWriter(%v) ", fReader, fWriter)
		return &File{
			super: s, info: i, fWriter: fWriter, fReader: fReader, parentIno: pino, name: filename,
			flag: flag, fullPath: "Invalid",
		}
	}
	log.LogDebugf("Trace NewFile:ino(%v) flag(%v) ", i, flag)
	return &File{
		super: s, info: i, parentIno: pino, name: filename,
		flag: flag, fullPath: "Invalid",
	}
}

// get file parentPath
func (f *File) getParentPath() string {
	return path.Dir(f.fullPath)
}

func (f *File) setFullPath(fullPath string) {
	f.fullPath = fixUnixPath(fullPath)
}

func (f *File) addParentInode(inos []uint64) {
	if f.pinos == nil {
		f.pinos = make([]uint64, 0)
	}
	f.pinos = append(f.pinos, inos...)
}

// Attr sets the attributes of a file.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Attr", err, bgTime, 1)
	}()

	ino := f.info.Inode
	info, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err)
		if err == fuse.ENOENT {
			a.Inode = ino
			return nil
		}
		return ParseError(err)
	}

	fillAttr(info, a)
	a.ParentIno = f.parentIno
	fileSize, gen := f.fileSizeVersion2(ino)
	log.LogDebugf("Attr: ino(%v) fileSize(%v) gen(%v) inode.gen(%v)", ino, fileSize, gen, info.Generation)
	if gen >= info.Generation {
		a.Size = uint64(fileSize)
	}
	if proto.IsSymlink(info.Mode) {
		a.Size = uint64(len(info.Target))
	}
	log.LogDebugf("TRACE Attr: inode(%v) attr(%v)", info, a)
	return nil
}

// Forget evicts the inode of the current file. This can only happen when the inode is on the orphan list.
func (f *File) Forget() {
	var err error
	bgTime := stat.BeginStat()

	ino := f.info.Inode
	defer func() {
		stat.EndStat("Forget:file", err, bgTime, 1)
		log.LogDebugf("TRACE Forget: ino(%v) %v", ino, f.name)
	}()

	//TODO:why cannot close fwriter
	//log.LogErrorf("TRACE Forget: ino(%v)", ino)
	//if f.fWriter != nil {
	//	f.fWriter.Close()
	//}

	if DisableMetaCache {
		f.super.ic.Delete(ino)
		f.super.fslock.Lock()
		delete(f.super.nodeCache, ino)
		f.super.fslock.Unlock()
		if err := f.super.ec.EvictStream(ino); err != nil {
			log.LogWarnf("Forget: stream not ready to evict, ino(%v) err(%v)", ino, err)
			return
		}
	}

	if !f.super.orphan.Evict(ino) {
		return
	}
	fullPath := f.getParentPath() + f.name
	if err := f.super.mw.Evict(ino, fullPath); err != nil {
		log.LogWarnf("Forget Evict: ino(%v) err(%v)", ino, err)
	}
}

// Open handles the open request.
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	bgTime := stat.BeginStat()
	var needBCache bool

	defer func() {
		stat.EndStat("Open", err, bgTime, 1)
	}()

	ino := f.info.Inode
	log.LogDebugf("TRACE open ino(%v) info(%v) fullPath(%v)", ino, f.info, f.fullPath)
	start := time.Now()

	if f.super.bcacheDir != "" && !f.filterFilesSuffix(f.super.bcacheFilterFiles) {
		parentPath := f.getParentPath()
		log.LogDebugf("TRACE open ino(%v) fullpath(%v)", ino, f.fullPath)
		if parentPath != "" && !strings.HasSuffix(parentPath, "/") {
			parentPath = parentPath + "/"
		}
		log.LogDebugf("TRACE open ino(%v) parentPath(%v)", ino, parentPath)
		if strings.HasPrefix(parentPath, f.super.bcacheDir) {
			needBCache = true
		}
	}
	openForWrite := false
	if req.Flags&0x0f != syscall.O_RDONLY {
		openForWrite = true
	}
	isCache := false
	if proto.IsCold(f.super.volType) || proto.IsStorageClassBlobStore(f.info.StorageClass) {
		isCache = true
	}
	if needBCache {
		f.super.ec.OpenStreamWithCache(ino, needBCache, openForWrite, isCache, f.fullPath)
	} else {
		f.super.ec.OpenStream(ino, openForWrite, isCache, f.fullPath)
	}
	log.LogDebugf("TRACE open ino(%v) f.super.bcacheDir(%v) needBCache(%v)", ino, f.super.bcacheDir, needBCache)

	f.super.ec.RefreshExtentsCache(ino)

	if f.super.keepCache && resp != nil {
		resp.Flags |= fuse.OpenKeepCache
	}
	if proto.IsCold(f.super.volType) || proto.IsStorageClassBlobStore(f.info.StorageClass) {
		log.LogDebugf("TRANCE open ino(%v) info(%v)", ino, f.info)
		fileSize, _ := f.fileSizeVersion2(ino)
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
			StorageClass:    f.info.StorageClass,
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
		log.LogDebugf("TRACE file open,ino(%v)  req.Flags(%v) reader(%v)  writer(%v)", ino, req.Flags, f.fReader, f.fWriter)
	}

	elapsed := time.Since(start)
	f.flag = uint32(req.Flags)
	log.LogDebugf("TRACE Open: ino(%v) req(%v) resp(%v) flags(%v) (%v)ns", ino, req, resp, f.flag, elapsed.Nanoseconds())

	return f, nil
}

// Release handles the release request.
func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	ino := f.info.Inode
	bgTime := stat.BeginStat()

	defer func() {
		stat.EndStat("Release:file", err, bgTime, 1)
		log.LogInfof("action[Release] %v", f.fWriter)
		f.fWriter.FreeCache()
		// keep nodeCache hold the latest inode info
		f.super.fslock.Lock()
		delete(f.super.nodeCache, ino)
		f.super.fslock.Unlock()
		if DisableMetaCache {
			f.super.ic.Delete(ino)
		}
		f.super.fslock.Lock()
		delete(f.super.nodeCache, ino)
		node, ok := f.super.nodeCache[f.parentIno]
		if ok {
			parent, ok := node.(*Dir)
			if ok {
				parent.dcache.Delete(f.name)
				log.LogDebugf("TRACE Release exit: ino(%v) name(%v) decache(%v)",
					parent.info.Inode, parent.name, parent.dcache.Len())
			}
		}
		f.super.fslock.Unlock()
	}()

	log.LogDebugf("TRACE Release enter: ino(%v) req(%v)", ino, req)

	start := time.Now()

	//log.LogErrorf("TRACE Release close stream: ino(%v) req(%v)", ino, req)
	//if f.fWriter != nil {
	//	f.fWriter.Close()
	//}
	// if proto.IsCold(f.super.volType) {
	//	err = f.fWriter.Flush(ino, ctx)
	// } else {
	//	err = f.super.ec.CloseStream(ino)
	// }
	f.super.ec.CloseStream(ino)
	if err != nil {
		log.LogErrorf("Release: close writer failed, ino(%v) req(%v) err(%v)", ino, req, err)
		return ParseError(err)
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Release: ino(%v) req(%v) name(%v)(%v)ns", ino, req, f.fullPath, elapsed.Nanoseconds())

	return nil
}

func (f *File) shouldAccessReplicaStorageClass() (accessReplicaStorageClass bool) {
	accessReplicaStorageClass = false
	if proto.IsValidStorageClass(f.info.StorageClass) {
		if proto.IsStorageClassReplica(f.info.StorageClass) {
			accessReplicaStorageClass = true
		}
	} else {
		// for compatability: old version server modules has no field StorageClass
		if proto.IsHot(f.super.volType) {
			accessReplicaStorageClass = true
		}
	}
	return
}

// Read handles the read request.
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Read", err, bgTime, 1)
		stat.StatBandWidth("Read", uint32(req.Size))
	}()

	log.LogDebugf("TRACE Read enter: ino(%v) storageClass(%v) offset(%v) filesize(%v) reqsize(%v) req(%v)",
		f.info.Inode, f.info.StorageClass, req.Offset, f.info.Size, req.Size, req)

	start := time.Now()

	metric := exporter.NewTPCnt("fileread")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: f.super.volname})
	}()

	var size int
	if f.shouldAccessReplicaStorageClass() {
		f.super.ec.GetStreamer(f.info.Inode).SetParentInode(f.parentIno)
		size, err = f.super.ec.Read(f.info.Inode, resp.Data[fuse.OutHeaderSize:], int(req.Offset),
			req.Size, f.info.StorageClass, false)
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

	// last read request of file
	if f.info.Size > uint64(req.Offset) && uint64(req.Offset+int64(req.Size)) >= f.info.Size {
		// at least read bytes: f.info.Size - req.Offset
		if size > 0 && uint64(size) < f.info.Size-uint64(req.Offset) {
			log.LogErrorf("Read: error data size, ino(%v) offset(%v) filesize(%v) reqsize(%v) size(%v)\n", f.info.Inode, req.Offset, f.info.Size, req.Size, size)
			errMetric := exporter.NewCounter("fileReadFailed")
			errMetric.AddWithLabels(1, map[string]string{exporter.Vol: f.super.volname, exporter.Err: "EIO"})
		}
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
		log.LogWarnf("Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v)", f.info.Inode, req.Offset, req.Size, req, size)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v) (%v)ns", f.info.Inode, req.Offset, req.Size, req, size, elapsed.Nanoseconds())

	return nil
}

// Write handles the write request.
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Write", err, bgTime, 1)
		stat.StatBandWidth("Write", uint32(len(req.Data)))
	}()

	ino := f.info.Inode
	reqlen := len(req.Data)
	log.LogDebugf("TRACE Write enter: ino(%v) storageClass(%v) offset(%v) len(%v) flags(%v) fileflags(%v) quotaIds(%v) req(%v)",
		ino, f.info.StorageClass, req.Offset, reqlen, req.Flags, req.FileFlags, f.info.QuotaInfos, req)
	if proto.IsHot(f.super.volType) || proto.IsStorageClassReplica(f.info.StorageClass) {
		filesize, _ := f.fileSize(ino)
		if req.Offset > int64(filesize) && reqlen == 1 && req.Data[0] == 0 {

			// workaround: posix_fallocate would write 1 byte if fallocate is not supported.
			fullPath := path.Join(f.getParentPath(), f.name)
			err = f.super.ec.Truncate(f.super.mw, f.parentIno, ino, int(req.Offset)+reqlen, fullPath)
			if err == nil {
				resp.Size = reqlen
			}
			log.LogDebugf("fallocate: ino(%v) origFilesize(%v) req(%v) err(%v)", f.info.Inode, filesize, req, err)
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
		if proto.IsCold(f.super.volType) || proto.IsStorageClassBlobStore(f.info.StorageClass) {
			waitForFlush = false
			flags |= proto.FlagsSyncWrite
		}
	}

	if req.FileFlags&fuse.OpenAppend != 0 || proto.IsCold(f.super.volType) || proto.IsStorageClassBlobStore(f.info.StorageClass) {
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
		if ok := f.super.ec.UidIsLimited(req.Uid); ok {
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
	if f.shouldAccessReplicaStorageClass() {
		f.super.ec.GetStreamer(ino).SetParentInode(f.parentIno)
		if size, err = f.super.ec.Write(ino, int(req.Offset), req.Data, flags, checkFunc, f.info.StorageClass, false); err == ParseError(syscall.ENOSPC) {
			return
		}
	} else {
		atomic.StoreInt32(&f.idle, 0)
		size, err = f.fWriter.Write(context.Background(), int(req.Offset), req.Data, flags)
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
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) size(%v)", ino, req.Offset, reqlen, size)
	}

	// only hot volType need to wait flush
	if waitForFlush {
		err = f.super.ec.Flush(ino)
		if err != nil {
			msg := fmt.Sprintf("Write: failed to wait for flush, ino(%v) offset(%v) len(%v) err(%v) req(%v)", ino, req.Offset, reqlen, err, req)
			f.super.handleError("Wrtie", msg)
			errMetric := exporter.NewCounter("fileWriteFailed")
			errMetric.AddWithLabels(1, map[string]string{exporter.Vol: f.super.volname, exporter.Err: "EIO"})
			return ParseError(err)
		}
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Write: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v) req(%v) (%v)ns ",
		ino, req.Offset, reqlen, req.Flags, req.FileFlags, req, elapsed.Nanoseconds())
	return nil
}

// Flush only when fsyncOnClose is enabled.
func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Flush", err, bgTime, 1)
	}()

	if !f.super.fsyncOnClose {
		return fuse.ENOSYS
	}
	log.LogDebugf("TRACE Flush enter: ino(%v)", f.info.Inode)
	start := time.Now()

	metric := exporter.NewTPCnt("filesync")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: f.super.volname})
	}()
	if proto.IsHot(f.super.volType) || proto.IsStorageClassReplica(f.info.StorageClass) {
		err = f.super.ec.Flush(f.info.Inode)
	} else {
		f.Lock()
		err = f.fWriter.Flush(f.info.Inode, context.Background())
		f.Unlock()
	}
	log.LogDebugf("TRACE Flush: ino(%v) err(%v)", f.info.Inode, err)
	if err != nil {
		msg := fmt.Sprintf("Flush: ino(%v) err(%v)", f.info.Inode, err)
		f.super.handleError("Flush", msg)
		log.LogErrorf("TRACE Flush err: ino(%v) err(%v)", f.info.Inode, err)
		return ParseError(err)
	}

	if DisableMetaCache {
		f.super.ic.Delete(f.info.Inode)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Flush: ino(%v) (%v)ns", f.info.Inode, elapsed.Nanoseconds())

	return nil
}

// Fsync hanldes the fsync request.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Fsync", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE Fsync enter: ino(%v)", f.info.Inode)
	start := time.Now()
	if proto.IsHot(f.super.volType) || proto.IsStorageClassReplica(f.info.StorageClass) {
		err = f.super.ec.Flush(f.info.Inode)
	} else {
		err = f.fWriter.Flush(f.info.Inode, context.Background())
	}
	if err != nil {
		msg := fmt.Sprintf("Fsync: ino(%v) err(%v)", f.info.Inode, err)
		f.super.handleError("Fsync", msg)
		return ParseError(err)
	}
	f.super.ic.Delete(f.info.Inode)
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Fsync: ino(%v) (%v)ns", f.info.Inode, elapsed.Nanoseconds())
	return nil
}

// Setattr handles the setattr request.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Setattr", err, bgTime, 1)
	}()

	ino := f.info.Inode
	start := time.Now()
	openForWrite := false
	if req.Flags&0x0f != syscall.O_RDONLY {
		openForWrite = true
	}
	isCache := false
	if proto.IsCold(f.super.volType) || proto.IsStorageClassBlobStore(f.info.StorageClass) {
		isCache = true
	}
	if req.Valid.Size() && (proto.IsHot(f.super.volType) || proto.IsStorageClassReplica(f.info.StorageClass)) {
		// when use trunc param in open request through nfs client and mount on cfs mountPoint, cfs client may not recv open message but only setAttr,
		// the streamer may not open and cause io error finally,so do a open no matter the stream be opened or not
		if err := f.super.ec.OpenStream(ino, openForWrite, isCache, f.fullPath); err != nil {
			log.LogErrorf("Setattr: OpenStream ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		defer f.super.ec.CloseStream(ino)

		if err := f.super.ec.Flush(ino); err != nil {
			log.LogErrorf("Setattr: truncate wait for flush ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		fullPath := path.Join(f.getParentPath(), f.name)
		if err := f.super.ec.Truncate(f.super.mw, f.parentIno, ino, int(req.Size), fullPath); err != nil {
			log.LogErrorf("Setattr: truncate ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		f.super.ic.Delete(ino)
		f.super.ec.RefreshExtentsCache(ino)
	}

	info, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Setattr: InodeGet failed, ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Size() && (proto.IsHot(f.super.volType) || proto.IsStorageClassReplica(f.info.StorageClass)) {
		if req.Size != info.Size {
			log.LogWarnf("Setattr: truncate ino(%v) reqSize(%v) inodeSize(%v)", ino, req.Size, info.Size)
		}
	}

	if valid := setattr(info, req); valid != 0 {
		err = f.super.mw.Setattr(ino, valid, info.Mode, info.Uid, info.Gid, info.AccessTime.Unix(),
			info.ModifyTime.Unix())
		if err != nil {
			f.super.ic.Delete(ino)
			return ParseError(err)
		}
	}

	fillAttr(info, &resp.Attr)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Setattr: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
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
	info, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Readlink: ino(%v) err(%v)", ino, err)
		return "", ParseError(err)
	}
	log.LogDebugf("TRACE Readlink: ino(%v) target(%v)", ino, string(info.Target))
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
		log.LogErrorf("GetXattr: ino(%v) name(%v) err(%v)", ino, name, err)
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
	log.LogDebugf("TRACE GetXattr: ino(%v) name(%v)", ino, name)
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
		log.LogErrorf("ListXattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}
	for _, key := range keys {
		resp.Append(key)
	}
	log.LogDebugf("TRACE Listxattr: ino(%v)", ino)
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
		log.LogErrorf("Setxattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	log.LogDebugf("TRACE Setxattr: ino(%v) name(%v)", ino, name)
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
		log.LogErrorf("Removexattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	log.LogDebugf("TRACE RemoveXattr: ino(%v) name(%v)", ino, name)
	return nil
}

func (f *File) fileSize(ino uint64) (size int, gen uint64) {
	size, gen, valid := f.super.ec.FileSize(ino)
	if !valid {
		if info, err := f.super.InodeGet(ino); err == nil {
			size = int(info.Size)
			gen = info.Generation
		}
	}

	log.LogDebugf("TRANCE fileSize: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)
	return
}

func (f *File) fileSizeVersion2(ino uint64) (size int, gen uint64) {
	size, gen, valid := f.super.ec.FileSize(ino)
	if proto.IsCold(f.super.volType) || proto.IsStorageClassBlobStore(f.info.StorageClass) {
		valid = false
	}
	if !valid {
		if info, err := f.super.InodeGet(ino); err == nil {
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

	log.LogDebugf("TRACE fileSizeVersion2: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)
	return
}

// return true mean this file will not cache in block cache
func (f *File) filterFilesSuffix(filterFiles string) bool {
	if f.name == "" {
		log.LogWarnf("this file inode[%v], name is nil", f.info)
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
			log.LogDebugf("fileName:%s,filter:%s,suffix:%s,suffixs:%v", f.name, filterFiles, suffix, suffixs)
			return true
		}
	}
	return false
}
