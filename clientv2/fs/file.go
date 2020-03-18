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
	"errors"
	"fmt"
	"io"
	"syscall"

	"golang.org/x/net/context"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (s *Super) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	s.ec.OpenStream(ino)
	op.Handle = s.hc.Assign(ino)
	op.KeepPageCache = s.keepCache

	log.LogDebugf("TRACE Open: op(%v) handle(%v)", desc, op.Handle)
	return nil
}

func (s *Super) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	desc := fuse.OpDescription(op)

	handle := s.hc.Release(op.Handle)
	if handle == nil {
		log.LogErrorf("ReleaseFileHandle: bad handle id, op(%v)", desc)
		return syscall.EBADF
	}

	ino := handle.ino
	err := s.ec.CloseStream(ino)
	if err != nil {
		log.LogErrorf("ReleaseFileHandle: close stream failed, op(%v) ino(%v) err(%v)", desc, ino, err)
		return fuse.EIO
	}

	s.ic.Delete(ino)
	log.LogDebugf("TRACE ReleaseFileHandle: op(%v) ino(%v)", desc, ino)
	return nil
}

func (s *Super) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	var err error

	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)
	offset := int(op.Offset)
	reqlen := len(op.Dst)

	log.LogDebugf("TRACE Read enter: op(%v)", desc)

	metric := exporter.NewTPCnt("fileread")
	defer metric.Set(err)

	size, err := s.ec.Read(ino, op.Dst, offset, reqlen)
	if err != nil && err != io.EOF {
		errmsg := fmt.Sprintf("ReadFile: Read failed, localIP(%v) op(%v) err(%v) size(%v)", s.localIP, desc, err, size)
		log.LogError(errmsg)
		exporter.Warning(errmsg)
		return fuse.EIO
	}

	if size > reqlen {
		errmsg := fmt.Sprintf("ReadFile: read size larger than request len, localIP(%v) op(%v) size(%v)", s.localIP, desc, size)
		log.LogError(errmsg)
		exporter.Warning(errmsg)
		err = errors.New(errmsg)
		return fuse.EIO
	}

	if size > 0 {
		op.BytesRead = size
	} else if size <= 0 {
		op.BytesRead = 0
		errmsg := fmt.Sprintf("ReadFile: read size is 0, localIP(%v) op(%v) size(%v)", s.localIP, desc, size)
		log.LogError(errmsg)
		exporter.Warning(errmsg)
		err = errors.New(errmsg)
	}

	log.LogDebugf("TRACE Read exit: op(%v) size(%v)", desc, size)
	return nil
}

func (s *Super) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	var err error

	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)
	offset := int(op.Offset)
	reqlen := len(op.Data)
	filesize, _ := s.fileSize(ino)
	flags := int(op.Flags)

	log.LogDebugf("TRACE Write enter: op(%v) filesize(%v)", desc, filesize)

	if offset > filesize && reqlen == 1 && op.Data[0] == 0 {
		// workaround: posix_fallocate would write 1 byte if fallocate is not supported.
		err = s.ec.Truncate(ino, offset+reqlen)
		if err != nil {
			errmsg := fmt.Sprintf("fallocate failed: localIP(%v) op(%v) origFilesize(%v) err(%v)", s.localIP, desc, filesize, err)
			log.LogError(errmsg)
			exporter.Warning(errmsg)
		}
		log.LogDebugf("fallocate: op(%v) origFilesize(%v) err(%v)", desc, filesize, err)
		return err
	}

	defer func() {
		s.ic.Delete(ino)
	}()

	var waitForFlush, enSyncWrite bool
	if isDirectIOEnabled(flags) || (flags&syscall.O_SYNC != 0) {
		waitForFlush = true
		enSyncWrite = s.enSyncWrite
	}

	metric := exporter.NewTPCnt("filewrite")
	defer metric.Set(err)

	size, err := s.ec.Write(ino, offset, op.Data, enSyncWrite)
	if err != nil {
		errmsg := fmt.Sprintf("WriteFile: Write failed, localIP(%v) op(%v) err(%v)", s.localIP, desc, err)
		log.LogErrorf(errmsg)
		exporter.Warning(errmsg)
		return fuse.EIO
	}

	if size != reqlen {
		errmsg := fmt.Sprintf("WriteFile: incomplete write, localIP(%v) op(%v) size(%v)", s.localIP, desc, size)
		log.LogErrorf(errmsg)
		exporter.Warning(errmsg)
		err = errors.New(errmsg)
		return fuse.EIO
	}

	if waitForFlush {
		if err = s.ec.Flush(ino); err != nil {
			errmsg := fmt.Sprintf("WriteFile: Flush failed, localIP(%v) op(%v) err(%v)", s.localIP, desc, err)
			log.LogErrorf(errmsg)
			exporter.Warning(errmsg)
			return fuse.EIO
		}
	}

	log.LogDebugf("TRACE Write exit: op(%v)", desc)
	return nil

}

func (s *Super) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	var err error

	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE Fsync enter: op(%v)", desc)

	metric := exporter.NewTPCnt("filesync")
	defer metric.Set(err)

	err = s.ec.Flush(ino)
	if err != nil {
		errmsg := fmt.Sprintf("SyncFile: Flush failed, localIP(%v) op(%v) err(%v)", s.localIP, desc, err)
		log.LogErrorf(errmsg)
		exporter.Warning(errmsg)
		return fuse.EIO
	}
	s.ic.Delete(ino)

	log.LogDebugf("TRACE Fsync exit: op(%v)", desc)
	return nil
}

func (s *Super) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE Flush enter: op(%v)", desc)

	err := s.ec.Flush(ino)
	if err != nil {
		errmsg := fmt.Sprintf("FlushFile: Flush failed, localIP(%v) op(%v) err(%v)", s.localIP, desc, err)
		log.LogErrorf(errmsg)
		exporter.Warning(errmsg)
		return fuse.EIO
	}
	s.ic.Delete(ino)

	log.LogDebugf("TRACE Flush exit: op(%v)", desc)
	return nil
}

func (s *Super) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	inode, err := s.InodeGet(ino)
	if err != nil {
		log.LogErrorf("ReadSymlink: op(%v) err(%v)", desc, err)
		return ParseError(err)
	}
	op.Target = string(inode.target)
	log.LogDebugf("TRACE ReadSymlink: op(%v) target(%v)", desc, op.Target)
	return nil
}

func (s *Super) RemoveXattr(ctx context.Context, op *fuseops.RemoveXattrOp) error {
	ino := uint64(op.Inode)
	name := op.Name
	desc := fuse.OpDescription(op)

	if err := s.mw.XAttrDel_ll(ino, name); err != nil {
		log.LogErrorf("RemoveXattr: op(%v) err(%v)", desc, err)
		return ParseError(err)
	}
	log.LogDebugf("TRACE RemoveXattr: op(%v)", desc)
	return nil
}

func (s *Super) GetXattr(ctx context.Context, op *fuseops.GetXattrOp) error {
	ino := uint64(op.Inode)
	name := op.Name
	desc := fuse.OpDescription(op)

	info, err := s.mw.XAttrGet_ll(ino, name)
	if err != nil {
		log.LogErrorf("GetXattr: op(%v) err(%v)", desc, err)
		return ParseError(err)
	}
	op.Dst = info.Get(name)
	op.BytesRead = len(op.Dst)
	log.LogDebugf("TRACE GetXattr: op(%v)", desc)
	return nil
}

func (s *Super) ListXattr(ctx context.Context, op *fuseops.ListXattrOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	info, err := s.mw.XAttrsList_ll(ino)

	if err != nil {
		log.LogErrorf("ListXattr: op(%v) err(%v)", desc, err)
		return ParseError(err)
	}

	dst := op.Dst[:]
	info.VisitAll(func(key string, value []byte) bool {
		keyLen := len(key) + 1

		if err == nil && len(dst) >= keyLen {
			copy(dst, key)
			dst = dst[keyLen:]
		} else {
			err = syscall.ERANGE
		}
		op.BytesRead += keyLen
		return true
	})
	log.LogDebugf("TRACE ListXattr: op(%v)", desc)
	return nil
}

func (s *Super) SetXattr(ctx context.Context, op *fuseops.SetXattrOp) error {
	ino := uint64(op.Inode)
	name := op.Name
	value := op.Value
	desc := fuse.OpDescription(op)

	// TODO： implement flag to improve compatible (Mofei Zhang)
	if err := s.mw.XAttrSet_ll(ino, []byte(name), []byte(value)); err != nil {
		log.LogErrorf("SetXattr: op(%v err(%v)", desc, err)
		return ParseError(err)
	}
	log.LogDebugf("TRACE SetXattr: op(%v)", desc)
	return nil
}

func (s *Super) fileSize(ino uint64) (size int, gen uint64) {
	size, gen, valid := s.ec.FileSize(ino)
	log.LogDebugf("fileSize: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)

	if !valid {
		s.ic.Delete(ino)
		if inode, err := s.InodeGet(ino); err == nil {
			size = int(inode.size)
			gen = inode.gen
		}
	}
	return
}
