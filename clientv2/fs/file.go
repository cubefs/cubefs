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
	"golang.org/x/net/context"
	"io"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

	"github.com/chubaofs/chubaofs/util/log"
)

func (s *Super) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	s.ec.OpenStream(ino)
	op.Handle = s.hc.Assign(ino)

	log.LogDebugf("TRACE Open: op(%v) handle(%v)", desc, op.Handle)
	return nil
}

func (s *Super) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	desc := fuse.OpDescription(op)

	handle := s.hc.Get(op.Handle)
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
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)
	offset := int(op.Offset)
	reqlen := len(op.Dst)

	log.LogDebugf("TRACE Read enter: op(%v)", desc)

	size, err := s.ec.Read(ino, op.Dst, offset, reqlen)
	if err != nil && err != io.EOF {
		log.LogErrorf("Read: op(%v) err(%v) size(%v)", desc, err, size)
		return fuse.EIO
	}

	if size > reqlen {
		log.LogErrorf("Read: op(%v) size(%v)", desc, size)
		return fuse.EIO
	}

	if size > 0 {
		op.BytesRead = size
	} else if size <= 0 {
		op.BytesRead = 0
		log.LogErrorf("Read: op(%v) size(%v)", desc, size)
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
	filesize, _ := s.ec.FileSize(ino)
	flags := int(op.Flags)

	log.LogDebugf("TRACE Write enter: op(%v) filesize(%v)", desc, filesize)

	if offset > filesize && reqlen == 1 && op.Data[0] == 0 {
		// workaround: posix_fallocate would write 1 byte if fallocate is not supported.
		err = s.ec.Truncate(ino, offset+reqlen)
		if err != nil {
			log.LogErrorf("fallocate failed: op(%v) origFilesize(%v) err(%v)", desc, filesize, err)
		}
		log.LogDebugf("fallocate: op(%v) origFilesize(%v) err(%v)", desc, filesize, err)
		return err
	}

	defer func() {
		s.ic.Delete(ino)
	}()

	var waitForFlush, enSyncWrite bool
	if (flags&syscall.O_DIRECT != 0) || (flags&syscall.O_SYNC != 0) {
		waitForFlush = true
		enSyncWrite = s.enSyncWrite
	}

	size, err := s.ec.Write(ino, offset, op.Data, enSyncWrite)
	if err != nil {
		log.LogErrorf("Write: failed to write, op(%v) err(%v)", desc, err)
		return fuse.EIO
	}

	if size != reqlen {
		log.LogErrorf("Write: incomplete write, op(%v) size(%v)", desc, size)
		return fuse.EIO
	}

	if waitForFlush {
		if err = s.ec.Flush(ino); err != nil {
			log.LogErrorf("Write: failed to wait for flush, op(%v) err(%v)", desc, err)
			return fuse.EIO
		}
	}

	log.LogDebugf("TRACE Write exit: op(%v)", desc)
	return nil

}

func (s *Super) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE Fsync enter: op(%v)", desc)

	err := s.ec.Flush(ino)
	if err != nil {
		log.LogErrorf("Fsync: op(%v) err(%v)", desc, err)
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
		log.LogErrorf("Flush: op(%v) err(%v)", desc, err)
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
	return fuse.ENOSYS
}

func (s *Super) GetXattr(ctx context.Context, op *fuseops.GetXattrOp) error {
	return fuse.ENOSYS
}

func (s *Super) ListXattr(ctx context.Context, op *fuseops.ListXattrOp) error {
	return fuse.ENOSYS
}

func (s *Super) SetXattr(ctx context.Context, op *fuseops.SetXattrOp) error {
	return fuse.ENOSYS
}
