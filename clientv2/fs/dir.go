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
	"os"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func (s *Super) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)

	info, err := s.mw.Create_ll(pino, op.Name, proto.Mode(os.ModeDir|op.Mode.Perm()), nil)
	if err != nil {
		log.LogErrorf("%v: err(%v)", desc, err)
		return ParseError(err)
	}

	child := NewInode(info)
	s.ic.Put(child)
	fillChildEntry(&op.Entry, child)

	log.LogDebugf("TRACE %v: child(%v)", desc, child)
	return nil
}

func (s *Super) MkNode(ctx context.Context, op *fuseops.MkNodeOp) error {
	return fuse.ENOSYS
}

func (s *Super) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)

	info, err := s.mw.Create_ll(pino, op.Name, proto.Mode(op.Mode.Perm()), nil)
	if err != nil {
		log.LogErrorf("%v: err(%v)", desc, err)
		return ParseError(err)
	}

	child := NewInode(info)
	s.ic.Put(child)
	s.ec.OpenStream(child.ino)

	fillChildEntry(&op.Entry, child)
	op.Handle = s.hc.Assign(child.ino)

	log.LogDebugf("TRACE %v: child(%v) handle(%v)", desc, child, op.Handle)
	return nil
}

func (s *Super) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)
	ino := uint64(op.Target)

	inode, err := s.InodeGet(ino)
	if err != nil {
		log.LogErrorf("%v: failed to get inode, err(%v)", desc, err)
		return ParseError(err)
	}

	if !inode.mode.IsRegular() {
		log.LogErrorf("%v: not regular, mode(%v)", desc, inode.mode)
		return fuse.EINVAL
	}

	info, err := s.mw.Link(pino, op.Name, ino)
	if err != nil {
		log.LogErrorf("%v: err(%v)", desc, err)
		return ParseError(err)
	}

	updatedInode := NewInode(info)
	s.ic.Put(updatedInode)
	fillChildEntry(&op.Entry, updatedInode)

	log.LogDebugf("TRACE %v: info(%v)", desc, info)
	return nil
}

func (s *Super) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)

	info, err := s.mw.Create_ll(pino, op.Name, proto.Mode(os.ModeSymlink|os.ModePerm), nil)
	if err != nil {
		log.LogErrorf("%v: err(%v)", desc, err)
		return ParseError(err)
	}

	child := NewInode(info)
	s.ic.Put(child)
	fillChildEntry(&op.Entry, child)

	log.LogDebugf("TRACE %v: child(%v)", desc, child)
	return nil
}

func (s *Super) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE enter %v: ", desc)

	ino, _, err := s.mw.Lookup_ll(pino, op.Name)
	if err != nil {
		if err != fuse.ENOENT {
			log.LogErrorf("%v: err(%v)", desc, err)
		}
		return ParseError(err)
	}

	inode, err := s.InodeGet(ino)
	if err != nil {
		log.LogErrorf("%v: ino(%v) err(%v)", desc, ino, err)
		return ParseError(err)
	}

	fillChildEntry(&op.Entry, inode)

	log.LogDebugf("TRACE exit %v: inode(%v)", desc, inode)
	return nil
}

func (s *Super) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE enter %v:", desc)

	info, err := s.mw.Delete_ll(pino, op.Name, true)
	if err != nil {
		log.LogErrorf("%v: err(%v)", desc, err)
		return ParseError(err)
	}

	log.LogDebugf("TRACE exit %v: info(%v)", desc, info)
	return nil
}

func (s *Super) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	pino := uint64(op.Parent)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE enter %v:", desc)

	info, err := s.mw.Delete_ll(pino, op.Name, false)
	if err != nil {
		log.LogErrorf("%v: err(%v)", desc, err)
		return ParseError(err)
	}

	if info != nil && info.Nlink == 0 {
		s.orphan.Put(info.Inode)
		log.LogDebugf("%v: add to orphan inode list, ino(%v)", desc, info.Inode)
	}

	log.LogDebugf("TRACE exit %v: info(%v)", desc, info)
	return nil
}

func (s *Super) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)
	op.Handle = s.hc.Assign(ino)
	log.LogDebugf("TRACE %v: handle(%v)", desc, op.Handle)
	return nil
}

func (s *Super) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	ino := uint64(op.Inode)
	pos := int(op.Offset)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE enter %v: offset(%v)", desc, op.Offset)

	handle := s.hc.Get(op.Handle)
	if handle == nil {
		log.LogErrorf("%v: dir not opened", desc)
		return syscall.EBADF
	}

	handle.lock.Lock()
	defer handle.lock.Unlock()

	if pos == 0 || handle.entries == nil {
		children, err := s.mw.ReadDir_ll(ino)
		if err != nil {
			log.LogErrorf("%v: failed to readdir from metanode, err(%v)", desc, err)
			return ParseError(err)
		}
		handle.entries = children
	}

	if pos > len(handle.entries) {
		log.LogErrorf("%v: offset beyond scope, pos(%v) num of entries(%v)", desc, pos, len(handle.entries))
		return fuse.EINVAL
	}

	inodes := make([]uint64, 0, len(handle.entries))

	for i := pos; i < len(handle.entries); i++ {
		child := handle.entries[i]
		dirent := fuseutil.Dirent{
			Offset: fuseops.DirOffset(i) + 1,
			Inode:  fuseops.InodeID(child.Inode),
			Name:   child.Name,
			Type:   ParseType(child.Type),
		}

		nbytes := fuseutil.WriteDirent(op.Dst[op.BytesRead:], dirent)
		if nbytes == 0 {
			break
		}
		op.BytesRead += nbytes
		inodes = append(inodes, child.Inode)
	}

	if pos == 0 {
		infos := s.mw.BatchInodeGet(inodes)
		for _, info := range infos {
			s.ic.Put(NewInode(info))
		}
	}

	log.LogDebugf("TRACE exit %v: handle(%v) BytesRead(%v)", desc, op.Handle, op.BytesRead)
	return nil
}

func (s *Super) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	desc := fuse.OpDescription(op)
	handle := s.hc.Release(op.Handle)
	log.LogDebugf("TRACE %v: ino(%v)", desc, handle.ino)
	return nil
}

func (s *Super) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	oldPino := uint64(op.OldParent)
	newPino := uint64(op.NewParent)
	desc := fuse.OpDescription(op)

	log.LogDebugf("TRACE enter %v: ", desc)

	err := s.mw.Rename_ll(oldPino, op.OldName, newPino, op.NewName)
	if err != nil {
		log.LogErrorf("Rename: op(%v) err(%v)", desc, err)
		return ParseError(err)
	}

	log.LogDebugf("TRACE exit %v: ", desc)
	return nil
}
