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
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func (s *Super) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	inode, err := s.InodeGet(ino)
	if err != nil {
		log.LogErrorf("GetInodeAttributes: op(%v) err(%v)", desc, err)
		return ParseError(err)
	}

	fillAttr(&op.Attributes, inode)
	op.AttributesExpiration = time.Now().Add(AttrValidDuration)

	if inode.mode.IsRegular() {
		fileSize, gen := s.ec.FileSize(ino)
		log.LogDebugf("GetInodeAttributes: op(%v) fileSize(%v) gen(%v) inode.gen(%v)", desc, fileSize, gen, inode.gen)
		if gen >= inode.gen {
			op.Attributes.Size = uint64(fileSize)
		}
	}

	log.LogDebugf("TRACE GetInodeAttributes: op(%v) attr(%v)", desc, op.Attributes.DebugString())
	return nil
}

func (s *Super) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	ino := uint64(op.Inode)
	desc := fuse.OpDescription(op)

	inode, err := s.InodeGet(ino)
	if err != nil {
		log.LogErrorf("SetInodeAttributes: InodeGet failed, op(%v) err(%v)", desc, err)
		return ParseError(err)
	}

	fillAttr(&op.Attributes, inode)
	op.AttributesExpiration = time.Now().Add(AttrValidDuration)

	if op.Size != nil && inode.mode.IsRegular() {
		if err := s.ec.Flush(ino); err != nil {
			log.LogErrorf("SetInodeAttributes: truncate wait for flush ino(%v) size(%v) err(%v)", ino, *op.Size, err)
			return ParseError(err)
		}
		if err := s.ec.Truncate(ino, int(*op.Size)); err != nil {
			log.LogErrorf("SetInodeAttributes: truncate ino(%v) size(%v) err(%v)", ino, *op.Size, err)
			return ParseError(err)
		}
		s.ic.Delete(ino)
		s.ec.RefreshExtentsCache(ino)

		op.Attributes.Size = *op.Size

		// print a warn log if truncate size down
		if *op.Size != inode.size {
			log.LogWarnf("SetInodeAttributes: truncate ino(%v) reqSize(%v) inodeSize(%v)", ino, *op.Size, inode.size)
		}
	}

	if valid := setattr(op, inode); valid != 0 {
		err = s.mw.Setattr(ino, valid, proto.Mode(inode.mode), inode.uid, inode.gid)
		if err != nil {
			s.ic.Delete(ino)
			return ParseError(err)
		}
	}

	log.LogDebugf("TRACE SetInodeAttributes: op(%v) returned attr(%v)", desc, op.Attributes.DebugString())
	return nil
}

func (s *Super) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) error {
	ino := uint64(op.Inode)
	defer func() {
		log.LogDebugf("TRACE Forget: ino(%v) nlookup(%v)", ino, op.N)
	}()

	if err := s.ec.EvictStream(ino); err != nil {
		log.LogWarnf("Forget: stream not ready to evict, ino(%v) err(%v)", ino, err)
		return nil
	}

	if !s.orphan.Evict(ino) {
		return nil
	}

	if err := s.mw.Evict(ino); err != nil {
		log.LogWarnf("Forget Evict: ino(%v) err(%v)", ino, err)
	}
	return nil
}
