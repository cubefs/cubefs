// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuse

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"time"
	"unsafe"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/internal/buffer"
	"github.com/jacobsa/fuse/internal/fusekernel"
)

////////////////////////////////////////////////////////////////////////
// Incoming messages
////////////////////////////////////////////////////////////////////////

// Convert a kernel message to an appropriate op. If the op is unknown, a
// special unexported type will be used.
//
// The caller is responsible for arranging for the message to be destroyed.
func convertInMessage(
	inMsg *buffer.InMessage,
	outMsg *buffer.OutMessage,
	protocol fusekernel.Protocol) (o interface{}, err error) {
	switch inMsg.Header().Opcode {
	case fusekernel.OpLookup:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpLookup")
			return
		}

		o = &fuseops.LookUpInodeOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(buf[:n-1]),
		}

	case fusekernel.OpGetattr:
		o = &fuseops.GetInodeAttributesOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
		}

	case fusekernel.OpSetattr:
		type input fusekernel.SetattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpSetattr")
			return
		}

		to := &fuseops.SetInodeAttributesOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
		}
		o = to

		valid := fusekernel.SetattrValid(in.Valid)
		if valid&fusekernel.SetattrSize != 0 {
			to.Size = &in.Size
		}

		if valid&fusekernel.SetattrMode != 0 {
			mode := convertFileMode(in.Mode)
			to.Mode = &mode
		}

		if valid&fusekernel.SetattrAtime != 0 {
			t := time.Unix(int64(in.Atime), int64(in.AtimeNsec))
			to.Atime = &t
		}

		if valid&fusekernel.SetattrMtime != 0 {
			t := time.Unix(int64(in.Mtime), int64(in.MtimeNsec))
			to.Mtime = &t
		}

		if valid&fusekernel.SetattrUid != 0 {
			to.Uid = &in.Uid
		}

		if valid&fusekernel.SetattrGid != 0 {
			to.Gid = &in.Gid
		}

	case fusekernel.OpForget:
		type input fusekernel.ForgetIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpForget")
			return
		}

		o = &fuseops.ForgetInodeOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
			N:     in.Nlookup,
		}

	case fusekernel.OpMkdir:
		in := (*fusekernel.MkdirIn)(inMsg.Consume(fusekernel.MkdirInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpMkdir")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpMkdir")
			return
		}
		name = name[:i]

		o = &fuseops.MkDirOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(name),

			// On Linux, vfs_mkdir calls through to the inode with at most
			// permissions and sticky bits set (cf. https://goo.gl/WxgQXk), and fuse
			// passes that on directly (cf. https://goo.gl/f31aMo). In other words,
			// the fact that this is a directory is implicit in the fact that the
			// opcode is mkdir. But we want the correct mode to go through, so ensure
			// that os.ModeDir is set.
			Mode: convertFileMode(in.Mode) | os.ModeDir,
		}

	case fusekernel.OpMknod:
		in := (*fusekernel.MknodIn)(inMsg.Consume(fusekernel.MknodInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpMknod")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpMknod")
			return
		}
		name = name[:i]

		o = &fuseops.MkNodeOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(name),
			Mode:   convertFileMode(in.Mode),
		}

	case fusekernel.OpCreate:
		in := (*fusekernel.CreateIn)(inMsg.Consume(fusekernel.CreateInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpCreate")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpCreate")
			return
		}
		name = name[:i]

		o = &fuseops.CreateFileOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(name),
			Mode:   convertFileMode(in.Mode),
		}

	case fusekernel.OpSymlink:
		// The message is "newName\0target\0".
		names := inMsg.ConsumeBytes(inMsg.Len())
		if len(names) == 0 || names[len(names)-1] != 0 {
			err = errors.New("Corrupt OpSymlink")
			return
		}
		i := bytes.IndexByte(names, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpSymlink")
			return
		}
		newName, target := names[0:i], names[i+1:len(names)-1]

		o = &fuseops.CreateSymlinkOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(newName),
			Target: string(target),
		}

	case fusekernel.OpRename:
		type input fusekernel.RenameIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpRename")
			return
		}

		names := inMsg.ConsumeBytes(inMsg.Len())
		// names should be "old\x00new\x00"
		if len(names) < 4 {
			err = errors.New("Corrupt OpRename")
			return
		}
		if names[len(names)-1] != '\x00' {
			err = errors.New("Corrupt OpRename")
			return
		}
		i := bytes.IndexByte(names, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpRename")
			return
		}
		oldName, newName := names[:i], names[i+1:len(names)-1]

		o = &fuseops.RenameOp{
			OldParent: fuseops.InodeID(inMsg.Header().Nodeid),
			OldName:   string(oldName),
			NewParent: fuseops.InodeID(in.Newdir),
			NewName:   string(newName),
		}

	case fusekernel.OpUnlink:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpUnlink")
			return
		}

		o = &fuseops.UnlinkOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(buf[:n-1]),
		}

	case fusekernel.OpRmdir:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpRmdir")
			return
		}

		o = &fuseops.RmDirOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(buf[:n-1]),
		}

	case fusekernel.OpOpen:
		o = &fuseops.OpenFileOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
		}

	case fusekernel.OpOpendir:
		o = &fuseops.OpenDirOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
		}

	case fusekernel.OpRead:
		in := (*fusekernel.ReadIn)(inMsg.Consume(fusekernel.ReadInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpRead")
			return
		}

		to := &fuseops.ReadFileOp{
			Inode:  fuseops.InodeID(inMsg.Header().Nodeid),
			Handle: fuseops.HandleID(in.Fh),
			Offset: int64(in.Offset),
		}
		o = to

		readSize := int(in.Size)
		p := outMsg.GrowNoZero(readSize)
		if p == nil {
			err = fmt.Errorf("Can't grow for %d-byte read", readSize)
			return
		}

		sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
		sh.Data = uintptr(p)
		sh.Len = readSize
		sh.Cap = readSize

	case fusekernel.OpReaddir:
		in := (*fusekernel.ReadIn)(inMsg.Consume(fusekernel.ReadInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpReaddir")
			return
		}

		to := &fuseops.ReadDirOp{
			Inode:  fuseops.InodeID(inMsg.Header().Nodeid),
			Handle: fuseops.HandleID(in.Fh),
			Offset: fuseops.DirOffset(in.Offset),
		}
		o = to

		readSize := int(in.Size)
		p := outMsg.GrowNoZero(readSize)
		if p == nil {
			err = fmt.Errorf("Can't grow for %d-byte read", readSize)
			return
		}

		sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
		sh.Data = uintptr(p)
		sh.Len = readSize
		sh.Cap = readSize

	case fusekernel.OpRelease:
		type input fusekernel.ReleaseIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpRelease")
			return
		}

		o = &fuseops.ReleaseFileHandleOp{
			Handle: fuseops.HandleID(in.Fh),
		}

	case fusekernel.OpReleasedir:
		type input fusekernel.ReleaseIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpReleasedir")
			return
		}

		o = &fuseops.ReleaseDirHandleOp{
			Handle: fuseops.HandleID(in.Fh),
		}

	case fusekernel.OpWrite:
		in := (*fusekernel.WriteIn)(inMsg.Consume(fusekernel.WriteInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpWrite")
			return
		}

		buf := inMsg.ConsumeBytes(inMsg.Len())
		if len(buf) < int(in.Size) {
			err = errors.New("Corrupt OpWrite")
			return
		}

		o = &fuseops.WriteFileOp{
			Inode:      fuseops.InodeID(inMsg.Header().Nodeid),
			Handle:     fuseops.HandleID(in.Fh),
			Data:       buf,
			Offset:     int64(in.Offset),
			Flags:      in.Flags,
			WriteFlags: in.WriteFlags,
		}

	case fusekernel.OpFsync:
		type input fusekernel.FsyncIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpFsync")
			return
		}

		o = &fuseops.SyncFileOp{
			Inode:  fuseops.InodeID(inMsg.Header().Nodeid),
			Handle: fuseops.HandleID(in.Fh),
		}

	case fusekernel.OpFlush:
		type input fusekernel.FlushIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpFlush")
			return
		}

		o = &fuseops.FlushFileOp{
			Inode:  fuseops.InodeID(inMsg.Header().Nodeid),
			Handle: fuseops.HandleID(in.Fh),
		}

	case fusekernel.OpReadlink:
		o = &fuseops.ReadSymlinkOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
		}

	case fusekernel.OpStatfs:
		o = &fuseops.StatFSOp{}

	case fusekernel.OpInterrupt:
		type input fusekernel.InterruptIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpInterrupt")
			return
		}

		o = &interruptOp{
			FuseID: in.Unique,
		}

	case fusekernel.OpInit:
		type input fusekernel.InitIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpInit")
			return
		}

		o = &initOp{
			Kernel:       fusekernel.Protocol{in.Major, in.Minor},
			MaxReadahead: in.MaxReadahead,
			Flags:        fusekernel.InitFlags(in.Flags),
		}

	case fusekernel.OpLink:
		type input fusekernel.LinkIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpLink")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpLink")
			return
		}
		name = name[:i]
		if len(name) == 0 {
			err = errors.New("Corrupt OpLink (Name not read)")
			return
		}

		o = &fuseops.CreateLinkOp{
			Parent: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:   string(name),
			Target: fuseops.InodeID(in.Oldnodeid),
		}

	case fusekernel.OpRemovexattr:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpRemovexattr")
			return
		}

		o = &fuseops.RemoveXattrOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:  string(buf[:n-1]),
		}

	case fusekernel.OpGetxattr:
		type input fusekernel.GetxattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpGetxattr")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpGetxattr")
			return
		}
		name = name[:i]

		to := &fuseops.GetXattrOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:  string(name),
		}
		o = to

		readSize := int(in.Size)
		p := outMsg.GrowNoZero(readSize)
		if p == nil {
			err = fmt.Errorf("Can't grow for %d-byte read", readSize)
			return
		}

		sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
		sh.Data = uintptr(p)
		sh.Len = readSize
		sh.Cap = readSize

	case fusekernel.OpListxattr:
		type input fusekernel.ListxattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpListxattr")
			return
		}

		to := &fuseops.ListXattrOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
		}
		o = to

		readSize := int(in.Size)
		if readSize != 0 {
			p := outMsg.GrowNoZero(readSize)
			if p == nil {
				err = fmt.Errorf("Can't grow for %d-byte read", readSize)
				return
			}
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
			sh.Data = uintptr(p)
			sh.Len = readSize
			sh.Cap = readSize
		}
	case fusekernel.OpSetxattr:
		type input fusekernel.SetxattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpSetxattr")
			return
		}

		payload := inMsg.ConsumeBytes(inMsg.Len())
		// payload should be "name\x00value"
		if len(payload) < 3 {
			err = errors.New("Corrupt OpSetxattr")
			return
		}
		i := bytes.IndexByte(payload, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpSetxattr")
			return
		}

		name, value := payload[:i], payload[i+1:len(payload)]

		o = &fuseops.SetXattrOp{
			Inode: fuseops.InodeID(inMsg.Header().Nodeid),
			Name:  string(name),
			Value: value,
			Flags: in.Flags,
		}

	default:
		o = &unknownOp{
			OpCode: inMsg.Header().Opcode,
			Inode:  fuseops.InodeID(inMsg.Header().Nodeid),
		}
	}

	return
}

////////////////////////////////////////////////////////////////////////
// Outgoing messages
////////////////////////////////////////////////////////////////////////

// Fill in the response that should be sent to the kernel, or set noResponse if
// the op requires no response.
func (c *Connection) kernelResponse(
	m *buffer.OutMessage,
	fuseID uint64,
	op interface{},
	opErr error) (noResponse bool) {
	h := m.OutHeader()
	h.Unique = fuseID

	// Special case: handle the ops for which the kernel expects no response.
	// interruptOp .
	switch op.(type) {
	case *fuseops.ForgetInodeOp:
		noResponse = true
		return

	case *interruptOp:
		noResponse = true
		return
	}

	// If the user returned the error, fill in the error field of the outgoing
	// message header.
	if opErr != nil {
		handled := false

		if opErr == syscall.ERANGE {
			switch o := op.(type) {
			case *fuseops.GetXattrOp:
				writeXattrSize(m, uint32(o.BytesRead))
				handled = true
			case *fuseops.ListXattrOp:
				writeXattrSize(m, uint32(o.BytesRead))
				handled = true
			}
		}

		if !handled {
			m.OutHeader().Error = -int32(syscall.EIO)
			if errno, ok := opErr.(syscall.Errno); ok {
				m.OutHeader().Error = -int32(errno)
			}

			// Special case: for some types, convertInMessage grew the message in order
			// to obtain a destination buffer. Make sure that we shrink back to just
			// the header, because on OS X the kernel otherwise returns EINVAL when we
			// attempt to write an error response with a length that extends beyond the
			// header.
			m.ShrinkTo(buffer.OutMessageHeaderSize)
		}
	}

	// Otherwise, fill in the rest of the response.
	if opErr == nil {
		c.kernelResponseForOp(m, op)
	}

	h.Len = uint32(m.Len())
	return
}

// Like kernelResponse, but assumes the user replied with a nil error to the
// op.
func (c *Connection) kernelResponseForOp(
	m *buffer.OutMessage,
	op interface{}) {
	// Create the appropriate output message
	switch o := op.(type) {
	case *fuseops.LookUpInodeOp:
		size := int(fusekernel.EntryOutSize(c.protocol))
		out := (*fusekernel.EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *fuseops.GetInodeAttributesOp:
		size := int(fusekernel.AttrOutSize(c.protocol))
		out := (*fusekernel.AttrOut)(m.Grow(size))
		out.AttrValid, out.AttrValidNsec = convertExpirationTime(
			o.AttributesExpiration)
		convertAttributes(o.Inode, &o.Attributes, &out.Attr)

	case *fuseops.SetInodeAttributesOp:
		size := int(fusekernel.AttrOutSize(c.protocol))
		out := (*fusekernel.AttrOut)(m.Grow(size))
		out.AttrValid, out.AttrValidNsec = convertExpirationTime(
			o.AttributesExpiration)
		convertAttributes(o.Inode, &o.Attributes, &out.Attr)

	case *fuseops.MkDirOp:
		size := int(fusekernel.EntryOutSize(c.protocol))
		out := (*fusekernel.EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *fuseops.MkNodeOp:
		size := int(fusekernel.EntryOutSize(c.protocol))
		out := (*fusekernel.EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *fuseops.CreateFileOp:
		eSize := int(fusekernel.EntryOutSize(c.protocol))

		e := (*fusekernel.EntryOut)(m.Grow(eSize))
		convertChildInodeEntry(&o.Entry, e)

		oo := (*fusekernel.OpenOut)(m.Grow(int(unsafe.Sizeof(fusekernel.OpenOut{}))))
		oo.Fh = uint64(o.Handle)

	case *fuseops.CreateSymlinkOp:
		size := int(fusekernel.EntryOutSize(c.protocol))
		out := (*fusekernel.EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *fuseops.CreateLinkOp:
		size := int(fusekernel.EntryOutSize(c.protocol))
		out := (*fusekernel.EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *fuseops.RenameOp:
		// Empty response

	case *fuseops.RmDirOp:
		// Empty response

	case *fuseops.UnlinkOp:
		// Empty response

	case *fuseops.OpenDirOp:
		out := (*fusekernel.OpenOut)(m.Grow(int(unsafe.Sizeof(fusekernel.OpenOut{}))))
		out.Fh = uint64(o.Handle)

	case *fuseops.ReadDirOp:
		// convertInMessage already set up the destination buffer to be at the end
		// of the out message. We need only shrink to the right size based on how
		// much the user read.
		m.ShrinkTo(buffer.OutMessageHeaderSize + o.BytesRead)

	case *fuseops.ReleaseDirHandleOp:
		// Empty response

	case *fuseops.OpenFileOp:
		out := (*fusekernel.OpenOut)(m.Grow(int(unsafe.Sizeof(fusekernel.OpenOut{}))))
		out.Fh = uint64(o.Handle)

		if o.KeepPageCache {
			out.OpenFlags |= uint32(fusekernel.OpenKeepCache)
		}

		if o.UseDirectIO {
			out.OpenFlags |= uint32(fusekernel.OpenDirectIO)
		}

	case *fuseops.ReadFileOp:
		// convertInMessage already set up the destination buffer to be at the end
		// of the out message. We need only shrink to the right size based on how
		// much the user read.
		m.ShrinkTo(buffer.OutMessageHeaderSize + o.BytesRead)

	case *fuseops.WriteFileOp:
		out := (*fusekernel.WriteOut)(m.Grow(int(unsafe.Sizeof(fusekernel.WriteOut{}))))
		out.Size = uint32(len(o.Data))

	case *fuseops.SyncFileOp:
		// Empty response

	case *fuseops.FlushFileOp:
		// Empty response

	case *fuseops.ReleaseFileHandleOp:
		// Empty response

	case *fuseops.ReadSymlinkOp:
		m.AppendString(o.Target)

	case *fuseops.StatFSOp:
		out := (*fusekernel.StatfsOut)(m.Grow(int(unsafe.Sizeof(fusekernel.StatfsOut{}))))
		out.St.Blocks = o.Blocks
		out.St.Bfree = o.BlocksFree
		out.St.Bavail = o.BlocksAvailable
		out.St.Files = o.Inodes
		out.St.Ffree = o.InodesFree

		// The posix spec for sys/statvfs.h (http://goo.gl/LktgrF) defines the
		// following fields of statvfs, among others:
		//
		//     f_bsize    File system block size.
		//     f_frsize   Fundamental file system block size.
		//     f_blocks   Total number of blocks on file system in units of f_frsize.
		//
		// It appears as though f_bsize was the only thing supported by most unixes
		// originally, but then f_frsize was added when new sorts of file systems
		// came about. Quoth The Linux Programming Interface by Michael Kerrisk
		// (https://goo.gl/5LZMxQ):
		//
		//     For most Linux file systems, the values of f_bsize and f_frsize are
		//     the same. However, some file systems support the notion of block
		//     fragments, which can be used to allocate a smaller unit of storage
		//     at the end of the file if if a full block is not required. This
		//     avoids the waste of space that would otherwise occur if a full block
		//     was allocated. On such file systems, f_frsize is the size of a
		//     fragment, and f_bsize is the size of a whole block. (The notion of
		//     fragments in UNIX file systems first appeared in the early 1980s
		//     with the 4.2BSD Fast File System.)
		//
		// Confusingly, it appears as though osxfuse surfaces fuse_kstatfs::bsize
		// as statfs::f_iosize (of advisory use only), and fuse_kstatfs::frsize as
		// statfs::f_bsize (which affects free space display in the Finder).
		out.St.Bsize = o.IoSize
		out.St.Frsize = o.BlockSize

	case *fuseops.RemoveXattrOp:
		// Empty response

	case *fuseops.GetXattrOp:
		// convertInMessage already set up the destination buffer to be at the end
		// of the out message. We need only shrink to the right size based on how
		// much the user read.
		if o.BytesRead == 0 {
			writeXattrSize(m, uint32(o.BytesRead))
		} else {
			m.ShrinkTo(buffer.OutMessageHeaderSize + o.BytesRead)
		}

	case *fuseops.ListXattrOp:
		if o.BytesRead == 0 {
			writeXattrSize(m, uint32(o.BytesRead))
		} else {
			m.ShrinkTo(buffer.OutMessageHeaderSize + o.BytesRead)
		}

	case *fuseops.SetXattrOp:
		// Empty response

	case *initOp:
		out := (*fusekernel.InitOut)(m.Grow(int(unsafe.Sizeof(fusekernel.InitOut{}))))

		out.Major = o.Library.Major
		out.Minor = o.Library.Minor
		out.MaxReadahead = o.MaxReadahead
		out.Flags = uint32(o.Flags)
		out.MaxWrite = o.MaxWrite

	default:
		panic(fmt.Sprintf("Unexpected op: %#v", op))
	}

	return
}

////////////////////////////////////////////////////////////////////////
// General conversions
////////////////////////////////////////////////////////////////////////

func convertTime(t time.Time) (secs uint64, nsec uint32) {
	totalNano := t.UnixNano()
	secs = uint64(totalNano / 1e9)
	nsec = uint32(totalNano % 1e9)
	return
}

func convertAttributes(
	inodeID fuseops.InodeID,
	in *fuseops.InodeAttributes,
	out *fusekernel.Attr) {
	out.Ino = uint64(inodeID)
	out.Size = in.Size
	out.Atime, out.AtimeNsec = convertTime(in.Atime)
	out.Mtime, out.MtimeNsec = convertTime(in.Mtime)
	out.Ctime, out.CtimeNsec = convertTime(in.Ctime)
	out.SetCrtime(convertTime(in.Crtime))
	out.Nlink = in.Nlink
	out.Uid = in.Uid
	out.Gid = in.Gid
	// round up to the nearest 512 boundary
	out.Blocks = (in.Size + 512 - 1) / 512

	// Set the mode.
	out.Mode = uint32(in.Mode) & 0777
	switch {
	default:
		out.Mode |= syscall.S_IFREG
	case in.Mode&os.ModeDir != 0:
		out.Mode |= syscall.S_IFDIR
	case in.Mode&os.ModeDevice != 0:
		if in.Mode&os.ModeCharDevice != 0 {
			out.Mode |= syscall.S_IFCHR
		} else {
			out.Mode |= syscall.S_IFBLK
		}
	case in.Mode&os.ModeNamedPipe != 0:
		out.Mode |= syscall.S_IFIFO
	case in.Mode&os.ModeSymlink != 0:
		out.Mode |= syscall.S_IFLNK
	case in.Mode&os.ModeSocket != 0:
		out.Mode |= syscall.S_IFSOCK
	}
}

// Convert an absolute cache expiration time to a relative time from now for
// consumption by the fuse kernel module.
func convertExpirationTime(t time.Time) (secs uint64, nsecs uint32) {
	// Fuse represents durations as unsigned 64-bit counts of seconds and 32-bit
	// counts of nanoseconds (cf. http://goo.gl/EJupJV). So negative durations
	// are right out. There is no need to cap the positive magnitude, because
	// 2^64 seconds is well longer than the 2^63 ns range of time.Duration.
	d := t.Sub(time.Now())
	if d > 0 {
		secs = uint64(d / time.Second)
		nsecs = uint32((d % time.Second) / time.Nanosecond)
	}

	return
}

func convertChildInodeEntry(
	in *fuseops.ChildInodeEntry,
	out *fusekernel.EntryOut) {
	out.Nodeid = uint64(in.Child)
	out.Generation = uint64(in.Generation)
	out.EntryValid, out.EntryValidNsec = convertExpirationTime(in.EntryExpiration)
	out.AttrValid, out.AttrValidNsec = convertExpirationTime(in.AttributesExpiration)

	convertAttributes(in.Child, &in.Attributes, &out.Attr)
}

func convertFileMode(unixMode uint32) os.FileMode {
	mode := os.FileMode(unixMode & 0777)
	switch unixMode & syscall.S_IFMT {
	case syscall.S_IFREG:
		// nothing
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFCHR:
		mode |= os.ModeCharDevice | os.ModeDevice
	case syscall.S_IFBLK:
		mode |= os.ModeDevice
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFSOCK:
		mode |= os.ModeSocket
	default:
		// no idea
		mode |= os.ModeDevice
	}
	if unixMode&syscall.S_ISUID != 0 {
		mode |= os.ModeSetuid
	}
	if unixMode&syscall.S_ISGID != 0 {
		mode |= os.ModeSetgid
	}
	return mode
}

func writeXattrSize(m *buffer.OutMessage, size uint32) {
	out := (*fusekernel.GetxattrOut)(m.Grow(int(unsafe.Sizeof(fusekernel.GetxattrOut{}))))
	out.Size = size
}
