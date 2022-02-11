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

package fuseutil

import (
	"context"
	"io"
	"sync"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

// An interface with a method for each op type in the fuseops package. This can
// be used in conjunction with NewFileSystemServer to avoid writing a "dispatch
// loop" that switches on op types, instead receiving typed method calls
// directly.
//
// The FileSystem implementation should not call Connection.Reply, instead
// returning the error with which the caller should respond.
//
// See NotImplementedFileSystem for a convenient way to embed default
// implementations for methods you don't care about.
type FileSystem interface {
	StatFS(context.Context, *fuseops.StatFSOp) error
	LookUpInode(context.Context, *fuseops.LookUpInodeOp) error
	GetInodeAttributes(context.Context, *fuseops.GetInodeAttributesOp) error
	SetInodeAttributes(context.Context, *fuseops.SetInodeAttributesOp) error
	ForgetInode(context.Context, *fuseops.ForgetInodeOp) error
	MkDir(context.Context, *fuseops.MkDirOp) error
	MkNode(context.Context, *fuseops.MkNodeOp) error
	CreateFile(context.Context, *fuseops.CreateFileOp) error
	CreateLink(context.Context, *fuseops.CreateLinkOp) error
	CreateSymlink(context.Context, *fuseops.CreateSymlinkOp) error
	Rename(context.Context, *fuseops.RenameOp) error
	RmDir(context.Context, *fuseops.RmDirOp) error
	Unlink(context.Context, *fuseops.UnlinkOp) error
	OpenDir(context.Context, *fuseops.OpenDirOp) error
	ReadDir(context.Context, *fuseops.ReadDirOp) error
	ReleaseDirHandle(context.Context, *fuseops.ReleaseDirHandleOp) error
	OpenFile(context.Context, *fuseops.OpenFileOp) error
	ReadFile(context.Context, *fuseops.ReadFileOp) error
	WriteFile(context.Context, *fuseops.WriteFileOp) error
	SyncFile(context.Context, *fuseops.SyncFileOp) error
	FlushFile(context.Context, *fuseops.FlushFileOp) error
	ReleaseFileHandle(context.Context, *fuseops.ReleaseFileHandleOp) error
	ReadSymlink(context.Context, *fuseops.ReadSymlinkOp) error
	RemoveXattr(context.Context, *fuseops.RemoveXattrOp) error
	GetXattr(context.Context, *fuseops.GetXattrOp) error
	ListXattr(context.Context, *fuseops.ListXattrOp) error
	SetXattr(context.Context, *fuseops.SetXattrOp) error

	// Regard all inodes (including the root inode) as having their lookup counts
	// decremented to zero, and clean up any resources associated with the file
	// system. No further calls to the file system will be made.
	Destroy()
}

// Create a fuse.Server that handles ops by calling the associated FileSystem
// method.Respond with the resulting error. Unsupported ops are responded to
// directly with ENOSYS.
//
// Each call to a FileSystem method (except ForgetInode) is made on
// its own goroutine, and is free to block. ForgetInode may be called
// synchronously, and should not depend on calls to other methods
// being received concurrently.
//
// (It is safe to naively process ops concurrently because the kernel
// guarantees to serialize operations that the user expects to happen in order,
// cf. http://goo.gl/jnkHPO, fuse-devel thread "Fuse guarantees on concurrent
// requests").
func NewFileSystemServer(fs FileSystem) fuse.Server {
	return &fileSystemServer{
		fs: fs,
	}
}

type fileSystemServer struct {
	fs          FileSystem
	opsInFlight sync.WaitGroup
}

func (s *fileSystemServer) ServeOps(c *fuse.Connection) {
	// When we are done, we clean up by waiting for all in-flight ops then
	// destroying the file system.
	defer func() {
		s.opsInFlight.Wait()
		s.fs.Destroy()
	}()

	for {
		ctx, op, err := c.ReadOp()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		s.opsInFlight.Add(1)
		if _, ok := op.(*fuseops.ForgetInodeOp); ok {
			// Special case: call in this goroutine for
			// forget inode ops, which may come in a
			// flurry from the kernel and are generally
			// cheap for the file system to handle
			s.handleOp(c, ctx, op)
		} else {
			go s.handleOp(c, ctx, op)
		}
	}
}

func (s *fileSystemServer) handleOp(
	c *fuse.Connection,
	ctx context.Context,
	op interface{}) {
	defer s.opsInFlight.Done()

	// Dispatch to the appropriate method.
	var err error
	switch typed := op.(type) {
	default:
		err = fuse.ENOSYS

	case *fuseops.StatFSOp:
		err = s.fs.StatFS(ctx, typed)

	case *fuseops.LookUpInodeOp:
		err = s.fs.LookUpInode(ctx, typed)

	case *fuseops.GetInodeAttributesOp:
		err = s.fs.GetInodeAttributes(ctx, typed)

	case *fuseops.SetInodeAttributesOp:
		err = s.fs.SetInodeAttributes(ctx, typed)

	case *fuseops.ForgetInodeOp:
		err = s.fs.ForgetInode(ctx, typed)

	case *fuseops.MkDirOp:
		err = s.fs.MkDir(ctx, typed)

	case *fuseops.MkNodeOp:
		err = s.fs.MkNode(ctx, typed)

	case *fuseops.CreateFileOp:
		err = s.fs.CreateFile(ctx, typed)

	case *fuseops.CreateLinkOp:
		err = s.fs.CreateLink(ctx, typed)

	case *fuseops.CreateSymlinkOp:
		err = s.fs.CreateSymlink(ctx, typed)

	case *fuseops.RenameOp:
		err = s.fs.Rename(ctx, typed)

	case *fuseops.RmDirOp:
		err = s.fs.RmDir(ctx, typed)

	case *fuseops.UnlinkOp:
		err = s.fs.Unlink(ctx, typed)

	case *fuseops.OpenDirOp:
		err = s.fs.OpenDir(ctx, typed)

	case *fuseops.ReadDirOp:
		err = s.fs.ReadDir(ctx, typed)

	case *fuseops.ReleaseDirHandleOp:
		err = s.fs.ReleaseDirHandle(ctx, typed)

	case *fuseops.OpenFileOp:
		err = s.fs.OpenFile(ctx, typed)

	case *fuseops.ReadFileOp:
		err = s.fs.ReadFile(ctx, typed)

	case *fuseops.WriteFileOp:
		err = s.fs.WriteFile(ctx, typed)

	case *fuseops.SyncFileOp:
		err = s.fs.SyncFile(ctx, typed)

	case *fuseops.FlushFileOp:
		err = s.fs.FlushFile(ctx, typed)

	case *fuseops.ReleaseFileHandleOp:
		err = s.fs.ReleaseFileHandle(ctx, typed)

	case *fuseops.ReadSymlinkOp:
		err = s.fs.ReadSymlink(ctx, typed)

	case *fuseops.RemoveXattrOp:
		err = s.fs.RemoveXattr(ctx, typed)

	case *fuseops.GetXattrOp:
		err = s.fs.GetXattr(ctx, typed)

	case *fuseops.ListXattrOp:
		err = s.fs.ListXattr(ctx, typed)

	case *fuseops.SetXattrOp:
		err = s.fs.SetXattr(ctx, typed)
	}

	c.Reply(ctx, err)
}
