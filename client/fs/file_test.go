// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package fs

import (
	"context"
	"errors"
	"os"
	"reflect"
	"syscall"
	"testing"
	"unsafe"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/stretchr/testify/require"
)

func superForFileTest(t *testing.T) *Super {
	t.Helper()
	s := superForDirMutationTest(t)
	s.ec = &stream.ExtentClient{}
	return s
}

func superForFileOpenTest(t *testing.T, getExtents stream.GetExtentsFunc) *Super {
	t.Helper()
	s := superForFileTest(t)
	ec := stream.NewTestExtentClient(getExtents)
	setUnexportedField(t, ec, "multiVerMgr", &stream.MultiVerMgr{})
	setUnexportedField(t, ec, "forbiddenMigration", stream.ForbiddenMigrationFunc(func(uint64) error { return nil }))
	setUnexportedField(t, ec, "renewalForbiddenMigration", stream.RenewalForbiddenMigrationFunc(func(uint64) error { return nil }))
	setUnexportedField(t, ec, "loadInodeInfo", stream.LoadInodeInfoFunc(func(uint64) (*proto.InodeInfo, error) { return nil, nil }))
	s.ec = ec
	return s
}

func setUnexportedField(t *testing.T, target any, name string, value any) {
	t.Helper()
	field := reflect.ValueOf(target).Elem().FieldByName(name)
	require.True(t, field.IsValid(), "field %s must exist", name)
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func symlinkInodeInfoForTest(ino uint64, target string) *proto.InodeInfo {
	info := fileInodeInfoForMutationTest(ino)
	info.Mode = uint32(os.ModeSymlink | 0o777)
	info.Target = []byte(target)
	return info
}

func newTestFile(s *Super, info *proto.InodeInfo, parentIno uint64, name string) *File {
	return NewFile(s, info, syscall.O_RDONLY, parentIno, name).(*File)
}

func TestIsWriteEio(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "eopnotsup", err: syscall.EOPNOTSUPP, want: false},
		{name: "enotsup", err: syscall.ENOTSUP, want: false},
		{name: "enoent", err: syscall.ENOENT, want: false},
		{name: "enoent_wrapped", err: errors.New("open: " + syscall.ENOENT.Error()), want: false},
		{name: "ebadf", err: syscall.EBADF, want: false},
		{name: "edquot", err: syscall.EDQUOT, want: false},
		{name: "stream_writer_error", err: errors.New("stream writer in error status"), want: false},
		{name: "generic_io", err: syscall.EIO, want: true},
		{name: "generic_msg", err: errors.New("disk failure"), want: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isWriteEio(tc.err))
		})
	}
}

func TestIsReadEio(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "eopnotsup", err: syscall.EOPNOTSUPP, want: false},
		{name: "enotsup", err: syscall.ENOTSUP, want: false},
		{name: "extent_not_found", err: errors.New("ExtentNotFoundError"), want: false},
		{name: "enoent", err: syscall.ENOENT, want: false},
		{name: "ebadf", err: syscall.EBADF, want: false},
		{name: "generic_io", err: syscall.EIO, want: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isReadEio(tc.err))
		})
	}
}

func TestGetStorageClassByPoolIdFromSuper(t *testing.T) {
	t.Parallel()
	s := superForFileTest(t)
	got := getStorageClassByPoolIdFromSuper(s, 0)
	require.NotNil(t, got)
	require.Equal(t, uint8(proto.StorageClass_Replica_HDD), got.StorageClass)

	require.Nil(t, getStorageClassByPoolIdFromSuper(s, 99))
}

func TestNewFile_replica_returnsFile(t *testing.T) {
	t.Parallel()
	s := superForFileTest(t)
	info := fileInodeInfoForMutationTest(90001)

	for _, flag := range []uint32{syscall.O_RDONLY, syscall.O_WRONLY, syscall.O_RDWR} {
		flag := flag
		t.Run(flagName(flag), func(t *testing.T) {
			t.Parallel()
			node := NewFile(s, info, flag, 1, "a.txt")
			require.NotNil(t, node)
			f := node.(*File)
			require.Equal(t, flag, f.flag)
			require.Equal(t, "a.txt", f.name)
			require.Nil(t, f.fReader)
			require.Nil(t, f.fWriter)
		})
	}
}

func TestNewFile_blobStore_noClient_returnsNil(t *testing.T) {
	t.Parallel()
	s := superForFileTest(t)
	info := fileInodeInfoForMutationTest(90002)
	info.StorageClass = proto.StorageClass_BlobStore

	require.Nil(t, NewFile(s, info, syscall.O_RDONLY, 1, "cold.txt"))
}

func TestFile_filterFilesSuffix(t *testing.T) {
	t.Parallel()
	f := &File{info: &proto.InodeInfo{Inode: 1}, name: "readme.txt"}

	require.True(t, (&File{info: &proto.InodeInfo{Inode: 2}, name: ""}).filterFilesSuffix(""))
	require.True(t, (&File{info: &proto.InodeInfo{Inode: 3}, name: ""}).filterFilesSuffix("py"))

	require.False(t, f.filterFilesSuffix("py"))
	require.False(t, (&File{name: "readme.txt"}).filterFilesSuffix(""))

	require.True(t, (&File{name: "app.py"}).filterFilesSuffix("py"))
	require.False(t, (&File{name: "main.go"}).filterFilesSuffix("py;log"))
}

func TestFile_getParentPath(t *testing.T) {
	t.Parallel()

	t.Run("parent_is_root", func(t *testing.T) {
		t.Parallel()
		s := superForFileTest(t)
		f := &File{super: s, parentIno: s.rootIno, name: "f.txt", info: &proto.InodeInfo{Inode: 10}}
		require.Equal(t, "/", f.getParentPath())
	})

	t.Run("cache_miss", func(t *testing.T) {
		t.Parallel()
		s := superForFileTest(t)
		f := &File{super: s, parentIno: 404, name: "f.txt", info: &proto.InodeInfo{Inode: 11}}
		require.Equal(t, "unknown", f.getParentPath())
	})

	t.Run("parent_not_dir", func(t *testing.T) {
		t.Parallel()
		const parentIno uint64 = 300
		s := superForFileTest(t)
		s.nodeCache[parentIno] = &File{super: s, parentIno: 1, name: "notdir", info: &proto.InodeInfo{Inode: parentIno}}
		f := &File{super: s, parentIno: parentIno, name: "f.txt", info: &proto.InodeInfo{Inode: 12}}
		require.Equal(t, "unknown", f.getParentPath())
	})

	t.Run("parent_dir_under_root", func(t *testing.T) {
		t.Parallel()
		const parentIno uint64 = 301
		s := superForFileTest(t)
		parent := &Dir{
			super:     s,
			info:      dirInodeInfoForMutationTest(parentIno),
			parentIno: s.rootIno,
			name:      "mydir",
		}
		s.nodeCache[parentIno] = parent
		f := &File{super: s, parentIno: parentIno, name: "f.txt", info: &proto.InodeInfo{Inode: 13}}
		require.Equal(t, "/mydir", f.getParentPath())
	})
}

func TestFile_Attr_inodeFromIcache_setsParentInoAndSize(t *testing.T) {
	t.Parallel()
	const fileIno uint64 = 91001
	s := superForFileTest(t)
	info := fileInodeInfoForMutationTest(fileIno)
	info.Size = 8192
	info.Generation = 0
	s.ic.Put(info)

	f := newTestFile(s, info, 1, "size.txt")
	attr := &fuse.Attr{}
	require.NoError(t, f.Attr(context.Background(), attr))
	require.Equal(t, uint64(1), attr.ParentIno)
	require.Equal(t, uint64(8192), attr.Size)
}

func TestFile_Attr_symlink_usesTargetLength(t *testing.T) {
	t.Parallel()
	const fileIno uint64 = 91002
	target := "/etc/passwd"
	s := superForFileTest(t)
	info := symlinkInodeInfoForTest(fileIno, target)
	info.Size = 9999
	s.ic.Put(info)

	f := newTestFile(s, info, 1, "link")
	attr := &fuse.Attr{}
	require.NoError(t, f.Attr(context.Background(), attr))
	require.Equal(t, uint64(len(target)), attr.Size)
}

func TestFile_Attr_coldVolume_usesInodeSize(t *testing.T) {
	t.Parallel()
	const fileIno uint64 = 91003
	s := superForFileTest(t)
	s.volType = proto.VolumeTypeCold
	info := fileInodeInfoForMutationTest(fileIno)
	info.Size = 2048
	s.ic.Put(info)

	f := newTestFile(s, info, 1, "cold.dat")
	attr := &fuse.Attr{}
	require.NoError(t, f.Attr(context.Background(), attr))
	require.Equal(t, uint64(2048), attr.Size)
}

func TestFile_OpenMetaCacheAccelerationReadUsesInodeExtentsCache(t *testing.T) {
	const ino uint64 = 91004
	var getExtentsCalls int
	s := superForFileOpenTest(t, func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
		getExtentsCalls++
		return 0, 0, nil, nil
	})
	info := fileInodeInfoForMutationTest(ino)
	info.Extents.Generation = 7
	info.Extents.Size = 128
	s.ic.Put(info)

	f := newTestFile(s, info, s.rootIno, "read.txt")
	_, err := f.Open(context.Background(), &fuse.OpenRequest{
		Header: fuse.Header{Pid: 91004},
		Flags:  syscall.O_RDONLY,
	}, &fuse.OpenResponse{})
	require.NoError(t, err)
	require.Equal(t, 0, getExtentsCalls)

	require.NoError(t, s.ec.CloseStream(ino))
	require.NoError(t, s.ec.EvictStream(ino))
}

func TestFile_OpenMetaCacheAccelerationWriteRefreshesExtentsWithOpenForWrite(t *testing.T) {
	const ino uint64 = 91005
	var (
		getExtentsCalls int
		gotOpenForWrite bool
	)
	s := superForFileOpenTest(t, func(inode uint64, isCache, openForWrite, isMigration bool) (uint64, uint64, []proto.ExtentKey, error) {
		require.Equal(t, ino, inode)
		getExtentsCalls++
		gotOpenForWrite = openForWrite
		return 9, 256, nil, nil
	})
	info := fileInodeInfoForMutationTest(ino)
	info.Extents.Generation = 3
	s.ic.Put(info)

	f := newTestFile(s, info, s.rootIno, "write.txt")
	_, err := f.Open(context.Background(), &fuse.OpenRequest{
		Header: fuse.Header{Pid: 91005},
		Flags:  syscall.O_WRONLY,
	}, &fuse.OpenResponse{})
	require.NoError(t, err)
	require.Equal(t, 1, getExtentsCalls)
	require.True(t, gotOpenForWrite)

	require.NoError(t, s.ec.CloseStream(ino))
	require.NoError(t, s.ec.EvictStream(ino))
}

func TestFile_Readlink_returnsTarget(t *testing.T) {
	t.Parallel()
	const fileIno uint64 = 92001
	target := "../other"
	s := superForFileTest(t)
	info := symlinkInodeInfoForTest(fileIno, target)
	s.ic.Put(info)

	f := newTestFile(s, info, 1, "sym")
	got, err := f.Readlink(context.Background(), &fuse.ReadlinkRequest{Header: fuse.Header{Pid: 1}})
	require.NoError(t, err)
	require.Equal(t, target, got)
}

func TestFile_xattr_disabled_returnsENOSYS(t *testing.T) {
	t.Parallel()
	s := superForFileTest(t)
	s.enableXattr = false
	f := newTestFile(s, fileInodeInfoForMutationTest(93001), 1, "x.txt")

	require.ErrorIs(t, f.Getxattr(context.Background(), &fuse.GetxattrRequest{Name: "user.a"}, &fuse.GetxattrResponse{}), fuse.ENOSYS)
	require.ErrorIs(t, f.Listxattr(context.Background(), &fuse.ListxattrRequest{}, &fuse.ListxattrResponse{}), fuse.ENOSYS)
	require.ErrorIs(t, f.Setxattr(context.Background(), &fuse.SetxattrRequest{Name: "user.a", Xattr: []byte("v")}), fuse.ENOSYS)
	require.ErrorIs(t, f.Removexattr(context.Background(), &fuse.RemovexattrRequest{Name: "user.a"}), fuse.ENOSYS)
}

func TestFile_Getxattr_securityCapability_returnsEmpty(t *testing.T) {
	t.Parallel()
	s := superForFileTest(t)
	s.enableXattr = true
	f := newTestFile(s, fileInodeInfoForMutationTest(93002), 1, "cap.txt")

	resp := &fuse.GetxattrResponse{}
	err := f.Getxattr(context.Background(), &fuse.GetxattrRequest{
		Header: fuse.Header{Pid: 2},
		Name:   "security.capability",
	}, resp)
	require.NoError(t, err)
	require.Empty(t, resp.Xattr)
}

func TestFile_Flush_fsyncOnCloseDisabled_returnsENOSYS(t *testing.T) {
	t.Parallel()
	s := superForFileTest(t)
	s.fsyncOnClose = false
	f := newTestFile(s, fileInodeInfoForMutationTest(94001), 1, "flush.txt")

	err := f.Flush(context.Background(), &fuse.FlushRequest{Header: fuse.Header{Pid: 3}})
	require.ErrorIs(t, err, fuse.ENOSYS)
}

func TestFile_Setattr_metaAccel_beginEndPaired_inodeFromIcache(t *testing.T) {
	t.Parallel()
	const (
		parentIno = uint64(88002)
		fileIno   = uint64(88003)
	)
	s := superForFileTest(t)
	s.ic.Put(fileInodeInfoForMutationTest(fileIno))

	f := newTestFile(s, fileInodeInfoForMutationTest(fileIno), parentIno, "f.txt")

	req := &fuse.SetattrRequest{Header: fuse.Header{Pid: 5151}}
	resp := &fuse.SetattrResponse{}

	err := f.Setattr(context.Background(), req, resp)
	require.NoError(t, err)

	_, inCount := s.dirDirtyCount[parentIno]
	require.False(t, inCount, "File.Setattr must pair EndDirMutation on parentIno")
}

func TestFile_Setattr_openForWriteFlag_stillPairsDirMutation(t *testing.T) {
	t.Parallel()
	const (
		parentIno = uint64(88004)
		fileIno   = uint64(88005)
	)
	s := superForFileTest(t)
	info := fileInodeInfoForMutationTest(fileIno)
	s.ic.Put(info)

	f := newTestFile(s, info, parentIno, "w.txt")
	f.flag = syscall.O_WRONLY

	req := &fuse.SetattrRequest{
		Header: fuse.Header{Pid: 5152},
		Flags:  syscall.O_WRONLY,
	}
	resp := &fuse.SetattrResponse{}

	require.NoError(t, f.Setattr(context.Background(), req, resp))
	_, inCount := s.dirDirtyCount[parentIno]
	require.False(t, inCount)
}

func flagName(flag uint32) string {
	switch flag {
	case syscall.O_RDONLY:
		return "O_RDONLY"
	case syscall.O_WRONLY:
		return "O_WRONLY"
	case syscall.O_RDWR:
		return "O_RDWR"
	default:
		return "unknown"
	}
}
