// Copyright 2025 The CubeFS Authors.
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

package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/helper/chroot"
	"github.com/willscott/go-nfs/file"

	"github.com/cubefs/cubefs/client/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

// CubefsFS implements billy.Filesystem and billy.Change interfaces for CubeFS
// This allows CubeFS to be used with go-nfs library
type CubefsFS struct {
	metaWrapper     *meta.MetaWrapper
	extentClient    *stream.ExtentClient
	super           *fs.Super // Keep reference to Super for proper cleanup
	rootIno         uint64
	volStorageClass uint32 // Volume default storage class
	mu              sync.RWMutex
	pathCache       map[string]uint64 // path -> inode cache
}

// Ensure CubefsFS implements required interfaces
var (
	_ billy.Filesystem = (*CubefsFS)(nil)
	_ billy.Change     = (*CubefsFS)(nil)
)

// NewCubefsFS creates a new CubeFS filesystem backend using NewSuper for standard initialization
// opt should be parsed from config file using parseMountOption, same as fuse client
func NewCubefsFS(opt *proto.MountOptions) (*CubefsFS, error) {
	// Initialize BufferPool if not already initialized
	// This is required for proto.Packet operations
	if proto.Buffers == nil {
		bufferLimit := opt.BuffersTotalLimit
		if bufferLimit <= 0 {
			bufferLimit = 32768 // Default buffer pool size
		}
		proto.InitBufferPoolEx(bufferLimit, int(opt.BufferChanSize))
	}

	// Use NewSuper to initialize with standard FUSE client configuration
	// This ensures all initialization parameters are set correctly
	super, err := fs.NewSuper(opt)
	if err != nil {
		return nil, err
	}

	// Extract MetaWrapper and ExtentClient from Super
	metaWrapper := super.GetMetaWrapper()
	extentClient := super.GetExtentClient()

	// Get root inode (use proto.RootIno = 1, or lookup if subdir is specified)
	rootIno := proto.RootIno

	return &CubefsFS{
		metaWrapper:     metaWrapper,
		extentClient:    extentClient,
		rootIno:         rootIno,
		volStorageClass: opt.VolStorageClass,
		pathCache:       make(map[string]uint64),
		super:           super, // Keep reference to super for cleanup
	}, nil
}

// pathToInode converts a path to an inode number
// Path format: "test.txt" (no leading slash) for root files, or "dir/file.txt" for nested files
func (fs *CubefsFS) pathToInode(path string) (uint64, error) {
	originalPath := path
	// Clean and normalize path
	path = filepath.Clean(path)
	log.LogDebugf("[CubefsFS.pathToInode] start: original=%s, cleaned=%s", originalPath, path)

	// Handle empty path or root
	// Note: billy.Filesystem paths don't have leading "/" for root files
	if path == "" || path == "." {
		log.LogDebugf("[CubefsFS.pathToInode] root path, returning rootIno=%d", fs.rootIno)
		return fs.rootIno, nil
	}

	// Remove leading "/" if present (billy paths typically don't have it)
	// But keep it for absolute paths from other sources
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
		log.LogDebugf("[CubefsFS.pathToInode] removed leading slash: %s", path)
		// If path becomes empty after removing "/", it's root
		if path == "" {
			return fs.rootIno, nil
		}
	}

	fs.mu.RLock()
	if ino, ok := fs.pathCache[path]; ok {
		fs.mu.RUnlock()
		log.LogDebugf("[CubefsFS.pathToInode] found in cache: path=%s, ino=%d", path, ino)
		// Verify cached inode still exists
		_, err := fs.metaWrapper.InodeGet_ll(ino)
		if err == nil {
			return ino, nil
		}
		// Cache entry is stale, remove it
		log.LogDebugf("[CubefsFS.pathToInode] cache entry stale, removing: path=%s, ino=%d, err=%v", path, ino, err)
		fs.mu.RUnlock()
		fs.mu.Lock()
		delete(fs.pathCache, path)
		fs.mu.Unlock()
		fs.mu.RLock()
	}
	fs.mu.RUnlock()

	// Split path into components (no leading "/" now)
	parts := strings.Split(path, "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		log.LogDebugf("[CubefsFS.pathToInode] empty parts after split, returning rootIno=%d", fs.rootIno)
		return fs.rootIno, nil
	}

	log.LogDebugf("[CubefsFS.pathToInode] resolving path: path=%s, parts=%v, rootIno=%d", path, parts, fs.rootIno)

	// Resolve path component by component starting from root
	currentIno := fs.rootIno
	for i, part := range parts {
		if part == "" || part == "." {
			log.LogDebugf("[CubefsFS.pathToInode] skipping empty/dot part: part=%s, index=%d", part, i)
			continue
		}
		if part == ".." {
			// For "..", we would need parent tracking
			// For now, just skip (simplified implementation)
			log.LogDebugf("[CubefsFS.pathToInode] skipping parent part: part=%s, index=%d", part, i)
			continue
		}

		log.LogDebugf("[CubefsFS.pathToInode] looking up: part=%s, currentIno=%d, index=%d/%d", part, currentIno, i, len(parts)-1)
		ino, _, err := fs.metaWrapper.Lookup_ll(currentIno, part)
		if err != nil {
			log.LogErrorf("[CubefsFS.pathToInode] Lookup_ll failed: path=%s, part=%s, currentIno=%d, index=%d/%d, err=%v",
				path, part, currentIno, i, len(parts)-1, err)
			return 0, err
		}
		log.LogDebugf("[CubefsFS.pathToInode] lookup success: part=%s, currentIno=%d -> ino=%d", part, currentIno, ino)
		currentIno = ino
	}

	// Cache the result only if lookup succeeded
	fs.mu.Lock()
	fs.pathCache[path] = currentIno
	fs.mu.Unlock()

	log.LogDebugf("[CubefsFS.pathToInode] success: path=%s, ino=%d", path, currentIno)
	return currentIno, nil
}

// Create creates a new file
func (fs *CubefsFS) Create(filename string) (billy.File, error) {
	log.LogDebugf("[CubefsFS.Create] filename=%s", filename)
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// Open opens a file for reading
func (fs *CubefsFS) Open(filename string) (billy.File, error) {
	log.LogDebugf("[CubefsFS.Open] filename=%s", filename)
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile opens a file with the specified flags
func (fs *CubefsFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	log.LogDebugf("[CubefsFS.OpenFile] filename=%s, flag=0x%x, perm=0%o", filename, flag, perm)
	dir, name := filepath.Split(filename)
	dir = filepath.Clean(dir)
	// Remove leading "/" if present (billy paths don't have it)
	if strings.HasPrefix(dir, "/") {
		dir = strings.TrimPrefix(dir, "/")
	}
	// Empty dir means root directory
	if dir == "" {
		dir = "."
	}

	parentIno, err := fs.pathToInode(dir)
	if err != nil {
		log.LogErrorf("[CubefsFS.OpenFile] pathToInode failed: filename=%s, dir=%s, err=%v", filename, dir, err)
		return nil, err
	}

	var ino uint64
	if flag&os.O_CREATE != 0 {
		// Create new file
		mode := proto.Mode(perm)
		log.LogDebugf("[CubefsFS.OpenFile] creating file: filename=%s, parentIno=%d, name=%s, mode=0%o", filename, parentIno, name, mode)
		info, err := fs.metaWrapper.Create_ll(parentIno, name, mode, 0, 0, nil, "", false)
		if err != nil {
			if err != syscall.EEXIST {
				log.LogErrorf("[CubefsFS.OpenFile] Create_ll failed: filename=%s, parentIno=%d, name=%s, err=%v", filename, parentIno, name, err)
				return nil, err
			}
			// File exists, lookup it
			log.LogDebugf("[CubefsFS.OpenFile] file exists, looking up: filename=%s, parentIno=%d, name=%s", filename, parentIno, name)
			ino, _, err = fs.metaWrapper.Lookup_ll(parentIno, name)
			if err != nil {
				log.LogErrorf("[CubefsFS.OpenFile] Lookup_ll failed: filename=%s, parentIno=%d, name=%s, err=%v", filename, parentIno, name, err)
				return nil, err
			}
		} else {
			ino = info.Inode
			log.LogDebugf("[CubefsFS.OpenFile] file created: filename=%s, ino=%d", filename, ino)
		}
	} else {
		// Open existing file
		log.LogDebugf("[CubefsFS.OpenFile] opening existing file: filename=%s, parentIno=%d, name=%s", filename, parentIno, name)
		ino, _, err = fs.metaWrapper.Lookup_ll(parentIno, name)
		if err != nil {
			log.LogErrorf("[CubefsFS.OpenFile] Lookup_ll failed: filename=%s, parentIno=%d, name=%s, err=%v", filename, parentIno, name, err)
			return nil, err
		}
		log.LogDebugf("[CubefsFS.OpenFile] file found: filename=%s, ino=%d", filename, ino)
	}

	// Get inode info
	info, err := fs.metaWrapper.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}

	// Handle O_TRUNC flag - truncate file to 0 if it exists and we're opening for write
	if flag&os.O_TRUNC != 0 && flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		log.LogDebugf("[CubefsFS.OpenFile] truncating file: filename=%s, ino=%d, current_size=%d", filename, ino, info.Size)
		if info.Size > 0 {
			err = fs.extentClient.Truncate(fs.metaWrapper, parentIno, ino, 0, filename)
			if err != nil {
				log.LogErrorf("[CubefsFS.OpenFile] Truncate failed: filename=%s, ino=%d, err=%v", filename, ino, err)
				return nil, err
			}
			// Refresh inode info after truncate
			info, err = fs.metaWrapper.InodeGet_ll(ino)
			if err != nil {
				return nil, err
			}
			log.LogDebugf("[CubefsFS.OpenFile] file truncated: filename=%s, ino=%d, new_size=%d", filename, ino, info.Size)
		}
	}

	// Open stream for reading/writing
	openForWrite := flag&(os.O_WRONLY|os.O_RDWR) != 0
	log.LogDebugf("[CubefsFS.OpenFile] opening stream: filename=%s, ino=%d, openForWrite=%v", filename, ino, openForWrite)
	err = fs.extentClient.OpenStream(ino, openForWrite, false, filename)
	if err != nil {
		log.LogErrorf("[CubefsFS.OpenFile] OpenStream failed: filename=%s, ino=%d, err=%v", filename, ino, err)
		return nil, err
	}

	log.LogDebugf("[CubefsFS.OpenFile] success: filename=%s, ino=%d, flag=0x%x, size=%d", filename, ino, flag, info.Size)
	file := &CubefsFile{
		fs:     fs,
		ino:    ino,
		name:   filename,
		info:   info,
		flag:   flag,
		offset: 0,
	}
	log.LogDebugf("[CubefsFS.OpenFile] created CubefsFile: name=%s, ino=%d, flag=0x%x, canWrite=%v", filename, ino, flag, flag&(os.O_WRONLY|os.O_RDWR) != 0)
	return file, nil
}

// Stat returns file info
func (fs *CubefsFS) Stat(filename string) (os.FileInfo, error) {
	log.LogDebugf("[CubefsFS.Stat] filename=%s", filename)
	ino, err := fs.pathToInode(filename)
	if err != nil {
		if err == syscall.ENOENT {
			log.LogDebugf("[CubefsFS.Stat] file not found: filename=%s", filename)
			return nil, syscall.ENOENT
		}
		log.LogErrorf("[CubefsFS.Stat] pathToInode failed: filename=%s, err=%v", filename, err)
		return nil, err
	}

	info, err := fs.metaWrapper.InodeGet_ll(ino)
	if err != nil {
		log.LogErrorf("[CubefsFS.Stat] InodeGet_ll failed: filename=%s, ino=%d, err=%v", filename, ino, err)
		return nil, err
	}
	log.LogDebugf("[CubefsFS.Stat] success: filename=%s, ino=%d, size=%d", filename, ino, info.Size)

	return &CubefsFileInfo{
		info: info,
		name: filepath.Base(filename),
	}, nil
}

// Lstat returns file info (same as Stat for non-symlinks)
func (fs *CubefsFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

// Rename renames a file
func (fs *CubefsFS) Rename(oldpath, newpath string) error {
	oldDir, oldName := filepath.Split(oldpath)
	newDir, newName := filepath.Split(newpath)
	oldDir = filepath.Clean(oldDir)
	newDir = filepath.Clean(newDir)
	// Remove leading "/" if present
	if strings.HasPrefix(oldDir, "/") {
		oldDir = strings.TrimPrefix(oldDir, "/")
	}
	if strings.HasPrefix(newDir, "/") {
		newDir = strings.TrimPrefix(newDir, "/")
	}
	// Empty dir means root directory
	if oldDir == "" {
		oldDir = "."
	}
	if newDir == "" {
		newDir = "."
	}

	oldParentIno, err := fs.pathToInode(oldDir)
	if err != nil {
		return err
	}

	newParentIno, err := fs.pathToInode(newDir)
	if err != nil {
		return err
	}

	err = fs.metaWrapper.Rename_ll(oldParentIno, oldName, newParentIno, newName, oldpath, newpath, false)
	if err == nil {
		// Invalidate cache for both old and new paths
		fs.invalidatePathCache(oldpath)
		fs.invalidatePathCache(newpath)
	}
	return err
}

// invalidatePathCache removes a path from the cache
func (fs *CubefsFS) invalidatePathCache(path string) {
	path = filepath.Clean(path)
	// Remove leading "/" to match pathToInode format
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	fs.mu.Lock()
	delete(fs.pathCache, path)
	fs.mu.Unlock()
}

// Remove removes a file
func (fs *CubefsFS) Remove(filename string) error {
	dir, name := filepath.Split(filename)
	dir = filepath.Clean(dir)
	// Remove leading "/" if present
	if strings.HasPrefix(dir, "/") {
		dir = strings.TrimPrefix(dir, "/")
	}
	// Empty dir means root directory
	if dir == "" {
		dir = "."
	}

	parentIno, err := fs.pathToInode(dir)
	if err != nil {
		return err
	}

	_, err = fs.metaWrapper.Delete_ll(parentIno, name, false, "")
	if err == nil {
		// Invalidate cache for the removed file
		fs.invalidatePathCache(filename)
	}
	return err
}

// Join joins path elements
func (fs *CubefsFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

// TempFile creates a temporary file
func (fs *CubefsFS) TempFile(dir, prefix string) (billy.File, error) {
	// Not implemented for now
	return nil, syscall.ENOTSUP
}

// ReadDir reads directory entries
// Note: For ReadDir (not ReadDirPlus), we only need Name and FileID.
// ReadDirPlus will call Stat() to get full attributes when needed.
// So we don't need to call InodeGet_ll for each entry here.
func (fs *CubefsFS) ReadDir(path string) ([]os.FileInfo, error) {
	log.LogDebugf("[CubefsFS.ReadDir] path=%s", path)
	ino, err := fs.pathToInode(path)
	if err != nil {
		log.LogErrorf("[CubefsFS.ReadDir] pathToInode failed: path=%s, err=%v", path, err)
		return nil, err
	}

	entries, err := fs.metaWrapper.ReadDir_ll(ino)
	if err != nil {
		log.LogErrorf("[CubefsFS.ReadDir] ReadDir_ll failed: path=%s, err=%v", path, err)
		return nil, err
	}

	var infos []os.FileInfo
	for _, entry := range entries {
		// Create a lightweight FileInfo with minimal information
		// ReadDir only needs Name and FileID (from Sys())
		// ReadDirPlus will call Stat() which will fetch full inode info
		infos = append(infos, &CubefsDirEntryInfo{
			name:  entry.Name,
			inode: entry.Inode,
			mode:  entry.Type, // proto.Dentry.Type contains the mode
		})
	}

	return infos, nil
}

// MkdirAll creates a directory and all parent directories
func (fs *CubefsFS) MkdirAll(filename string, perm os.FileMode) error {
	parts := filepath.SplitList(filename)
	currentIno := fs.rootIno

	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}

		ino, _, err := fs.metaWrapper.Lookup_ll(currentIno, part)
		if err == nil {
			// Directory exists
			currentIno = ino
			continue
		}

		// Create directory
		mode := proto.Mode(perm | os.ModeDir)
		info, err := fs.metaWrapper.Create_ll(currentIno, part, mode, 0, 0, nil, "", false)
		if err != nil {
			return err
		}
		currentIno = info.Inode
	}

	return nil
}

// Symlink creates a symbolic link (not implemented)
func (fs *CubefsFS) Symlink(target, link string) error {
	return syscall.ENOTSUP
}

// Readlink reads the target of a symbolic link (not implemented)
func (fs *CubefsFS) Readlink(link string) (string, error) {
	return "", syscall.ENOTSUP
}

// Chroot returns a new filesystem rooted at the given path
func (fs *CubefsFS) Chroot(path string) (billy.Filesystem, error) {
	return chroot.New(fs, path), nil
}

// Root returns the root path
func (fs *CubefsFS) Root() string {
	return "/"
}

// Chmod changes the mode of the named file
func (fs *CubefsFS) Chmod(name string, mode os.FileMode) error {
	log.LogDebugf("[CubefsFS.Chmod] name=%s, mode=0%o", name, mode)
	// CubeFS doesn't support changing file mode after creation
	// Return nil to avoid "Operation not supported" error
	return nil
}

// Chown changes the owner and group of the named file
func (fs *CubefsFS) Chown(name string, uid, gid int) error {
	log.LogDebugf("[CubefsFS.Chown] name=%s, uid=%d, gid=%d", name, uid, gid)
	// CubeFS doesn't support changing ownership after creation
	// Return nil to avoid "Operation not supported" error
	return nil
}

// Lchown changes the owner and group of the named file (does not follow symlinks)
// Required by billy.Change interface
func (fs *CubefsFS) Lchown(name string, uid, gid int) error {
	log.LogDebugf("[CubefsFS.Lchown] name=%s, uid=%d, gid=%d", name, uid, gid)
	// CubeFS doesn't support changing ownership after creation
	// Return nil to avoid "Operation not supported" error
	return nil
}

// Chtimes changes the access and modification times of the named file
// Required by billy.Change interface
func (fs *CubefsFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	log.LogDebugf("[CubefsFS.Chtimes] name=%s, atime=%v, mtime=%v", name, atime, mtime)
	// CubeFS doesn't support changing file times after creation
	// Return nil to avoid "Operation not supported" error
	return nil
}

// Capabilities returns the filesystem capabilities
func (fs *CubefsFS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.WriteCapability | billy.SeekCapability
}

// Close closes the filesystem
func (fs *CubefsFS) Close() error {
	log.LogInfof("[CubefsFS.Close] closing filesystem")
	// Close super which will properly cleanup MetaWrapper and ExtentClient
	if fs.super != nil {
		fs.super.Close()
	}
	return nil
}

// CubefsFile implements billy.File interface
// It also implements io.Reader, io.Writer, io.ReaderAt, io.WriterAt, io.Seeker, io.Closer
type CubefsFile struct {
	fs     *CubefsFS
	ino    uint64
	name   string
	info   *proto.InodeInfo
	flag   int
	offset int64
	mu     sync.RWMutex
}

// Ensure CubefsFile implements required interfaces
var (
	_ billy.File  = (*CubefsFile)(nil)
	_ io.Reader   = (*CubefsFile)(nil)
	_ io.Writer   = (*CubefsFile)(nil)
	_ io.ReaderAt = (*CubefsFile)(nil)
	_ io.WriterAt = (*CubefsFile)(nil)
	_ io.Seeker   = (*CubefsFile)(nil)
	_ io.Closer   = (*CubefsFile)(nil)
)

func (f *CubefsFile) Name() string {
	return f.name
}

func (f *CubefsFile) Write(p []byte) (n int, err error) {
	log.LogDebugf("[CubefsFile.Write] name=%s, ino=%d, offset=%d, len=%d, flag=0x%x", f.name, f.ino, f.offset, len(p), f.flag)
	f.mu.Lock()
	defer f.mu.Unlock()

	// Get storage class from inode info, fallback to volume default if Unspecified
	storageClass := f.info.StorageClass
	if !proto.IsValidStorageClass(storageClass) {
		storageClass = f.fs.volStorageClass
	}

	n, err = f.fs.extentClient.Write(f.ino, int(f.offset), p, f.flag, nil, storageClass, false, true)
	if err != nil {
		log.LogErrorf("[CubefsFile.Write] failed: name=%s, ino=%d, offset=%d, len=%d, err=%v", f.name, f.ino, f.offset, len(p), err)
		return 0, err
	}

	f.offset += int64(n)
	log.LogDebugf("[CubefsFile.Write] success: name=%s, ino=%d, wrote=%d, new_offset=%d", f.name, f.ino, n, f.offset)
	return n, nil
}

func (f *CubefsFile) Read(p []byte) (n int, err error) {
	log.LogDebugf("[CubefsFile.Read] name=%s, ino=%d, offset=%d, len=%d", f.name, f.ino, f.offset, len(p))
	f.mu.RLock()
	defer f.mu.RUnlock()

	n, err = f.fs.extentClient.Read(f.ino, p, int(f.offset), len(p), 0, false)
	if err != nil && err != io.EOF {
		log.LogErrorf("[CubefsFile.Read] failed: name=%s, ino=%d, offset=%d, err=%v", f.name, f.ino, f.offset, err)
		return 0, err
	}

	f.offset += int64(n)
	log.LogDebugf("[CubefsFile.Read] success: name=%s, ino=%d, read=%d, new_offset=%d, eof=%v", f.name, f.ino, n, f.offset, err == io.EOF)
	return n, err
}

func (f *CubefsFile) ReadAt(p []byte, off int64) (n int, err error) {
	log.LogDebugf("[CubefsFile.ReadAt] name=%s, ino=%d, off=%d, len=%d", f.name, f.ino, off, len(p))
	f.mu.RLock()
	defer f.mu.RUnlock()

	n, err = f.fs.extentClient.Read(f.ino, p, int(off), len(p), 0, false)
	if err != nil && err != io.EOF {
		log.LogErrorf("[CubefsFile.ReadAt] failed: name=%s, ino=%d, off=%d, err=%v", f.name, f.ino, off, err)
		return 0, err
	}
	log.LogDebugf("[CubefsFile.ReadAt] success: name=%s, ino=%d, read=%d, eof=%v", f.name, f.ino, n, err == io.EOF)
	return n, err
}

func (f *CubefsFile) WriteAt(p []byte, off int64) (n int, err error) {
	log.LogDebugf("[CubefsFile.WriteAt] name=%s, ino=%d, off=%d, len=%d, flag=0x%x", f.name, f.ino, off, len(p), f.flag)
	f.mu.Lock()
	defer f.mu.Unlock()

	// Get storage class from inode info, fallback to volume default if Unspecified
	storageClass := f.info.StorageClass
	if !proto.IsValidStorageClass(storageClass) {
		storageClass = f.fs.volStorageClass
	}

	n, err = f.fs.extentClient.Write(f.ino, int(off), p, f.flag, nil, storageClass, false, true)
	if err != nil {
		log.LogErrorf("[CubefsFile.WriteAt] failed: name=%s, ino=%d, off=%d, len=%d, err=%v", f.name, f.ino, off, len(p), err)
	} else {
		log.LogDebugf("[CubefsFile.WriteAt] success: name=%s, ino=%d, wrote=%d", f.name, f.ino, n)
	}
	return n, err
}

func (f *CubefsFile) Seek(offset int64, whence int) (int64, error) {
	log.LogDebugf("[CubefsFile.Seek] name=%s, ino=%d, offset=%d, whence=%d, current_offset=%d", f.name, f.ino, offset, whence, f.offset)
	f.mu.Lock()
	defer f.mu.Unlock()

	switch whence {
	case 0: // io.SeekStart
		f.offset = offset
	case 1: // io.SeekCurrent
		f.offset += offset
	case 2: // io.SeekEnd
		info, err := f.fs.metaWrapper.InodeGet_ll(f.ino)
		if err != nil {
			log.LogErrorf("[CubefsFile.Seek] InodeGet_ll failed: name=%s, ino=%d, err=%v", f.name, f.ino, err)
			return 0, err
		}
		f.offset = int64(info.Size) + offset
	default:
		return 0, syscall.EINVAL
	}

	log.LogDebugf("[CubefsFile.Seek] success: name=%s, ino=%d, new_offset=%d", f.name, f.ino, f.offset)
	return f.offset, nil
}

func (f *CubefsFile) Close() error {
	log.LogDebugf("[CubefsFile.Close] name=%s, ino=%d, flag=0x%x", f.name, f.ino, f.flag)
	// Flush any pending writes before closing
	if f.flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		log.LogDebugf("[CubefsFile.Close] flushing writes: name=%s, ino=%d", f.name, f.ino)
		_ = f.fs.extentClient.Flush(f.ino)
	}
	// Close stream
	_ = f.fs.extentClient.CloseStream(f.ino)
	_ = f.fs.extentClient.EvictStream(f.ino)
	log.LogDebugf("[CubefsFile.Close] closed: name=%s, ino=%d", f.name, f.ino)
	return nil
}

func (f *CubefsFile) Lock() error {
	return nil // Not implemented
}

func (f *CubefsFile) Unlock() error {
	return nil // Not implemented
}

func (f *CubefsFile) Truncate(size int64) error {
	log.LogDebugf("[CubefsFile.Truncate] name=%s, ino=%d, size=%d", f.name, f.ino, size)
	err := f.fs.extentClient.Truncate(f.fs.metaWrapper, 0, f.ino, int(size), f.name)
	if err != nil {
		log.LogErrorf("[CubefsFile.Truncate] failed: name=%s, ino=%d, size=%d, err=%v", f.name, f.ino, size, err)
	} else {
		log.LogDebugf("[CubefsFile.Truncate] success: name=%s, ino=%d, size=%d", f.name, f.ino, size)
	}
	return err
}

// Stat returns file info (required by billy.File interface)
func (f *CubefsFile) Stat() (os.FileInfo, error) {
	log.LogDebugf("[CubefsFile.Stat] name=%s, ino=%d", f.name, f.ino)
	info, err := f.fs.metaWrapper.InodeGet_ll(f.ino)
	if err != nil {
		log.LogErrorf("[CubefsFile.Stat] InodeGet_ll failed: name=%s, ino=%d, err=%v", f.name, f.ino, err)
		return nil, err
	}
	// Update cached info
	f.info = info
	log.LogDebugf("[CubefsFile.Stat] success: name=%s, ino=%d, size=%d", f.name, f.ino, info.Size)
	return &CubefsFileInfo{
		info: info,
		name: f.name,
	}, nil
}

// Sync synchronizes file data to storage (required by billy.File interface)
func (f *CubefsFile) Sync() error {
	log.LogDebugf("[CubefsFile.Sync] name=%s, ino=%d", f.name, f.ino)
	if f.flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		err := f.fs.extentClient.Flush(f.ino)
		if err != nil {
			log.LogErrorf("[CubefsFile.Sync] Flush failed: name=%s, ino=%d, err=%v", f.name, f.ino, err)
			return err
		}
		log.LogDebugf("[CubefsFile.Sync] success: name=%s, ino=%d", f.name, f.ino)
	}
	return nil
}

// CubefsFileInfo implements os.FileInfo interface
type CubefsFileInfo struct {
	info *proto.InodeInfo
	name string
}

// CubefsDirEntryInfo is a lightweight FileInfo for directory entries
// Used in ReadDir to avoid calling InodeGet_ll for each entry
// ReadDirPlus will call Stat() which will fetch full inode info when needed
type CubefsDirEntryInfo struct {
	name  string
	inode uint64
	mode  uint32 // proto.Dentry.Type contains the mode
}

func (fi *CubefsFileInfo) Name() string {
	return fi.name
}

func (fi *CubefsFileInfo) Size() int64 {
	return int64(fi.info.Size)
}

func (fi *CubefsFileInfo) Mode() os.FileMode {
	return os.FileMode(fi.info.Mode)
}

func (fi *CubefsFileInfo) ModTime() time.Time {
	return fi.info.ModifyTime
}

func (fi *CubefsFileInfo) IsDir() bool {
	return proto.IsDir(fi.info.Mode)
}

func (fi *CubefsFileInfo) Sys() interface{} {
	return fi.info
}

// CubefsDirEntryInfo implements os.FileInfo interface
func (fi *CubefsDirEntryInfo) Name() string {
	return fi.name
}

func (fi *CubefsDirEntryInfo) Size() int64 {
	// Return 0 for directory entries, actual size will be fetched by Stat() if needed
	return 0
}

func (fi *CubefsDirEntryInfo) Mode() os.FileMode {
	return os.FileMode(fi.mode)
}

func (fi *CubefsDirEntryInfo) ModTime() time.Time {
	// Return zero time for directory entries, actual time will be fetched by Stat() if needed
	return time.Time{}
}

func (fi *CubefsDirEntryInfo) IsDir() bool {
	return proto.IsDir(fi.mode)
}

func (fi *CubefsDirEntryInfo) Sys() interface{} {
	// Return file.FileInfo with Fileid so ToFileAttribute can use it directly
	// This avoids hash-based FileID generation
	return &file.FileInfo{
		Fileid: fi.inode, // Use inode number as FileID
		Nlink:  1,        // Default value, will be updated by Stat() if needed
		UID:    0,        // Default value, will be updated by Stat() if needed
		GID:    0,        // Default value, will be updated by Stat() if needed
	}
}
