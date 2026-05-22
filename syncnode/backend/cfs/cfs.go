// Copyright 2026 The CubeFS Authors.
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

//go:build linux

// Package cfs is the CubeFS storage backend for the syncnode task executor.
// It maps the Backend interface onto the CubeFS SDK (MetaWrapper +
// ExtentClient). The read path mirrors tool/cfs-sync's prefetch-reader
// approach to lift the single-streamer ~330 MB/s ceiling; the write path is
// **rewritten** for syncnode-C-5: instead of one goroutine serially calling
// ec.Write, large bodies are split into N fixed-size chunks each driven by
// its own goroutine so multi-file concurrent writes don't sit waiting on
// SDK packet round-trips.
package cfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/util/log"
)

// Config holds the cfs backend connection parameters. Mirrors the cross-
// platform stub Config so callers compile on both linux and macOS.
type Config struct {
	// Masters is the list of CubeFS master HTTP endpoints (host:port).
	Masters []string
	// Volume is the CubeFS volume name to mount.
	Volume string
	// LogDir is the SDK log directory. Empty disables SDK log init.
	LogDir string
	// LogLevel is the SDK log level (debug/info/warn/error). Used only
	// when LogDir is non-empty.
	LogLevel string

	// ReadChunkSize controls the per-chunk size of the parallel read
	// prefetcher. Zero => default (4 MiB).
	ReadChunkSize int
	// ReadPrefetch is the number of in-flight read chunks per file.
	// Zero => default (4).
	ReadPrefetch int

	// WriteChunkMiB is the size in MiB of one write chunk dispatched to
	// a single Write goroutine. Zero => default (4 MiB).
	WriteChunkMiB int
	// WriteParallel is the maximum number of concurrent Write goroutines
	// per file. Zero => default (4). 1 disables parallel write entirely.
	WriteParallel int
}

const (
	defaultReadChunkSize  = 4 * 1024 * 1024
	defaultReadPrefetch   = 4
	defaultWriteChunkSize = 4 * 1024 * 1024
	defaultWriteParallel  = 4
	// parallelWriteMinBytes is the file-size threshold above which we
	// engage the multi-goroutine write path. Below it sequential Write
	// is faster (fewer goroutines, no extra synchronization).
	parallelWriteMinBytes = 16 * 1024 * 1024
)

func (c *Config) resolvedReadChunkSize() int {
	if c.ReadChunkSize > 0 {
		return c.ReadChunkSize
	}
	return defaultReadChunkSize
}

func (c *Config) resolvedReadPrefetch() int {
	if c.ReadPrefetch > 0 {
		return c.ReadPrefetch
	}
	return defaultReadPrefetch
}

func (c *Config) resolvedWriteChunkSize() int {
	if c.WriteChunkMiB > 0 {
		return c.WriteChunkMiB * 1024 * 1024
	}
	return defaultWriteChunkSize
}

func (c *Config) resolvedWriteParallel() int {
	if c.WriteParallel > 0 {
		return c.WriteParallel
	}
	return defaultWriteParallel
}

// metaClient is the subset of MetaWrapper used by Backend. Defining it as
// an interface lets tests swap a fake implementation without standing up a
// real meta cluster.
type metaClient interface {
	LookupPath(subdir string) (uint64, error)
	Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error)
	Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte, fullPath string, ignoreExist bool) (*proto.InodeInfo, error)
	InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
	Delete_ll(parentID uint64, name string, isDir bool, fullPath string) (*proto.InodeInfo, error)
	Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, srcFullPath string, dstFullPath string, overwritten bool) error
	ReadDir_ll(parentID uint64) ([]proto.Dentry, error)
	BatchInodeGet(inodes []uint64) []*proto.InodeInfo
	Setattr(inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error
	Close() error
}

// extentAPI is the subset of stream.ExtentClient used by Backend.
type extentAPI interface {
	OpenStream(inode uint64, openForWrite, isCache bool, fullPath string) error
	CloseStream(inode uint64) error
	Read(inode uint64, data []byte, offset int, size int, storageClass uint32, isMigration bool) (int, error)
	Write(inode uint64, offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration, waitForFlush bool) (int, error)
	Flush(inode uint64) error
	Truncate(mw *meta.MetaWrapper, parentIno uint64, inode uint64, size int, fullPath string) error
	Close() error
}

// realMetaWrapper wraps *meta.MetaWrapper into metaClient. We expose this
// thin shim so the *Backend can hold the interface value while the SDK's
// concrete type is used end-to-end in production.
type realMetaWrapper struct{ *meta.MetaWrapper }

// Close is delegated to the underlying MetaWrapper.Close().
func (w realMetaWrapper) Close() error {
	if w.MetaWrapper == nil {
		return nil
	}
	return w.MetaWrapper.Close()
}

// Backend implements backend.Backend backed by a CubeFS volume.
type Backend struct {
	cfg          *Config
	mw           metaClient
	ec           extentAPI
	mwReal       *meta.MetaWrapper // real type retained for ec.Truncate
	storageClass uint32

	closeOnce sync.Once
	closeErr  error
}

func init() {
	backend.Register("cfs", New)
}

// New constructs a real cfs Backend by dialing the masters + SDK init.
func New(cfgI interface{}) (backend.Backend, error) {
	cfg, ok := cfgI.(*Config)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("%w: cfs config must be *cfs.Config", backend.ErrConfigInvalid)
	}
	if len(cfg.Masters) == 0 {
		return nil, fmt.Errorf("%w: Masters required", backend.ErrConfigInvalid)
	}
	if cfg.Volume == "" {
		return nil, fmt.Errorf("%w: Volume required", backend.ErrConfigInvalid)
	}

	if cfg.LogDir != "" {
		level := log.ParseLogLevel(cfg.LogLevel)
		log.InitLog(cfg.LogDir, "syncnode-cfs", level, nil, log.DefaultLogLeftSpaceLimitRatio)
	}
	proto.InitBufferPool(32768)

	mc := masterSDK.NewMasterClient(cfg.Masters, false)
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(cfg.Volume)
	if err != nil {
		return nil, fmt.Errorf("cfs: get volume info: %w", err)
	}
	if proto.IsCold(volInfo.VolType) {
		return nil, fmt.Errorf("%w: cold (BlobStore) volumes unsupported, vol=%s VolType=%d",
			backend.ErrConfigInvalid, cfg.Volume, volInfo.VolType)
	}

	mw, err := meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        cfg.Volume,
		Masters:       cfg.Masters,
		ValidateOwner: false,
	})
	if err != nil {
		return nil, fmt.Errorf("cfs: init meta wrapper: %w", err)
	}
	mw.DefaultStorageClass = volInfo.VolStorageClass

	ec, err := stream.NewExtentClient(&stream.ExtentConfig{
		Volume:                 cfg.Volume,
		Masters:                cfg.Masters,
		OnAppendExtentKey:      mw.AppendExtentKey,
		OnGetExtents:           mw.GetExtents,
		OnTruncate:             mw.Truncate,
		DisableMetaCache:       true,
		MetaWrapper:            mw,
		VolStorageClass:        volInfo.VolStorageClass,
		VolAllowedStorageClass: volInfo.AllowedStorageClass,
	})
	if err != nil {
		return nil, fmt.Errorf("cfs: init extent client: %w", err)
	}

	return &Backend{
		cfg:          cfg,
		mw:           realMetaWrapper{MetaWrapper: mw},
		ec:           ec,
		mwReal:       mw,
		storageClass: volInfo.VolStorageClass,
	}, nil
}

// newWithDeps is the test-friendly constructor. It accepts injected
// meta/extent implementations and skips SDK dial-up. Not exported; tests in
// the same package use it directly.
func newWithDeps(cfg *Config, mw metaClient, ec extentAPI, storageClass uint32) *Backend {
	return &Backend{cfg: cfg, mw: mw, ec: ec, storageClass: storageClass}
}

// Kind returns "cfs".
func (b *Backend) Kind() string { return "cfs" }

// Capabilities reports the static capability set declared in §9 C-5.
func (b *Backend) Capabilities() backend.Caps {
	return backend.Caps{
		RangeRead:         true,
		Multipart:         false,
		AtomicRename:      true,
		ListMaxKeys:       0,
		StrongConsistency: true,
		// CubeFS has no server-side digest. We persist sha256 in a
		// companion file (`<key>.syncnode.sha256`) on Put, which makes
		// GetChecksum O(64-byte read) on the hot path — but it's still
		// not "native" in the executor's sense (the volume itself isn't
		// telling us anything). Reported false so executor knows it
		// should still gate on src-side compute.
		NativeChecksum: false,
		// CubeFS exposes meta.Setattr which can update ModifyTime with the
		// proto.AttrModifyTime valid-flag — Put honors PutOptions.Mtime via
		// that call after the data stream is flushed.
		NativeMtimeWrite: true,
		// TODO: 待 metanode inode-clone API 落地后改为 true。当前 CubeFS 的
		// 写路径必须流式过 ExtentClient.Write，没有原生的 same-volume copy
		// 接口，所以同实例的 Get→Put 仍然走通用路径。
		ServerSideCopy: false,
	}
}

// SameInstance reports whether other targets the same CubeFS volume on
// the same master cluster. Comparison normalises master order so a config
// with [m1,m2] matches [m2,m1] — masters form a quorum and order is not
// semantically meaningful.
func (b *Backend) SameInstance(other backend.Backend) bool {
	o, ok := other.(*Backend)
	if !ok || o == nil || b.cfg == nil || o.cfg == nil {
		return false
	}
	if b.cfg.Volume != o.cfg.Volume {
		return false
	}
	if len(b.cfg.Masters) != len(o.cfg.Masters) {
		return false
	}
	// Compare master sets order-independently via sorted copies.
	a := append([]string(nil), b.cfg.Masters...)
	c := append([]string(nil), o.cfg.Masters...)
	sortStrings(a)
	sortStrings(c)
	for i := range a {
		if a[i] != c[i] {
			return false
		}
	}
	return true
}

// sortStrings is a small in-place sort helper used by SameInstance to keep
// the cfs.go imports minimal — we don't want to pull "sort" just for this.
func sortStrings(s []string) {
	// Insertion sort is fine: master lists are tiny (≤ 5 entries in practice).
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// ServerSideCopy is declared so cfs.Backend satisfies the
// ServerSideCopier interface, but returns ErrBackendUnsupported until a
// metanode-side inode-clone API is available. The executor short-circuits
// on Caps.ServerSideCopy long before reaching here, so callers should
// only see this error if they bypass the capability check.
func (b *Backend) ServerSideCopy(_ context.Context, _, _ string, _ backend.PutOptions) (backend.PutResult, error) {
	return backend.PutResult{}, backend.ErrBackendUnsupported
}

// Close releases the ExtentClient. The MetaWrapper does not have a public
// Close so it's allowed to go through garbage collection.
func (b *Backend) Close() error {
	b.closeOnce.Do(func() {
		if b.ec != nil {
			if err := b.ec.Close(); err != nil {
				b.closeErr = err
			}
		}
	})
	return b.closeErr
}

// normalizeKey makes the key a CubeFS-absolute path. Keys without a
// leading slash are treated as already absolute (e.g. "data/x" → "/data/x").
func normalizeKey(key string) string {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	return key
}

// splitPath splits an absolute path into (dir, name). Returns ("/", name)
// for top-level entries.
func splitPath(p string) (string, string) {
	p = strings.TrimSuffix(p, "/")
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return "/", p
	}
	dir := p[:idx]
	if dir == "" {
		dir = "/"
	}
	return dir, p[idx+1:]
}

// translateErr converts SDK errors to backend sentinel errors where possible.
func translateErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOENT) {
		return backend.ErrKeyNotFound
	}
	return fmt.Errorf("cfs %s: %w", op, err)
}

// Head returns size + mtime of the file at key. CubeFS does not provide an
// etag — the returned etag is always empty.
func (b *Backend) Head(_ context.Context, key string) (int64, string, time.Time, error) {
	full := normalizeKey(key)
	ino, err := b.mw.LookupPath(full)
	if err != nil {
		return 0, "", time.Time{}, translateErr("lookup "+full, err)
	}
	info, err := b.mw.InodeGet_ll(ino)
	if err != nil {
		return 0, "", time.Time{}, translateErr("inode get "+full, err)
	}
	return int64(info.Size), "", info.ModifyTime, nil
}

// Delete removes the file at key. ENOENT is silently treated as success
// (Backend.Delete is idempotent per the interface contract).
func (b *Backend) Delete(_ context.Context, key string) error {
	full := normalizeKey(key)
	dir, name := splitPath(full)
	dirIno, err := b.mw.LookupPath(dir)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return translateErr("lookup parent "+dir, err)
	}
	_, err = b.mw.Delete_ll(dirIno, name, false, full)
	if err != nil && errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cfs delete %s: %w", full, err)
	}
	return nil
}

// Rename moves oldKey to newKey atomically via meta.Rename_ll. Assumed
// atomic by CubeFS (the rule is verified by G-4 in design.md if not).
func (b *Backend) Rename(_ context.Context, oldKey, newKey string) error {
	oldFull := normalizeKey(oldKey)
	newFull := normalizeKey(newKey)
	oldDir, oldName := splitPath(oldFull)
	newDir, newName := splitPath(newFull)

	oldParent, err := b.mw.LookupPath(oldDir)
	if err != nil {
		return translateErr("lookup old parent "+oldDir, err)
	}
	newParent, err := b.mkdirAll(newDir)
	if err != nil {
		return fmt.Errorf("cfs rename mkdir %s: %w", newDir, err)
	}
	if err := b.mw.Rename_ll(oldParent, oldName, newParent, newName, oldFull, newFull, true); err != nil {
		return fmt.Errorf("cfs rename %s -> %s: %w", oldFull, newFull, err)
	}
	return nil
}

// mkdirAll creates all missing directories along dirPath and returns the
// inode of the last segment (the leaf directory).
func (b *Backend) mkdirAll(dirPath string) (uint64, error) {
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	parentIno := proto.RootIno
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		current += "/" + part
		child, childMode, lerr := b.mw.Lookup_ll(parentIno, part)
		if lerr == nil {
			if !proto.IsDir(childMode) {
				return 0, fmt.Errorf("%s is not a directory", current)
			}
			parentIno = child
			continue
		}
		if !errors.Is(lerr, syscall.ENOENT) {
			return 0, fmt.Errorf("lookup %s: %w", current, lerr)
		}
		info, cerr := b.mw.Create_ll(parentIno, part, 0o755|uint32(os.ModeDir), 0, 0, nil, current, false)
		if cerr != nil {
			if errors.Is(cerr, syscall.EEXIST) {
				child, _, _ = b.mw.Lookup_ll(parentIno, part)
				parentIno = child
				continue
			}
			return 0, fmt.Errorf("mkdir %s: %w", current, cerr)
		}
		parentIno = info.Inode
	}
	return parentIno, nil
}

// Get returns a reader over [off, off+size) bytes of key. size==0 reads to
// end of file. The returned reader is backed by N prefetch workers
// (configurable via Config.ReadPrefetch / ReadChunkSize) to break the
// single-streamer ~330 MB/s ceiling.
func (b *Backend) Get(_ context.Context, key string, off, size int64) (io.ReadCloser, error) {
	full := normalizeKey(key)
	ino, err := b.mw.LookupPath(full)
	if err != nil {
		return nil, translateErr("lookup "+full, err)
	}
	if err := b.ec.OpenStream(ino, false, false, full); err != nil {
		return nil, fmt.Errorf("cfs open stream %s: %w", full, err)
	}
	info, err := b.mw.InodeGet_ll(ino)
	if err != nil {
		_ = b.ec.CloseStream(ino)
		return nil, fmt.Errorf("cfs inode get %s: %w", full, err)
	}
	fileSize := int64(info.Size)
	if off < 0 {
		off = 0
	}
	if off > fileSize {
		off = fileSize
	}
	if size <= 0 || off+size > fileSize {
		size = fileSize - off
	}

	ec := b.ec
	sc := b.storageClass
	fetch := func(p []byte, fetchOff int64) (int, error) {
		return ec.Read(ino, p, int(fetchOff), len(p), sc, false)
	}
	pr := newPrefetchReader(fetch, off, size, b.cfg.resolvedReadChunkSize(), b.cfg.resolvedReadPrefetch())
	return &cfsReader{pr: pr, ec: b.ec, ino: ino}, nil
}

// cfsReader is the io.ReadCloser returned by Get. Close drains prefetch
// workers before calling CloseStream so no in-flight Read races with the
// stream teardown.
type cfsReader struct {
	pr  *prefetchReader
	ec  extentAPI
	ino uint64
}

func (r *cfsReader) Read(p []byte) (int, error) { return r.pr.Read(p) }

func (r *cfsReader) Close() error {
	err := r.pr.Close()
	if cerr := r.ec.CloseStream(r.ino); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// writeChunk is a unit of work fed to one Write goroutine.
type writeChunk struct {
	off  int64
	data []byte
}

// Put creates (or overwrites) key with body bytes. For bodies above
// parallelWriteMinBytes we buffer chunks of WriteChunkMiB and dispatch them
// to up to WriteParallel goroutines; below the threshold a single
// goroutine is used to avoid synchronization overhead.
//
// The size parameter is advisory: actual bytes written come from io.EOF on
// body. opts.StorageClass / Multipart are unused by cfs (no storage class
// concept here). When opts.ComputeChecksum is true we tee the body through
// a sha256 hasher and, on a successful write, persist the digest in a
// companion file `<key>.syncnode.sha256` so subsequent GetChecksum calls
// avoid a full re-read.
func (b *Backend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	full := normalizeKey(key)
	dir, name := splitPath(full)

	dirIno, err := b.mkdirAll(dir)
	if err != nil {
		return backend.PutResult{}, fmt.Errorf("cfs put mkdir %s: %w", dir, err)
	}

	var ino uint64
	info, cerr := b.mw.Create_ll(dirIno, name, 0o644, 0, 0, nil, full, true)
	if cerr == nil {
		ino = info.Inode
	} else if errors.Is(cerr, syscall.EEXIST) {
		existIno, _, lerr := b.mw.Lookup_ll(dirIno, name)
		if lerr != nil {
			return backend.PutResult{}, fmt.Errorf("cfs put lookup existing %s: %w", full, lerr)
		}
		ino = existIno
	} else {
		return backend.PutResult{}, fmt.Errorf("cfs put create %s: %w", full, cerr)
	}

	if err := b.ec.OpenStream(ino, true, false, full); err != nil {
		return backend.PutResult{}, fmt.Errorf("cfs put open stream %s: %w", full, err)
	}
	// Truncate to zero first so a previously larger file does not leak
	// stale tail bytes.
	if err := b.truncate(dirIno, ino, full); err != nil {
		_ = b.ec.CloseStream(ino)
		return backend.PutResult{}, fmt.Errorf("cfs put truncate %s: %w", full, err)
	}

	parallel := b.cfg.resolvedWriteParallel()
	chunkSize := b.cfg.resolvedWriteChunkSize()

	// Wrap body so reads are counted; optionally also tee'd through a
	// sha256 sink. This keeps both sequential and parallel write paths
	// agnostic to checksum computation — they just consume an io.Reader.
	src := &countingReader{r: body}
	var sumFn func() string
	if opts.ComputeChecksum {
		h, fn := backend.NewSHA256Sink()
		src.r = io.TeeReader(body, h)
		sumFn = fn
	}

	// Choice of write path: small bodies bypass the goroutine pool to
	// avoid the per-file orchestration cost when it doesn't pay off.
	if size > 0 && size <= parallelWriteMinBytes || parallel <= 1 {
		if err := b.writeSequential(ctx, ino, full, src, chunkSize); err != nil {
			_ = b.ec.CloseStream(ino)
			return backend.PutResult{}, err
		}
	} else {
		if err := b.writeParallel(ctx, ino, full, src, chunkSize, parallel); err != nil {
			_ = b.ec.CloseStream(ino)
			return backend.PutResult{}, err
		}
	}

	if err := b.ec.Flush(ino); err != nil {
		_ = b.ec.CloseStream(ino)
		return backend.PutResult{}, fmt.Errorf("cfs put flush %s: %w", full, err)
	}
	if err := b.ec.CloseStream(ino); err != nil {
		return backend.PutResult{}, fmt.Errorf("cfs put close stream %s: %w", full, err)
	}

	// Preserve source mtime when requested. Called after CloseStream so the
	// data path is finalized first; if Setattr fails the data is still good
	// and we return the error so the caller knows the requested timestamp
	// was not applied (an idempotency comparison that keys off mtime would
	// otherwise silently mis-match on the next sync run).
	//
	// CubeFS stores mtime as Unix-seconds on the wire (metanode reconstructs
	// via time.Unix(mt, 0)) so sub-second precision is truncated. Callers
	// that need ns-precision comparison must tolerate this on cfs backends.
	if opts.Mtime != nil {
		if err := b.mw.Setattr(ino, proto.AttrModifyTime, 0, 0, 0, 0, opts.Mtime.Unix()); err != nil {
			return backend.PutResult{}, fmt.Errorf("cfs put setattr mtime %s: %w", full, err)
		}
	}

	res := backend.PutResult{BytesPut: src.n}
	if sumFn != nil {
		hexSum := sumFn()
		res.Checksum = hexSum
		res.Algorithm = backend.ChecksumAlgorithmSHA256
		// Write the companion file. Best-effort: if this fails the data
		// upload still succeeded and the checksum lives in res.Checksum;
		// the next GetChecksum will simply re-stream and re-write.
		if cerr := b.writeCompanion(ctx, full, hexSum); cerr != nil {
			log.LogWarnf("cfs put: write companion sha256 for %s: %v", full, cerr)
		}
	}
	return res, nil
}

// countingReader wraps an io.Reader and counts bytes read. Used by Put to
// report PutResult.BytesPut without making writeSequential / writeParallel
// return byte counts (which would touch their hot paths).
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// companionPath returns the path for the sha256 companion file given a
// normalized full path.
func companionPath(full string) string {
	return full + backend.SHA256CompanionSuffix
}

// writeCompanion stores hexSum in the companion file next to full. The
// companion is a tiny 64-byte object so we use the sequential write path
// regardless of write-parallel config.
func (b *Backend) writeCompanion(ctx context.Context, full, hexSum string) error {
	companion := companionPath(full)
	dir, name := splitPath(companion)
	dirIno, err := b.mkdirAll(dir)
	if err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	var ino uint64
	info, cerr := b.mw.Create_ll(dirIno, name, 0o644, 0, 0, nil, companion, true)
	if cerr == nil {
		ino = info.Inode
	} else if errors.Is(cerr, syscall.EEXIST) {
		existIno, _, lerr := b.mw.Lookup_ll(dirIno, name)
		if lerr != nil {
			return fmt.Errorf("lookup existing %s: %w", companion, lerr)
		}
		ino = existIno
	} else {
		return fmt.Errorf("create %s: %w", companion, cerr)
	}
	if err := b.ec.OpenStream(ino, true, false, companion); err != nil {
		return fmt.Errorf("open stream %s: %w", companion, err)
	}
	if err := b.truncate(dirIno, ino, companion); err != nil {
		_ = b.ec.CloseStream(ino)
		return fmt.Errorf("truncate %s: %w", companion, err)
	}
	data := []byte(hexSum)
	if _, werr := b.ec.Write(ino, 0, data, 0, nil, b.storageClass, false, false); werr != nil {
		_ = b.ec.CloseStream(ino)
		return fmt.Errorf("write %s: %w", companion, werr)
	}
	if err := b.ec.Flush(ino); err != nil {
		_ = b.ec.CloseStream(ino)
		return fmt.Errorf("flush %s: %w", companion, err)
	}
	if err := b.ec.CloseStream(ino); err != nil {
		return fmt.Errorf("close stream %s: %w", companion, err)
	}
	_ = ctx // ctx parameter retained for symmetry / future cancellation
	return nil
}

// GetChecksum returns the sha256 of key. Resolution order:
//
//  1. If a companion file `<key>.syncnode.sha256` exists and contains a
//     well-formed 64-char hex digest, return it (cheap path — one tiny read).
//  2. Otherwise stream the main file through sha256, then write the digest
//     back into the companion file (best-effort, log+continue on failure
//     so the next call still hits the cheap path) and return the digest.
//  3. If the main file does not exist, return backend.ErrKeyNotFound.
//
// Algorithm is always backend.ChecksumAlgorithmSHA256 — cfs never returns md5.
func (b *Backend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	full := normalizeKey(key)
	// Cheap path: try the companion file first.
	if hexSum, ok := b.readCompanion(ctx, full); ok {
		return hexSum, backend.ChecksumAlgorithmSHA256, nil
	}
	// Fall back to streaming sha256 of the main file.
	rc, err := b.Get(ctx, key, 0, 0)
	if err != nil {
		// Get already maps ENOENT to ErrKeyNotFound via translateErr.
		return "", "", err
	}
	defer rc.Close()
	hexSum, _, serr := backend.SHA256Stream(rc)
	if serr != nil {
		return "", "", fmt.Errorf("cfs sha256 stream %s: %w", full, serr)
	}
	// Best-effort writeback; never fails GetChecksum.
	if werr := b.writeCompanion(ctx, full, hexSum); werr != nil {
		log.LogWarnf("cfs writeCompanion %s: %v", full, werr)
	}
	return hexSum, backend.ChecksumAlgorithmSHA256, nil
}

// readCompanion attempts to read and validate the companion sha256 file for
// full. Returns (hexSum, true) on success. On any error or malformed content
// it returns ("", false) — callers fall back to streaming the main file.
func (b *Backend) readCompanion(ctx context.Context, full string) (string, bool) {
	_ = ctx
	companion := companionPath(full)
	ino, err := b.mw.LookupPath(companion)
	if err != nil {
		return "", false
	}
	info, err := b.mw.InodeGet_ll(ino)
	if err != nil || !proto.IsRegular(info.Mode) {
		return "", false
	}
	// sha256 hex is exactly 64 bytes; reject anything outside a tight window
	// to avoid reading absurd payloads if the companion got corrupted.
	if info.Size == 0 || info.Size > 128 {
		return "", false
	}
	if err := b.ec.OpenStream(ino, false, false, companion); err != nil {
		return "", false
	}
	defer func() { _ = b.ec.CloseStream(ino) }()
	buf := make([]byte, info.Size)
	n, rerr := b.ec.Read(ino, buf, 0, len(buf), b.storageClass, false)
	if rerr != nil && rerr != io.EOF {
		return "", false
	}
	if n != int(info.Size) {
		return "", false
	}
	hexSum := strings.TrimSpace(string(buf[:n]))
	if len(hexSum) != 64 || !isHex(hexSum) {
		return "", false
	}
	return hexSum, true
}

// isHex reports whether s consists entirely of lowercase or uppercase hex
// digits. Cheaper than encoding/hex.DecodeString for the validate-only path.
func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// truncate is the test-friendly wrapper over ec.Truncate; in real builds it
// uses the real meta wrapper, in tests with a fake meta we pass nil.
func (b *Backend) truncate(dirIno, ino uint64, fullPath string) error {
	return b.ec.Truncate(b.mwReal, dirIno, ino, 0, fullPath)
}

// writeSequential streams body via a single goroutine calling ec.Write
// repeatedly.
func (b *Backend) writeSequential(ctx context.Context, ino uint64, fullPath string, body io.Reader, chunkSize int) error {
	buf := make([]byte, chunkSize)
	var written int
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, rerr := body.Read(buf)
		if n > 0 {
			wn, werr := b.ec.Write(ino, written, buf[:n], 0, nil, b.storageClass, false, false)
			written += wn
			if werr != nil {
				return fmt.Errorf("cfs write %s @%d: %w", fullPath, written, werr)
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return fmt.Errorf("cfs read source: %w", rerr)
		}
	}
}

// writeParallel reads body in chunkSize blocks, dispatching each one to a
// pool of writers. Each writer holds its own (offset, []byte) chunk. The
// SDK ExtentClient is safe to call concurrently from multiple goroutines on
// the same inode — different offsets touch different extent ranges.
func (b *Backend) writeParallel(ctx context.Context, ino uint64, fullPath string, body io.Reader, chunkSize, parallel int) error {
	jobs := make(chan writeChunk, parallel)
	errCh := make(chan error, parallel)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Use a sync.Pool so re-reading from body can recycle buffers. Each
	// chunk grabbed from the pool is returned by the worker after the
	// write succeeds (or on error).
	pool := sync.Pool{New: func() interface{} { b := make([]byte, chunkSize); return &b }}

	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-wctx.Done():
					return
				case ch, ok := <-jobs:
					if !ok {
						return
					}
					// Note: SDK guarantees ec.Write at distinct offsets
					// can run concurrently on the same inode. The
					// IssueWriteRequest path is goroutine-safe for
					// distinct (inode, offset) ranges per the streamer.
					_, werr := b.ec.Write(ino, int(ch.off), ch.data, 0, nil, b.storageClass, false, false)
					// Release buffer back to pool — workers can be
					// reused immediately.
					bp := ch.data[:cap(ch.data)]
					pool.Put(&bp)
					if werr != nil {
						select {
						case errCh <- fmt.Errorf("cfs parallel write %s @%d: %w", fullPath, ch.off, werr):
						default:
						}
						cancel()
						return
					}
				}
			}
		}()
	}

	var off int64
	var dispatchErr error
	for {
		if err := wctx.Err(); err != nil {
			dispatchErr = err
			break
		}
		bp := pool.Get().(*[]byte)
		buf := (*bp)[:chunkSize]
		n, rerr := io.ReadFull(body, buf)
		if n > 0 {
			cpy := buf[:n]
			select {
			case jobs <- writeChunk{off: off, data: cpy}:
				off += int64(n)
			case <-wctx.Done():
				pool.Put(bp)
				dispatchErr = wctx.Err()
			}
		} else {
			// Nothing read — return the buffer immediately.
			pool.Put(bp)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			dispatchErr = fmt.Errorf("cfs read source: %w", rerr)
			break
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	// Surface the first worker error if any.
	for werr := range errCh {
		if werr != nil {
			return werr
		}
	}
	return dispatchErr
}

// List enumerates entries under prefix. If recursive is true we walk the
// directory tree depth-first; otherwise only the immediate children of
// prefix are returned. The channel is buffered to allow producer/consumer
// decoupling; errors abort the listing by emitting one final Entry with
// Err set and closing the channel.
func (b *Backend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	full := normalizeKey(prefix)
	ch := make(chan backend.Entry, 256)

	go func() {
		defer close(ch)

		rootIno, err := b.mw.LookupPath(full)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				// Empty prefix → empty listing, no error.
				return
			}
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("cfs list lookup %s: %w", full, err)})
			return
		}
		rootInfo, err := b.mw.InodeGet_ll(rootIno)
		if err != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("cfs list inode get %s: %w", full, err)})
			return
		}
		// If prefix resolves to a regular file emit it as a single entry.
		if !proto.IsDir(rootInfo.Mode) {
			emit(ctx, ch, backend.Entry{
				Key:   strings.TrimPrefix(full, "/"),
				Size:  int64(rootInfo.Size),
				Mtime: rootInfo.ModifyTime,
			})
			return
		}
		b.walkDir(ctx, ch, rootIno, full, recursive)
	}()

	return ch, nil
}

// walkDir is a serial DFS over the directory tree. We keep it simple
// (no worker pool) because List is not on the hot path for syncnode —
// reads/writes dominate. If listing becomes a bottleneck the BFS+worker
// version in tool/cfs-sync/storage/cfs_linux.go can be ported.
func (b *Backend) walkDir(ctx context.Context, ch chan<- backend.Entry, ino uint64, base string, recursive bool) {
	dentries, err := b.mw.ReadDir_ll(ino)
	if err != nil {
		emit(ctx, ch, backend.Entry{Err: fmt.Errorf("cfs readdir %s: %w", base, err)})
		return
	}
	inos := make([]uint64, 0, len(dentries))
	for _, d := range dentries {
		inos = append(inos, d.Inode)
	}
	infos := b.mw.BatchInodeGet(inos)
	infoMap := make(map[uint64]*proto.InodeInfo, len(infos))
	for _, info := range infos {
		infoMap[info.Inode] = info
	}

	for _, d := range dentries {
		select {
		case <-ctx.Done():
			emit(ctx, ch, backend.Entry{Err: ctx.Err()})
			return
		default:
		}
		// Hide sha256 companion files from List. They are an internal
		// implementation detail of GetChecksum and should never appear
		// to executor / replicator code.
		if strings.HasSuffix(d.Name, backend.SHA256CompanionSuffix) {
			continue
		}
		childPath := base
		if strings.HasSuffix(childPath, "/") {
			childPath += d.Name
		} else {
			childPath += "/" + d.Name
		}
		info := infoMap[d.Inode]
		var sz int64
		var mt time.Time
		if info != nil {
			sz = int64(info.Size)
			mt = info.ModifyTime
		}
		isDir := proto.IsDir(d.Type)
		entry := backend.Entry{
			Key:   strings.TrimPrefix(childPath, "/"),
			Size:  sz,
			Mtime: mt,
			IsDir: isDir,
		}
		if !emit(ctx, ch, entry) {
			return
		}
		if isDir && recursive {
			b.walkDir(ctx, ch, d.Inode, childPath, recursive)
		}
	}
}

// emit sends e on ch and returns false if ctx is done.
func emit(ctx context.Context, ch chan<- backend.Entry, e backend.Entry) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- e:
		return true
	}
}
