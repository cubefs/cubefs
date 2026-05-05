package storage

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

// CFSStorage implements Storage using the CubeFS SDK directly (no FUSE).
type CFSStorage struct {
	cfg  CFSConfig
	mw   *meta.MetaWrapper
	ec   *stream.ExtentClient
	root string // path prefix within the volume
}

// NewCFS creates a CubeFS Storage backend.
func NewCFS(cfg CFSConfig, root string) (*CFSStorage, error) {
	if cfg.LogDir != "" {
		level := log.ParseLogLevel(cfg.LogLevel)
		log.InitLog(cfg.LogDir, "cfs-sync", level, nil, log.DefaultLogLeftSpaceLimitRatio)
	}
	proto.InitBufferPool(32768)

	mc := masterSDK.NewMasterClient(cfg.Masters, false)
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(cfg.Vol)
	if err != nil {
		return nil, fmt.Errorf("get volume info: %w", err)
	}
	if proto.IsCold(volInfo.VolType) {
		return nil, fmt.Errorf("cfs-sync does not support BlobStore (cold) volumes: vol %q has VolType=%d StorageClass=%d",
			cfg.Vol, volInfo.VolType, volInfo.VolStorageClass)
	}

	mw, err := meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        cfg.Vol,
		Masters:       cfg.Masters,
		ValidateOwner: false,
	})
	if err != nil {
		return nil, fmt.Errorf("init meta: %w", err)
	}

	ec, err := stream.NewExtentClient(&stream.ExtentConfig{
		Volume:                 cfg.Vol,
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
		return nil, fmt.Errorf("init extent client: %w", err)
	}

	// Normalise root: no trailing slash
	root = "/" + strings.Trim(root, "/")

	return &CFSStorage{cfg: cfg, mw: mw, ec: ec, root: root}, nil
}

func (c *CFSStorage) Close() {
	if c.ec != nil {
		_ = c.ec.Close()
	}
}

func (c *CFSStorage) String() string {
	return fmt.Sprintf("cfs://%s%s", c.cfg.Vol, c.root)
}

func (c *CFSStorage) fullKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	if key == "" {
		return c.root
	}
	return c.root + "/" + key
}

type dirWork struct {
	ino  uint64
	path string // relative path from root
}

// List streams Objects from the CubeFS volume using concurrent BFS + BatchInodeGet.
func (c *CFSStorage) List(ctx context.Context, prefix string) (<-chan *Object, <-chan error) {
	objects := make(chan *Object, 512)
	errc := make(chan error, 1)

	go func() {
		defer close(objects)
		defer close(errc)

		baseDir := c.fullKey(prefix)
		baseIno, err := c.mw.LookupPath(baseDir)
		if err != nil {
			if err == syscall.ENOENT {
				return
			}
			errc <- fmt.Errorf("lookup %s: %w", baseDir, err)
			return
		}

		const listWorkers = 20
		queue := make(chan dirWork, 1024)
		queue <- dirWork{ino: baseIno, path: ""}

		var wg sync.WaitGroup
		var mu sync.Mutex
		var listErr error
		pending := sync.WaitGroup{}
		pending.Add(1) // for the initial item

		for i := 0; i < listWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case work, ok := <-queue:
						if !ok {
							return
						}
						newDirs, err := c.listDirInto(ctx, work, objects)
						pending.Done()
						if err != nil {
							mu.Lock()
							if listErr == nil {
								listErr = err
							}
							mu.Unlock()
						} else {
							pending.Add(len(newDirs))
							for _, d := range newDirs {
								select {
								case <-ctx.Done():
									pending.Done()
								case queue <- d:
								}
							}
						}
					}
				}
			}()
		}

		// close queue when all work is done
		go func() {
			pending.Wait()
			close(queue)
		}()

		wg.Wait()
		if listErr != nil {
			errc <- listErr
		}
	}()

	return objects, errc
}

// listDirInto reads one directory, sends file Objects to objects chan, and returns subdirectory work items.
func (c *CFSStorage) listDirInto(ctx context.Context, work dirWork, objects chan<- *Object) ([]dirWork, error) {
	dentries, err := c.mw.ReadDir_ll(work.ino)
	if err != nil {
		return nil, fmt.Errorf("readdir ino=%d: %w", work.ino, err)
	}

	inos := make([]uint64, 0, len(dentries))
	for _, d := range dentries {
		inos = append(inos, d.Inode)
	}
	inodeInfos := c.mw.BatchInodeGet(inos)

	infoMap := make(map[uint64]*proto.InodeInfo, len(inodeInfos))
	for _, info := range inodeInfos {
		infoMap[info.Inode] = info
	}

	sort.Slice(dentries, func(i, j int) bool { return dentries[i].Name < dentries[j].Name })

	var subdirs []dirWork
	for _, d := range dentries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var size int64
		var mtime time.Time
		if info := infoMap[d.Inode]; info != nil {
			size = int64(info.Size)
			mtime = info.ModifyTime
		}

		childPath := d.Name
		if work.path != "" {
			childPath = work.path + "/" + d.Name
		}

		if proto.IsDir(d.Type) {
			subdirs = append(subdirs, dirWork{ino: d.Inode, path: childPath})
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case objects <- &Object{Key: childPath + "/", IsDir: true, Mtime: mtime}:
			}
		} else {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case objects <- &Object{Key: childPath, Size: size, Mtime: mtime}:
			}
		}
	}
	return subdirs, nil
}

func (c *CFSStorage) Get(_ context.Context, key string, off, size int64) (io.ReadCloser, error) {
	fpath := c.fullKey(key)
	ino, err := c.mw.LookupPath(fpath)
	if err != nil {
		return nil, fmt.Errorf("lookup %s: %w", fpath, err)
	}
	if err = c.ec.OpenStream(ino, false, false, fpath); err != nil {
		return nil, fmt.Errorf("open stream %s: %w", fpath, err)
	}

	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		_ = c.ec.CloseStream(ino)
		return nil, fmt.Errorf("inode get %s: %w", fpath, err)
	}
	fileSize := int64(info.Size)
	if size <= 0 || off+size > fileSize {
		size = fileSize - off
	}

	return &cfsReader{ec: c.ec, ino: ino, off: off, size: size}, nil
}

func (c *CFSStorage) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	fpath := c.fullKey(key)
	dir, name := splitPath(fpath)

	dirIno, err := c.mkdirsInternal(dir)
	if err != nil {
		return fmt.Errorf("mkdirs %s: %w", dir, err)
	}

	info, err := c.mw.Create_ll(dirIno, name, 0o644|syscall.S_IFREG, 0, 0, nil, fpath, true)
	if err != nil {
		return fmt.Errorf("create %s: %w", fpath, err)
	}
	ino := info.Inode

	if err = c.ec.OpenStream(ino, true, false, fpath); err != nil {
		return fmt.Errorf("open stream %s: %w", fpath, err)
	}
	// Truncate extents to 0 before writing so stale tail data from a previous
	// (larger) version of the file is not left behind.
	if err = c.ec.Truncate(c.mw, dirIno, ino, 0, fpath); err != nil {
		_ = c.ec.CloseStream(ino)
		return fmt.Errorf("truncate %s: %w", fpath, err)
	}

	buf := make([]byte, 2*1024*1024)
	var written int
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			wn, werr := c.ec.Write(ino, written, buf[:n], 0, nil, 0, false, false)
			written += wn
			if werr != nil {
				_ = c.ec.CloseStream(ino)
				return fmt.Errorf("write %s: %w", fpath, werr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = c.ec.CloseStream(ino)
			return fmt.Errorf("read source: %w", rerr)
		}
	}

	if err = c.ec.Flush(ino); err != nil {
		_ = c.ec.CloseStream(ino)
		return fmt.Errorf("flush %s: %w", fpath, err)
	}
	return c.ec.CloseStream(ino)
}

func (c *CFSStorage) Delete(_ context.Context, key string) error {
	fpath := c.fullKey(key)
	dir, name := splitPath(fpath)
	dirIno, err := c.mw.LookupPath(dir)
	if err != nil {
		return fmt.Errorf("lookup parent %s: %w", dir, err)
	}
	_, err = c.mw.Delete_ll(dirIno, name, false, fpath)
	return err
}

func (c *CFSStorage) MkdirAll(_ context.Context, key string) error {
	_, err := c.mkdirsInternal(c.fullKey(key))
	return err
}

func (c *CFSStorage) mkdirsInternal(dirPath string) (uint64, error) {
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	parentIno := proto.RootIno
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		current += "/" + part
		child, childMode, lerr := c.mw.Lookup_ll(parentIno, part)
		if lerr == nil {
			if !proto.IsDir(childMode) {
				return 0, fmt.Errorf("%s is not a directory", current)
			}
			parentIno = child
			continue
		}
		if lerr != syscall.ENOENT {
			return 0, fmt.Errorf("lookup %s: %w", current, lerr)
		}
		info, cerr := c.mw.Create_ll(parentIno, part, 0o755|syscall.S_IFDIR, 0, 0, nil, current, false)
		if cerr != nil {
			if cerr == syscall.EEXIST {
				child, _, _ = c.mw.Lookup_ll(parentIno, part)
				parentIno = child
				continue
			}
			return 0, fmt.Errorf("mkdir %s: %w", current, cerr)
		}
		parentIno = info.Inode
	}
	return parentIno, nil
}

// splitPath splits a full path into (dir, name).
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

// cfsReader implements io.ReadCloser for a CubeFS file extent stream.
type cfsReader struct {
	ec   *stream.ExtentClient
	ino  uint64
	off  int64
	size int64
	read int64
}

func (r *cfsReader) Read(p []byte) (int, error) {
	if r.read >= r.size {
		return 0, io.EOF
	}
	toRead := int64(len(p))
	if r.read+toRead > r.size {
		toRead = r.size - r.read
		p = p[:toRead]
	}
	n, err := r.ec.Read(r.ino, p, int(r.off+r.read), len(p), 0, false)
	r.read += int64(n)
	if err != nil && err != io.EOF {
		return n, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (r *cfsReader) Close() error {
	return r.ec.CloseStream(r.ino)
}
