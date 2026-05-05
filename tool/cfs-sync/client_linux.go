package main

import (
	"fmt"
	"path"
	"strings"
	"syscall"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/remotecache"
	"github.com/cubefs/cubefs/util/log"
)

type cfsFile struct {
	ino      uint64
	flags    int
	fullPath string
	c        *cfsClient
}

type cfsClient struct {
	mw *meta.MetaWrapper
	ec *stream.ExtentClient
	rc *remotecache.RemoteCacheClient
}

func newCFSClient(masters []string, vol, logDir, logLevel string) (*cfsClient, error) {
	if logDir != "" {
		level := log.ParseLogLevel(logLevel)
		log.InitLog(logDir, "cfs-sync", level, nil, log.DefaultLogLeftSpaceLimitRatio)
	}
	proto.InitBufferPool(32768)

	mc := masterSDK.NewMasterClient(masters, false)
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(vol)
	if err != nil {
		return nil, fmt.Errorf("get volume info from master: %w", err)
	}
	if proto.IsCold(volInfo.VolType) {
		return nil, fmt.Errorf("cfs-sync does not support BlobStore (cold) volumes: vol %q has VolType=%d StorageClass=%d",
			vol, volInfo.VolType, volInfo.VolStorageClass)
	}

	mw, err := meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        vol,
		Masters:       masters,
		ValidateOwner: false,
	})
	if err != nil {
		return nil, fmt.Errorf("init meta: %w", err)
	}

	ec, err := stream.NewExtentClient(&stream.ExtentConfig{
		Volume:                 vol,
		Masters:                masters,
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

	return &cfsClient{mw: mw, ec: ec}, nil
}

func newFlashClient(masters []string, logDir, logLevel string) (*cfsClient, error) {
	rc, err := remotecache.NewRemoteCacheClient(&remotecache.ClientConfig{
		Masters:            masters,
		LogDir:             logDir,
		LogLevelStr:        logLevel,
		FirstPacketTimeout: 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("remotecache start: %w", err)
	}
	return &cfsClient{rc: rc}, nil
}

func (c *cfsClient) close() {
	if c.ec != nil {
		_ = c.ec.Close()
	}
	if c.rc != nil {
		c.rc.Stop()
	}
}

func (c *cfsClient) getAttr(filePath string) (size uint64, mode uint32, err error) {
	ino, err := c.mw.LookupPath(filePath)
	if err != nil {
		return 0, 0, err
	}
	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		return 0, 0, err
	}
	return info.Size, info.Mode, nil
}

func (c *cfsClient) mkdirs(dirPath string, mode uint32) error {
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
				return fmt.Errorf("%s is not a directory", current)
			}
			parentIno = child
			continue
		}
		if !isNotExist(lerr) {
			return fmt.Errorf("lookup %s: %w", current, lerr)
		}
		info, cerr := c.mw.Create_ll(parentIno, part, mode|syscall.S_IFDIR, 0, 0, nil, current, false)
		if cerr != nil {
			if isExist(cerr) {
				child, _, _ = c.mw.Lookup_ll(parentIno, part)
				parentIno = child
				continue
			}
			return fmt.Errorf("mkdir %s: %w", current, cerr)
		}
		parentIno = info.Inode
	}
	return nil
}

func (c *cfsClient) openFile(filePath string, flags int, fileMode uint32) (*cfsFile, error) {
	dir, name := path.Split(filePath)
	if dir == "" {
		dir = "/"
	}
	dirIno, err := c.mw.LookupPath(strings.TrimSuffix(dir, "/"))
	if err != nil {
		return nil, fmt.Errorf("lookup parent dir: %w", err)
	}

	var ino uint64
	openForWrite := (flags & syscall.O_ACCMODE) != syscall.O_RDONLY

	if flags&syscall.O_CREAT != 0 {
		info, cerr := c.mw.Create_ll(dirIno, name, fileMode|syscall.S_IFREG, 0, 0, nil, filePath, flags&syscall.O_EXCL == 0)
		if cerr != nil {
			return nil, fmt.Errorf("create %s: %w", filePath, cerr)
		}
		ino = info.Inode
	} else {
		child, _, lerr := c.mw.Lookup_ll(dirIno, name)
		if lerr != nil {
			return nil, fmt.Errorf("lookup %s: %w", filePath, lerr)
		}
		ino = child
	}

	if err = c.ec.OpenStream(ino, openForWrite, false, filePath); err != nil {
		return nil, fmt.Errorf("open stream %s: %w", filePath, err)
	}

	if flags&syscall.O_TRUNC != 0 && openForWrite {
		if terr := c.mw.Truncate(ino, 0, filePath); terr != nil {
			_ = c.ec.CloseStream(ino)
			return nil, fmt.Errorf("truncate %s: %w", filePath, terr)
		}
	}

	return &cfsFile{ino: ino, flags: flags, fullPath: filePath, c: c}, nil
}

func (f *cfsFile) readFile(buf []byte, off int64) (int, error) {
	return f.c.ec.Read(f.ino, buf, int(off), len(buf), 0, false)
}

func (f *cfsFile) writeFile(data []byte, off int64) (int, error) {
	return f.c.ec.Write(f.ino, int(off), data, 0, nil, 0, false, false)
}

func (f *cfsFile) flush() error {
	return f.c.ec.Flush(f.ino)
}

func (f *cfsFile) closeFile() error {
	if err := f.c.ec.Flush(f.ino); err != nil {
		return err
	}
	return f.c.ec.CloseStream(f.ino)
}

func isNotExist(err error) bool {
	return err == syscall.ENOENT
}

func isExist(err error) bool {
	return err == syscall.EEXIST
}
