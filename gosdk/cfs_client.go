package gosdk

import (
	"context"
	"fmt"
	"github.com/bits-and-blooms/bitset"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/client/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"io"
	syslog "log"
	"os"
	gopath "path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultBlkSize      = uint32(1) << 12
	maxFdNum       uint = 10240000
	MaxSizePutOnce      = int64(1) << 23
)

var (
	gid int64
)

type (
	Config struct {
		VolName    string `json:"volName"`
		MasterAddr string `json:"masterAddr"`
		AccessKey  string `json:"accessKey"`
		SecretKey  string `json:"secretKey"`

		FollowerRead     bool   `json:"followerRead,omitempty"`
		EnableBcache     bool   `json:"enableBcache,omitempty"`
		EnableSummary    bool   `json:"enableSummary,omitempty"`
		EnableAudit      bool   `json:"enableAudit,omitempty"`
		ReadBlockThread  int    `json:"readBlockThread,omitempty"`
		WriteBlockThread int    `json:"writeBlockThread,omitempty"`
		LogDir           string `json:"logDir,omitempty"`
		LogLevel         string `json:"logLevel,omitempty"`
		PushAddr         string `json:"pushAddr,omitempty"`
	}

	Client struct {
		// Client ID allocated by libsdk
		ID int64

		// mount config
		cfg Config

		ebsEndpoint         string
		servicePath         string
		volType             int
		cacheAction         int
		ebsBlockSize        int
		cacheRuleKey        string
		cacheThreshold      int
		subDir              string
		cluster             string
		dirChildrenNumLimit uint32

		// runtime context
		cwd    string // current working directory
		fdmap  map[uint]*file
		fdset  *bitset.BitSet
		fdlock sync.RWMutex

		// server info
		mw   *meta.MetaWrapper
		ec   *stream.ExtentClient
		ic   *fs.InodeCache
		dc   *fs.DentryCache
		bc   *bcache.BcacheClient
		ebsc *blobstore.BlobStoreClient
		sc   *fs.SummaryCache
	}

	StatInfo struct {
		AtimeNsec uint32
		MtimeNsec uint32
		CtimeNsec uint32
		Mode      uint32
		Nlink     uint32
		BlkSize   uint32
		Uid       uint32
		Gid       uint32

		Ino    uint64
		Size   uint64
		Blocks uint64
		Atime  uint64
		Mtime  uint64
		Ctime  uint64
	}

	Dirent struct {
		Name  string
		DType uint8
		Ino   uint64
	}

	HdfsStatInfo struct {
		AtimeNsec uint32
		MtimeNsec uint32
		Mode      uint32
		Size      uint64
		Atime     uint64
		Mtime     uint64
	}
	DirentInfo struct {
		DType uint8
		Name  string
		Stat  HdfsStatInfo
	}

	SummaryInfo struct {
		Files   int64
		Subdirs int64
		Fbytes  int64
	}

	file struct {
		fd    uint
		ino   uint64
		pino  uint64
		flags int
		mode  uint32

		// dir only
		dirp *dirStream

		//rw
		fileWriter *blobstore.Writer
		fileReader *blobstore.Reader
	}

	dirStream struct {
		pos     int
		dirents []proto.Dentry
	}
)

func New(cfg Config) *Client {
	id := atomic.AddInt64(&gid, 1)
	c := &Client{
		ID:                  id,
		cfg:                 cfg,
		fdmap:               make(map[uint]*file),
		fdset:               bitset.New(maxFdNum),
		dirChildrenNumLimit: proto.DefaultDirChildrenNumLimit,
		cwd:                 "/",
		sc:                  fs.NewSummaryCache(fs.DefaultSummaryExpiration, fs.MaxSummaryCache),
		ic:                  fs.NewInodeCache(fs.DefaultInodeExpiration, fs.MaxInodeCache),
		dc:                  fs.NewDentryCache(),
	}
	// Just skip fd 0, 1, 2, to avoid confusion.
	c.fdset.Set(0).Set(1).Set(2)
	return c
}

func (c *Client) Start() (err error) {
	var masters = strings.Split(c.cfg.MasterAddr, ",")
	if c.cfg.LogDir != "" {
		if c.cfg.LogLevel == "" {
			c.cfg.LogLevel = "WARN"
		}
		level := parseLogLevel(c.cfg.LogLevel)
		log.InitLog(c.cfg.LogDir, "libcfs", level, nil, log.DefaultLogLeftSpaceLimit)
		stat.NewStatistic(c.cfg.LogDir, "libcfs", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
	}
	proto.InitBufferPool(int64(32768))
	if c.cfg.ReadBlockThread == 0 {
		c.cfg.ReadBlockThread = 10
	}
	if c.cfg.WriteBlockThread == 0 {
		c.cfg.WriteBlockThread = 10
	}
	if err = c.loadConfFromMaster(masters); err != nil {
		return
	}
	if err = c.checkPermission(); err != nil {
		err = errors.NewErrorf("check permission failed: %v", err)
		syslog.Println(err)
		return
	}

	if c.cfg.EnableAudit {
		_, err = auditlog.InitAudit(c.cfg.LogDir, "clientSdk", int64(auditlog.DefaultAuditLogSize))
		if err != nil {
			log.LogWarnf("Init audit log fail: %v", err)
		}
	}

	if c.cfg.EnableSummary {
		c.sc = fs.NewSummaryCache(fs.DefaultSummaryExpiration, fs.MaxSummaryCache)
	}
	if c.cfg.EnableBcache {
		c.bc = bcache.NewBcacheClient()
	}
	var ebsc *blobstore.BlobStoreClient
	if c.ebsEndpoint != "" {
		if ebsc, err = blobstore.NewEbsClient(access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: access.ConsulConfig{
				Address: c.ebsEndpoint,
			},
			MaxSizePutOnce: MaxSizePutOnce,
			Logger: &access.Logger{
				Filename: gopath.Join(c.cfg.LogDir, "libcfs/ebs.log"),
			},
		}); err != nil {
			return
		}
	}
	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        c.cfg.VolName,
		Masters:       masters,
		ValidateOwner: false,
		EnableSummary: c.cfg.EnableSummary,
	}); err != nil {
		log.LogErrorf("newClient NewMetaWrapper failed(%v)", err)
		return err
	}
	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            c.cfg.VolName,
		VolumeType:        c.volType,
		Masters:           masters,
		FollowerRead:      c.cfg.FollowerRead,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		BcacheEnable:      c.cfg.EnableBcache,
		OnLoadBcache:      c.bc.Get,
		OnCacheBcache:     c.bc.Put,
		OnEvictBcache:     c.bc.Evict,
		DisableMetaCache:  true,
	}); err != nil {
		log.LogErrorf("newClient NewExtentClient failed(%v)", err)
		return
	}

	c.mw = mw
	c.ec = ec
	c.ebsc = ebsc
	return nil
}

func (c *Client) Close() {
	if c.ec != nil {
		_ = c.ec.Close()
	}
	if c.mw != nil {
		_ = c.mw.Close()
	}
	auditlog.StopAudit()
	log.LogFlush()
}

func (c *Client) Chdir(path string) error {
	cwd := c.absPath(path)
	dirInfo, err := c.lookupPath(cwd)
	if err != nil {
		return err
	}
	if !proto.IsDir(dirInfo.Mode) {
		return syscall.ENOTDIR
	}
	c.cwd = cwd
	return nil
}

func (c *Client) GetCwd() string {
	return c.cwd
}

func (c *Client) GetAttr(path string) (stat *StatInfo, err error) {

	info, err := c.lookupPath(c.absPath(path))
	if err != nil {
		return nil, err
	}

	// fill up the stat
	stat = &StatInfo{}
	stat.Ino = info.Inode
	stat.Size = info.Size
	stat.Nlink = info.Nlink
	stat.BlkSize = defaultBlkSize
	stat.Uid = info.Uid
	stat.Gid = info.Gid

	if info.Size%512 != 0 {
		stat.Blocks = (info.Size >> 9) + 1
	} else {
		stat.Blocks = info.Size >> 9
	}
	// fill up the mode
	if proto.IsRegular(info.Mode) {
		stat.Mode = (syscall.S_IFREG) | (info.Mode & 0777)
	} else if proto.IsDir(info.Mode) {
		stat.Mode = (syscall.S_IFDIR) | (info.Mode & 0777)
	} else if proto.IsSymlink(info.Mode) {
		stat.Mode = (syscall.S_IFLNK) | (info.Mode & 0777)
	} else {
		stat.Mode = (syscall.S_IFSOCK) | (info.Mode & 0777)
	}

	// fill up the time struct
	t := info.AccessTime.UnixNano()
	stat.Atime = uint64(t / 1e9)
	stat.AtimeNsec = uint32(t % 1e9)

	t = info.ModifyTime.UnixNano()
	stat.Mtime = uint64(t / 1e9)
	stat.MtimeNsec = uint32(t % 1e9)

	t = info.CreateTime.UnixNano()
	stat.Ctime = uint64(t / 1e9)
	stat.CtimeNsec = uint32(t % 1e9)

	return stat, nil
}

func (c *Client) SetAttr(path string, stat *StatInfo, valid uint32) error {
	info, err := c.lookupPath(c.absPath(path))
	if err != nil {
		return err
	}

	err = c.setattr(info, valid, uint32(stat.Mode), uint32(stat.Uid), uint32(stat.Gid), int64(stat.Atime), int64(stat.Mtime))
	if err != nil {
		return err
	}

	c.ic.Delete(info.Inode)
	return nil
}

func (c *Client) OpenFile(path string, flags int, mode uint32) (uint, error) {
	start := time.Now()
	absPath := c.absPath(path)

	fuseMode := mode & uint32(0777)
	fuseFlags := flags &^ 0x8000
	accFlags := fuseFlags & syscall.O_ACCMODE

	var info *proto.InodeInfo
	var parentIno uint64

	/*
	 * Note that the rwx mode is ignored when using libsdk
	 */

	if fuseFlags&syscall.O_CREAT != 0 {
		if accFlags != syscall.O_WRONLY && accFlags != syscall.O_RDWR {
			return 0, syscall.EACCES
		}
		dirpath, name := gopath.Split(absPath)
		dirInfo, err := c.lookupPath(dirpath)
		if err != nil {
			return 0, err
		}
		parentIno = dirInfo.Inode
		defer func() {
			if info != nil {
				auditlog.FormatLog("Create", dirpath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
			} else {
				auditlog.FormatLog("Create", dirpath, "nil", err, time.Since(start).Microseconds(), 0, 0)
			}
		}()
		newInfo, err := c.create(dirInfo.Inode, name, fuseMode)
		if err != nil {
			if err != syscall.EEXIST {
				return 0, err
			}
			newInfo, err = c.lookupPath(absPath)
			if err != nil {
				return 0, err
			}
		}
		info = newInfo
	} else {
		dirpath, _ := gopath.Split(absPath)
		dirInfo, err := c.lookupPath(dirpath)
		if err != nil {
			return 0, err
		}
		parentIno = dirInfo.Inode // parent inode
		newInfo, err := c.lookupPath(absPath)
		if err != nil {
			return 0, err
		}
		info = newInfo
	}
	var fileCache bool
	if c.cacheRuleKey == "" {
		fileCache = false
	} else {
		fileCachePattern := fmt.Sprintf(".*%s.*", c.cacheRuleKey)
		fileCache, _ = regexp.MatchString(fileCachePattern, absPath)
	}
	f := c.allocFD(info.Inode, fuseFlags, fuseMode, fileCache, info.Size, parentIno)
	if f == nil {
		return 0, syscall.EMFILE
	}

	if proto.IsRegular(info.Mode) {
		c.openStream(f)
		if fuseFlags&(syscall.O_TRUNC) != 0 {
			if accFlags != (syscall.O_WRONLY) && accFlags != (syscall.O_RDWR) {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return 0, syscall.EACCES
			}
			if err := c.truncate(f, 0); err != nil {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return 0, syscall.EIO
			}
		}
	}

	return f.fd, nil
}

func (c *Client) Flush(fd uint) error {

	f := c.getFile(uint(fd))
	if f == nil {
		return syscall.EBADFD
	}

	err := c.flush(f)
	if err != nil {
		return err
	}
	c.ic.Delete(f.ino)
	return nil
}

func (c *Client) CloseFile(fd uint) {
	f := c.releaseFD(fd)
	if f != nil {
		c.flush(f)
		c.closeStream(f)
	}
}

func (c *Client) WriteFile(fd uint, data []byte, off int64) (n int, err error) {
	f := c.getFile(fd)
	if f == nil {
		return 0, syscall.EBADFD
	}

	accFlags := f.flags & (syscall.O_ACCMODE)
	if accFlags != (syscall.O_WRONLY) && accFlags != (syscall.O_RDWR) {
		return 0, syscall.EACCES
	}

	var flags int
	var wait bool

	if f.flags&(syscall.O_DIRECT) != 0 || f.flags&(syscall.O_SYNC) != 0 || f.flags&(syscall.O_DSYNC) != 0 {
		if proto.IsHot(c.volType) {
			wait = true
		}
	}
	if f.flags&(syscall.O_APPEND) != 0 || proto.IsCold(c.volType) {
		flags |= proto.FlagsAppend
		flags |= proto.FlagsSyncWrite
	}

	n, err = c.write(f, off, data, flags)
	if err != nil {
		return 0, err
	}

	if wait {
		if err = c.flush(f); err != nil {
			return 0, err
		}
	}

	return n, nil
}

func (c *Client) ReadFile(fd uint, buf []byte, off int64) (n int, err error) {
	f := c.getFile(fd)
	if f == nil {
		return 0, syscall.EBADFD
	}

	accFlags := f.flags & (syscall.O_ACCMODE)
	if accFlags == (syscall.O_WRONLY) {
		return 0, syscall.EACCES
	}

	n, err = c.read(f, off, buf)
	if err != nil {
		return 0, err
	}

	return n, err
}

func (c *Client) BatchGetInodes(fd uint, inodeIDS []uint64, count int) (stats []StatInfo, err error) {

	f := c.getFile(fd)
	if f == nil {
		return nil, syscall.EBADFD
	}

	infos := c.mw.BatchInodeGet(inodeIDS)
	if len(infos) > count {
		return nil, syscall.EINVAL
	}

	stats = make([]StatInfo, len(infos))
	for i := 0; i < len(infos); i++ {
		// fill up the stat
		stats[i].Ino = infos[i].Inode
		stats[i].Size = infos[i].Size
		stats[i].Blocks = infos[i].Size >> 9
		stats[i].Nlink = infos[i].Nlink
		stats[i].BlkSize = defaultBlkSize
		stats[i].Uid = infos[i].Uid
		stats[i].Gid = infos[i].Gid

		// fill up the mode
		if proto.IsRegular(infos[i].Mode) {
			stats[i].Mode = syscall.S_IFREG | (infos[i].Mode & 0777)
		} else if proto.IsDir(infos[i].Mode) {
			stats[i].Mode = syscall.S_IFDIR | (infos[i].Mode & 0777)
		} else if proto.IsSymlink(infos[i].Mode) {
			stats[i].Mode = syscall.S_IFLNK | (infos[i].Mode & 0777)
		} else {
			stats[i].Mode = syscall.S_IFSOCK | (infos[i].Mode & 0777)
		}

		// fill up the time struct
		t := infos[i].AccessTime.UnixNano()
		stats[i].Atime = uint64(t / 1e9)
		stats[i].AtimeNsec = uint32(t % 1e9)

		t = infos[i].ModifyTime.UnixNano()
		stats[i].Mtime = uint64(t / 1e9)
		stats[i].MtimeNsec = uint32(t % 1e9)

		t = infos[i].CreateTime.UnixNano()
		stats[i].Ctime = uint64(t / 1e9)
		stats[i].CtimeNsec = uint32(t % 1e9)
	}

	return stats, nil
}

func (c *Client) RefreshSummary(path string, goroutineNum int32) error {

	if !c.cfg.EnableSummary {
		return syscall.EINVAL
	}
	info, err := c.lookupPath(c.absPath(path))
	var ino uint64
	if err != nil {
		ino = proto.RootIno
	} else {
		ino = info.Inode
	}

	err = c.mw.RefreshSummary_ll(ino, goroutineNum)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Readdir(fd uint, count int) (dirents []Dirent, err error) {

	f := c.getFile(uint(fd))
	if f == nil {
		return nil, syscall.EBADFD
	}

	if f.dirp == nil {
		f.dirp = &dirStream{}
		dentries, err := c.mw.ReadDir_ll(f.ino)
		if err != nil {
			return nil, err
		}
		f.dirp.dirents = dentries
	}

	dirp := f.dirp
	dirents = make([]Dirent, len(dirp.dirents)-dirp.pos)
	for n := 0; dirp.pos < len(dirp.dirents) && n < count; {
		// fill up ino
		dirents[n].Ino = dirp.dirents[dirp.pos].Inode

		// fill up d_type
		if proto.IsRegular(dirp.dirents[dirp.pos].Type) {
			dirents[n].DType = syscall.DT_REG
		} else if proto.IsDir(dirp.dirents[dirp.pos].Type) {
			dirents[n].DType = syscall.DT_DIR
		} else if proto.IsSymlink(dirp.dirents[dirp.pos].Type) {
			dirents[n].DType = syscall.DT_LNK
		} else {
			dirents[n].DType = syscall.DT_UNKNOWN
		}

		// fill up name
		dirents[n].Name = dirp.dirents[dirp.pos].Name
		// advance cursor
		dirp.pos++
		n++
	}

	return dirents, nil
}

func (c *Client) Lsdir(fd uint, count int) (direntsInfo []DirentInfo, err error) {

	f := c.getFile(fd)
	if f == nil {
		return nil, syscall.EBADFD
	}

	if f.dirp == nil {
		f.dirp = &dirStream{}
		dentries, err := c.mw.ReadDir_ll(f.ino)
		if err != nil {
			return nil, err
		}
		f.dirp.dirents = dentries
	}

	var (
		n        int
		dirp     = f.dirp
		inodeIDS = make([]uint64, count, count)
		inodeMap = make(map[uint64]int)
	)
	direntsInfo = make([]DirentInfo, len(dirp.dirents)-dirp.pos)

	for dirp.pos < len(dirp.dirents) && n < count {
		inodeIDS[n] = dirp.dirents[dirp.pos].Inode
		inodeMap[dirp.dirents[dirp.pos].Inode] = n
		// fill up d_type
		if proto.IsRegular(dirp.dirents[dirp.pos].Type) {
			direntsInfo[n].DType = syscall.DT_REG
		} else if proto.IsDir(dirp.dirents[dirp.pos].Type) {
			direntsInfo[n].DType = syscall.DT_DIR
		} else if proto.IsSymlink(dirp.dirents[dirp.pos].Type) {
			direntsInfo[n].DType = syscall.DT_LNK
		} else {
			direntsInfo[n].DType = syscall.DT_UNKNOWN
		}

		direntsInfo[n].Name = dirp.dirents[dirp.pos].Name
		// advance cursor
		dirp.pos++
		n++
	}
	if n == 0 {
		return direntsInfo, nil
	}

	infos := c.mw.BatchInodeGet(inodeIDS)
	if len(infos) != n {
		return nil, syscall.EIO
	}

	for i := 0; i < len(infos); i++ {
		// fill up the stat
		index := inodeMap[infos[i].Inode]
		direntsInfo[index].Stat.Size = infos[i].Size

		// fill up the mode
		if proto.IsRegular(infos[i].Mode) {
			direntsInfo[index].Stat.Mode = syscall.S_IFREG | (infos[i].Mode & 0777)
		} else if proto.IsDir(infos[i].Mode) {
			direntsInfo[index].Stat.Mode = syscall.S_IFDIR | (infos[i].Mode & 0777)
		} else if proto.IsSymlink(infos[i].Mode) {
			direntsInfo[index].Stat.Mode = syscall.S_IFLNK | (infos[i].Mode & 0777)
		} else {
			direntsInfo[index].Stat.Mode = syscall.S_IFSOCK | (infos[i].Mode & 0777)
		}

		// fill up the time struct
		t := infos[index].AccessTime.UnixNano()
		direntsInfo[index].Stat.Atime = uint64(t / 1e9)
		direntsInfo[index].Stat.AtimeNsec = uint32(t % 1e9)

		t = infos[index].ModifyTime.UnixNano()
		direntsInfo[index].Stat.Mtime = uint64(t / 1e9)
		direntsInfo[index].Stat.MtimeNsec = uint32(t % 1e9)
	}
	return direntsInfo, nil

}

func (c *Client) Mkdirs(path string, mode uint32) error {

	start := time.Now()
	var gerr error
	var gino uint64

	dirpath := c.absPath(path)
	if dirpath == "/" {
		return syscall.EEXIST
	}

	defer func() {
		if gerr == nil {
			auditlog.FormatLog("Mkdir", dirpath, "nil", gerr, time.Since(start).Microseconds(), gino, 0)
		} else {
			auditlog.FormatLog("Mkdir", dirpath, "nil", gerr, time.Since(start).Microseconds(), 0, 0)
		}
	}()

	pino := proto.RootIno
	dirs := strings.Split(dirpath, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := c.mw.Lookup_ll(pino, dir)
		if err != nil {
			if err == syscall.ENOENT {
				info, err := c.mkdir(pino, dir, mode)

				if err != nil {
					if err != syscall.EEXIST {
						gerr = err
						return err
					}
				} else {
					child = info.Inode
				}
			} else {
				gerr = err
				return err
			}
		}
		pino = child
		gino = child
	}

	return nil
}

func (c *Client) Rmdir(path string) error {

	start := time.Now()
	var err error
	var info *proto.InodeInfo

	absPath := c.absPath(path)
	defer func() {
		if info == nil {
			auditlog.FormatLog("Rmdir", absPath, "nil", err, time.Since(start).Microseconds(), 0, 0)
		} else {
			auditlog.FormatLog("Rmdir", absPath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
		}
	}()
	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return err
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, true)
	c.ic.Delete(dirInfo.Inode)
	c.dc.Delete(absPath)
	return err
}

func (c *Client) Unlink(path string) error {

	start := time.Now()
	var err error
	var info *proto.InodeInfo

	absPath := c.absPath(path)
	dirpath, name := gopath.Split(absPath)

	defer func() {
		if info == nil {
			auditlog.FormatLog("Unlink", absPath, "nil", err, time.Since(start).Microseconds(), 0, 0)
		} else {
			auditlog.FormatLog("Unlink", absPath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
		}
	}()
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return err
	}

	_, mode, err := c.mw.Lookup_ll(dirInfo.Inode, name)
	if err != nil {
		return err
	}
	if proto.IsDir(mode) {
		return syscall.EISDIR
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, false)
	if err != nil {
		return err
	}

	if info != nil {
		_ = c.mw.Evict(info.Inode)
		c.ic.Delete(info.Inode)
	}
	return nil
}

func (c *Client) Rename(from, to string) error {

	start := time.Now()
	var err error

	absFrom := c.absPath(from)
	absTo := c.absPath(to)

	defer func() {
		auditlog.FormatLog("Rename", absFrom, absTo, err, time.Since(start).Microseconds(), 0, 0)
	}()

	srcDirPath, srcName := gopath.Split(absFrom)
	dstDirPath, dstName := gopath.Split(absTo)

	srcDirInfo, err := c.lookupPath(srcDirPath)
	if err != nil {
		return err
	}
	dstDirInfo, err := c.lookupPath(dstDirPath)
	if err != nil {
		return err
	}

	err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstDirInfo.Inode, dstName, false)
	c.ic.Delete(srcDirInfo.Inode)
	c.ic.Delete(dstDirInfo.Inode)
	c.dc.Delete(absFrom)
	return err
}

func (c *Client) Fchmod(fd uint, mode uint32) error {

	f := c.getFile(uint(fd))
	if f == nil {
		return syscall.EBADFD
	}

	info, err := c.mw.InodeGet_ll(f.ino)
	if err != nil {
		return err
	}

	err = c.setattr(info, proto.AttrMode, uint32(mode), 0, 0, 0, 0)
	if err != nil {
		return err
	}
	c.ic.Delete(info.Inode)
	return nil
}

func (c *Client) GetSummary(path string, useCache string, goroutineNum int32) (summary *SummaryInfo, err error) {

	info, err := c.lookupPath(c.absPath(path))
	if err != nil {
		return nil, err
	}

	summary = &SummaryInfo{}
	if strings.ToLower(useCache) == "true" {
		cacheSummaryInfo := c.sc.Get(info.Inode)
		if cacheSummaryInfo != nil {
			summary.Files = cacheSummaryInfo.Files
			summary.Subdirs = cacheSummaryInfo.Subdirs
			summary.Fbytes = cacheSummaryInfo.Fbytes
			return summary, nil
		}
	}

	if !proto.IsDir(info.Mode) {
		return nil, syscall.ENOTDIR
	}

	summaryInfo, err := c.mw.GetSummary_ll(info.Inode, goroutineNum)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(useCache) != "false" {
		c.sc.Put(info.Inode, &summaryInfo)
	}
	summary.Files = summaryInfo.Files
	summary.Subdirs = summaryInfo.Subdirs
	summary.Fbytes = summaryInfo.Fbytes
	return summary, nil
}

func (c *Client) create(pino uint64, name string, mode uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil)
}

func (c *Client) mkdir(pino uint64, name string, mode uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil)
}

func (c *Client) openStream(f *file) {
	_ = c.ec.OpenStream(f.ino)
}

func (c *Client) closeStream(f *file) {
	_ = c.ec.CloseStream(f.ino)
	_ = c.ec.EvictStream(f.ino)
	f.fileWriter.FreeCache()
	f.fileWriter = nil
	f.fileReader = nil
}

func (c *Client) truncate(f *file, size int) error {
	err := c.ec.Truncate(c.mw, f.pino, f.ino, size)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) releaseFD(fd uint) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	f, ok := c.fdmap[fd]
	if !ok {
		return nil
	}
	delete(c.fdmap, fd)
	c.fdset.Clear(fd)
	c.ic.Delete(f.ino)
	return f
}

func (c *Client) getFile(fd uint) *file {
	c.fdlock.Lock()
	f := c.fdmap[fd]
	c.fdlock.Unlock()
	return f
}

func (c *Client) flush(f *file) error {
	if proto.IsHot(c.volType) {
		return c.ec.Flush(f.ino)
	} else {
		if f.fileWriter != nil {
			return f.fileWriter.Flush(f.ino, c.ctx(c.ID, f.ino))
		}
	}
	return nil
}

func (c *Client) write(f *file, offset int64, data []byte, flags int) (n int, err error) {
	if proto.IsHot(c.volType) {
		c.ec.GetStreamer(f.ino).SetParentInode(f.pino) // set the parent inode
		checkFunc := func() error {
			if !c.mw.EnableQuota {
				return nil
			}

			if ok := c.ec.UidIsLimited(0); ok {
				return syscall.ENOSPC
			}

			if c.mw.IsQuotaLimitedById(f.ino, true, false) {
				return syscall.ENOSPC
			}
			return nil
		}
		n, err = c.ec.Write(f.ino, int(offset), data, flags, checkFunc)
	} else {
		n, err = f.fileWriter.Write(c.ctx(c.ID, f.ino), int(offset), data, flags)
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *Client) read(f *file, offset int64, data []byte) (n int, err error) {
	if proto.IsHot(c.volType) {
		n, err = c.ec.Read(f.ino, data, int(offset), len(data))
	} else {
		n, err = f.fileReader.Read(c.ctx(c.ID, f.ino), data, int(offset), len(data))
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	return n, nil
}

func (c *Client) setattr(info *proto.InodeInfo, valid uint32, mode, uid, gid uint32, atime, mtime int64) error {
	// Only rwx mode bit can be set
	if valid&proto.AttrMode != 0 {
		fuseMode := mode & uint32(0777)
		mode = info.Mode &^ uint32(0777) // clear rwx mode bit
		mode |= fuseMode
	}
	return c.mw.Setattr(info.Inode, valid, mode, uid, gid, atime, mtime)
}

func (c *Client) lookupPath(path string) (*proto.InodeInfo, error) {
	ino, ok := c.dc.Get(gopath.Clean(path))
	if !ok {
		inoInterval, err := c.mw.LookupPath(gopath.Clean(path))
		if err != nil {
			return nil, err
		}
		c.dc.Put(gopath.Clean(path), inoInterval)
		ino = inoInterval
	}
	info := c.ic.Get(ino)
	if info != nil {
		return info, nil
	}
	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}
	c.ic.Put(info)

	return info, nil
}

func (c *Client) allocFD(ino uint64, flags int, mode uint32, fileCache bool, fileSize uint64, parentInode uint64) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fd, ok := c.fdset.NextClear(0)
	if !ok || fd > maxFdNum {
		return nil
	}
	c.fdset.Set(fd)
	f := &file{fd: fd, ino: ino, flags: flags, mode: mode, pino: parentInode}
	if proto.IsCold(c.volType) {
		clientConf := blobstore.ClientConfig{
			VolName:         c.cfg.VolName,
			VolType:         c.volType,
			BlockSize:       c.ebsBlockSize,
			Ino:             ino,
			Bc:              c.bc,
			Mw:              c.mw,
			Ec:              c.ec,
			Ebsc:            c.ebsc,
			EnableBcache:    c.cfg.EnableBcache,
			WConcurrency:    c.cfg.WriteBlockThread,
			ReadConcurrency: c.cfg.ReadBlockThread,
			CacheAction:     c.cacheAction,
			FileCache:       fileCache,
			FileSize:        fileSize,
			CacheThreshold:  c.cacheThreshold,
		}
		f.fileWriter.FreeCache()
		switch flags & 0xff {
		case syscall.O_RDONLY:
			f.fileReader = blobstore.NewReader(clientConf)
			f.fileWriter = nil
		case syscall.O_WRONLY:
			f.fileWriter = blobstore.NewWriter(clientConf)
			f.fileReader = nil
		case syscall.O_RDWR:
			f.fileReader = blobstore.NewReader(clientConf)
			f.fileWriter = blobstore.NewWriter(clientConf)
		default:
			f.fileWriter = blobstore.NewWriter(clientConf)
			f.fileReader = nil
		}
	}
	c.fdmap[fd] = f
	return f
}

func (c *Client) absPath(path string) string {
	p := gopath.Clean(path)
	if !gopath.IsAbs(p) {
		p = gopath.Join(c.cwd, p)
	}
	return gopath.Clean(p)
}

func (c *Client) loadConfFromMaster(masters []string) (err error) {
	mc := masterSDK.NewMasterClient(masters, false)
	var volumeInfo *proto.SimpleVolView
	volumeInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(c.cfg.VolName)
	if err != nil {
		return
	}
	c.volType = volumeInfo.VolType
	c.ebsBlockSize = volumeInfo.ObjBlockSize
	c.cacheAction = volumeInfo.CacheAction
	c.cacheRuleKey = volumeInfo.CacheRule
	c.cacheThreshold = volumeInfo.CacheThreshold

	var clusterInfo *proto.ClusterInfo
	clusterInfo, err = mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return
	}
	c.ebsEndpoint = clusterInfo.EbsAddr
	c.servicePath = clusterInfo.ServicePath
	c.cluster = clusterInfo.Cluster
	c.dirChildrenNumLimit = clusterInfo.DirChildrenNumLimit
	buf.InitCachePool(c.ebsBlockSize)
	return
}

func (c *Client) checkPermission() (err error) {
	if c.cfg.AccessKey == "" || c.cfg.SecretKey == "" {
		err = errors.New("invalid AccessKey or SecretKey")
		return
	}

	// checkPermission
	var mc = masterSDK.NewMasterClientFromString(c.cfg.MasterAddr, false)
	var userInfo *proto.UserInfo
	if userInfo, err = mc.UserAPI().GetAKInfo(c.cfg.AccessKey); err != nil {
		return
	}
	if userInfo.SecretKey != c.cfg.SecretKey {
		err = proto.ErrNoPermission
		return
	}
	var policy = userInfo.Policy
	if policy.IsOwn(c.cfg.VolName) {
		return
	}
	// read write
	if policy.IsAuthorized(c.cfg.VolName, c.subDir, proto.POSIXWriteAction) &&
		policy.IsAuthorized(c.cfg.VolName, c.subDir, proto.POSIXReadAction) {
		return
	}
	// read only
	if policy.IsAuthorized(c.cfg.VolName, c.subDir, proto.POSIXReadAction) &&
		!policy.IsAuthorized(c.cfg.VolName, c.subDir, proto.POSIXWriteAction) {
		return
	}
	err = proto.ErrNoPermission
	return
}

func (c *Client) ctx(cid int64, ino uint64) context.Context {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", fmt.Sprintf("cid=%v,ino=%v", cid, ino))
	return ctx
}

func parseLogLevel(loglvl string) log.Level {
	var level log.Level
	switch strings.ToLower(loglvl) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	return level
}
