// Copyright 2018 The Cubefs Authors.
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
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/blobstore/api/access"
	"github.com/hashicorp/consul/api"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"

	"github.com/chubaofs/chubaofs/blockcache/bcache"
	"github.com/chubaofs/chubaofs/sdk/data/blobstore"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	volname     string
	owner       string
	ic          *InodeCache
	mw          *meta.MetaWrapper
	ec          *stream.ExtentClient
	orphan      *OrphanInodeList
	enSyncWrite bool
	keepCache   bool

	nodeCache map[uint64]fs.Node
	fslock    sync.Mutex

	disableDcache bool
	fsyncOnClose  bool
	enableXattr   bool
	rootIno       uint64

	//data lake
	volType        int
	ebsEndpoint    string
	CacheAction    int
	CacheThreshold int
	EbsBlockSize   int
	enableBcache   bool
	readThreads    int
	writeThreads   int
	bc             *bcache.BcacheClient
	ebsc           *blobstore.BlobStoreClient
	sc             *SummaryCache
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

const BlobWriterIdleTimeoutPeriod = 10

// NewSuper returns a new Super.
func NewSuper(opt *proto.MountOptions) (s *Super, err error) {
	s = new(Super)
	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	var metaConfig = &meta.MetaConfig{
		Volume:        opt.Volname,
		Owner:         opt.Owner,
		Masters:       masters,
		Authenticate:  opt.Authenticate,
		TicketMess:    opt.TicketMess,
		ValidateOwner: opt.Authenticate || opt.AccessKey == "",
		EnableSummary: opt.EnableSummary && opt.EnableXattr,
	}
	s.mw, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!"+err.Error())
	}

	s.volname = opt.Volname
	s.owner = opt.Owner
	s.cluster = s.mw.Cluster()
	inodeExpiration := DefaultInodeExpiration
	if opt.IcacheTimeout >= 0 {
		inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
	}
	if opt.LookupValid >= 0 {
		LookupValidDuration = time.Duration(opt.LookupValid) * time.Second
	}
	if opt.AttrValid >= 0 {
		AttrValidDuration = time.Duration(opt.AttrValid) * time.Second
	}
	if opt.EnSyncWrite > 0 {
		s.enSyncWrite = true
	}
	s.keepCache = opt.KeepCache
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	s.nodeCache = make(map[uint64]fs.Node)
	s.disableDcache = opt.DisableDcache
	s.fsyncOnClose = opt.FsyncOnClose
	s.enableXattr = opt.EnableXattr

	s.volType = opt.VolType
	s.ebsEndpoint = opt.EbsEndpoint
	s.CacheAction = opt.CacheAction
	s.CacheThreshold = opt.CacheThreshold
	s.EbsBlockSize = opt.EbsBlockSize
	s.enableBcache = opt.EnableBcache
	s.readThreads = int(opt.ReadThreads)
	s.writeThreads = int(opt.WriteThreads)
	if s.enableBcache {
		s.bc = bcache.NewBcacheClient()
	}
	var extentConfig = &stream.ExtentConfig{
		Volume:            opt.Volname,
		Masters:           masters,
		FollowerRead:      opt.FollowerRead,
		NearRead:          opt.NearRead,
		ReadRate:          opt.ReadRate,
		WriteRate:         opt.WriteRate,
		VolumeType:        opt.VolType,
		BcacheEnable:      opt.EnableBcache,
		OnAppendExtentKey: s.mw.AppendExtentKey,
		OnGetExtents:      s.mw.GetExtents,
		OnTruncate:        s.mw.Truncate,
		OnEvictIcache:     s.ic.Delete,
		OnLoadBcache:      s.bc.Get,
		OnCacheBcache:     s.bc.Put,
		OnEvictBcache:     s.bc.Evict,
	}
	s.ec, err = stream.NewExtentClient(extentConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}
	if proto.IsCold(opt.VolType) {
		s.ebsc, err = blobstore.NewEbsClient(access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: api.Config{
				Address: opt.EbsEndpoint,
			},
			MaxSizePutOnce: MaxSizePutOnce,
			Logger: &access.Logger{
				Filename: path.Join(opt.Logpath, "client/ebs.log"),
			},
		})
		if err != nil {
			return nil, errors.Trace(err, "NewEbsClient failed!")
		}
	}

	if s.rootIno, err = s.mw.GetRootIno(opt.SubDir); err != nil {
		return nil, err
	}
	if proto.IsCold(opt.VolType) {
		go s.scheduleFlush()
	}
	if s.mw.EnableSummary {
		s.sc = NewSummaryCache(DefaultSummaryExpiration, MaxSummaryCache)
	}

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

func (s *Super) scheduleFlush() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			ctx := context.Background()
			s.fslock.Lock()
			for ino, node := range s.nodeCache {
				if _, ok := node.(*File); !ok {
					continue
				}
				file := node.(*File)
				if atomic.LoadInt32(&file.idle) >= BlobWriterIdleTimeoutPeriod {
					if file.fWriter != nil {
						atomic.StoreInt32(&file.idle, 0)
						go file.fWriter.Flush(ino, ctx)
					}
				} else {
					atomic.AddInt32(&file.idle, 1)
				}
			}
			s.fslock.Unlock()
		}
	}
}

// Root returns the root directory where it resides.
func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(s.rootIno)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

// Statfs handles the Statfs request and returns a set of statistics.
func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	const defaultMaxMetaPartitionInodeID uint64 = 1<<63 - 1
	total, used, inodeCount := s.mw.Statfs()
	resp.Blocks = total / uint64(DefaultBlksize)
	resp.Bfree = (total - used) / uint64(DefaultBlksize)
	resp.Bavail = resp.Bfree
	resp.Bsize = DefaultBlksize
	resp.Namelen = DefaultMaxNameLen
	resp.Frsize = DefaultBlksize
	resp.Files = inodeCount
	resp.Ffree = defaultMaxMetaPartitionInodeID - inodeCount
	return nil
}

// ClusterName returns the cluster name.
func (s *Super) ClusterName() string {
	return s.cluster
}

func (s *Super) GetRate(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(s.ec.GetRate()))
}

func (s *Super) SetRate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if rate := r.FormValue("read"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set read rate failed\n"))
		} else {
			msg := s.ec.SetReadRate(val)
			w.Write([]byte(fmt.Sprintf("Set read rate to %v successfully\n", msg)))
		}
	}

	if rate := r.FormValue("write"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set write rate failed\n"))
		} else {
			msg := s.ec.SetWriteRate(val)
			w.Write([]byte(fmt.Sprintf("Set write rate to %v successfully\n", msg)))
		}
	}
}

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) handleError(op, msg string) {
	log.LogError(msg)
	ump.Alarm(s.umpKey(op), msg)
}
