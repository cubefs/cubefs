// Copyright 2018 The CubeFS Authors.
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

package meta

import (
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/proto"
	authSDK "github.com/cubefs/cubefs/sdk/auth"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auth"
	"github.com/cubefs/cubefs/util/bloom"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	HostsSeparator                = ","
	RefreshMetaPartitionsInterval = time.Minute * 5
)

const (
	_ int = iota
	statusOK
	statusExist
	statusNoent
	statusFull
	statusAgain
	statusError
	statusInval
	statusNotPerm
	StatusConflictExtents
	statusOpDirQuota
	statusNoSpace
	statusForbid
	statusTxInodeInfoNotExist
	statusTxConflict
	statusTxTimeout
	statusUploadPartConflict
	statusNotEmpty
	statusLeaseOccupiedByOthers
	statusLeaseGenerationNotMatch
)

const (
	MaxMountRetryLimit = 6
	MountRetryInterval = time.Second * 5

	/*
	 * Minimum interval of forceUpdateMetaPartitions in seconds,
	 * i.e. only one force update request is allowed every 5 sec.
	 */
	MinForceUpdateMetaPartitionsInterval = 5
	DefaultQuotaExpiration               = 120 * time.Second
	MaxQuotaCache                        = 10000
)

type AsyncTaskErrorFunc func(err error)

func (f AsyncTaskErrorFunc) OnError(err error) {
	if f != nil {
		f(err)
	}
}

type MetaConfig struct {
	Volume           string
	Owner            string
	Masters          []string
	Authenticate     bool
	TicketMess       auth.TicketMess
	ValidateOwner    bool
	OnAsyncTaskError AsyncTaskErrorFunc
	EnableSummary    bool
	MetaSendTimeout  int64
	// EnableTransaction uint8
	// EnableTransaction bool
	MountPoint                 string
	SubDir                     string
	TrashTraverseLimit         int
	TrashRebuildGoroutineLimit int

	VerReadSeq           uint64
	InnerReq             bool
	DisableTrashByClient bool
}

type MetaWrapper struct {
	sync.RWMutex
	cluster           string
	localIP           string
	volname           string
	ossSecure         *OSSSecure
	volCreateTime     int64
	volDeleteLockTime int64
	owner             string
	ownerValidation   bool
	mc                *masterSDK.MasterClient
	ac                *authSDK.AuthClient
	conns             *util.ConnectPool

	// Callback handler for handling asynchronous task errors.
	onAsyncTaskError AsyncTaskErrorFunc

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	rwPartitions []*MetaPartition
	epoch        uint64

	totalSize  uint64
	usedSize   uint64
	inodeCount uint64

	authenticate bool
	Ticket       auth.Ticket
	accessToken  proto.APIAccessReq
	sessionKey   string
	ticketMess   auth.TicketMess

	closeCh   chan struct{}
	closeOnce sync.Once

	// Allocated to signal the go routines which are waiting for partition view update
	partMutex sync.Mutex
	partCond  *sync.Cond

	// Allocated to trigger and throttle instant partition updates
	forceUpdate             chan struct{}
	forceUpdateLimit        *rate.Limiter
	singleflight            singleflight.Group
	EnableSummary           bool
	metaSendTimeout         int64
	leaderRetryTimeout      int64 // s
	DirChildrenNumLimit     uint32
	EnableTransaction       proto.TxOpMask
	TxTimeout               int64
	TxConflictRetryNum      int64
	TxConflictRetryInterval int64
	EnableQuota             bool
	QuotaInfoMap            map[uint32]*proto.QuotaInfo
	QuotaLock               sync.RWMutex

	// uniqidRange for request dedup
	uniqidRangeMap   map[uint64]*uniqidRange
	uniqidRangeMutex sync.Mutex

	qc *QuotaCache
	// trash
	TrashInterval int64
	trashPolicy   *Trash
	disableTrash  bool
	rootIno       uint64
	dirCache      map[uint64]dirInfoCache
	inoInfoLk     sync.RWMutex
	subDir        string

	disableTrashByClient bool

	VerReadSeq          uint64
	LastVerSeq          uint64
	Client              wrapper.SimpleClientInfo
	IsSnapshotEnabled   bool
	DefaultStorageClass uint32
	InnerReq            bool
	FollowerRead        bool

	RemoteCacheBloom func() *bloom.BloomFilter
}

type uniqidRange struct {
	cur uint64
	end uint64
}

type dirInfoCache struct {
	ino       uint64
	parentIno uint64
	name      string
}

// the ticket from authnode
type Ticket struct {
	ID         string `json:"client_id"`
	SessionKey string `json:"session_key"`
	ServiceID  string `json:"service_id"`
	Ticket     string `json:"ticket"`
}

func NewMetaWrapper(config *MetaConfig) (*MetaWrapper, error) {
	var err error
	mw := new(MetaWrapper)
	mw.closeCh = make(chan struct{}, 1)

	if config.Authenticate {
		ticketMess := config.TicketMess
		mw.ac = authSDK.NewAuthClient(ticketMess.TicketHosts, ticketMess.EnableHTTPS, ticketMess.CertFile)
		ticket, err := mw.ac.API().GetTicket(config.Owner, ticketMess.ClientKey, proto.MasterServiceID)
		if err != nil {
			return nil, errors.Trace(err, "Get ticket from authnode failed!")
		}
		mw.authenticate = config.Authenticate
		mw.accessToken.Ticket = ticket.Ticket
		mw.accessToken.ClientID = config.Owner
		mw.accessToken.ServiceID = proto.MasterServiceID
		mw.sessionKey = ticket.SessionKey
		mw.ticketMess = ticketMess
	}

	mw.volname = config.Volume
	mw.owner = config.Owner
	mw.ownerValidation = config.ValidateOwner
	mw.mc = masterSDK.NewMasterClient(config.Masters, false)
	mw.onAsyncTaskError = config.OnAsyncTaskError
	mw.metaSendTimeout = config.MetaSendTimeout
	mw.conns = util.NewConnectPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.partCond = sync.NewCond(&mw.partMutex)
	mw.forceUpdate = make(chan struct{}, 1)
	mw.forceUpdateLimit = rate.NewLimiter(1, MinForceUpdateMetaPartitionsInterval)
	mw.EnableSummary = config.EnableSummary
	mw.DirChildrenNumLimit = proto.DefaultDirChildrenNumLimit
	mw.uniqidRangeMap = make(map[uint64]*uniqidRange)
	mw.qc = NewQuotaCache(DefaultQuotaExpiration, MaxQuotaCache)
	mw.VerReadSeq = config.VerReadSeq
	mw.dirCache = make(map[uint64]dirInfoCache)
	mw.subDir = config.SubDir
	limit := MaxMountRetryLimit
	mw.DefaultStorageClass = proto.StorageClass_Unspecified
	mw.InnerReq = config.InnerReq
	mw.disableTrashByClient = config.DisableTrashByClient

	for limit > 0 {
		err = mw.initMetaWrapper()
		// When initializing the volume, if the master explicitly responds that the specified
		// volume does not exist, it will not retry.
		if err != nil {
			if strings.Contains(err.Error(), "auth key do not match") {
				limit = 0
				break
			}
			log.LogErrorf("NewMetaWrapper: init meta wrapper failed: volume(%v) err(%v)", mw.volname, err)
		}
		if err == proto.ErrVolNotExists {
			return nil, err
		}
		if err != nil {
			limit--
			time.Sleep(MountRetryInterval * time.Duration(limit))
			continue
		}
		break
	}
	if !mw.disableTrashByClient {
		mw.enableTrash()
	}
	if limit <= 0 && err != nil {
		return nil, err
	}

	go mw.updateQuotaInfoTick()
	go mw.refresh()
	return mw, nil
}

func (mw *MetaWrapper) enableTrash() error {
	if mw.disableTrash {
		return errors.NewErrorf("trash is disabled")
	}
	if mw.TrashInterval > 0 {
		// default value for sdk
		trashTraverseLimit := 10
		trashRebuildGoroutineLimit := 10
		var err error
		mw.trashPolicy, err = NewTrash(mw, mw.TrashInterval, mw.subDir,
			trashTraverseLimit, trashRebuildGoroutineLimit)

		if err != nil {
			log.LogErrorf("action[initMetaWrapper] init trash failed, err %s", err.Error())
			return err
		} else {
			mw.trashPolicy.StartScheduleTask()
		}
	}
	return nil
}

func (mw *MetaWrapper) initMetaWrapper() (err error) {
	if err = mw.updateClusterInfo(); err != nil {
		return err
	}

	if err = mw.updateVolStatInfo(); err != nil {
		return err
	}

	if err = mw.updateMetaPartitions(); err != nil {
		return err
	}

	if err = mw.updateDirChildrenNumLimit(); err != nil {
		return err
	}

	return nil
}

func (mw *MetaWrapper) Owner() string {
	return mw.owner
}

func (mw *MetaWrapper) DirCacheLen() int {
	return len(mw.dirCache)
}

func (mw *MetaWrapper) enableTx(mask proto.TxOpMask) bool {
	return mw.EnableTransaction != proto.TxPause && mw.EnableTransaction&mask > 0
}

func (mw *MetaWrapper) OSSSecure() (accessKey, secretKey string) {
	return mw.ossSecure.AccessKey, mw.ossSecure.SecretKey
}

func (mw *MetaWrapper) VolCreateTime() int64 {
	return mw.volCreateTime
}

func (mw *MetaWrapper) Close() error {
	mw.closeOnce.Do(func() {
		close(mw.closeCh)
		mw.conns.Close()
		mw.qc.Close()
	})
	return nil
}

func (mw *MetaWrapper) Cluster() string {
	return mw.cluster
}

func (mw *MetaWrapper) LocalIP() string {
	return mw.localIP
}

// Proto ResultCode to status
func parseStatus(result uint8) (status int) {
	switch result {
	case proto.OpOk:
		status = statusOK
	case proto.OpExistErr:
		status = statusExist
	case proto.OpNotExistErr:
		status = statusNoent
	case proto.OpInodeFullErr:
		status = statusFull
	case proto.OpAgain:
		status = statusAgain
	case proto.OpArgMismatchErr:
		status = statusInval
	case proto.OpNotPerm:
		status = statusNotPerm
	case proto.OpConflictExtentsErr:
		status = StatusConflictExtents
	case proto.OpDirQuota:
		status = statusOpDirQuota
	case proto.OpNotEmpty:
		status = statusNotEmpty
	case proto.OpNoSpaceErr:
		status = statusNoSpace
	case proto.OpTxInodeInfoNotExistErr:
		status = statusTxInodeInfoNotExist
	case proto.OpTxConflictErr:
		status = statusTxConflict
	case proto.OpTxTimeoutErr:
		status = statusTxTimeout
	case proto.OpUploadPartConflictErr:
		status = statusUploadPartConflict
	case proto.OpForbidErr:
		status = statusForbid
	case proto.OpLeaseOccupiedByOthers:
		status = statusLeaseOccupiedByOthers
	case proto.OpLeaseGenerationNotMatch:
		status = statusLeaseGenerationNotMatch
	default:
		status = statusError
	}
	return
}

func statusErrToErrno(status int, err error) error {
	if status == statusOK && err != nil {
		return syscall.EAGAIN
	}

	return statusToErrno(status)
}

func statusToErrno(status int) error {
	switch status {
	case statusOK:
		// return error anyway
		return syscall.EAGAIN
	case statusExist:
		return syscall.EEXIST
	case statusNotEmpty:
		return syscall.ENOTEMPTY
	case statusNoent:
		return syscall.ENOENT
	case statusFull:
		return syscall.ENOMEM
	case statusAgain:
		return syscall.EAGAIN
	case statusInval:
		return syscall.EINVAL
	case statusNotPerm:
		return syscall.EPERM
	case statusError:
		return syscall.EAGAIN
	case StatusConflictExtents:
		return syscall.ENOTSUP
	case statusOpDirQuota:
		return syscall.EDQUOT
	case statusNoSpace:
		return syscall.ENOSPC
	case statusTxInodeInfoNotExist:
		return syscall.EAGAIN
	case statusTxConflict:
		return syscall.EAGAIN
	case statusTxTimeout:
		return syscall.EAGAIN
	case statusUploadPartConflict:
		return syscall.EEXIST
	case statusForbid:
		return syscall.EPERM
	case statusLeaseOccupiedByOthers:
		return errors.New("lease occupied by others")
	case statusLeaseGenerationNotMatch:
		return errors.New("lease generation not match")
	default:
	}
	return syscall.EIO
}
