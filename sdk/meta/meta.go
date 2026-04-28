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
	"net"
	"runtime/debug"
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
	RefreshHostLatencyInterval    = time.Second * 30
	DefaultMetaNodeTimeoutCount   = 3   // Default timeout count threshold for meta nodes
	DefaultMetaNodeTimeoutMs      = 100 // Default timeout threshold in milliseconds for resetting timeout count
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
	statusLimitedIo
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
	MetaNearRead         bool
	RegionNearRead       bool
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
	NearRead            bool
	NearReadClientCfg   bool
	RegionNearRead      bool
	dirtyInodes         *dirtyInodeCache

	HostPingStats    sync.Map // [string]*util.AddressPingStats - host address to ping stats mapping
	HostLatency      sync.Map // [string]time.Duration - host address to average latency mapping
	HostTimeoutCount sync.Map // [string]int32 - host address to timeout count mapping

	RemoteCacheBloom func() *bloom.BloomFilter

	// Client specified pool ID for new inodes (0 means use volume default)
	clientPoolId  uint8
	defaultPoolId uint8

	// Client specified meta region for creating inodes (empty means use volume default region)
	clientMetaRegionCfg string
	defaultMetaRegion   string
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

	// Get master client for pool cache access
	mw.metaSendTimeout = config.MetaSendTimeout
	mw.conns = util.NewConnectPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.partCond = sync.NewCond(&mw.partMutex)
	mw.forceUpdate = make(chan struct{}, 1)
	mw.forceUpdateLimit = rate.NewLimiter(1, MinForceUpdateMetaPartitionsInterval)
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
	mw.NearReadClientCfg = config.MetaNearRead
	mw.RegionNearRead = config.RegionNearRead
	mw.dirtyInodes = newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache)

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

func (mw *MetaWrapper) GetClientPoolId() uint8 {
	return mw.defaultPoolId
}

// NewMetaWrapperForDefaultPool returns a MetaWrapper with only defaultPoolId initialized.
// It is for unit tests (e.g. data stream) that need GetClientPoolId without a full mount.
func NewMetaWrapperForDefaultPool(defaultPoolID uint8) *MetaWrapper {
	return &MetaWrapper{defaultPoolId: defaultPoolID}
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

func (mw *MetaWrapper) GetSubDir() string {
	return mw.subDir
}

func (mw *MetaWrapper) DirCacheLen() int {
	return len(mw.dirCache)
}

func (mw *MetaWrapper) enableTx(mask proto.TxOpMask) bool {
	return mw.EnableTransaction != proto.TxPause && mw.EnableTransaction&mask > 0
}

// nearReadEnabled reports whether meta near-read is active (follower read + near read both on).
func (mw *MetaWrapper) nearReadEnabled(mp *MetaPartition) bool {
	if mp == nil {
		log.LogWarnf("nearReadEnabled: mp is nil, stack %v", string(debug.Stack()))
		return false
	}

	if mw.defaultMetaRegion == mp.Region {
		return mw.FollowerRead && mw.NearRead
	}

	return mw.FollowerRead && mw.NearRead || mw.RegionNearRead
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

// GetMasterClient returns the master client for accessing master APIs
func (mw *MetaWrapper) GetMasterClient() *masterSDK.MasterClient {
	return mw.mc
}

// updateHostLatency updates the ping latency information for meta hosts
func (mw *MetaWrapper) updateHostLatency() {
	log.LogInfof("action[updateHostLatency] start")
	defer log.LogInfof("action[updateHostLatency] end")

	hosts, err := mw.getMetaHostsMap()
	if err != nil {
		log.LogWarnf("action[updateHostLatency] failed to get cluster meta nodes, err(%v)", err)
		return
	}
	needPings := make([]string, 0)

	// Remove hosts that are no longer available
	mw.HostLatency.Range(func(key, value interface{}) bool {
		host := key.(string)
		if _, exist := hosts[host]; !exist {
			mw.HostLatency.Delete(host)
			mw.HostTimeoutCount.Delete(host)
			log.LogInfof("action[updateHostLatency] HostLatency remove metaNode(%v)", host)
		} else {
			needPings = append(needPings, host)
		}
		return true
	})

	// Add new hosts
	for host := range hosts {
		if _, exist := mw.HostLatency.Load(host); !exist {
			needPings = append(needPings, host)
		}
	}

	mw.pingHosts(needPings)
}

// getMetaHostsMap returns a map of all meta hosts
func (mw *MetaWrapper) getMetaHostsMap() (map[string]bool, error) {
	// Get all meta nodes from master API
	nodes, err := mw.mc.AdminAPI().GetClusterMetaNodes()
	if err != nil {
		return make(map[string]bool), err
	}

	allHosts := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		if node.Addr != "" && node.Status {
			allHosts[node.Addr] = true
		}
	}

	return allHosts, nil
}

// HeartBeat sends a lightweight ping packet to the specified host and measures the latency
func (mw *MetaWrapper) HeartBeat(addr string) (duration time.Duration, err error) {
	var conn *net.TCPConn
	packet := proto.NewPacket()
	packet.Opcode = proto.OpPing

	defer func() {
		mw.conns.PutConnect(conn, err != nil)
	}()

	if conn, err = mw.conns.GetConnect(addr); err != nil {
		log.LogWarnf("action[HeartBeat] get connection to addr failed, addr(%v) err(%v)", addr, err)
		return
	}
	start := time.Now()
	if err = packet.WriteToConn(conn); err != nil {
		log.LogWarnf("action[HeartBeat] failed write to addr(%v) err(%v)", addr, err)
		return
	}
	if err = packet.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
		log.LogWarnf("action[HeartBeat] failed to ReadFromConn addr(%v) err(%v)", addr, err)
		return
	}

	duration = time.Since(start)
	log.LogDebugf("action[HeartBeat] from addr(%v) cost(%v)", addr, duration)
	return
}

// pingHosts pings the given hosts and updates their latency information
func (mw *MetaWrapper) pingHosts(hosts []string) {
	for _, host := range hosts {
		rtt, err := mw.HeartBeat(host)
		if err != nil {
			mw.addTimeoutCount(host)
			log.LogWarnf("action[pingHosts] host(%v) err(%v)", host, err)
			continue
		}

		// Add measurement to ping statistics
		v, _ := mw.HostPingStats.LoadOrStore(host, &util.AddressPingStats{})
		aps := v.(*util.AddressPingStats)
		aps.Add(rtt)

		// Calculate average and store in latency map
		avgLatency := aps.Average()
		mw.HostLatency.Store(host, avgLatency)

		// Reset timeout count if ping was successful and latency is within acceptable range
		if timeoutCount, exists := mw.HostTimeoutCount.Load(host); exists {
			count := timeoutCount.(int32)
			if count > 0 && avgLatency > 0 && avgLatency < time.Millisecond*time.Duration(DefaultMetaNodeTimeoutMs) {
				mw.HostTimeoutCount.Store(host, int32(0))
				log.LogDebugf("action[pingHosts] host(%v) timeoutCount reset from %v to 0 after successful ping, avgLatency(%v)",
					host, count, avgLatency)
			}
		}

		log.LogDebugf("action[pingHosts] host(%v) rtt(%v) avgLatency(%v)", host, rtt.String(), avgLatency)
	}
}

func (mw *MetaWrapper) addTimeoutCount(host string) {
	// Increment timeout count on ping failure
	// Do not update HostLatency to 0, keep the historical average for reset logic
	v, _ := mw.HostTimeoutCount.LoadOrStore(host, int32(0))
	count := v.(int32)
	count++

	// delete HostLatency if timeout count exceeds threshold
	if count >= DefaultMetaNodeTimeoutCount {
		mw.HostLatency.Delete(host)
		log.LogWarnf("action[addTimeoutCount] host(%v) timeoutCount(%v) exceeded threshold, removed from HostLatency", host, count)
	} else {
		mw.HostTimeoutCount.Store(host, count)
		log.LogWarnf("action[addTimeoutCount] host(%v) timeoutCount(%v)", host, count)
	}
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
	case proto.OpLimitedIoErr:
		status = statusLimitedIo
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
	case statusLimitedIo:
		return errors.New("operation rate limited")
	default:
	}
	return syscall.EIO
}
