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
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/chubaofs/chubaofs/proto"
	authSDK "github.com/chubaofs/chubaofs/sdk/auth"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/auth"
	"github.com/chubaofs/chubaofs/util/btree"
	"github.com/chubaofs/chubaofs/util/connpool"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	HostsSeparator                = ","
	RefreshMetaPartitionsInterval = time.Minute * 1
	IdleConnTimeoutMeta           = 30  //seconds
	ConnectTimeoutMetaMs          = 500 //milliseconds
	ReadTimeoutMeta               = 3   //seconds
	WriteTimeoutMeta              = 3   //seconds
)

const (
	statusUnknown int = iota
	statusOK
	statusExist
	statusNoent
	statusFull
	statusAgain
	statusError
	statusInval
	statusNotPerm
	statusOutOfRange
)

const (
	StatusOK    = statusOK
	StatusExist = statusExist
	StatusNoent = statusNoent
)

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5

	/*
	 * Minimum interval of forceUpdateMetaPartitions in seconds,
	 * i.e. only one force update request is allowed every 5 sec.
	 */
	MinForceUpdateMetaPartitionsInterval = 5

	defaultOpLimitBurst = 128

	VolNotExistInterceptThresholdMin = 60 * 24
	VolNotExistClearViewThresholdMin = 0

	MasterNoCacheAPIRetryTimeout = 5 * time.Minute
)

type AsyncTaskErrorFunc func(err error)

func (f AsyncTaskErrorFunc) OnError(err error) {
	if f != nil {
		f(err)
	}
}

type MetaConfig struct {
	Modulename       string
	Volume           string
	Owner            string
	Masters          []string
	Authenticate     bool
	TicketMess       auth.TicketMess
	ValidateOwner    bool
	OnAsyncTaskError AsyncTaskErrorFunc
	InfiniteRetry    bool
}

type MetaWrapper struct {
	sync.RWMutex
	cluster         string
	modulename      string
	localIP         string
	volname         string
	ossSecure       *OSSSecure
	volCreateTime   int64
	owner           string
	ownerValidation bool
	ossBucketPolicy proto.BucketAccessPolicy
	mc              *masterSDK.MasterClient
	ac              *authSDK.AuthClient
	conns           *connpool.ConnectPool
	connConfig      *proto.ConnConfig

	volNotExistCount  int32
	crossRegionHAType proto.CrossRegionHAType

	// Callback handler for handling asynchronous task errors.
	onAsyncTaskError AsyncTaskErrorFunc

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	rwPartitions      []*MetaPartition
	unavailPartitions []*MetaPartition
	epoch             uint64

	totalSize uint64
	usedSize  uint64

	authenticate bool
	Ticket       auth.Ticket
	accessToken  proto.APIAccessReq
	sessionKey   string
	ticketMess   auth.TicketMess

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// Used to signal the go routines which are waiting for partition view update
	partMutex sync.Mutex
	partCond  *sync.Cond

	// Used to trigger and throttle instant partition updates
	forceUpdate      chan struct{}
	forceUpdateLimit *rate.Limiter
	// meta op limit rate
	opLimiter     map[uint8]*rate.Limiter // key: op
	actionLimiter map[proto.Action]*rate.Limiter
	limitMapMutex sync.RWMutex
	// infinite retry send to mp
	InfiniteRetry bool
}

type MetaState struct {
	Ticket_Ticket     string
	Ticket_SessionKey string
	Cluster           string
	LocalIP           string
	TotalSize         uint64
	UsedSize          uint64
	VolNotExistCount  int32
	View              *proto.VolView
}

//the ticket from authnode
type Ticket struct {
	ID         string `json:"client_id"`
	SessionKey string `json:"session_key"`
	ServiceID  string `json:"service_id"`
	Ticket     string `json:"ticket"`
}

func NewMetaWrapper(config *MetaConfig) (*MetaWrapper, error) {
	var err error
	var ticket *auth.Ticket
	mw := new(MetaWrapper)
	mw.closeCh = make(chan struct{}, 1)

	if config.Authenticate {
		var ticketMess = config.TicketMess
		mw.ac = authSDK.NewAuthClient(ticketMess.TicketHosts, ticketMess.EnableHTTPS, ticketMess.CertFile)
		for i := 0; i < MaxMountRetryLimit; i++ {
			ticket, err = mw.ac.API().GetTicket(config.Owner, ticketMess.ClientKey, proto.MasterServiceID)
			if err == nil {
				break
			}
			time.Sleep(MountRetryInterval)
		}
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

	mw.modulename = config.Modulename
	mw.volname = config.Volume
	mw.owner = config.Owner
	mw.ownerValidation = config.ValidateOwner
	mw.mc = masterSDK.NewMasterClient(config.Masters, false)
	mw.onAsyncTaskError = config.OnAsyncTaskError
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.partCond = sync.NewCond(&mw.partMutex)
	mw.forceUpdate = make(chan struct{}, 1)
	mw.forceUpdateLimit = rate.NewLimiter(1, MinForceUpdateMetaPartitionsInterval)
	mw.InfiniteRetry = config.InfiniteRetry
	mw.opLimiter = make(map[uint8]*rate.Limiter)
	mw.actionLimiter = make(map[proto.Action]*rate.Limiter)
	mw.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutMeta,
		ConnectTimeoutNs: ConnectTimeoutMetaMs * int64(time.Millisecond),
		WriteTimeoutNs:   WriteTimeoutMeta * int64(time.Second),
		ReadTimeoutNs:    ReadTimeoutMeta * int64(time.Second),
	}

	limit := MaxMountRetryLimit

	for limit > 0 {
		err = mw.initMetaWrapper()
		// When initializing the volume, if the master explicitly responds that the specified
		// volume does not exist, it will not retry.
		if err == proto.ErrVolNotExists {
			return nil, err
		}
		if err != nil {
			limit--
			if limit == 0 {
				return nil, err
			}
			time.Sleep(MountRetryInterval)
			continue
		}
		break
	}
	mw.conns = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, mw.connConfig.IdleTimeoutSec, mw.connConfig.ConnectTimeoutNs)

	mw.wg.Add(2)
	go mw.refresh()

	go mw.startUpdateLimiterConfig()

	return mw, nil
}

func RebuildMetaWrapper(config *MetaConfig, metaState *MetaState) *MetaWrapper {
	mw := new(MetaWrapper)
	mw.closeCh = make(chan struct{}, 1)

	if config.Authenticate {
		var ticketMess = config.TicketMess
		mw.ac = authSDK.NewAuthClient(ticketMess.TicketHosts, ticketMess.EnableHTTPS, ticketMess.CertFile)
		mw.authenticate = config.Authenticate
		mw.accessToken.Ticket = metaState.Ticket_Ticket
		mw.accessToken.ClientID = config.Owner
		mw.accessToken.ServiceID = proto.MasterServiceID
		mw.sessionKey = metaState.Ticket_SessionKey
		mw.ticketMess = ticketMess
	}

	mw.modulename = config.Modulename
	mw.volname = config.Volume
	mw.owner = config.Owner
	mw.ownerValidation = config.ValidateOwner
	mw.mc = masterSDK.NewMasterClient(config.Masters, false)
	mw.onAsyncTaskError = config.OnAsyncTaskError
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.partCond = sync.NewCond(&mw.partMutex)
	mw.forceUpdate = make(chan struct{}, 1)
	mw.forceUpdateLimit = rate.NewLimiter(1, MinForceUpdateMetaPartitionsInterval)
	mw.InfiniteRetry = config.InfiniteRetry
	mw.opLimiter = make(map[uint8]*rate.Limiter)
	mw.actionLimiter = make(map[proto.Action]*rate.Limiter)
	mw.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutMeta,
		ConnectTimeoutNs: ConnectTimeoutMetaMs * int64(time.Millisecond),
		WriteTimeoutNs:   WriteTimeoutMeta * int64(time.Second),
		ReadTimeoutNs:    ReadTimeoutMeta * int64(time.Second),
	}

	mw.cluster = metaState.Cluster
	mw.localIP = metaState.LocalIP
	atomic.StoreUint64(&mw.totalSize, metaState.TotalSize)
	atomic.StoreUint64(&mw.usedSize, metaState.UsedSize)
	mw.volNotExistCount = metaState.VolNotExistCount
	if !mw.VolNotExists() {
		view := mw.convertVolumeView(metaState.View)
		rwPartitions := make([]*MetaPartition, 0)
		for _, mp := range view.MetaPartitions {
			mw.replaceOrInsertPartition(mp)
			log.LogInfof("updateMetaPartition: mp(%v)", mp)
			if mp.Status == proto.ReadWrite {
				rwPartitions = append(rwPartitions, mp)
			}
		}
		mw.ossSecure = view.OSSSecure
		mw.ossBucketPolicy = view.OSSBucketPolicy
		mw.volCreateTime = view.CreateTime
		mw.crossRegionHAType = view.CrossRegionHAType
		mw.updateConnConfig(view.ConnConfig)

		if len(rwPartitions) > 0 {
			mw.Lock()
			mw.rwPartitions = rwPartitions
			mw.Unlock()
		}
	}

	mw.conns = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, mw.connConfig.IdleTimeoutSec, mw.connConfig.ConnectTimeoutNs)

	mw.wg.Add(2)
	go mw.refresh()
	go mw.startUpdateLimiterConfig()
	return mw
}

func (mw *MetaWrapper) SaveMetaState() *MetaState {
	metaState := new(MetaState)
	metaState.Ticket_Ticket = mw.Ticket.Ticket
	metaState.Ticket_SessionKey = mw.Ticket.SessionKey
	metaState.Cluster = mw.cluster
	metaState.LocalIP = mw.localIP
	metaState.TotalSize = mw.totalSize
	metaState.UsedSize = mw.usedSize
	metaState.VolNotExistCount = mw.volNotExistCount
	metaState.View = mw.saveVolView()
	return metaState
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

	return nil
}

func (mw *MetaWrapper) Owner() string {
	return mw.owner
}

func (mw *MetaWrapper) OSSSecure() (accessKey, secretKey string) {
	return mw.ossSecure.AccessKey, mw.ossSecure.SecretKey
}

func (mw *MetaWrapper) VolCreateTime() int64 {
	return mw.volCreateTime
}

func (mw *MetaWrapper) OSSBucketPolicy() proto.BucketAccessPolicy {
	return mw.ossBucketPolicy
}

func (mw *MetaWrapper) Close() error {
	mw.closeOnce.Do(func() {
		close(mw.closeCh)
		mw.wg.Wait()
		mw.conns.Close()
	})
	return nil
}

func (mw *MetaWrapper) Cluster() string {
	return mw.cluster
}

//func (mw *MetaWrapper) WorkerAddr() string {
//	return mw.localIP
//}

func (mw *MetaWrapper) startUpdateLimiterConfig() {
	defer mw.wg.Done()
	for {
		err := mw.startUpdateLimiterConfigWithRecover()
		if err == nil {
			break
		}
		log.LogErrorf("refreshMetaLimitInfo: err(%v) try next update", err)
	}
}

func (mw *MetaWrapper) startUpdateLimiterConfigWithRecover() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("refreshMetaLimitInfo panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("refreshMetaLimitInfo panic: err(%v)", r)
			handleUmpAlarm(mw.cluster, mw.volname, "refreshMetaLimitInfo", msg)
			err = errors.New(msg)
		}
	}()

	updateConfigTicket := time.Second * 120
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-mw.closeCh:
			return
		case <-timer.C:
			mw.updateLimiterConfig()
			timer.Reset(updateConfigTicket)
		}
	}
}

func (mw *MetaWrapper) updateLimiterConfig() {
	limitInfo, err := mw.mc.AdminAPI().GetLimitInfo(mw.volname)
	if err != nil {
		log.LogWarnf("meta: updateLimiterConfig err(%s)", err.Error())
		return
	}
	mw.limitMapMutex.Lock()
	defer mw.limitMapMutex.Unlock()
	// delete op which not stored on master
	for op, _ := range mw.opLimiter {
		if _, exist := limitInfo.ClientVolOpRateLimit[op]; !exist {
			delete(mw.opLimiter, op)
		}
	}
	for op, val := range limitInfo.ClientVolOpRateLimit {
		if val < 0 {
			delete(mw.opLimiter, op)
			continue
		}
		if opLimit, ok := mw.opLimiter[op]; ok {
			opLimit.SetLimit(rate.Limit(val))
			opLimit.SetBurst(int(val))
		} else {
			mw.opLimiter[op] = rate.NewLimiter(rate.Limit(val), int(val))
		}
	}
	// delete action which not stored on master
	for action := range mw.actionLimiter {
		if _, exist := limitInfo.ObjectNodeActionRateLimit[action.String()]; !exist {
			delete(mw.actionLimiter, action)
		}
	}
	for as, av := range limitInfo.ObjectNodeActionRateLimit {
		var action proto.Action
		if action = proto.ParseAction(as); action == proto.NoneAction {
			log.LogWarnf("updateLimiterConfig: invalid objectNode action, vol(%v) action(%v)", mw.volname, as)
			continue
		}
		if av < 0 {
			delete(mw.actionLimiter, action)
			continue
		}
		if opLimit, ok := mw.actionLimiter[action]; ok {
			opLimit.SetLimit(rate.Limit(av))
			opLimit.SetBurst(int(av))
		} else {
			mw.actionLimiter[action] = rate.NewLimiter(rate.Limit(av), int(av))
		}
	}
	log.LogInfof("updateLimiterConfig: vol(%v) opLimiter(%v)", mw.volname, mw.opLimiter)
}

func (mw *MetaWrapper) checkLimiter(ctx context.Context, opCode uint8) error {
	limiter := mw.getOpLimiter(opCode)
	if limiter != nil {
		log.LogDebugf("check limiter begin: op(%v) limit(%v) burst(%v)", opCode, limiter.Limit(), limiter.Burst())
		if limiter.Burst() == 0 {
			return syscall.EPERM
		}
		if ctx == nil {
			ctx = context.Background()
		}
		limitErr := limiter.Wait(ctx)
		log.LogDebugf("check limiter end: op(%v) limit(%v) burst(%v) err(%v)", opCode, limiter.Limit(), limiter.Burst(), limitErr)
	}
	return nil
}

func (mw *MetaWrapper) CheckActionLimiter(ctx context.Context, action proto.Action) error {
	limiter := mw.getActionLimiter(action)
	if limiter != nil {
		log.LogDebugf("check action limiter begin: vol(%v), action(%v), limit(%v), burst(%v)", mw.volname, action.String(), limiter.Limit(), limiter.Burst())
		if limiter.Burst() == 0 {
			return syscall.EPERM
		}
		limitErr := limiter.Wait(ctx)
		log.LogDebugf("check action limiter end: vol(%v), action(%v), limit(%v), burst(%v), err(%v)", mw.volname, action.String(), limiter.Limit(), limiter.Burst(), limitErr)
	}
	return nil
}

func (mw *MetaWrapper) getOpLimiter(op uint8) (limiter *rate.Limiter) {
	mw.limitMapMutex.RLock()
	limiter = mw.opLimiter[op]
	mw.limitMapMutex.RUnlock()
	return
}

func (mw *MetaWrapper) getActionLimiter(action proto.Action) (limiter *rate.Limiter) {
	mw.limitMapMutex.RLock()
	limiter = mw.actionLimiter[action]
	mw.limitMapMutex.RUnlock()
	return
}

func (mw *MetaWrapper) GetOpLimitRate() string {
	res := ""
	p := proto.NewPacket(context.Background())
	mw.limitMapMutex.RLock()
	for op, limiter := range mw.opLimiter {
		var limit string
		val := limiter.Limit()
		burst := limiter.Burst()
		if val == 0 {
			limit = "disable"
		} else if val > 0 {
			limit = fmt.Sprintf("%v/s", val)
		} else {
			limit = "unLimit"
		}
		p.Opcode = op
		res = fmt.Sprintf("%vop: %v, limit: %v, burst: %v\n", res, p.GetOpMsg(), limit, burst)
	}
	mw.limitMapMutex.RUnlock()
	return res
}

func (mw *MetaWrapper) CrossRegionHATypeQuorum() bool {
	return mw.crossRegionHAType == proto.CrossRegionHATypeQuorum
}

func (mw *MetaWrapper) updateConnConfig(config *proto.ConnConfig) {
	if config == nil {
		return
	}
	log.LogInfof("updateConnConfig: (%v)", config)
	updateConnPool := false
	if config.IdleTimeoutSec > 0 && config.IdleTimeoutSec != mw.connConfig.IdleTimeoutSec {
		mw.connConfig.IdleTimeoutSec = config.IdleTimeoutSec
		updateConnPool = true
	}
	if config.ConnectTimeoutNs > 0 && config.ConnectTimeoutNs != mw.connConfig.ConnectTimeoutNs {
		mw.connConfig.ConnectTimeoutNs = config.ConnectTimeoutNs
		updateConnPool = true
	}
	if config.WriteTimeoutNs > 0 && config.WriteTimeoutNs != mw.connConfig.WriteTimeoutNs {
		atomic.StoreInt64(&mw.connConfig.WriteTimeoutNs, config.WriteTimeoutNs)
	}
	if config.ReadTimeoutNs > 0 && config.ReadTimeoutNs != mw.connConfig.ReadTimeoutNs {
		atomic.StoreInt64(&mw.connConfig.ReadTimeoutNs, config.ReadTimeoutNs)
	}
	if updateConnPool && mw.conns != nil {
		mw.conns.UpdateTimeout(mw.connConfig.IdleTimeoutSec, mw.connConfig.ConnectTimeoutNs)
	}
}

func (mw *MetaWrapper) VolNotExists() bool {
	if mw.volNotExistCount > VolNotExistInterceptThresholdMin {
		log.LogWarnf("VolNotExists: vol(%v) count(%v) threshold(%v)", mw.volname, mw.volNotExistCount, VolNotExistInterceptThresholdMin)
		return true
	}
	return false
}

//func (mw *MetaWrapper) exporterKey(act string) string {
//	return fmt.Sprintf("%s_sdk_meta_%s", mw.cluster, act)
//}

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
	case proto.OpInodeOutOfRange:
		status = statusOutOfRange
	default:
		status = statusError
	}
	return
}

func statusToErrno(status int) error {
	switch status {
	case statusOK:
		// return error anyway
		return syscall.EAGAIN
	case statusExist:
		return syscall.EEXIST
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
	default:
	}
	return syscall.EIO
}
