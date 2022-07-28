// Copyright 2018 The Chubao Authors.
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
	"fmt"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/proto"
	authSDK "github.com/cubefs/cubefs/sdk/auth"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auth"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	HostsSeparator                = ","
	RefreshMetaPartitionsInterval = time.Minute * 5
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
	statusConflictExtents
)

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5

	/*
	 * Minimum interval of forceUpdateMetaPartitions in seconds,
	 * i.e. only one force update request is allowed every 5 sec.
	 */
	MinForceUpdateMetaPartitionsInterval = 5
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
}

type MetaWrapper struct {
	sync.RWMutex
	cluster         string
	localIP         string
	volname         string
	ossSecure       *OSSSecure
	volCreateTime   int64
	owner           string
	ownerValidation bool
	mc              *masterSDK.MasterClient
	ac              *authSDK.AuthClient
	conns           *util.ConnectPool

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
	forceUpdate      chan struct{}
	forceUpdateLimit *rate.Limiter
	EnableSummary    bool
	metaSendTimeout  int64
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
	mw := new(MetaWrapper)
	mw.closeCh = make(chan struct{}, 1)

	if config.Authenticate {
		var ticketMess = config.TicketMess
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

	limit := MaxMountRetryLimit

	for limit > 0 {
		err = mw.initMetaWrapper()
		// When initializing the volume, if the master explicitly responds that the specified
		// volume does not exist, it will not retry.
		if err != nil {
			log.LogErrorf("initMetaWrapper failed, err %s", err.Error())
		}

		if err == proto.ErrVolNotExists {
			return nil, err
		}
		if err != nil {
			limit--
			time.Sleep(MountRetryInterval)
			continue
		}
		break
	}

	if limit <= 0 && err != nil {
		return nil, err
	}

	go mw.refresh()
	return mw, nil
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

func (mw *MetaWrapper) Close() error {
	mw.closeOnce.Do(func() {
		close(mw.closeCh)
		mw.conns.Close()
	})
	return nil
}

func (mw *MetaWrapper) Cluster() string {
	return mw.cluster
}

func (mw *MetaWrapper) LocalIP() string {
	return mw.localIP
}

func (mw *MetaWrapper) exporterKey(act string) string {
	return fmt.Sprintf("%s_sdk_meta_%s", mw.cluster, act)
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
		status = statusConflictExtents
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
	case statusConflictExtents:
		return syscall.ENOTSUP
	default:
	}
	return syscall.EIO
}
