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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/btree"
	"github.com/chubaofs/chubaofs/util/errors"
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
)

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5
)

type MetaWrapper struct {
	sync.RWMutex
	cluster string
	localIP string
	volname string
	owner   string
	master  util.MasterHelper
	conns   *util.ConnectPool

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	rwPartitions []*MetaPartition
	epoch        uint64

	totalSize uint64
	usedSize  uint64
}

func NewMetaWrapper(volname, owner, masterHosts string) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	mw.volname = volname
	mw.owner = owner
	master := strings.Split(masterHosts, HostsSeparator)
	mw.master = util.NewMasterHelper()
	for _, ip := range master {
		mw.master.AddNode(ip)
	}
	mw.conns = util.NewConnectPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.updateClusterInfo()
	mw.updateVolStatInfo()

	limit := MaxMountRetryLimit
retry:
	if err := mw.updateMetaPartitions(); err != nil {
		if limit <= 0 {
			return nil, errors.Trace(err, "Init meta wrapper failed!")
		} else {
			limit--
			time.Sleep(MountRetryInterval)
			goto retry
		}

	}

	go mw.refresh()
	return mw, nil
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
		return syscall.EPERM
	default:
	}
	return syscall.EIO
}
