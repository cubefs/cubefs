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

package storage

import (
	"errors"
	"fmt"
)

var (
	ErrExtentHasBeenDeleted           = errors.New("extent has been deleted")
	ErrParameterMismatch              = errors.New("parameter mismatch error")
	ErrNoAvailableExtent              = errors.New("no available extent")
	ErrNoBrokenExtent                 = errors.New("no unavailable extent")
	ErrNoSpace                        = errors.New("no space left on the device")
	ErrForbiddenDataPartition         = errors.New("the data partition is forbidden")
	ErrForbiddenMetaPartition         = errors.New("meta partition is forbidden")
	ErrTryAgain                       = errors.New("try again")
	ErrLimitedIo                      = errors.New("limited io error")
	ErrTinyRecover                    = errors.New("tiny extent recovering error")
	ErrDpDecommissionRepair           = errors.New("data partition decommission repairing error")
	ErrDpRepair                       = errors.New("data partition is repairing error")
	ErrCrcMismatch                    = errors.New("packet Crc is incorrect")
	ErrNoLeader                       = errors.New("no raft leader")
	ErrExtentNotFound                 = errors.New("extent does not exist")
	ErrExtentExists                   = errors.New("extent already exists")
	ErrExtentIsFull                   = errors.New("extent is full")
	ErrBrokenExtent                   = errors.New("extent has been broken")
	ErrBrokenDisk                     = errors.New("disk has broken")
	ErrForbidWrite                    = errors.New("single replica decommission forbid write")
	ErrVerNotConsistent               = errors.New("ver not consistent")
	ErrSnapshotNeedNewExtent          = errors.New("snapshot need new extent error")
	ErrNoDiskReadRepairExtentToken    = errors.New("no disk read repair extent token")
	ErrReachMaxExtentsCount           = errors.New("reached max extents count")
	ErrClusterForbidWriteOpOfProtoVer = errors.New("cluster forbid write operate of packet protocol version")
	ErrVolForbidWriteOpOfProtoVer     = errors.New("vol forbid write operate of packet protocol version")
)

func newParameterError(format string, a ...interface{}) error {
	return fmt.Errorf("parameter mismatch error: "+format, a...)
}
