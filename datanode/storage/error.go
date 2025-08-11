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
	ExtentHasBeenDeletedError        = errors.New("extent has been deleted")
	ParameterMismatchError           = errors.New("parameter mismatch error")
	NoAvailableExtentError           = errors.New("no available extent")
	NoBrokenExtentError              = errors.New("no unavailable extent")
	NoSpaceError                     = errors.New("no space left on the device")
	ForbiddenDataPartitionError      = errors.New("the data partition is forbidden")
	ForbiddenMetaPartitionError      = errors.New("meta partition is forbidden")
	TryAgainError                    = errors.New("try again")
	LimitedIoError                   = errors.New("limited io error")
	TinyRecoverError                 = errors.New("tiny extent recovering error")
	DpDecommissionRepairError        = errors.New("data partition decommission repairing error")
	DpRepairError                    = errors.New("data partition is repairing error")
	CrcMismatchError                 = errors.New("packet Crc is incorrect")
	NoLeaderError                    = errors.New("no raft leader")
	ExtentNotFoundError              = errors.New("extent does not exist")
	ExtentExistsError                = errors.New("extent already exists")
	ExtentIsFullError                = errors.New("extent is full")
	BrokenExtentError                = errors.New("extent has been broken")
	BrokenDiskError                  = errors.New("disk has broken")
	ForbidWriteError                 = errors.New("single replica decommission forbid write")
	VerNotConsistentError            = errors.New("ver not consistent")
	SnapshotNeedNewExtentError       = errors.New("snapshot need new extent error")
	NoDiskReadRepairExtentTokenError = errors.New("no disk read repair extent token")
	ReachMaxExtentsCountError        = errors.New("reached max extents count")
	ClusterForbidWriteOpOfProtoVer   = errors.New("cluster forbid write operate of packet protocol version")
	VolForbidWriteOpOfProtoVer       = errors.New("vol forbid write operate of packet protocol version")
)

func newParameterError(format string, a ...interface{}) error {
	return fmt.Errorf("parameter mismatch error: %s", fmt.Sprintf(format, a...))
}
