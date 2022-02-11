// Copyright 2022 The CubeFS Authors.
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

package proto

import (
	"math"
)

type DiskStatus uint8

// disk status
const (
	DiskStatusNormal    = DiskStatus(iota + 1) // 1
	DiskStatusBroken                           // 2
	DiskStatusRepairing                        // 3
	DiskStatusRepaired                         // 4
	DiskStatusDropped                          // 5
	DiskStatusMax                              // 6
)

func (status DiskStatus) IsValid() bool {
	return status >= DiskStatusNormal && status < DiskStatusMax
}

const (
	InvalidDiskID = DiskID(0)
	InValidBlobID = BlobID(0)
	InvalidCrc32  = uint32(0)
	InvalidVid    = Vid(0)
)

const (
	MaxBlobID = BlobID(math.MaxUint64)
)

// volume status
type VolumeStatus uint8

func (status VolumeStatus) IsValid() bool {
	return status > volumeStatusMin && status < volumeStatusMax
}

func (status VolumeStatus) String() string {
	switch status {
	case VolumeStatusIdle:
		return "idle"
	case VolumeStatusActive:
		return "active"
	case VolumeStatusLock:
		return "lock"
	case VolumeStatusUnlocking:
		return "unlocking"
	}
	return "unknown"
}

const (
	volumeStatusMin = VolumeStatus(iota)
	VolumeStatusIdle
	VolumeStatusActive
	VolumeStatusLock
	VolumeStatusUnlocking
	volumeStatusMax
)

// Unified service name definition
const (
	AllocatorSvrName = "ALLOCATOR"
)

// config key
const (
	CodeModeConfigKey    = "code_mode"
	VolumeReserveSizeKey = "volume_reserve_size"
	VolumeChunkSizeKey   = "volume_chunk_size"
)
