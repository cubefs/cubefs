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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// service names
const (
	ServiceNameBlobNode  = "BLOBNODE"
	ServiceNameProxy     = "PROXY"
	ServiceNameScheduler = "SCHEDULER"
	ServiceNameShardNode = "SHARDNODE"
)

type (
	DiskStatus uint8
	NodeStatus uint8
	DiskType   uint8
	NodeRole   uint8

	CatalogChangeItemType uint8
)

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

func (status DiskStatus) String() string {
	switch status {
	case DiskStatusNormal:
		return "normal"
	case DiskStatusBroken:
		return "broken"
	case DiskStatusRepairing:
		return "repairing"
	case DiskStatusRepaired:
		return "repaired"
	case DiskStatusDropped:
		return "dropped"
	default:
		return "unknown"
	}
}

// node status
const (
	NodeStatusNormal  = NodeStatus(iota + 1) // 1
	NodeStatusDropped                        // 2
	NodeStatusMax                            // 3
)

// disk type
const (
	DiskTypeHDD     = DiskType(iota + 1) // 1
	DiskTypeSSD                          // 2
	DiskTypeNVMeSSD                      // 3
	DiskTypeMax                          // 4
)

func (t DiskType) String() string {
	switch t {
	case DiskTypeHDD:
		return "hdd"
	case DiskTypeSSD:
		return "ssd"
	case DiskTypeNVMeSSD:
		return "nvmessd"
	default:
		return "unknown"
	}
}

func (t DiskType) IsValid() bool {
	return t >= DiskTypeHDD && t < DiskTypeMax
}

func (t *DiskType) UnmarshalText(text []byte) error { return nil }

func (t *DiskType) UnmarshalJSON(data []byte) error {
	if diskType, err := strconv.ParseInt(string(data), 10, 8); err == nil {
		if !DiskType(diskType).IsValid() {
			return fmt.Errorf("invalid diskType: %s", string(data))
		}
		*t = DiskType(diskType)
		return nil
	}
	var diskTypeName string
	json.Unmarshal(data, &diskTypeName)
	diskType, exist := diskTypeMapping[strings.ToLower(diskTypeName)]
	if !exist {
		return fmt.Errorf("invalid diskType: %s", string(data))
	}
	*t = diskType
	return nil
}

var diskTypeMapping = map[string]DiskType{
	"hdd":     DiskTypeHDD,
	"ssd":     DiskTypeSSD,
	"nvmessd": DiskTypeNVMeSSD,
}

// node role
const (
	NodeRoleBlobNode = NodeRole(iota + 1)
	NodeRoleShardNode
	NodeRoleMax
)

func (role NodeRole) String() string {
	switch role {
	case NodeRoleBlobNode:
		return "blobnode"
	case NodeRoleShardNode:
		return "shardnode"
	default:
		return "unknown"
	}
}

func (role NodeRole) IsValid() bool {
	return role >= NodeRoleBlobNode && role < NodeRoleMax
}

func (role *NodeRole) UnmarshalText(text []byte) error { return nil }

func (role *NodeRole) UnmarshalJSON(data []byte) error {
	if nodeRole, err := strconv.ParseInt(string(data), 10, 8); err == nil {
		if !NodeRole(nodeRole).IsValid() {
			return fmt.Errorf("invalid nodeRole: %s", string(data))
		}
		*role = NodeRole(nodeRole)
		return nil
	}
	var nodeRoleName string
	json.Unmarshal(data, &nodeRoleName)
	nodeRole, exist := nodeRoleMapping[strings.ToLower(nodeRoleName)]
	if !exist {
		return fmt.Errorf("invalid nodeRole: %s", string(data))
	}
	*role = nodeRole
	return nil
}

var nodeRoleMapping = map[string]NodeRole{
	"blobnode":  NodeRoleBlobNode,
	"shardnode": NodeRoleShardNode,
}

const (
	InvalidDiskID = DiskID(0)
	InValidBlobID = BlobID(0)
	InvalidNodeID = NodeID(0)
	InvalidCrc32  = uint32(0)
	InvalidVid    = Vid(0)
	InvalidVuid   = Vuid(0)

	InvalidSpaceID = SpaceID(0)
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
	default:
		return "unknown"
	}
}

const (
	volumeStatusMin = VolumeStatus(iota)
	VolumeStatusIdle
	VolumeStatusActive
	VolumeStatusLock
	VolumeStatusUnlocking
	volumeStatusMax
)

// system config key,not allow delete
const (
	CodeModeConfigKey    = "code_mode"
	VolumeReserveSizeKey = "volume_reserve_size"
	VolumeChunkSizeKey   = "volume_chunk_size"
)

func IsSysConfigKey(key string) bool {
	switch key {
	case VolumeChunkSizeKey, VolumeReserveSizeKey, CodeModeConfigKey:
		return true
	default:
		return false
	}
}

type TaskSwitch string

const (
	TaskSwitchDataInspect TaskSwitch = "data_inspect"
)

func (t TaskSwitch) Valid() bool {
	switch t {
	case TaskSwitchDataInspect:
		return true
	default:
		return false
	}
}

func (t TaskSwitch) String() string {
	return string(t)
}

// MaxShardSize max size of a single shard
const MaxShardSize = 512 << 20

// catalog changeItem type
const (
	CatalogChangeItemAddShard = CatalogChangeItemType(iota + 1)
	CatalogChangeItemUpdateShard
)
