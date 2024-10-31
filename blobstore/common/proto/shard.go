// Copyright 2024 The CubeFS Authors.
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

const (
	InvalidShardID = ShardID(0)
	InvalidSuid    = Suid(0)
)

type (
	ShardID    uint32
	Suid       uint64
	SuidPrefix uint64
)

func (s Suid) ShardID() ShardID {
	return ShardID(s >> 32)
}

func (s Suid) Index() uint8 {
	return uint8(s & 0xff000000 >> 24)
}

func (s Suid) Epoch() uint32 {
	return uint32(s & 0xffffff)
}

func (s Suid) IsValid() bool {
	return s > InvalidSuid && IsValidEpoch(s.Epoch()) && IsValidIndex(s.Index())
}

func (s Suid) SuidPrefix() SuidPrefix {
	suidPre := uint64(s) - uint64(s.Epoch())
	return SuidPrefix(suidPre)
}

func (s SuidPrefix) ShardID() ShardID {
	return ShardID(s & 0xffffffff00000000 >> 32)
}

func (s SuidPrefix) Index() uint8 {
	return uint8(s & 0xff000000 >> 24)
}

func EncodeSuid(shardID ShardID, index uint8, epoch uint32) Suid {
	return Suid(uint64(shardID)<<32 + uint64(index)<<24 + uint64(epoch))
}

func EncodeSuidPrefix(shardID ShardID, idx uint8) SuidPrefix {
	u64 := uint64(shardID)<<32 + uint64(idx)<<24
	return SuidPrefix(u64)
}

type SpaceStatus uint8

const (
	SpaceStatusInit = SpaceStatus(iota + 1)
	SpaceStatusNormal
)

type ShardUpdateType uint8

const (
	ShardUpdateTypeAddMember    ShardUpdateType = 1
	ShardUpdateTypeRemoveMember ShardUpdateType = 2
	ShardUpdateTypeUpdateMember ShardUpdateType = 3
)

type FieldType uint8

const (
	FieldTypeBool = FieldType(iota + 1)
	FieldTypeInt
	FieldTypeFloat
	FieldTypeString
	FieldTypeBytes
)

func (field FieldType) IsValid() bool {
	return field >= FieldTypeBool && field <= FieldTypeBytes
}

type IndexOption uint8

const (
	IndexOptionNull = IndexOption(iota)
	IndexOptionIndexed
	IndexOptionFulltext
	IndexOptionUnique
)

func (index IndexOption) IsValid() bool {
	return index <= IndexOptionUnique
}

type FieldID uint32

type ShardTaskType uint8

const (
	ShardTaskTypeClearShard = ShardTaskType(iota + 1)
	ShardTaskTypeCheckpoint
	ShardTaskTypeSyncRouteVersion
)

type ShardUnitStatus uint8

const (
	ShardUnitStatusNormal = ShardUnitStatus(iota + 1)
	ShardUnitStatusOffline
)

func (status ShardUnitStatus) String() string {
	switch status {
	case ShardUnitStatusNormal:
		return "normal"
	case ShardUnitStatusOffline:
		return "offline"
	default:
		return "unknown"
	}
}

func (status ShardUnitStatus) IsValid() bool {
	return status >= ShardUnitStatusNormal && status <= ShardUnitStatusOffline
}
