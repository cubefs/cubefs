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

package authnode

// Keys in the request
const (
	AuthService = "AuthService"
)

const (
	defaultInitMetaPartitionCount                = 3
	defaultMaxInitMetaPartitionCount             = 100
	defaultMaxMetaPartitionInodeID        uint64 = 1<<63 - 1
	defaultMetaPartitionInodeIDStep       uint64 = 1 << 24
	defaultMetaNodeReservedMem            uint64 = 1 << 30
	runtimeStackBufSize                          = 4096
	spaceAvailableRate                           = 0.90
	defaultNodeSetCapacity                       = 18
	minNumOfRWDataPartitions                     = 10
	intervalToCheckMissingReplica                = 600
	intervalToLoadDataPartition                  = 12 * 60 * 60
	defaultInitDataPartitionCnt                  = 10
	volExpansionRatio                            = 0.1
	maxNumberOfDataPartitionsForExpansion        = 100
	EmptyCrcValue                         uint32 = 4045511210
	DefaultRackName                              = "default"
)

const (
	opSyncAddKey     uint32 = 0x01
	opSyncDeleteKey  uint32 = 0x02
	opSyncGetKey     uint32 = 0x03
	opSyncAddCaps    uint32 = 0x04
	opSyncDeleteCaps uint32 = 0x05
	opSyncGetCaps    uint32 = 0x06
)

const (
	keySeparator = "#"
	keyAcronym   = "key"
	ksPrefix     = keySeparator + keyAcronym + keySeparator
)
