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

package proto

import (
	"github.com/juju/errors"
)

//err
var (
	ErrSuc                    = errors.New("success")
	ErrInternalError          = errors.New("internal error")
	ErrParamError             = errors.New("parameter error")
	ErrInvalidCfg             = errors.New("bad configuration file")
	ErrPersistenceByRaft      = errors.New("persistence by raft occurred error")
	ErrMarshalData            = errors.New("marshal data error")
	ErrUnmarshalData          = errors.New("unmarshal data error")
	ErrVolNotExists           = errors.New("vol not exists")
	ErrMetaPartitionNotExists = errors.New("meta partition not exists")
	ErrDataPartitionNotExists = errors.New("data partition not exists")
	ErrDataNodeNotExists      = errors.New("data partition not exists")
	ErrMetaNodeNotExists      = errors.New("data partition not exists")
	ErrDuplicateVol           = errors.New("duplicate vol")
	ErrActiveDataNodesTooLess = errors.New("no enough active data node")
	ErrActiveMetaNodesTooLess = errors.New("no enough active meta node")
	ErrInvalidMpStart         = errors.New("invalid meta partition start value")
	ErrNoAvailDataPartition   = errors.New("no available data partition")
	ErrReshuffleArray         = errors.New("the array to be reshuffled is nil")

	ErrIllegalDataReplica = errors.New("data replica is illegal")

	ErrMissingReplica       = errors.New("a missing data replica is found")
	ErrHasOneMissingReplica = errors.New("there is a missing data replica")

	ErrNoDataNodeToWrite = errors.New("No data node available for creating a data partition")
	ErrNoMetaNodeToWrite = errors.New("No meta node available for creating a meta partition")

	ErrCannotBeOffLine                 = errors.New("cannot take the data replica offline")
	ErrNoDataNodeToCreateDataPartition = errors.New("no enough data nodes for creating a data partition")
	ErrNoRackToCreateDataPartition     = errors.New("no rack available for creating a data partition")
	ErrNoNodeSetToCreateDataPartition  = errors.New("no node set available for creating a data partition")
	ErrNoNodeSetToCreateMetaPartition  = errors.New("no node set available for creating a meta partition")
	ErrNoMetaNodeToCreateMetaPartition = errors.New("no enough meta nodes for creating a meta partition")
	ErrIllegalMetaReplica              = errors.New("illegal meta replica")
	ErrNoEnoughReplica                 = errors.New("no enough replicas")
	ErrNoLeader                        = errors.New("no leader")
	ErrVolAuthKeyNotMatch              = errors.New("client and server auth key do not match")
)

// http response error code and error message definitions
const (
	ErrCodeSuccess                         = iota
	ErrCodeInternalError
	ErrCodeParamError
	ErrCodeInvalidCfg
	ErrCodePersistenceByRaft
	ErrCodeMarshalData
	ErrCodeUnmarshalData
	ErrCodeVolNotExists
	ErrCodeMetaPartitionNotExists
	ErrCodeDataPartitionNotExists
	ErrCodeDataNodeNotExists
	ErrCodeMetaNodeNotExists
	ErrCodeDuplicateVol
	ErrCodeActiveDataNodesTooLess
	ErrCodeActiveMetaNodesTooLess
	ErrCodeInvalidMpStart
	ErrCodeNoAvailDataPartition
	ErrCodeReshuffleArray
	ErrCodeIllegalDataReplica
	ErrCodeMissingReplica
	ErrCodeHasOneMissingReplica
	ErrCodeNoDataNodeToWrite
	ErrCodeNoMetaNodeToWrite
	ErrCodeCannotBeOffLine
	ErrCodeNoDataNodeToCreateDataPartition
	ErrCodeNoRackToCreateDataPartition
	ErrCodeNoNodeSetToCreateDataPartition
	ErrCodeNoNodeSetToCreateMetaPartition
	ErrCodeNoMetaNodeToCreateMetaPartition
	ErrCodeIllegalMetaReplica
	ErrCodeNoEnoughReplica
	ErrCodeNoLeader
	ErrCodeVolAuthKeyNotMatch
)

// Err2CodeMap error map to code
var Err2CodeMap = map[error]int32{
	ErrSuc:                             ErrCodeSuccess,
	ErrInternalError:                   ErrCodeInternalError,
	ErrParamError:                      ErrCodeParamError,
	ErrInvalidCfg:                      ErrCodeInvalidCfg,
	ErrPersistenceByRaft:               ErrCodePersistenceByRaft,
	ErrMarshalData:                     ErrCodeMarshalData,
	ErrUnmarshalData:                   ErrCodeUnmarshalData,
	ErrVolNotExists:                    ErrCodeVolNotExists,
	ErrMetaPartitionNotExists:          ErrCodeMetaPartitionNotExists,
	ErrDataPartitionNotExists:          ErrCodeDataPartitionNotExists,
	ErrDataNodeNotExists:               ErrCodeDataNodeNotExists,
	ErrMetaNodeNotExists:               ErrCodeMetaNodeNotExists,
	ErrDuplicateVol:                    ErrCodeDuplicateVol,
	ErrActiveDataNodesTooLess:          ErrCodeActiveDataNodesTooLess,
	ErrActiveMetaNodesTooLess:          ErrCodeActiveMetaNodesTooLess,
	ErrInvalidMpStart:                  ErrCodeInvalidMpStart,
	ErrNoAvailDataPartition:            ErrCodeNoAvailDataPartition,
	ErrReshuffleArray:                  ErrCodeReshuffleArray,
	ErrIllegalDataReplica:              ErrCodeIllegalDataReplica,
	ErrMissingReplica:                  ErrCodeMissingReplica,
	ErrHasOneMissingReplica:            ErrCodeHasOneMissingReplica,
	ErrNoDataNodeToWrite:               ErrCodeNoDataNodeToWrite,
	ErrNoMetaNodeToWrite:               ErrCodeNoMetaNodeToWrite,
	ErrCannotBeOffLine:                 ErrCodeCannotBeOffLine,
	ErrNoDataNodeToCreateDataPartition: ErrCodeNoDataNodeToCreateDataPartition,
	ErrNoRackToCreateDataPartition:     ErrCodeNoRackToCreateDataPartition,
	ErrNoNodeSetToCreateDataPartition:  ErrCodeNoNodeSetToCreateDataPartition,
	ErrNoNodeSetToCreateMetaPartition:  ErrCodeNoNodeSetToCreateMetaPartition,
	ErrNoMetaNodeToCreateMetaPartition: ErrCodeNoMetaNodeToCreateMetaPartition,
	ErrIllegalMetaReplica:              ErrCodeIllegalMetaReplica,
	ErrNoEnoughReplica:                 ErrCodeNoEnoughReplica,
	ErrNoLeader:                        ErrCodeNoLeader,
	ErrVolAuthKeyNotMatch:              ErrCodeVolAuthKeyNotMatch,
}
