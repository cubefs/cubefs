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

package proto

import "github.com/cubefs/cubefs/util/errors"

// err
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
	ErrDataNodeNotExists      = errors.New("data node not exists")
	ErrMetaNodeNotExists      = errors.New("meta node not exists")
	ErrDuplicateVol           = errors.New("duplicate vol")
	ErrActiveDataNodesTooLess = errors.New("no enough active data node")
	ErrActiveMetaNodesTooLess = errors.New("no enough active meta node")
	ErrInvalidMpStart         = errors.New("invalid meta partition start value")
	ErrNoAvailDataPartition   = errors.New("no available data partition")
	ErrReshuffleArray         = errors.New("the array to be reshuffled is nil")

	ErrIllegalDataReplica = errors.New("data replica is illegal")

	ErrMissingReplica       = errors.New("a missing data replica is found")
	ErrHasOneMissingReplica = errors.New("there is a missing replica")

	ErrNoDataNodeToWrite = errors.New("No data node available for creating a data partition")
	ErrNoMetaNodeToWrite = errors.New("No meta node available for creating a meta partition")

	ErrCannotBeOffLine                         = errors.New("cannot take the data replica offline")
	ErrNoDataNodeToCreateDataPartition         = errors.New("no enough data nodes for creating a data partition")
	ErrNoZoneToCreateDataPartition             = errors.New("no zone available for creating a data partition")
	ErrNoZoneToCreateMetaPartition             = errors.New("no zone available for creating a meta partition")
	ErrNoNodeSetToCreateDataPartition          = errors.New("no node set available for creating a data partition, no node set has enough data node to allocate partitions")
	ErrNoNodeSetToCreateMetaPartition          = errors.New("no node set available for creating a meta partition, no node set has enough meta node to allocate partitions")
	ErrNoMetaNodeToCreateMetaPartition         = errors.New("no enough meta nodes for creating a meta partition")
	ErrIllegalMetaReplica                      = errors.New("illegal meta replica")
	ErrNoEnoughReplica                         = errors.New("no enough replicas")
	ErrNoLeader                                = errors.New("no leader")
	ErrVolAuthKeyNotMatch                      = errors.New("client and server auth key do not match")
	ErrAuthKeyStoreError                       = errors.New("auth keystore error")
	ErrAuthAPIAccessGenRespError               = errors.New("auth API access response error")
	ErrAuthOSCapsOpGenRespError                = errors.New("auth Object Storage node API response error")
	ErrKeyNotExists                            = errors.New("key not exists")
	ErrDuplicateKey                            = errors.New("duplicate key")
	ErrAccessKeyNotExists                      = errors.New("access key not exists")
	ErrInvalidTicket                           = errors.New("invalid ticket")
	ErrInvalidClientIDKey                      = errors.New("invalid clientIDKey")
	ErrExpiredTicket                           = errors.New("expired ticket")
	ErrMasterAPIGenRespError                   = errors.New("master API generate response error")
	ErrDuplicateUserID                         = errors.New("duplicate user id")
	ErrUserNotExists                           = errors.New("user not exists")
	ErrReadBodyError                           = errors.New("read request body failed")
	ErrVolPolicyNotExists                      = errors.New("vol policy not exists")
	ErrDuplicateAccessKey                      = errors.New("duplicate access key")
	ErrHaveNoPolicy                            = errors.New("no vol policy")
	ErrZoneNotExists                           = errors.New("zone not exists")
	ErrOwnVolExists                            = errors.New("own vols not empty")
	ErrSuperAdminExists                        = errors.New("super administrator exists ")
	ErrInvalidUserID                           = errors.New("invalid user ID")
	ErrInvalidUserType                         = errors.New("invalid user type")
	ErrNoPermission                            = errors.New("no permission")
	ErrTokenNotFound                           = errors.New("token not found")
	ErrInvalidAccessKey                        = errors.New("invalid access key")
	ErrInvalidSecretKey                        = errors.New("invalid secret key")
	ErrIsOwner                                 = errors.New("user owns the volume")
	ErrZoneNum                                 = errors.New("zone num not qualified")
	ErrNoNodeSetToUpdateDecommissionLimit      = errors.New("no node set available for updating decommission limit")
	ErrNoNodeSetToQueryDecommissionLimitStatus = errors.New("no node set available for query decommission limit status")
	ErrNoNodeSetToDecommission                 = errors.New("no node set available to decommission ")
	ErrVolNoAvailableSpace                     = errors.New("vol has no available space")
	ErrVolNoCacheAndRule                       = errors.New("vol has no cache and rule")
	ErrNoAclPermission                         = errors.New("acl no permission")
	ErrQuotaNotExists                          = errors.New("quota not exists")
	ErrVolNotDelete                            = errors.New("vol was not previously deleted or already deleted")
	ErrVolHasDeleted                           = errors.New("vol has been deleted")
	ErrCodeVersionOp                           = errors.New("version op failed")
	ErrNoNodeSetToUpdateDecommissionDiskFactor = errors.New("no node set available for updating decommission disk factor")
	ErrNoNodeSetToQueryDecommissionDiskLimit   = errors.New("no node set available for query decommission disk limit")
	ErrNodeSetNotExists                        = errors.New("node set not exists")
	ErrCompressFailed                          = errors.New("compress data failed")
	ErrDecompressFailed                        = errors.New("decompress data failed")
	ErrDecommissionDiskErrDPFirst              = errors.New("decommission disk error data partition first")
	ErrAllReplicaUnavailable                   = errors.New("all replica unavailable")
	ErrDiskNotExists                           = errors.New("disk not exists")
	ErrPerformingRestoreReplica                = errors.New("is performing restore replica")
	ErrPerformingDecommission                  = errors.New("one replica is performing decommission")
	ErrWaitForAutoAddReplica                   = errors.New("wait for auto add replica")
	ErrBufferSizeExceedMaximum                 = errors.New("buffer size exceeds maximum")
	ErrVolNameRegExpNotMatch                   = errors.New("name can only be number and letters")
	ErrSnapshotNotEnabled                      = errors.New("cluster not enable snapshot")
	ErrMemberChange                            = errors.New("raft prev member change is not finished.")
	ErrNoSuchLifecycleConfiguration            = errors.New("The lifecycle configuration does not exist")
	ErrNoSupportStorageClass                   = errors.New("Lifecycle storage class not allowed")
	ErrDataNodeAdd                             = errors.New("DataNode mediaType not match")
	ErrNeedForbidVer0                          = errors.New("Need set volume ForbidWriteOpOfProtoVer0 first")
	ErrTmpfsNoSpace                            = errors.New("no space left on device")
)

// http response error code and error message definitions
const (
	ErrCodeSuccess = iota
	ErrCodeInternalError
	ErrCodeParamError
	ErrCodeInvalidCfg
	ErrCodePersistenceByRaft
	ErrCodeMarshalData
	ErrCodeUnmarshalData
	ErrCodeVolNotExists
	ErrCodeVolHasDeleted
	ErrCodeVolNotDelete
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
	ErrCodeNoZoneToCreateDataPartition
	ErrCodeNoNodeSetToCreateDataPartition
	ErrCodeNoNodeSetToCreateMetaPartition
	ErrCodeNoMetaNodeToCreateMetaPartition
	ErrCodeIllegalMetaReplica
	ErrCodeNoEnoughReplica
	ErrCodeNoLeader
	ErrCodeVolAuthKeyNotMatch
	ErrCodeAuthKeyStoreError
	ErrCodeAuthAPIAccessGenRespError
	ErrCodeAuthRaftNodeGenRespError
	ErrCodeAuthOSCapsOpGenRespError
	ErrCodeAuthReqRedirectError
	ErrCodeAccessKeyNotExists
	ErrCodeInvalidTicket
	ErrCodeInvalidClientIDKey
	ErrCodeExpiredTicket
	ErrCodeMasterAPIGenRespError
	ErrCodeDuplicateUserID
	ErrCodeUserNotExists
	ErrCodeReadBodyError
	ErrCodeVolPolicyNotExists
	ErrCodeDuplicateAccessKey
	ErrCodeHaveNoPolicy
	ErrCodeNoZoneToCreateMetaPartition
	ErrCodeZoneNotExists
	ErrCodeOwnVolExists
	ErrCodeSuperAdminExists
	ErrCodeInvalidUserID
	ErrCodeInvalidUserType
	ErrCodeNoPermission
	ErrCodeTokenNotExist
	ErrCodeInvalidAccessKey
	ErrCodeInvalidSecretKey
	ErrCodeIsOwner
	ErrCodeZoneNumError
	ErrCodeVersionOpError
	ErrCodeNodeSetNotExists
	ErrCodeNoSuchLifecycleConfiguration
	ErrCodeNoSupportStorageClass
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
	ErrNoZoneToCreateDataPartition:     ErrCodeNoZoneToCreateDataPartition,
	ErrNoZoneToCreateMetaPartition:     ErrCodeNoZoneToCreateMetaPartition,
	ErrNoNodeSetToCreateDataPartition:  ErrCodeNoNodeSetToCreateDataPartition,
	ErrNoNodeSetToCreateMetaPartition:  ErrCodeNoNodeSetToCreateMetaPartition,
	ErrNoMetaNodeToCreateMetaPartition: ErrCodeNoMetaNodeToCreateMetaPartition,
	ErrIllegalMetaReplica:              ErrCodeIllegalMetaReplica,
	ErrNoEnoughReplica:                 ErrCodeNoEnoughReplica,
	ErrNoLeader:                        ErrCodeNoLeader,
	ErrVolAuthKeyNotMatch:              ErrCodeVolAuthKeyNotMatch,
	ErrAuthKeyStoreError:               ErrCodeAuthKeyStoreError,
	ErrAuthAPIAccessGenRespError:       ErrCodeAuthAPIAccessGenRespError,
	ErrAuthOSCapsOpGenRespError:        ErrCodeAuthOSCapsOpGenRespError,
	ErrAccessKeyNotExists:              ErrCodeAccessKeyNotExists,
	ErrInvalidTicket:                   ErrCodeInvalidTicket,
	ErrInvalidClientIDKey:              ErrCodeInvalidClientIDKey,
	ErrExpiredTicket:                   ErrCodeExpiredTicket,
	ErrMasterAPIGenRespError:           ErrCodeMasterAPIGenRespError,
	ErrDuplicateUserID:                 ErrCodeDuplicateUserID,
	ErrUserNotExists:                   ErrCodeUserNotExists,
	ErrReadBodyError:                   ErrCodeReadBodyError,
	ErrVolPolicyNotExists:              ErrCodeVolPolicyNotExists,
	ErrDuplicateAccessKey:              ErrCodeDuplicateAccessKey,
	ErrHaveNoPolicy:                    ErrCodeHaveNoPolicy,
	ErrZoneNotExists:                   ErrCodeZoneNotExists,
	ErrOwnVolExists:                    ErrCodeOwnVolExists,
	ErrSuperAdminExists:                ErrCodeSuperAdminExists,
	ErrInvalidUserID:                   ErrCodeInvalidUserID,
	ErrInvalidUserType:                 ErrCodeInvalidUserType,
	ErrNoPermission:                    ErrCodeNoPermission,
	ErrTokenNotFound:                   ErrCodeTokenNotExist,
	ErrInvalidAccessKey:                ErrCodeInvalidAccessKey,
	ErrInvalidSecretKey:                ErrCodeInvalidSecretKey,
	ErrIsOwner:                         ErrCodeIsOwner,
	ErrZoneNum:                         ErrCodeZoneNumError,
	ErrCodeVersionOp:                   ErrCodeVersionOpError,
	ErrNodeSetNotExists:                ErrCodeNodeSetNotExists,
	ErrNoSuchLifecycleConfiguration:    ErrCodeNoSuchLifecycleConfiguration,
	ErrNoSupportStorageClass:           ErrCodeNoSupportStorageClass,
}

func ParseErrorCode(code int32) error {
	if err, exist := code2ErrMap[code]; exist {
		return err
	}
	return ErrInternalError
}

// Code2ErrMap error map to code
var code2ErrMap = map[int32]error{
	ErrCodeSuccess:                         ErrSuc,
	ErrCodeInternalError:                   ErrInternalError,
	ErrCodeParamError:                      ErrParamError,
	ErrCodeInvalidCfg:                      ErrInvalidCfg,
	ErrCodePersistenceByRaft:               ErrPersistenceByRaft,
	ErrCodeMarshalData:                     ErrMarshalData,
	ErrCodeUnmarshalData:                   ErrUnmarshalData,
	ErrCodeVolNotExists:                    ErrVolNotExists,
	ErrCodeMetaPartitionNotExists:          ErrMetaPartitionNotExists,
	ErrCodeDataPartitionNotExists:          ErrDataPartitionNotExists,
	ErrCodeDataNodeNotExists:               ErrDataNodeNotExists,
	ErrCodeMetaNodeNotExists:               ErrMetaNodeNotExists,
	ErrCodeDuplicateVol:                    ErrDuplicateVol,
	ErrCodeActiveDataNodesTooLess:          ErrActiveDataNodesTooLess,
	ErrCodeActiveMetaNodesTooLess:          ErrActiveMetaNodesTooLess,
	ErrCodeInvalidMpStart:                  ErrInvalidMpStart,
	ErrCodeNoAvailDataPartition:            ErrNoAvailDataPartition,
	ErrCodeReshuffleArray:                  ErrReshuffleArray,
	ErrCodeIllegalDataReplica:              ErrIllegalDataReplica,
	ErrCodeMissingReplica:                  ErrMissingReplica,
	ErrCodeHasOneMissingReplica:            ErrHasOneMissingReplica,
	ErrCodeNoDataNodeToWrite:               ErrNoDataNodeToWrite,
	ErrCodeNoMetaNodeToWrite:               ErrNoMetaNodeToWrite,
	ErrCodeCannotBeOffLine:                 ErrCannotBeOffLine,
	ErrCodeNoDataNodeToCreateDataPartition: ErrNoDataNodeToCreateDataPartition,
	ErrCodeNoZoneToCreateDataPartition:     ErrNoZoneToCreateDataPartition,
	ErrCodeNoZoneToCreateMetaPartition:     ErrNoZoneToCreateMetaPartition,
	ErrCodeNoNodeSetToCreateDataPartition:  ErrNoNodeSetToCreateDataPartition,
	ErrCodeNoNodeSetToCreateMetaPartition:  ErrNoNodeSetToCreateMetaPartition,
	ErrCodeNoMetaNodeToCreateMetaPartition: ErrNoMetaNodeToCreateMetaPartition,
	ErrCodeIllegalMetaReplica:              ErrIllegalMetaReplica,
	ErrCodeNoEnoughReplica:                 ErrNoEnoughReplica,
	ErrCodeNoLeader:                        ErrNoLeader,
	ErrCodeVolAuthKeyNotMatch:              ErrVolAuthKeyNotMatch,
	ErrCodeAuthKeyStoreError:               ErrAuthKeyStoreError,
	ErrCodeAuthAPIAccessGenRespError:       ErrAuthAPIAccessGenRespError,
	ErrCodeAuthOSCapsOpGenRespError:        ErrAuthOSCapsOpGenRespError,
	ErrCodeAccessKeyNotExists:              ErrAccessKeyNotExists,
	ErrCodeInvalidTicket:                   ErrInvalidTicket,
	ErrCodeInvalidClientIDKey:              ErrInvalidClientIDKey,
	ErrCodeExpiredTicket:                   ErrExpiredTicket,
	ErrCodeMasterAPIGenRespError:           ErrMasterAPIGenRespError,
	ErrCodeDuplicateUserID:                 ErrDuplicateUserID,
	ErrCodeUserNotExists:                   ErrUserNotExists,
	ErrCodeReadBodyError:                   ErrReadBodyError,
	ErrCodeVolPolicyNotExists:              ErrVolPolicyNotExists,
	ErrCodeDuplicateAccessKey:              ErrDuplicateAccessKey,
	ErrCodeHaveNoPolicy:                    ErrHaveNoPolicy,
	ErrCodeZoneNotExists:                   ErrZoneNotExists,
	ErrCodeOwnVolExists:                    ErrOwnVolExists,
	ErrCodeSuperAdminExists:                ErrSuperAdminExists,
	ErrCodeInvalidUserType:                 ErrInvalidUserType,
	ErrCodeInvalidUserID:                   ErrInvalidUserID,
	ErrCodeNoPermission:                    ErrNoPermission,
	ErrCodeTokenNotExist:                   ErrTokenNotFound,
	ErrCodeInvalidAccessKey:                ErrInvalidAccessKey,
	ErrCodeInvalidSecretKey:                ErrInvalidSecretKey,
	ErrCodeIsOwner:                         ErrIsOwner,
	ErrCodeZoneNumError:                    ErrZoneNum,
	ErrCodeVersionOpError:                  ErrCodeVersionOp,
	ErrCodeNodeSetNotExists:                ErrNodeSetNotExists,
	ErrCodeVolNotDelete:                    ErrVolNotDelete,
	ErrCodeVolHasDeleted:                   ErrVolHasDeleted,
	ErrCodeNoSuchLifecycleConfiguration:    ErrNoSuchLifecycleConfiguration,
	ErrCodeNoSupportStorageClass:           ErrNoSupportStorageClass,
}

type GeneralResp struct {
	Message string
	Code    int32
}

func Success(msg string) *GeneralResp {
	return &GeneralResp{Message: msg, Code: ErrCodeSuccess}
}

const (
	KeyWordInHttpApiNotSupportErr = "404 page not found"
)
