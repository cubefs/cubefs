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

package errors

import (
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// access             550-599
// blobnode           600-699
// scheduler          700-799
// proxy              800-899
// clusterMgr         900-999

// Error http status code for all application
type Error int

var _ rpc.HTTPError = Error(0)

// Error implements error and rpc.HTTPError
func (e Error) Error() string {
	return errCodeMap[int(e)]
}

// StatusCode implements rpc.HTTPError
func (e Error) StatusCode() int {
	return int(e)
}

// ErrorCode implements rpc.HTTPError
func (e Error) ErrorCode() string {
	return ""
}

var errCodeMap = map[int]string{
	// access
	CodeAccessReadRequestBody:  "access read request body",
	CodeAccessUnexpect:         "access unexpected error",
	CodeAccessServiceDiscovery: "access client service discovery disconnect",
	CodeAccessLimited:          "access limited",
	CodeAccessExceedSize:       "access exceed object size",

	// clustermgr
	CodeCMUnexpect:                   "cm: unexpected error",
	CodeLockNotAllow:                 "lock volume not allow",
	CodeUnlockNotAllow:               "unlock volume not allow",
	CodeVolumeNotExist:               "volume not exist",
	CodeRaftPropose:                  "raft propose error",
	CodeNoLeader:                     "no leader",
	CodeRaftReadIndex:                "raft read index error",
	CodeDuplicatedMemberInfo:         "duplicated member info",
	CodeCMDiskNotFound:               "disk not found",
	CodeInvalidDiskStatus:            "invalid status",
	CodeChangeDiskStatusNotAllow:     "not allow to change status back",
	CodeConcurrentAllocVolumeUnit:    "alloc volume unit concurrently",
	CodeNoAvailableVolume:            "no available volume",
	CodeAllocVolumeInvalidParams:     "alloc volume request params is invalid",
	CodeOldVuidNotMatch:              "update volume unit, old vuid not match",
	CodeNewVuidNotMatch:              "update volume unit, new vuid not match",
	CodeNewDiskIDNotMatch:            "update volume unit, new diskID not match",
	CodeConfigArgument:               "config argument marshal error",
	CodeInvalidClusterID:             "request params error, invalid clusterID",
	CodeInvalidIDC:                   "request params error,invalid idc",
	CodeVolumeUnitNotExist:           "volume unit not exist",
	CodeDiskAbnormalOrNotReadOnly:    "disk is abnormal or not readonly, can't add into dropping list",
	CodeStatChunkFailed:              "stat blob node chunk failed",
	CodeInvalidCodeMode:              "request alloc volume codeMode not invalid",
	CodeRetainVolumeNotAlloc:         "retain volume is not alloc",
	CodeDroppedDiskHasVolumeUnit:     "dropped disk still has volume unit remain, migrate them firstly",
	CodeNotSupportIdle:               "list volume v2 not support idle status",
	CodeDiskIsDropping:               "dropping disk not allow change state or set readonly",
	CodeRejectDeleteSystemConfig:     "reject delete system config",
	CodeRegisterServiceInvalidParams: "register service params is invalid",

	// scheduler
	CodeNotingTodo: "nothing to do",

	// proxy
	CodeNoAvaliableVolume: "this codemode has no avaliable volume",
	CodeAllocBidFromCm:    "alloc bid from clustermgr error",
	CodeClusterIDNotMatch: "clusterId not match",

	// blobnode
	CodeInvalidParam:   "blobnode: invalid params",
	CodeAlreadyExist:   "blobnode: entry already exist",
	CodeOutOfLimit:     "blobnode: out of limit",
	CodeInternal:       "blobnode: internal error",
	CodeOverload:       "blobnode: service is overload",
	CodePathNotExist:   "blobnode: path is not exist",
	CodePathNotEmpty:   "blobnode: path is not empty",
	CodePathFindOnline: "blobnode: path find online disk",

	CodeDiskNotFound:  "disk not found",
	CodeDiskBroken:    "disk is broken",
	CodeInvalidDiskId: "disk id is invalid",
	CodeDiskNoSpace:   "disk no space",

	CodeVuidNotFound:     "vuid not found",
	CodeVUIDReadonly:     "vuid readonly",
	CodeVUIDRelease:      "vuid released",
	CodeVuidNotMatch:     "vuid not match",
	CodeChunkNotReadonly: "chunk must readonly",
	CodeChunkNotNormal:   "chunk must normal",
	CodeChunkNoSpace:     "chunk no space",
	CodeChunkCompacting:  "chunk is compacting",
	CodeInvalidChunkId:   "chunk id is invalid",
	CodeTooManyChunks:    "too many chunks",
	CodeChunkInuse:       "chunk in use",
	CodeSizeOverBurst:    "request size over limit burst",

	CodeBidNotFound:          "bid not found",
	CodeShardSizeTooLarge:    "shard size too large",
	CodeShardNotMarkDelete:   "shard must mark delete",
	CodeShardMarkDeleted:     "shard already mark delete",
	CodeShardInvalidOffset:   "shard offset is invalid",
	CodeShardInvalidBid:      "shard key bid is invalid",
	CodeShardListExceedLimit: "shard list exceed the limit",

	CodeDestReplicaBad: "dest replica is bad can not repair",
	CodeOrphanShard:    "shard is an orphan",
	CodeIllegalTask:    "illegal task",
	CodeRequestLimited: "request limited",
}

// HTTPError make rpc.HTTPError
func HTTPError(statusCode int, errCode string, err error) error {
	return rpc.NewError(statusCode, errCode, err)
}

// Error2HTTPError transfer error to rpc.HTTPError
func Error2HTTPError(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(rpc.HTTPError); ok {
		return e
	}
	if code, ok := err.(Error); ok {
		return code
	}
	return rpc.NewError(http.StatusInternalServerError, "ServerError", err)
}

// DetectCode detect code
func DetectCode(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if code, ok := err.(Error); ok {
		return int(code)
	}
	if httpErr, ok := err.(rpc.HTTPError); ok {
		return httpErr.StatusCode()
	}
	return http.StatusInternalServerError
}
