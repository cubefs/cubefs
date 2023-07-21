// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"github.com/cubefs/cubefs/proto"
)

const (
	CannedPrivate           = "private"
	CannedPublicRead        = "public-read"
	CannedPublicReadWrite   = "public-read-write"
	CannedAuthenticatedRead = "authenticated-read"
)

const (
	XAmzAcl              = "X-Amz-Acl"
	XAmzGrantRead        = "X-Amz-Grant-Read"
	XAmzGrantWrite       = "X-Amz-Grant-Write"
	XAmzGrantReadAcp     = "X-Amz-Grant-Read-Acp"
	XAmzGrantWriteAcp    = "X-Amz-Grant-Write-Acp"
	XAmzGrantFullControl = "X-Amz-Grant-Full-Control"
)

const (
	PermissionRead        = "READ"
	PermissionWrite       = "WRITE"
	PermissionReadAcp     = "READ_ACP"
	PermissionWriteAcp    = "WRITE_ACP"
	PermissionFullControl = "FULL_CONTROL"
)

const (
	TypeCanonicalUser = "CanonicalUser"
	TypeGroup         = "Group"
)

const (
	XMLNS              = "http://s3.amazonaws.com/doc/2006-03-01/"
	XMLSI              = "http://www.w3.org/2001/XMLSchema-instance"
	GroupAllUser       = "http://acs.amazonaws.com/groups/global/AllUsers"
	GroupAuthenticated = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

var (
	apiToPermission = map[proto.Action]string{
		// bucket read
		proto.OSSListObjectsAction:          PermissionRead,
		proto.OSSHeadBucketAction:           PermissionRead,
		proto.OSSListMultipartUploadsAction: PermissionRead,
		// bucket write
		proto.OSSPutObjectAction:               PermissionWrite,
		proto.OSSCopyObjectAction:              PermissionWrite,
		proto.OSSCreateMultipartUploadAction:   PermissionWrite,
		proto.OSSUploadPartAction:              PermissionWrite,
		proto.OSSCompleteMultipartUploadAction: PermissionWrite,
		proto.OSSDeleteObjectAction:            PermissionWrite,
		proto.OSSDeleteObjectsAction:           PermissionWrite,
		// bucket acp
		proto.OSSPutBucketAclAction: PermissionWriteAcp,
		proto.OSSGetBucketAclAction: PermissionReadAcp,
		// object read
		proto.OSSGetObjectAction:  PermissionRead,
		proto.OSSHeadObjectAction: PermissionRead,
		// object acp
		proto.OSSPutObjectAclAction: PermissionWriteAcp,
		proto.OSSGetObjectAclAction: PermissionReadAcp,
	}
	aclApiList             = []proto.Action{proto.OSSPutBucketAclAction, proto.OSSGetBucketAclAction, proto.OSSPutObjectAclAction, proto.OSSGetObjectAclAction}
	objectACLSupportedApis = []proto.Action{proto.OSSGetObjectAction, proto.OSSHeadObjectAction, proto.OSSPutObjectAclAction, proto.OSSGetObjectAclAction}
)

var (
	XAmzGrantHeaders      = []string{XAmzGrantRead, XAmzGrantWrite, XAmzGrantReadAcp, XAmzGrantWriteAcp, XAmzGrantFullControl}
	XAmzGrantToPermission = map[string]string{
		XAmzGrantRead:        PermissionRead,
		XAmzGrantWrite:       PermissionWrite,
		XAmzGrantReadAcp:     PermissionReadAcp,
		XAmzGrantWriteAcp:    PermissionWriteAcp,
		XAmzGrantFullControl: PermissionFullControl,
	}
)
