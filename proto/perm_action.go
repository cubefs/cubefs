// Copyright 2018 The ChubaoFS Authors.
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
	"regexp"
)

type Action string

func (a Action) String() string {
	return string(a)
}

func (a Action) IsNone() bool {
	return len(a) == 0 || a == NoneAction
}

const (
	ActionPrefix      = "action:"
	OSSActionPrefix   = ActionPrefix + "oss:"
	POSIXActionPrefix = ActionPrefix + "posix:"

	// constants for object storage interfaces
	OSSGetObjectAction                  Action = OSSActionPrefix + "GetObject"
	OSSPutObjectAction                  Action = OSSActionPrefix + "PutObject"
	OSSCopyObjectAction                 Action = OSSActionPrefix + "CopyObject"
	OSSListObjectsAction                Action = OSSActionPrefix + "ListObjects"
	OSSDeleteObjectAction               Action = OSSActionPrefix + "DeleteObject"
	OSSDeleteObjectsAction              Action = OSSActionPrefix + "DeleteObjects"
	OSSHeadObjectAction                 Action = OSSActionPrefix + "HeadObject"
	OSSCreateBucketAction               Action = OSSActionPrefix + "CreateBucket"
	OSSDeleteBucketAction               Action = OSSActionPrefix + "DeleteBucket"
	OSSHeadBucketAction                 Action = OSSActionPrefix + "HeadBucket"
	OSSListBucketAction                 Action = OSSActionPrefix + "ListBucket"
	OSSListBucketVersionsAction         Action = OSSActionPrefix + "ListBucketVersions"
	OSSListBucketMultipartUploadsAction Action = OSSActionPrefix + "ListBucketMultipartUploads"
	OSSGetBucketPolicyAction            Action = OSSActionPrefix + "GetBucketPolicy"
	OSSPutBucketPolicyAction            Action = OSSActionPrefix + "PutBucketPolicy"
	OSSGetBucketAclAction               Action = OSSActionPrefix + "GetBucketAcl"
	OSSPutBucketAclAction               Action = OSSActionPrefix + "PutBucketAcl"
	OSSGetObjectAclAction               Action = OSSActionPrefix + "GetObjectAcl"
	OSSGetObjectVersionAction           Action = OSSActionPrefix + "GetObjectVersion"
	OSSPutObjectVersionAction           Action = OSSActionPrefix + "PutObjectVersion"
	OSSGetObjectTorrentAction           Action = OSSActionPrefix + "GetObjectTorrent"
	OSSPutObjectTorrentAction           Action = OSSActionPrefix + "PutObjectTorrent"
	OSSPutObjectAclAction               Action = OSSActionPrefix + "PutObjectAcl"
	OSSGetObjectVersionAclAction        Action = OSSActionPrefix + "GetObjectVersionAcl"
	OSSPutObjectVersionAclAction        Action = OSSActionPrefix + "PutObjectVersionAcl"
	OSSDeleteBucketPolicyAction         Action = OSSActionPrefix + "DeleteBucketPolicy"
	OSSCreateMultipartUploadAction      Action = OSSActionPrefix + "CreateMultipartUpload"
	OSSListMultipartUploadsAction       Action = OSSActionPrefix + "ListMultipartUploads"
	OSSUploadPartAction                 Action = OSSActionPrefix + "UploadPart"
	OSSListPartsAction                  Action = OSSActionPrefix + "ListParts"
	OSSCompleteMultipartUploadAction    Action = OSSActionPrefix + "CompleteMultipartUpload"
	OSSAbortMultipartUploadAction       Action = OSSActionPrefix + "AbortMultipartUpload"
	OSSGetBucketLocationAction          Action = OSSActionPrefix + "GetBucketLocation"
	OSSGetObjectXAttrAction             Action = OSSActionPrefix + "GetObjectXAttr"
	OSSPutObjectXAttrAction             Action = OSSActionPrefix + "PutObjectXAttr"
	OSSListObjectXAttrsAction           Action = OSSActionPrefix + "ListObjectXAttrs"
	OSSDeleteObjectXAttrAction          Action = OSSActionPrefix + "DeleteObjectXAttr"
	OSSGetObjectTaggingAction           Action = OSSActionPrefix + "GetObjectTagging"
	OSSPutObjectTaggingAction           Action = OSSActionPrefix + "PutObjectTagging"
	OSSDeleteObjectTaggingAction        Action = OSSActionPrefix + "DeleteObjectTagging"
	OSSGetBucketTaggingAction           Action = OSSActionPrefix + "GetBucketTagging"
	OSSPutBucketTaggingAction           Action = OSSActionPrefix + "PutBucketTagging"
	OSSDeleteBucketTaggingAction        Action = OSSActionPrefix + "DeleteBucketTagging"

	// constants for POSIX file system interface
	POSIXReadAction  Action = POSIXActionPrefix + "Read"
	POSIXWriteAction Action = POSIXActionPrefix + "Write"

	NoneAction Action = ""
)

var (
	AllActions = []Action{
		// object storage interface actions
		OSSGetObjectAction,
		OSSPutObjectAction,
		OSSCopyObjectAction,
		OSSListObjectsAction,
		OSSDeleteObjectAction,
		OSSHeadObjectAction,
		OSSCreateBucketAction,
		OSSDeleteBucketAction,
		OSSHeadBucketAction,
		OSSListBucketAction,
		OSSListBucketVersionsAction,
		OSSListBucketMultipartUploadsAction,
		OSSGetBucketPolicyAction,
		OSSPutBucketPolicyAction,
		OSSGetBucketAclAction,
		OSSPutBucketAclAction,
		OSSGetObjectAclAction,
		OSSGetObjectVersionAction,
		OSSPutObjectVersionAction,
		OSSGetObjectTorrentAction,
		OSSPutObjectTorrentAction,
		OSSPutObjectAclAction,
		OSSGetObjectVersionAclAction,
		OSSPutObjectVersionAclAction,
		OSSDeleteBucketPolicyAction,
		OSSCreateMultipartUploadAction,
		OSSListMultipartUploadsAction,
		OSSUploadPartAction,
		OSSListPartsAction,
		OSSCompleteMultipartUploadAction,
		OSSAbortMultipartUploadAction,
		OSSGetBucketLocationAction,
		OSSGetObjectXAttrAction,
		OSSPutObjectXAttrAction,
		OSSListObjectXAttrsAction,
		OSSDeleteObjectXAttrAction,
		OSSGetObjectTaggingAction,
		OSSPutObjectTaggingAction,
		OSSDeleteObjectTaggingAction,
		OSSGetBucketTaggingAction,
		OSSPutBucketTaggingAction,
		OSSDeleteBucketTaggingAction,

		// posix file system interface actions
		POSIXReadAction,
		POSIXWriteAction,
	}
)

var (
	actionRegexp = regexp.MustCompile("^action:((oss:(\\w+))|(posix:(\\w)+))$")
)

func ParseAction(str string) Action {
	if len(str) == 0 || !actionRegexp.MatchString(str) {
		return NoneAction
	}
	for _, act := range AllActions {
		if act.String() == str {
			return act
		}
	}
	return NoneAction
}

type Actions []Action

func (actions Actions) Contains(action Action) bool {
	if len(actions) == 0 {
		return false
	}
	for _, a := range actions {
		if a == action {
			return true
		}
	}
	return false
}

func (actions Actions) Len() int {
	return len(actions)
}

type Permission string

func (p Permission) String() string {
	return string(p)
}

func (p Permission) ReadableString() string {
	if p.Valid() {
		if p.IsBuiltin() {
			return p.String()[len(BuiltinPermissionPrefix.String()):] + "(builtin)"
		}
		if p.IsCustom() {
			return p.String()[len(CustomPermissionPrefix.String()):] + "(custom)"
		}
		return p.String()
	}
	return "None"
}

func (p Permission) IsBuiltin() bool {
	return builtinPermRegexp.MatchString(string(p))
}

func (p Permission) IsCustom() bool {
	return customPermRegexp.MatchString(string(p))
}

func (p Permission) Valid() bool {
	return permRegexp.MatchString(string(p))
}

func (p Permission) IsNone() bool {
	return p == NonePermission
}

const (
	// prefixes for value organization
	PermissionPrefix        Permission = "perm:"
	BuiltinPermissionPrefix Permission = PermissionPrefix + "builtin:"
	CustomPermissionPrefix  Permission = PermissionPrefix + "custom:"

	// constants for builtin permissions
	BuiltinPermissionReadOnly Permission = BuiltinPermissionPrefix + "ReadOnly"
	BuiltinPermissionWritable Permission = BuiltinPermissionPrefix + "Writable"

	// constants for unknown permission
	NonePermission Permission = ""
)

var (
	permRegexp        = regexp.MustCompile("^perm:((builtin:(Writable|ReadOnly))|(custom:(\\w)+))$")
	builtinPermRegexp = regexp.MustCompile("^perm:builtin:(Writable|ReadOnly)$")
	customPermRegexp  = regexp.MustCompile("^perm:custom:(\\w)+$")
)

func ParsePermission(value string) Permission {
	if permRegexp.MatchString(value) {
		return Permission(value)
	}
	return NonePermission
}

func NewCustomPermission(name string) Permission {
	return Permission(CustomPermissionPrefix + Permission(name))
}

var (
	builtinPermissionActionsMap = map[Permission]Actions{
		BuiltinPermissionReadOnly: {
			// object storage interface
			OSSGetObjectAction,
			OSSListObjectsAction,
			OSSHeadObjectAction,
			OSSHeadBucketAction,
			OSSListBucketAction,
			OSSListBucketVersionsAction,
			OSSListBucketMultipartUploadsAction,
			OSSGetBucketPolicyAction,
			OSSGetBucketAclAction,
			OSSPutBucketAclAction,
			OSSGetObjectAclAction,
			OSSGetObjectVersionAction,
			OSSPutObjectVersionAction,
			OSSGetObjectTorrentAction,
			OSSGetObjectVersionAclAction,
			OSSListMultipartUploadsAction,
			OSSListPartsAction,
			OSSGetBucketLocationAction,
			OSSGetObjectXAttrAction,
			OSSListObjectXAttrsAction,
			OSSGetObjectTaggingAction,
			OSSGetBucketTaggingAction,

			// file system interface
			POSIXReadAction,
		},
		BuiltinPermissionWritable: {
			// object storage interface
			OSSGetObjectAction,
			OSSPutObjectAction,
			OSSCopyObjectAction,
			OSSListObjectsAction,
			OSSDeleteObjectAction,
			OSSHeadObjectAction,
			OSSHeadBucketAction,
			OSSListBucketAction,
			OSSListBucketVersionsAction,
			OSSListBucketMultipartUploadsAction,
			OSSGetBucketPolicyAction,
			OSSGetBucketAclAction,
			OSSGetObjectAclAction,
			OSSGetObjectVersionAction,
			OSSPutObjectVersionAction,
			OSSGetObjectTorrentAction,
			OSSPutObjectTorrentAction,
			OSSPutObjectAclAction,
			OSSGetObjectVersionAclAction,
			OSSPutObjectVersionAclAction,
			OSSCreateMultipartUploadAction,
			OSSListMultipartUploadsAction,
			OSSUploadPartAction,
			OSSListPartsAction,
			OSSCompleteMultipartUploadAction,
			OSSAbortMultipartUploadAction,
			OSSGetBucketLocationAction,
			OSSGetObjectXAttrAction,
			OSSPutObjectXAttrAction,
			OSSListObjectXAttrsAction,
			OSSDeleteObjectXAttrAction,
			OSSGetObjectTaggingAction,
			OSSPutObjectTaggingAction,
			OSSDeleteObjectTaggingAction,
			OSSGetBucketTaggingAction,

			// file system interface
			POSIXReadAction,
			POSIXWriteAction,
		},
	}
)

func BuiltinPermissionActions(perm Permission) Actions {
	if actions, exists := builtinPermissionActionsMap[perm]; exists {
		return actions
	}
	return nil
}
