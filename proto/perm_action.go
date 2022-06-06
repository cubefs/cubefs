// Copyright 2020 The CubeFS Authors.
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
	"path"
	"regexp"
	"strings"
)

var (
	actionRegexp       = regexp.MustCompile("^action:((oss:(\\w+))|(posix:(\\w)+))$")
	actionPrefixRegexp = regexp.MustCompile("^action:((oss)|(posix)):")
)

type Action string

func (a Action) String() string {
	return string(a)
}

func (a Action) IsNone() bool {
	return len(a) == 0 || a == NoneAction
}

func (a Action) Name() string {
	var loc = actionPrefixRegexp.FindStringIndex(a.String())
	if len(loc) != 2 {
		return "Unknown"
	}
	return a.String()[loc[1]:]
}

const (
	ActionPrefix      = "action:"
	OSSActionPrefix   = ActionPrefix + "oss:"
	POSIXActionPrefix = ActionPrefix + "posix:"

	// Object actions
	OSSGetObjectAction     Action = OSSActionPrefix + "GetObject"
	OSSPutObjectAction     Action = OSSActionPrefix + "PutObject"
	OSSCopyObjectAction    Action = OSSActionPrefix + "CopyObject"
	OSSListObjectsAction   Action = OSSActionPrefix + "ListObjects"
	OSSDeleteObjectAction  Action = OSSActionPrefix + "DeleteObject"
	OSSDeleteObjectsAction Action = OSSActionPrefix + "DeleteObjects"
	OSSHeadObjectAction    Action = OSSActionPrefix + "HeadObject"

	// Bucket actions
	OSSCreateBucketAction Action = OSSActionPrefix + "CreateBucket"
	OSSDeleteBucketAction Action = OSSActionPrefix + "DeleteBucket"
	OSSHeadBucketAction   Action = OSSActionPrefix + "HeadBucket"
	OSSListBucketsAction  Action = OSSActionPrefix + "ListBuckets"

	// Bucket policy actions
	OSSGetBucketPolicyAction       Action = OSSActionPrefix + "GetBucketPolicy"
	OSSPutBucketPolicyAction       Action = OSSActionPrefix + "PutBucketPolicy"
	OSSDeleteBucketPolicyAction    Action = OSSActionPrefix + "DeleteBucketPolicy"
	OSSGetBucketPolicyStatusAction Action = OSSActionPrefix + "GetBucketPolicyStatus" // unsupported

	// Bucket ACL actions
	OSSGetBucketAclAction Action = OSSActionPrefix + "GetBucketAcl"
	OSSPutBucketAclAction Action = OSSActionPrefix + "PutBucketAcl"

	// Bucket CORS actions
	OSSGetBucketCorsAction    Action = OSSActionPrefix + "GetBucketCors"
	OSSPutBucketCorsAction    Action = OSSActionPrefix + "PutBucketCors"
	OSSDeleteBucketCorsAction Action = OSSActionPrefix + "DeleteBucketCors"
	OSSOptionsObjectAction    Action = OSSActionPrefix + "OptionsObject"

	// Object torrent actions
	OSSGetObjectTorrentAction Action = OSSActionPrefix + "GetObjectTorrent" // unsupported

	// Object ACL actions
	OSSGetObjectAclAction Action = OSSActionPrefix + "GetObjectAcl"
	OSSPutObjectAclAction Action = OSSActionPrefix + "PutObjectAcl"

	// Multipart actions
	OSSCreateMultipartUploadAction   Action = OSSActionPrefix + "CreateMultipartUpload"
	OSSListMultipartUploadsAction    Action = OSSActionPrefix + "ListMultipartUploads"
	OSSUploadPartAction              Action = OSSActionPrefix + "UploadPart"
	OSSUploadPartCopyAction          Action = OSSActionPrefix + "UploadPartCopy" // unsupported
	OSSListPartsAction               Action = OSSActionPrefix + "ListParts"
	OSSCompleteMultipartUploadAction Action = OSSActionPrefix + "CompleteMultipartUpload"
	OSSAbortMultipartUploadAction    Action = OSSActionPrefix + "AbortMultipartUpload"

	// Bucket location
	OSSGetBucketLocationAction Action = OSSActionPrefix + "GetBucketLocation"

	// Object extend attributes (xattr)
	OSSGetObjectXAttrAction    Action = OSSActionPrefix + "GetObjectXAttr"
	OSSPutObjectXAttrAction    Action = OSSActionPrefix + "PutObjectXAttr"
	OSSListObjectXAttrsAction  Action = OSSActionPrefix + "ListObjectXAttrs"
	OSSDeleteObjectXAttrAction Action = OSSActionPrefix + "DeleteObjectXAttr"

	// Object tagging actions
	OSSGetObjectTaggingAction    Action = OSSActionPrefix + "GetObjectTagging"
	OSSPutObjectTaggingAction    Action = OSSActionPrefix + "PutObjectTagging"
	OSSDeleteObjectTaggingAction Action = OSSActionPrefix + "DeleteObjectTagging"

	// Bucket tagging actions
	OSSGetBucketTaggingAction    Action = OSSActionPrefix + "GetBucketTagging"
	OSSPutBucketTaggingAction    Action = OSSActionPrefix + "PutBucketTagging"
	OSSDeleteBucketTaggingAction Action = OSSActionPrefix + "DeleteBucketTagging"

	// Bucket lifecycle actions
	OSSGetBucketLifecycleAction    Action = OSSActionPrefix + "GetBucketLifecycle"    // unsupported
	OSSPutBucketLifecycleAction    Action = OSSActionPrefix + "PutBucketLifecycle"    // unsupported
	OSSDeleteBucketLifecycleAction Action = OSSActionPrefix + "DeleteBucketLifecycle" // unsupported

	// Object storage version actions
	OSSGetBucketVersioningAction Action = OSSActionPrefix + "GetBucketVersioning" // unsupported
	OSSPutBucketVersioningAction Action = OSSActionPrefix + "PutBucketVersioning" // unsupported
	OSSListObjectVersionsAction  Action = OSSActionPrefix + "ListObjectVersions"  // unsupported

	// Object legal hold actions
	OSSGetObjectLegalHoldAction Action = OSSActionPrefix + "GetObjectLegalHold" // unsupported
	OSSPutObjectLegalHoldAction Action = OSSActionPrefix + "PutObjectLegalHold" // unsupported

	// Object retention actions
	OSSGetObjectRetentionAction Action = OSSActionPrefix + "GetObjectRetention" // unsupported
	OSSPutObjectRetentionAction Action = OSSActionPrefix + "PutObjectRetention" // unsupported

	// Bucket encryption actions
	OSSGetBucketEncryptionAction    Action = OSSActionPrefix + "GetBucketEncryption"    // unsupported
	OSSPutBucketEncryptionAction    Action = OSSActionPrefix + "PutBucketEncryption"    // unsupported
	OSSDeleteBucketEncryptionAction Action = OSSActionPrefix + "DeleteBucketEncryption" // unsupported

	// Bucket website actions
	OSSGetBucketWebsiteAction    Action = OSSActionPrefix + "GetBucketWebsite"    // unsupported
	OSSPutBucketWebsiteAction    Action = OSSActionPrefix + "PutBucketWebsite"    // unsupported
	OSSDeleteBucketWebsiteAction Action = OSSActionPrefix + "DeleteBucketWebsite" // unsupported

	// Object restore actions
	OSSRestoreObjectAction Action = OSSActionPrefix + "RestoreObject" // unsupported

	// Public access block actions
	OSSGetPublicAccessBlockAction    Action = OSSActionPrefix + "GetPublicAccessBlock"   // unsupported
	OSSPutPublicAccessBlockAction    Action = OSSActionPrefix + "PutPublicAccessBlock"   // unsupported
	OSSDeletePublicAccessBlockAction Action = OSSActionPrefix + "DeletePulicAccessBlock" // unuspported

	// Bucket request payment actions
	OSSGetBucketRequestPaymentAction Action = OSSActionPrefix + "GetBucketRequestPayment" // unsupported
	OSSPutBucketRequestPaymentAction Action = OSSActionPrefix + "PutBucketRequestPayment" // unsupported

	// Bucket replication actions
	OSSGetBucketReplicationAction    Action = OSSActionPrefix + "GetBucketReplicationAction"    // unsupported
	OSSPutBucketReplicationAction    Action = OSSActionPrefix + "PutBucketReplicationAction"    // unsupported
	OSSDeleteBucketReplicationAction Action = OSSActionPrefix + "DeleteBucketReplicationAction" // unsupported

	// constants for POSIX file system interface
	POSIXReadAction  Action = POSIXActionPrefix + "Read"
	POSIXWriteAction Action = POSIXActionPrefix + "Write"

	// Object lock actions
	OSSPutObjectLockConfigurationAction Action = OSSActionPrefix + "PutObjectLockConfiguration" // unsupported
	OSSGetObjectLockConfigurationAction Action = OSSActionPrefix + "GetObjectLockConfiguration" // unsupported

	NoneAction Action = ""
)

var (
	AllActions = []Action{
		// Object storage interface actions
		OSSGetObjectAction,
		OSSPutObjectAction,
		OSSCopyObjectAction,
		OSSListObjectsAction,
		OSSDeleteObjectAction,
		OSSDeleteObjectsAction,
		OSSHeadObjectAction,
		OSSCreateBucketAction,
		OSSDeleteBucketAction,
		OSSHeadBucketAction,
		OSSListBucketsAction,
		OSSGetBucketPolicyAction,
		OSSPutBucketPolicyAction,
		OSSDeleteBucketPolicyAction,
		OSSGetBucketPolicyStatusAction,
		OSSGetBucketAclAction,
		OSSPutBucketAclAction,
		OSSGetObjectTorrentAction,
		OSSGetObjectAclAction,
		OSSPutObjectAclAction,
		OSSCreateMultipartUploadAction,
		OSSListMultipartUploadsAction,
		OSSUploadPartAction,
		OSSUploadPartCopyAction,
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
		OSSGetBucketLifecycleAction,
		OSSPutBucketLifecycleAction,
		OSSDeleteBucketLifecycleAction,
		OSSGetBucketVersioningAction,
		OSSPutBucketVersioningAction,
		OSSListObjectVersionsAction,
		OSSGetObjectLegalHoldAction,
		OSSPutObjectLegalHoldAction,
		OSSGetObjectRetentionAction,
		OSSPutObjectRetentionAction,
		OSSGetBucketEncryptionAction,
		OSSPutBucketEncryptionAction,
		OSSDeleteBucketEncryptionAction,
		OSSGetBucketCorsAction,
		OSSPutBucketCorsAction,
		OSSDeleteBucketCorsAction,
		OSSGetBucketWebsiteAction,
		OSSPutBucketWebsiteAction,
		OSSDeleteBucketWebsiteAction,
		OSSRestoreObjectAction,
		OSSGetPublicAccessBlockAction,
		OSSPutPublicAccessBlockAction,
		OSSDeletePublicAccessBlockAction,
		OSSGetBucketRequestPaymentAction,
		OSSPutBucketRequestPaymentAction,
		OSSGetBucketReplicationAction,
		OSSPutBucketReplicationAction,
		OSSDeleteBucketReplicationAction,
		OSSOptionsObjectAction,

		// POSIX file system interface actions
		POSIXReadAction,
		POSIXWriteAction,

		OSSPutObjectLockConfigurationAction,
		OSSGetObjectLockConfigurationAction,
	}
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

func (p Permission) MatchSubdir(subdir string) bool {
	if !strings.HasPrefix(string(p), string(BuiltinPermissionPrefix)) {
		return false
	}

	s := strings.TrimPrefix(string(p), string(BuiltinPermissionPrefix))

	if !subdirRegexp.MatchString(s) {
		return true
	}

	pars := strings.Split(s, ":")
	pars = pars[:len(pars)-1] //trim (Writable|ReadOnly) at the end
	for _, toCmp := range pars {
		if toCmp == "/" || toCmp == "" {
			return true
		}
		subdir = path.Clean("/" + subdir)
		toCmp = path.Clean("/" + toCmp)
		if strings.HasPrefix(subdir, toCmp) {
			tail := strings.TrimPrefix(subdir, toCmp)
			//match case 1:
			//subdir = "/a/b/c"
			//toCmp  = "/a/b/c"
			//tail   =       ""

			//match case 2:
			//subdir = "/a/b/c"
			//toCmp  = "/a/b"
			//tail   =     "/c"

			if tail == "" || strings.HasPrefix(tail, "/") {
				return true
			}
		}
	}

	return false
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
	permRegexp                = regexp.MustCompile("^perm:((builtin:((.*/*)([^/]*):*)(Writable|ReadOnly))|(custom:(\\w)+))$")
	builtinPermRegexp         = regexp.MustCompile("^perm:builtin:((.*/*)([^/]*):*)(Writable|ReadOnly)$")
	builtinWritablePermRegexp = regexp.MustCompile("^perm:builtin:((.*/*)([^/]*):*)Writable$")
	builtinReadOnlyPermRegexp = regexp.MustCompile("^perm:builtin:((.*/*)([^/]*):*)ReadOnly$")
	customPermRegexp          = regexp.MustCompile("^perm:custom:(\\w)+$")
	subdirRegexp              = regexp.MustCompile("((.*/*)([^/]*)):(Writable|ReadOnly)$")
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
			// Object storage interface actions
			OSSGetObjectAction,
			OSSListObjectsAction,
			OSSHeadObjectAction,
			OSSHeadBucketAction,
			OSSGetObjectTorrentAction,
			OSSGetObjectAclAction,
			OSSListPartsAction,
			OSSGetBucketLocationAction,
			OSSGetObjectTaggingAction,
			OSSListObjectVersionsAction,
			OSSGetObjectLegalHoldAction,
			OSSGetObjectRetentionAction,
			OSSGetBucketEncryptionAction,

			// file system interface
			POSIXReadAction,
		},
		BuiltinPermissionWritable: {
			// Object storage interface actions
			OSSGetObjectAction,
			OSSPutObjectAction,
			OSSCopyObjectAction,
			OSSListObjectsAction,
			OSSDeleteObjectAction,
			OSSDeleteObjectsAction,
			OSSHeadObjectAction,
			OSSHeadBucketAction,
			OSSGetObjectTorrentAction,
			OSSGetObjectAclAction,
			OSSPutObjectAclAction,
			OSSCreateMultipartUploadAction,
			OSSListMultipartUploadsAction,
			OSSUploadPartAction,
			OSSUploadPartCopyAction,
			OSSListPartsAction,
			OSSCompleteMultipartUploadAction,
			OSSAbortMultipartUploadAction,
			OSSGetBucketLocationAction,
			OSSGetObjectTaggingAction,
			OSSPutObjectTaggingAction,
			OSSDeleteObjectTaggingAction,
			OSSListObjectVersionsAction,
			OSSGetObjectLegalHoldAction,
			OSSPutObjectLegalHoldAction,
			OSSGetObjectRetentionAction,
			OSSPutObjectRetentionAction,
			OSSGetBucketEncryptionAction,

			// POSIX file system interface actions
			POSIXReadAction,
			POSIXWriteAction,
		},
	}
)

func BuiltinPermissionActions(perm Permission) Actions {
	var p Permission

	if builtinWritablePermRegexp.MatchString(string(perm)) {
		p = BuiltinPermissionWritable
	} else if builtinReadOnlyPermRegexp.MatchString(string(perm)) {
		p = BuiltinPermissionReadOnly
	}
	if actions, exists := builtinPermissionActionsMap[p]; exists {
		return actions
	}
	return nil
}
