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

package objectnode

import (
	"strings"

	"github.com/chubaofs/chubaofs/util"
	"github.com/google/uuid"
)

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/amazon-s3-policy-keys.html
type Action string

func (a Action) String() string {
	return string(a)
}

func (a Action) IsKnown() bool {
	return len(a) != 0 && a != UnknownAction
}

func (a Action) UniqueRouteName() (name string) {
	var id uuid.UUID
	var err error
	if id, err = uuid.NewRandom(); err != nil {
		name = a.String() + ":" + util.RandomString(32, util.UpperLetter|util.LowerLetter)
		return
	}
	name = a.String() + ":" + strings.ReplaceAll(id.String(), "-", "")
	return
}

const (
	OSSActionPrefix = "oss:action:"

	GetObjectAction                  Action = OSSActionPrefix + "GetObject"
	PutObjectAction                  Action = OSSActionPrefix + "PutObject"
	CopyObjectAction                 Action = OSSActionPrefix + "CopyObject"
	ListObjectsAction                Action = OSSActionPrefix + "ListObjects"
	DeleteObjectAction               Action = OSSActionPrefix + "DeleteObject"
	DeleteObjectsAction              Action = OSSActionPrefix + "DeleteObjects"
	HeadObjectAction                 Action = OSSActionPrefix + "HeadObject"
	CreateBucketAction               Action = OSSActionPrefix + "CreateBucket"
	DeleteBucketAction               Action = OSSActionPrefix + "DeleteBucket"
	HeadBucketAction                 Action = OSSActionPrefix + "HeadBucket"
	ListBucketAction                 Action = OSSActionPrefix + "ListBucket"
	ListBucketVersionsAction         Action = OSSActionPrefix + "ListBucketVersions"
	ListBucketMultipartUploadsAction Action = OSSActionPrefix + "ListBucketMultipartUploads"
	GetBucketPolicyAction            Action = OSSActionPrefix + "GetBucketPolicy"
	PutBucketPolicyAction            Action = OSSActionPrefix + "PutBucketPolicy"
	GetBucketAclAction               Action = OSSActionPrefix + "GetBucketAcl"
	PutBucketAclAction               Action = OSSActionPrefix + "PutBucketAcl"
	GetObjectAclAction               Action = OSSActionPrefix + "GetObjectAcl"
	GetObjectVersionAction           Action = OSSActionPrefix + "GetObjectVersion"
	PutObjectVersionAction           Action = OSSActionPrefix + "PutObjectVersion"
	GetObjectTorrentAction           Action = OSSActionPrefix + "GetObjectTorrent"
	PutObjectTorrentAction           Action = OSSActionPrefix + "PutObjectTorrent"
	PutObjectAclAction               Action = OSSActionPrefix + "PutObjectAcl"
	GetObjectVersionAclAction        Action = OSSActionPrefix + "GetObjectVersionAcl"
	PutObjectVersionAclAction        Action = OSSActionPrefix + "PutObjectVersionAcl"
	DeleteBucketPolicyAction         Action = OSSActionPrefix + "DeleteBucketPolicy"
	CreateMultipartUploadAction      Action = OSSActionPrefix + "CreateMultipartUpload"
	ListMultipartUploadsAction       Action = OSSActionPrefix + "ListMultipartUploads"
	UploadPartAction                 Action = OSSActionPrefix + "UploadPart"
	ListPartsAction                  Action = OSSActionPrefix + "ListParts"
	CompleteMultipartUploadAction    Action = OSSActionPrefix + "CompleteMultipartUpload"
	AbortMultipartUploadAction       Action = OSSActionPrefix + "AbortMultipartUpload"
	GetBucketLocationAction          Action = OSSActionPrefix + "GetBucketLocation"
	GetObjectXAttrAction             Action = OSSActionPrefix + "GetObjectXAttr"
	PutObjectXAttrAction             Action = OSSActionPrefix + "PutObjectXAttr"
	ListObjectXAttrsAction           Action = OSSActionPrefix + "ListObjectXAttrs"
	DeleteObjectXAttrAction          Action = OSSActionPrefix + "DeleteObjectXAttr"
	GetObjectTaggingAction           Action = OSSActionPrefix + "GetObjectTagging"
	PutObjectTaggingAction           Action = OSSActionPrefix + "PutObjectTagging"
	DeleteObjectTaggingAction        Action = OSSActionPrefix + "DeleteObjectTagging"
	GetBucketTaggingAction           Action = OSSActionPrefix + "GetBucketTagging"
	PutBucketTaggingAction           Action = OSSActionPrefix + "PutBucketTagging"
	DeleteBucketTaggingAction        Action = OSSActionPrefix + "DeleteBucketTagging"

	UnknownAction Action = OSSActionPrefix + "Unknown"
)

var (
	AllActions = []Action{
		GetObjectAction,
		PutObjectAction,
		CopyObjectAction,
		ListObjectsAction,
		DeleteObjectAction,
		HeadObjectAction,
		CreateBucketAction,
		DeleteBucketAction,
		HeadBucketAction,
		ListBucketAction,
		ListBucketVersionsAction,
		ListBucketMultipartUploadsAction,
		GetBucketPolicyAction,
		PutBucketPolicyAction,
		GetBucketAclAction,
		PutBucketAclAction,
		GetObjectAclAction,
		GetObjectVersionAction,
		PutObjectVersionAction,
		GetObjectTorrentAction,
		PutObjectTorrentAction,
		PutObjectAclAction,
		GetObjectVersionAclAction,
		PutObjectVersionAclAction,
		DeleteBucketPolicyAction,
		CreateMultipartUploadAction,
		ListMultipartUploadsAction,
		UploadPartAction,
		ListPartsAction,
		CompleteMultipartUploadAction,
		AbortMultipartUploadAction,
		GetBucketLocationAction,
		GetObjectXAttrAction,
		PutObjectXAttrAction,
		ListObjectXAttrsAction,
		DeleteObjectXAttrAction,
		GetObjectTaggingAction,
		PutObjectTaggingAction,
		DeleteObjectTaggingAction,
		GetBucketTaggingAction,
		PutBucketTaggingAction,
		DeleteBucketTaggingAction,
	}
)

func ActionFromString(str string) Action {
	if len(str) == 0 {
		return UnknownAction
	}
	for _, act := range AllActions {
		if act.String() == str {
			return act
		}
	}
	return UnknownAction
}

func ActionFromRouteName(name string) Action {
	routeSNLoc := routeSNRegexp.FindStringIndex(name)
	if len(routeSNLoc) != 2 {
		return ActionFromString(name)
	}
	return ActionFromString(name[:len(name)-33])
}

func (s Statement) checkActions(p *RequestParam) bool {
	if s.Actions.Empty() {
		return true
	}
	if s.Actions.ContainsWithAny(p.Action().String()) {
		return true
	}
	return false
}

func (s Statement) checkNotActions(p *RequestParam) bool {
	if s.NotActions.Empty() {
		return true
	}
	if s.NotActions.ContainsWithAny(p.Action().String()) {
		return false
	}
	return true
}

//
func IsIntersectionActions(actions []Action, action Action) bool {
	if len(actions) == 0 {
		return true
	}
	for _, act := range actions {
		if act == action {
			return true
		}
	}
	return false
}

type Actions []Action

func (actions Actions) Constant(action Action) bool {
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
