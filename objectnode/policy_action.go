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

const (
	OSSActionPrefix = "oss:action:"

	GetObjectAction                  Action = OSSActionPrefix + "GetObject"
	PutObjectAction                         = OSSActionPrefix + "PutObject"
	CopyObjectAction                        = OSSActionPrefix + "CopyObject"
	ListObjectsAction                       = OSSActionPrefix + "ListObjects"
	DeleteObjectAction                      = OSSActionPrefix + "DeleteObject"
	DeleteObjectsAction                     = OSSActionPrefix + "DeleteObjects"
	HeadObjectAction                        = OSSActionPrefix + "HeadObject"
	CreateBucketAction                      = OSSActionPrefix + "CreateBucket"
	DeleteBucketAction                      = OSSActionPrefix + "DeleteBucket"
	HeadBucketAction                        = OSSActionPrefix + "HeadBucket"
	ListBucketAction                        = OSSActionPrefix + "ListBucket"
	ListBucketVersionsAction                = OSSActionPrefix + "ListBucketVersions"
	ListBucketMultipartUploadsAction        = OSSActionPrefix + "ListBucketMultipartUploads"
	GetBucketPolicyAction                   = OSSActionPrefix + "GetBucketPolicy"
	PutBucketPolicyAction                   = OSSActionPrefix + "PutBucketPolicy"
	GetBucketAclAction                      = OSSActionPrefix + "GetBucketAcl"
	PutBucketAclAction                      = OSSActionPrefix + "PutBucketAcl"
	GetObjectAclAction                      = OSSActionPrefix + "GetObjectAcl"
	GetObjectVersionAction                  = OSSActionPrefix + "GetObjectVersion"
	PutObjectVersionAction                  = OSSActionPrefix + "PutObjectVersion"
	GetObjectTorrentAction                  = OSSActionPrefix + "GetObjectTorrent"
	PutObjectTorrentAction                  = OSSActionPrefix + "PutObjectTorrent"
	PutObjectAclAction                      = OSSActionPrefix + "PutObjectAcl"
	GetObjectVersionAclAction               = OSSActionPrefix + "GetObjectVersionAcl"
	PutObjectVersionAclAction               = OSSActionPrefix + "PutObjectVersionAcl"
	DeleteBucketPolicyAction                = OSSActionPrefix + "DeleteBucketPolicy"
	CreateMultipartUploadAction             = OSSActionPrefix + "CreateMultipartUpload"
	ListMultipartUploadsAction              = OSSActionPrefix + "ListMultipartUploads"
	UploadPartAction                        = OSSActionPrefix + "UploadPart"
	ListPartsAction                         = OSSActionPrefix + "ListParts"
	CompleteMultipartUploadAction           = OSSActionPrefix + "CompleteMultipartUpload"
	AbortMultipartUploadAction              = OSSActionPrefix + "AbortMultipartUpload"
	GetBucketLocationAction                 = OSSActionPrefix + "GetBucketLocation"
	GetObjectXAttrAction                    = OSSActionPrefix + "GetObjectXAttr"
	PutObjectXAttrAction                    = OSSActionPrefix + "PutObjectAttr"
	ListObjectXAttrsAction                  = OSSActionPrefix + "ListObjectXAttrs"
	DeleteObjectXAttrAction                 = OSSActionPrefix + "DeleteObjectXAttr"
	GetObjectTaggingAction                  = OSSActionPrefix + "GetObjectTagging"
	PutObjectTaggingAction                  = OSSActionPrefix + "PutObjectTagging"
	DeleteObjectTaggingAction               = OSSActionPrefix + "DeleteObjectTagging"
	GetBucketTaggingAction                  = OSSActionPrefix + "GetBucketTagging"
	PutBucketTaggingAction                  = OSSActionPrefix + "PutBucketTagging"
	DeleteBucketTaggingAction               = OSSActionPrefix + "DeleteBucketTagging"

	UnknownAction = OSSActionPrefix + "Unknown"
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

func (s Statement) checkActions(p *RequestParam) bool {
	if s.Actions.Empty() {
		return true
	}
	for _, pa := range p.actions {
		if s.Actions.ContainsWithAny(string(pa)) {
			return true
		}
	}

	return false
}

func (s Statement) checkNotActions(p *RequestParam) bool {
	if s.NotActions.Empty() {
		return true
	}
	for _, pa := range p.actions {
		if s.NotActions.ContainsWithAny(string(pa)) {
			return false
		}
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
