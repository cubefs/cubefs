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

const (
	GetObjectAction                  Action = "s3:GetObject"
	PutObjectAction                         = "s3:PutObject"
	DeleteObjectAction                      = "s3:DeleteObject" //
	HeadObjectAction                        = "s3:HeadObject"
	CreateBucketAction                      = "s3:CreateBucket" //
	DeleteBucketAction                      = "s3:DeleteBucket"
	ListBucketAction                        = "s3:ListBucket"
	ListBucketVersionsAction                = "s3:ListBucketVersions" // List
	ListBucketMultipartUploadsAction        = "s3:ListBucketMultipartUploads"
	GetBucketPolicyAction                   = "s3:GetBucketPolicy"
	PutBucketPolicyAction                   = "s3:PutBucketPolicy"
	GetBucketAclAction                      = "s3:GetBucketAcl"
	PutBucketAclAction                      = "s3:PutBucketAcl"
	GetObjectAclAction                      = "s3:GetObjectAcl"
	GetObjectVersionAction                  = "s3:GetObjectVersion"
	PutObjectVersionAction                  = "s3:PutObjectVersion"
	GetObjectTorrentAction                  = "s3:GetObjectTorrent"
	PutObjectTorrentAction                  = "s3:PutObjectTorrent"
	PutObjectAclAction                      = "s3:PutObjectAcl"
	GetObjectVersionAclAction               = "s3:GetObjectVersionAcl"
	PutObjectVersionAclAction               = "s3:PutObjectVersionAcl"
	DeleteBucketPolicyAction                = "s3:DeleteBucketPolicy"
	ListMultipartUploadPartsAction          = "s3:ListMultipartUploadParts"
	AbortMultipartUploadAction              = "s3:AbortMultipartUpload"
	GetBucketLocationAction                 = "s3:GetBucketLocation"
)

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
func IsIntersectionActions(actions1, actions2 []Action) bool {
	if len(actions1) == 0 && len(actions2) == 0 {
		return true
	}
	as1, as2 := actions1, actions2
	if len(actions1) > len(actions2) {
		as1, as2 = actions2, actions1
	}
	for _, action1 := range as1 {
		for _, action2 := range as2 {
			if action1 == action2 {
				return true
			}
		}
	}

	return false
}
