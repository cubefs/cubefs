/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The ChubaoFS Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package objectnode

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type ActionType string

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
const (
	PolicyDefaultVersion  = "2012-10-17"
	BucketPolicyLimitSize = 20 * 1024 //Bucket policies are limited to 20KB
	ArnSplitToken         = ":"
)

//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type Policy struct {
	Version    string      `json:"Version"`
	Id         string      `json:"Id,omitempty"`
	Statements []Statement `json:"Statement,omitempty"`
}

func (p *Policy) IsEmpty() bool {
	return len(p.Statements) == 0
}

// arn:partition:service:region:account-id:resource-id
// arn:partition:service:region:account-id:resource-type/resource-id
// arn:partition:service:region:account-id:resource-type:resource-id
type Arn struct {
	arn          Resource
	partition    string //aws
	service      string //s3/iam
	region       string //
	accountId    string //
	resourceType string //
	resourceId   string //
}

func parseArn(str string) (*Arn, error) {
	items := strings.Split(str, ArnSplitToken)
	if len(items) < 4 {
		log.LogErrorf("Arn is invalid: %v", str)
		return nil, errors.New("invalid arn")
	}
	arn := &Arn{
		partition: items[1],
		service:   items[2],
		region:    items[3],
		accountId: items[4],
	}

	if len(items) > 6 {
		arn.resourceType = items[5]
		arn.resourceId = items[6]
	} else {
		arn.resourceId = items[5]
	}

	return arn, nil
}

// write bucket policy into store and update vol policy meta
func storeBucketPolicy(bytes []byte, vol *Volume) (*Policy, error) {
	policy := &Policy{}
	err2 := json.Unmarshal(bytes, policy)
	if err2 != nil {
		log.LogErrorf("policy unmarshal err: %v", err2)
		return nil, err2
	}

	// validate policy
	ok, err3 := policy.Validate(vol.name)
	if err3 != nil {
		log.LogErrorf("policy validate err: %v", err2)
		return nil, err3
	}
	if !ok {
		return nil, errors.New("policy is invalid")
	}

	// put policy bytes into store
	err4 := vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSPolicy, bytes)
	if err4 != nil {
		return nil, err4
	}

	vol.metaLoader.storePolicy(policy)

	return policy, nil
}

func deleteBucketPolicy(vol *Volume) (err error) {
	if err = vol.store.Delete(vol.name, bucketRootPath, XAttrKeyOSSPolicy); err != nil {
		return err
	}
	return nil
}

func ParsePolicy(r io.Reader, bucket string) (*Policy, error) {
	var policy Policy
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(&policy); err != nil {
		return nil, err
	}

	if ok, err := policy.Validate(bucket); !ok {
		return nil, err
	}

	return &policy, nil
}

func (p Policy) isValid() (bool, error) {
	if p.Version == "" {
		return false, errors.New("policy version cannot be empty")
	}

	return true, nil
}

func (p Policy) Validate(bucket string) (bool, error) {
	if ok, err1 := p.isValid(); !ok {
		return false, err1
	}

	for _, s := range p.Statements {
		if ok, err := s.Validate(bucket); !ok {
			return false, err
		}
	}

	return true, nil
}

// check policy is allowed for request
// https://docs.aws.amazon.com/zh_cn/IAM/latest/UserGuide/reference_policies_evaluation-logic.html
func (p *Policy) IsAllowed(params *RequestParam, isOwner bool) bool {
	for _, s := range p.Statements {
		if s.Effect == Deny {
			if !s.IsAllowed(params) {
				log.LogDebugf("policy deny cause of %v, %v", s, params)
				return false
			}
		}
	}

	//is owner
	if isOwner {
		return true
	}

	for _, s := range p.Statements {
		if s.Effect == Allow {
			if s.IsAllowed(params) {
				log.LogDebugf("policy allow cause of %v, %v", s, params)
				return true
			}
		}
	}

	log.LogDebugf("policy deny cause of %v, request: %v", p, params)

	return false
}

func (o *ObjectNode) policyCheck(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			err error
			ec  *ErrorCode
		)
		allowed := false
		defer func() {
			if allowed {
				f(w, r)
			} else {
				if ec == nil {
					ec = AccessDenied
				}
				o.errorResponse(w, r, err, ec)
			}
		}()

		param := ParseRequestParam(r)

		if param.Bucket() == "" {
			log.LogDebugf("policyCheck: no bucket specified: requestID(%v)", GetRequestID(r))
			allowed = true
			return
		}

		// A create bucket action do not need to check any user policy and volume policy.
		if param.action == proto.OSSCreateBucketAction {
			allowed = true
			return
		}

		// Check user policy
		var volume *Volume
		if bucket := mux.Vars(r)["bucket"]; len(bucket) > 0 {
			if volume, err = o.getVol(bucket); err != nil {
				allowed = false
				if err == proto.ErrVolNotExists {
					ec = NoSuchBucket
					return
				}
				ec = InternalErrorCode(err)
				return
			}
		}
		var userInfo *proto.UserInfo
		isOwner := false
		if userInfo, err = o.getUserInfoByAccessKey(param.AccessKey()); err == nil {
			// White list for admin and root user.
			if userInfo.UserType == proto.UserTypeRoot || userInfo.UserType == proto.UserTypeAdmin {
				log.LogDebugf("policyCheck: user is admin: requestID(%v) userID(%v) accessKey(%v) volume(%v)",
					GetRequestID(r), userInfo.UserID, param.AccessKey(), param.Bucket())
				allowed = true
				return
			}
			var userPolicy = userInfo.Policy
			isOwner = userPolicy.IsOwn(param.Bucket())
			subdir := strings.TrimRight(param.Object(), "/")
			if subdir == "" {
				subdir = r.URL.Query().Get(ParamPrefix)
			}
			if !isOwner && !userPolicy.IsAuthorized(param.Bucket(), subdir, param.Action()) {
				log.LogDebugf("policyCheck: user no permission: url(%v) subdir(%v) requestID(%v) userID(%v) accessKey(%v) volume(%v) object(%v) action(%v)",
					r.URL, subdir, GetRequestID(r), userInfo.UserID, param.AccessKey(), param.Bucket(), param.Object(), param.Action())
				allowed = false
				return
			}
		} else if (err == proto.ErrAccessKeyNotExists || err == proto.ErrUserNotExists) && volume != nil {
			if ak, _ := volume.OSSSecure(); ak != param.AccessKey() {
				allowed = false
				return
			}
			isOwner = true
		} else {
			log.LogErrorf("policyCheck: load user policy from master fail: requestID(%v) accessKey(%v) err(%v)",
				GetRequestID(r), param.AccessKey(), err)
			allowed = false
			return
		}

		var vol *Volume
		var acl *AccessControlPolicy
		var policy *Policy
		var loadBucketMeta = func(bucket string) (err error) {
			if vol, err = o.getVol(bucket); err != nil {
				return
			}
			if acl, err = vol.metaLoader.loadACL(); err != nil {
				return
			}
			if policy, err = vol.metaLoader.loadPolicy(); err != nil {
				return
			}
			return
		}
		if err = loadBucketMeta(param.Bucket()); err != nil {
			log.LogErrorf("policyCheck: load bucket metadata fail: requestID(%v) err(%v)", GetRequestID(r), err)
			allowed = false
			ec = NoSuchBucket
			return
		}

		if vol != nil && policy != nil && !policy.IsEmpty() {
			allowed = policy.IsAllowed(param, isOwner)
			if !allowed {
				log.LogWarnf("policyCheck: bucket policy not allowed: requestID(%v) userID(%v) accessKey(%v) volume(%v) action(%v)",
					GetRequestID(r), userInfo, param.AccessKey(), param.Bucket(), param.Action())
				return
			}
		}

		if vol != nil && acl != nil && !acl.IsAclEmpty() {
			allowed = acl.IsAllowed(param, isOwner)
			if !allowed {
				log.LogWarnf("policyCheck: bucket ACL not allowed: requestID(%v) userID(%v) accessKey(%v) volume(%v) action(%v)",
					GetRequestID(r), userInfo, param.AccessKey(), param.Bucket(), param.Action())
				return
			}
		}

		allowed = true
		log.LogDebugf("policyCheck: action allowed: requestID(%v) userID(%v) accessKey(%v) volume(%v) action(%v)",
			GetRequestID(r), userInfo, param.AccessKey(), param.Bucket(), param.Action())
	}
}
