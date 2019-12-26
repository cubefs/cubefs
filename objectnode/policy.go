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

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/chubaofs/chubaofs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
const (
	PolicyDefaultVersion  = "2012-10-17"
	BucketPolicyLimitSize = 20 * 1024 //Bucket policies are limited to 20KB
	ArnSplitToken         = ":"
)

//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type Policy struct {
	Version    string      `json:"Version"`
	Id         string      `json:"Id,omnistring"`
	Statements []Statement `json:"Statement,omitempty"`
}

//
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
func storeBucketPolicy(bytes []byte, vol *volume) (*Policy, error) {
	store, err1 := vol.vm.GetStore()
	if err1 != nil {
		return nil, err1
	}

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
	err4 := store.Put(vol.name, bucketRootPath, XAttrKeyOSSPolicy, bytes)
	if err4 != nil {
		return nil, err4
	}

	vol.storePolicy(policy)

	return policy, nil
}

//
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
// 如果适用策略包含 Deny 语句，则请求会导致显式拒绝。
// 如果应用于请求的策略包含一个 Allow 语句和一个 Deny 语句，Deny 语句优先于 Allow 语句。将显式拒绝请求。
// 当没有适用的 Deny 语句但也没有适用的 Allow 语句时，会发生隐式拒绝。
func (p *Policy) IsAllowed(params *RequestParam) bool {
	for _, s := range p.Statements {
		if s.Effect == Deny {
			if !s.IsAllowed(params) {
				return false
			}
		}
	}

	if params.isOwner {
		return true
	}

	for _, s := range p.Statements {
		if s.Effect == Allow {
			if s.IsAllowed(params) {
				return true
			}
		}
	}

	return false
}

func (o *ObjectNode) policyCheck(f http.HandlerFunc, actions []Action) http.HandlerFunc {
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
					ec = &AccessDenied
				}
				o.errorResponse(w, r, err, ec)
			}
		}()

		param, err1 := o.parseRequestParam(r)
		if err1 != nil {
			err = err1
			log.LogInfof("parse Request Param err %v", err)
			return
		}
		if param.vol == nil {
			log.LogInfof("vol is null")
			allowed = true
			return
		}

		param.actions = actions

		//check policy and acl
		acl := param.vol.loadACL()
		policy := param.vol.loadPolicy()
		//check ip policy
		if policy != nil {
			allowed = policy.IsAllowed(param)
			if !allowed {
				log.LogWarnf("policy not allowed %v", param)
				return
			}
		}

		if acl != nil {
			allowed = acl.IsAllowed(param)
			if !allowed {
				log.LogWarnf("acl not allowed %v", param)
				return
			}
		}

	}
}
