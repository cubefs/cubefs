/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The CubeFS Authors.
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

import (
	"strings"

	"github.com/cubefs/cubefs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type Principal map[string]StringSet
type Resource string

const (
	Allow = "Allow"
	Deny  = "Deny"
)

type Statement struct {
	Sid       string      `json:"Sid"`
	Effect    string      `json:"Effect"`
	Principal interface{} `json:"Principal"` // map or string
	Action    interface{} `json:"Action"`    // []string or string
	Resource  interface{} `json:"Resource"`  // []string or string
	Condition Condition   `json:"Condition,omitempty"`
}

func (s Statement) IsAllowed(p *RequestParam) bool {

	return s.Effect == Allow
}

func (s *Statement) Validate(bucket string) (bool, error) {
	return s.isValid(bucket)
}

func (s *Statement) isValid(bucket string) (bool, error) {
	log.LogDebug("start to validate statement")
	// step 1: check required field
	if err := s.checkRequiredField(); err != nil {
		return false, err
	}

	//step2: check each field
	if !s.isEffectValid() {
		return false, ErrInvalidEffectValue
	}

	if !s.isPrincipalValid() {
		return false, ErrInvalidPricipalInPolicy
	}

	if !s.isActionValid() {
		return false, ErrInvalidActionInPolicy
	}

	if !s.isResourceValid(bucket) {
		return false, ErrInvalidResourceInPolicy
	}

	//step3: check action & resource valid combination
	if !s.isValidCombination() {
		return false, ErrInvalidActionResourceCombination
	}
	return true, nil
}

func (s *Statement) checkRequiredField() error {
	switch {
	case s.Effect == "":
		return ErrMissingEffectInPolicy
	case s.Principal == nil:
		return ErrMissingPrincipalInPolicy
	case s.Action == nil:
		return ErrMissingActionInPolicy
	case s.Resource == nil:
		return ErrMissingResourceInPolicy
	default:
		return nil
	}
}

func (s *Statement) isEffectValid() bool {
	e := strings.ToLower(s.Effect)
	if e == "allow" || e == "deny" {
		return true
	}
	return false
}

//  "Principal": "*" or "Principal" : {"AWS":"111122223333"} or "Principal" : {"AWS":["111122223333","444455556666"]}
func (s *Statement) isPrincipalValid() bool {
	//principal: uid must be "*" or uint32, and can't be 0
	switch s.Principal.(type) {
	case string: // "*" or "123"
		p := s.Principal.(string)
		if !PrincipalElementType(p).valid() {
			return false
		}
	case map[string]interface{}:
		p := s.Principal.(map[string]interface{})
		if !PrincipalType(p).valid() {
			return false
		}
	default:
		return false
	}
	return true
}

func (p PrincipalType) valid() bool {
	p1, ok := p[S3_PRINCIPAL_PREFIX]
	if !ok {
		return false
	}
	switch p1.(type) {
	case []interface{}:
		p2 := p1.([]interface{})
		if len(p2) == 0 {
			return false
		}
		for _, p3 := range p2 {
			p4, ok := p3.(string)
			if !ok {
				return false
			}
			if !PrincipalElementType(p4).valid() {
				return false
			}
		}
		return true
	case string:
		if !PrincipalElementType(p1.(string)).valid() {
			return false
		}
	default:
		return false
	}
	return true
}

func (p PrincipalElementType) valid() bool {
	return true
}

func (s *Statement) isActionValid() bool {
	switch s.Action.(type) {
	case []interface{}: //["s3:PutObject", "s3:GetObject","s3:DeleteObject"]
		actions := s.Action.([]interface{})
		return ActionType(actions).valid()
	case string: //"s3:ListBucket"
		action := s.Action.(string)
		return ActionElementType(action).valid()
	default:
		return false
	}
}

func (a ActionElementType) valid() bool {
	a1 := strings.TrimPrefix(strings.ToLower(string(a)), S3_ACTION_PREFIX)
	if a1 == ACTION_ANY {
		return true
	}
	_, ok := S3ActionToApis[a1]
	return ok
}

func (actions ActionType) valid() bool {
	if len(actions) == 0 {
		return false
	}
	for _, a := range actions {
		a1, ok := a.(string)
		if !ok {
			return false
		}
		if !ActionElementType(a1).valid() {
			return false
		}
	}
	return true
}

func (s *Statement) isResourceValid(bucketId string) bool {
	switch s.Resource.(type) {
	case string: // "Resource":"arn:aws:s3:::bucket/*"
		r := s.Resource.(string)
		return ResourceElement(r).valid(bucketId)
	case []interface{}: // "Resource":["arn:aws:s3:::bucket/abc/*"]
		r := s.Resource.([]interface{})
		if len(r) == 0 {
			return false
		}
		for _, r1 := range r {
			r2, ok := r1.(string)
			if !ok {
				return false
			}
			if !ResourceElement(r2).valid(bucketId) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (r ResourceElementType) valid(bucketId string) bool {
	bucket_key := strings.SplitN(string(r), "/", 2)
	if len(bucket_key) < 2 {
		return r == ResourceElementType(bucketId)
	}
	if bucket_key[0] != bucketId { //bucketId in resource list must be same with current bucketId
		return false
	}
	if bucket_key[1] == "" { // key can't be empty when bucket is followed by a slash
		return false
	}
	return true
}

func (s *Statement) isValidCombination() bool {
	//check action & resource valid combination
	hasBucketFormat, hasObjectFormat := s.getResourceFormat()
	switch s.Action.(type) {
	case string:
		action := s.Action.(string)
		return ActionElementType(action).matchResource(hasBucketFormat, hasObjectFormat)
	case []interface{}:
		actions := s.Action.([]interface{})
		return ActionType(actions).matchResource(hasBucketFormat, hasObjectFormat)
	default:
		return false
	}
}

func (a ActionElementType) matchResource(hasBucketFormat, hasObjectFormat bool) bool {
	a1 := strings.TrimPrefix(strings.ToLower(string(a)), S3_ACTION_PREFIX)
	if a1 == ACTION_ANY { // when action = "*", resource can contain "bucket" or "bucket/key"
		return hasBucketFormat || hasObjectFormat
	}
	if validBucketActions.Contain(string(a1)) {
		return hasBucketFormat
	}
	return hasObjectFormat
}

func (actions ActionType) matchResource(hasBucketFormat, hasObjectFormat bool) bool {
	if len(actions) == 0 {
		return false
	}
	for _, a := range actions { //every action should match
		if a1, ok := a.(string); ok {
			if !ActionElementType(a1).matchResource(hasBucketFormat, hasObjectFormat) {
				return false
			}
		}
	}
	return true
}

func (s *Statement) getResourceFormat() (hasBucketFormat, hasObjectFormat bool) {
	switch s.Resource.(type) {
	case string:
		r := s.Resource.(string)
		return ResourceElement(r).format()
	case []interface{}:
		r := s.Resource.([]interface{})
		if len(r) == 0 {
			return
		}
		for _, r1 := range r {
			if r2, ok := r1.(string); ok {
				isBucketFormat, isKeyFormat := ResourceElement(r2).format()
				if isBucketFormat {
					hasBucketFormat = true
				}
				if isKeyFormat {
					hasObjectFormat = true
				}
			}
		}
		return
	default:
		return false, false
	}
}

func (r ResourceElementType) format() (isBucketFormat, isKeyFormat bool) {
	isBucketFormat = r.isBucketFormat()
	isKeyFormat = r.isKeyFormat()
	return
}
