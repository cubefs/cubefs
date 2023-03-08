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
	"regexp"
	"strings"

	"github.com/cubefs/cubefs/util/log"
)

type ActionElementType string
type ActionType []interface{}
type PrincipalElementType string
type PrincipalType map[string]interface{}
type ResourceElementType string

func (s *Statement) CheckPolicy(apiName string, uid string, conditionCheck map[string]string) PolicyCheckResult {
	if s.match(apiName, uid, conditionCheck) {
		return s.effect()
	}
	return POLICY_UNKNOW
}

func (s *Statement) effect() PolicyCheckResult {
	switch strings.ToLower(s.Effect) {
	case "allow":
		return POLICY_ALLOW
	case "deny":
		return POLICY_DENY
	default:
		return POLICY_UNKNOW
	}
}

func (s *Statement) match(apiName string, uid string, conditionCheck map[string]string) bool {
	if !s.matchPrincipal(uid) {
		log.LogDebugf("cannot match principal, uid:%v", uid)
		return false
	}
	if !s.matchAction(apiName) {
		log.LogDebugf("cannot match action, apiName:%v", apiName)
		return false
	}
	if !s.matchResource(apiName, conditionCheck[KEYNAME]) {
		log.LogDebugf("cannot match resource, apiName:%v, keyname:%v", apiName, conditionCheck[KEYNAME])
		return false
	}
	if !s.matchCondition(conditionCheck) {
		log.LogDebugf("cannot match condition, conditionCheck:%v", conditionCheck)
		return false
	}
	return true
}

//----------------------------------------------------------------------------------------------------------------
func (s *Statement) matchAction(apiName string) bool {

	log.LogDebug("start to match action")
	switch s.Action.(type) {
	case []interface{}: //["s3:PutObject", "s3:GetObject","s3:DeleteObject"]
		actions := s.Action.([]interface{})
		return ActionType(actions).match(apiName)

	case string: //"s3:ListBucket"
		return ActionElementType(s.Action.(string)).match(apiName)
	default:
		return false
	}
}

func (a ActionElementType) match(apiToMatch string) bool {
	a1 := strings.TrimPrefix(strings.ToLower(string(a)), S3_ACTION_PREFIX)
	if a1 == ACTION_ANY {
		return true
	}
	for _, action := range S3ActionToApis[a1] {
		if action == apiToMatch {
			return true
		}
	}
	return false
}

func (actions ActionType) match(apiToMatch string) bool {
	for _, a := range actions {
		if a1, ok := a.(string); ok {
			if ActionElementType(a1).match(apiToMatch) {
				return true
			}
		}
	}
	return false
}

//----------------------------------------------------------------------------------------------------------------
func (s *Statement) matchPrincipal(uid string) bool {
	switch s.Principal.(type) {
	case string: // "*" or "123"
		return PrincipalElementType(s.Principal.(string)).match(uid)
	case map[string]interface{}: //from json, {"AWS":["11", "22"]} or  {"AWS":"11"}  or {"AWS":"*"}
		p := s.Principal.(map[string]interface{})
		return PrincipalType(p).match(uid)
	default:
		return false
	}
}

func (p PrincipalElementType) match(uid string) bool {
	return p == Principal_Any || p == PrincipalElementType(uid)
}

func (p PrincipalType) match(uid string) bool {
	p1, ok := p[S3_PRINCIPAL_PREFIX]
	if !ok {
		return false
	}
	switch p1.(type) {
	case []interface{}:
		p2 := p1.([]interface{})
		for _, p3 := range p2 {
			if p4, ok := p3.(string); ok {
				if PrincipalElementType(p4).match(uid) {
					return true
				}
			}
		}
		return false
	case string:
		return PrincipalElementType(p1.(string)).match(uid)
	default:
		return false
	}
}

//----------------------------------------------------------------------------------------------------------------
func (s *Statement) matchResource(apiName string, keyname interface{}) bool {

	if IsBucketApi(apiName) {
		return s.matchBucketInResource()
	}

	if keyname == nil { //keyname shoudn't be nil for object api, even if keyname = ""
		return false
	}
	if _, ok := keyname.(string); !ok {
		return false
	}
	return s.matchKeyInResource(keyname.(string))

}

func (s *Statement) matchBucketInResource() bool {
	//if resource list contains bucket format, then match, since bucketId already checked when put policy.
	switch s.Resource.(type) {
	case string:
		return ResourceElement(s.Resource.(string)).isBucketFormat()
	case []interface{}:
		r := s.Resource.([]interface{})
		for _, r1 := range r {
			if r2, ok := r1.(string); ok {
				if ResourceElement(r2).isBucketFormat() {
					return true
				}
			}
		}
		return false
	default:
		return false
	}

}

func (s *Statement) matchKeyInResource(keyname string) bool { //可以处理 keyname="" 的case

	switch s.Resource.(type) {
	case string:
		return ResourceElement(s.Resource.(string)).match(keyname)
	case []interface{}:
		r := s.Resource.([]interface{})
		for _, r1 := range r {
			if r2, ok := r1.(string); ok {
				if ResourceElement(r2).match(keyname) {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

func ResourceElement(r string) ResourceElementType {
	r1 := strings.TrimPrefix(r, S3_RESOURCE_PREFIX)
	return ResourceElementType(r1)
}

func (r ResourceElementType) isBucketFormat() bool { //bucketFormat  support bucket api
	return !r.isKeyFormat()
}

func (r ResourceElementType) isKeyFormat() bool { //keyFormat  support object api
	return strings.Contains(string(r), "/")
}

func (r ResourceElementType) match(keyname string) bool {
	if !r.isKeyFormat() {
		return false
	}
	//extract regex :  "examplebucket/abc/*"  => rawPattern="abc/*"
	r1 := string(r)
	i := strings.Index(r1, "/")
	rawPattern := r1[i+1:]
	if rawPattern == "" {
		return false
	}
	keyPattern := makeRegexPattern(rawPattern)
	ok, _ := regexp.MatchString(keyPattern, keyname)
	return ok
}

func makeRegexPattern(raw string) string {
	pattern := strings.Replace(raw, "?", ".", -1)
	pattern = strings.Replace(pattern, "*", ".*", -1)
	pattern = "^" + pattern + "$"
	return pattern
}

//----------------------------------------------------------------------------------------------------------------
func (s *Statement) matchCondition(conditionCheck map[string]string) bool {
	//condition is optional
	if s.Condition == nil {
		return true
	}
	return s.Condition.Evaluate(conditionCheck)

}
