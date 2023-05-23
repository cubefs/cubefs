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
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	defaultPolicyVersion   = "2012-10-17"
	policyS3ResourcePrefix = "arn:aws:s3:::"
)

type PolicyV2 struct {
	Version    string        `json:"Version"`
	Statements []StatementV2 `json:"Statement"`
}

func (p *PolicyV2) isValid() error {
	if p.Version != defaultPolicyVersion {
		return errors.New("invalid version expecting 2012-10-17")
	}
	if len(p.Statements) == 0 {
		return errors.New("statement cannot be empty")
	}
	if len(p.Statements) > maxStatementNum {
		return errors.New("too many policy statement")
	}
	for _, stmt := range p.Statements {
		if err := stmt.IsValid(); err != nil {
			return err
		}
	}
	return nil
}

func (p *PolicyV2) IsAllow(action, bucket, key string) bool {
	var allow bool
	for _, stmt := range p.Statements {
		if stmt.MatchAction(action) && stmt.MatchResource(bucket, key) {
			if !stmt.IsAllow() {
				return false
			}
			allow = true
		}
	}
	return allow
}

func (p *PolicyV2) Validate() error {
	return p.isValid()
}

func ParsePolicyV2Config(data string) (*PolicyV2, error) {
	if len(data) < 1 || len(data) > 2048 {
		return nil, errors.New("policy should not be less than 1 or more than 2048 characters")
	}
	policy := new(PolicyV2)
	dec := json.NewDecoder(strings.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(policy); err != nil {
		return nil, err
	}

	return policy, policy.Validate()
}

type StatementV2 struct {
	Sid       string      `json:"Sid,omitempty"`
	Effect    string      `json:"Effect"`
	Action    interface{} `json:"Action"`
	Resource  interface{} `json:"Resource"`
	Condition Condition   `json:"Condition,omitempty"`
}

func (s *StatementV2) IsValid() error {
	if !utf8.ValidString(s.Sid) {
		return errors.New("invalid sid")
	}
	if s.Effect != Allow && s.Effect != Deny {
		return errors.New("invalid effect")
	}
	if s.Action == nil {
		return errors.New("action must not be empty")
	}
	if s.Resource == nil {
		return errors.New("resource must not be empty")
	}

	if !s.isActionValid() {
		return errors.New("invalid action")
	}

	if !s.isResourceValid() {
		return errors.New("invalid resource")
	}

	return nil
}

func (s *StatementV2) isActionValid() bool {
	switch action := s.Action.(type) {
	case string:
		return isActionValid(action)
	case []interface{}:
		if len(action) == 0 {
			return false
		}
		for _, a1 := range action {
			if a2, ok := a1.(string); !ok || !isActionValid(a2) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (s *StatementV2) isResourceValid() bool {
	switch resource := s.Resource.(type) {
	case string:
		return isResourceValid(resource)
	case []interface{}:
		if len(resource) == 0 {
			return false
		}
		for _, r1 := range resource {
			if r2, ok := r1.(string); !ok || !isResourceValid(r2) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (s *StatementV2) IsAllow() bool {
	return s.Effect == Allow
}

func (s *StatementV2) MatchAction(action string) bool {
	switch act := s.Action.(type) {
	case string:
		return policyActionMatch(act, action)
	case []interface{}:
		for _, a1 := range act {
			if a2, ok := a1.(string); ok && policyActionMatch(a2, action) {
				return true
			}
		}
	}
	return false
}

func (s *StatementV2) MatchResource(bucket, key string) bool {
	switch resource := s.Resource.(type) {
	case string:
		return policyResourceMatch(resource, bucket, key)
	case []interface{}:
		for _, r1 := range resource {
			r2, ok := r1.(string)
			if ok && policyResourceMatch(r2, bucket, key) {
				return true
			}
		}
	}
	return false
}

func isActionValid(action string) bool {
	// todo: action supported check
	return action == "*" || strings.HasPrefix(action, "s3:")
}

func isResourceValid(resource string) bool {
	if resource == "*" { // all resource
		return true
	}
	if !strings.HasPrefix(resource, policyS3ResourcePrefix) { // not s3 resource
		return false
	}
	resource = strings.TrimSpace(strings.TrimPrefix(resource, policyS3ResourcePrefix))
	if resource == "" { // invalid s3 resource
		return false
	}
	bucketKey := strings.SplitN(resource, "/", 2)
	// only bucket, otherwise none of them can be empty
	return len(bucketKey) < 2 || (bucketKey[0] != "" && bucketKey[1] != "")
}

func policyActionMatch(act, api string) bool {
	return act == "*" || act == "s3:*" || act == api
}

func policyResourceMatch(resource, bucket, key string) bool {
	if resource == "*" {
		return true
	}
	if !strings.HasPrefix(resource, policyS3ResourcePrefix) {
		return false
	}
	resource = strings.TrimSpace(strings.TrimPrefix(resource, policyS3ResourcePrefix))
	if resource == "" {
		return false
	}

	bucketKey := strings.SplitN(resource, "/", 2)
	matched, _ := regexp.MatchString(makeRegexPattern(bucketKey[0]), bucket)
	if !matched {
		return false
	}
	if len(bucketKey) == 1 && key == "" {
		return true
	}
	if len(bucketKey) == 1 {
		return bucketKey[0] == "*"
	}

	matched, _ = regexp.MatchString(makeRegexPattern(bucketKey[1]), key)
	return matched
}
