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

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type Effect string
type Principal map[string]StringSet
type Resource string

const (
	Allow Effect = "Allow"
	Deny         = "Deny"
)

type Statement struct {
	Sid          string    `json:"Sid,omitempty"`
	Effect       Effect    `json:"Effect"`
	Principal    Principal `json:"Principal"`
	Actions      StringSet `json:"Action,omitempty"`
	NotActions   StringSet `json:"NotAction,omitempty"`
	Resources    StringSet `json:"Resource,omitempty"`
	NotResources StringSet `json:"NotResource,omitempty"`
	Condition    Condition `json:"Condition,omitempty"`
}

func (s *Statement) Validate(bucket string) (bool, error) {
	return s.isValid(bucket)
}

func (s *Statement) isValid(bucket string) (bool, error) {
	//TODO:

	return true, nil
}

func (s Statement) IsAllowed(p *RequestParam) bool {
	checked := s.check(p)

	if s.Effect != Allow {
		return !checked
	}

	return checked
}

type CheckFuncs func(p *RequestParam) bool

func (s Statement) check(p *RequestParam) bool {
	checkFuncs := []CheckFuncs{
		s.checkPrincipal,
		s.checkNotActions,
		s.checkActions,
		s.checkNotResources,
		s.checkResources,
		s.checkConditions,
	}
	for _, f := range checkFuncs {
		checked := f(p)
		if !checked {
			return false
		}
	}

	return true
}

func (s Statement) checkPrincipal(p *RequestParam) bool {
	if len(s.Principal) == 0 {
		return true
	}
	for _, principal := range s.Principal {
		if principal.ContainsWild(p.AccessKey()) {
			return true
		}
	}

	return false
}

func (s Statement) checkResources(p *RequestParam) bool {
	if s.Resources.Empty() {
		return true
	}
	if s.Resources.ContainsRegex(p.resource) {
		return true
	}
	return false
}

func (s Statement) checkNotResources(p *RequestParam) bool {
	if s.NotResources.Empty() {
		return true
	}
	if s.NotResources.ContainsRegex(p.resource) {
		return false
	}
	return true
}
