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

import (
	"strings"

	"github.com/cubefs/cubefs/proto"

	"github.com/cubefs/cubefs/util"
	"github.com/google/uuid"
)

// Reference:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/amazon-s3-policy-keys.html
func ActionToUniqueRouteName(action proto.Action) (name string) {
	var id uuid.UUID
	var err error
	if id, err = uuid.NewRandom(); err != nil {
		name = action.String() + ":" + util.RandomString(32, util.UpperLetter|util.LowerLetter)
		return
	}
	name = action.String() + ":" + strings.ReplaceAll(id.String(), "-", "")
	return
}

func ActionFromRouteName(name string) proto.Action {
	routeSNLoc := routeSNRegexp.FindStringIndex(name)
	if len(routeSNLoc) != 2 {
		return proto.ParseAction(name)
	}
	return proto.ParseAction(name[:len(name)-33])
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
func IsIntersectionActions(actions proto.Actions, action proto.Action) bool {
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
