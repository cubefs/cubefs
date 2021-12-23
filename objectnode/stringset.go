/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"strings"
)

//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type null struct{}

var (
	void null = struct{}{}
)

type StringSet struct {
	values map[string]null
}

func (ss StringSet) String() string {
	s := make([]string, 0, len(ss.values))
	for k := range ss.values {
		s = append(s, k)
	}
	return "[" + strings.Join(s, ",") + "]"
}

func (ss StringSet) MarshalJSON() ([]byte, error) {
	if len(ss.values) == 0 {
		return json.Marshal(nil)
	}
	array := make([]string, 0)
	for v := range ss.values {
		array = append(array, v)
	}
	return json.Marshal(array)
}

func (ss *StringSet) UnmarshalJSON(b []byte) error {
	ss.values = make(map[string]null)
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		ss.values[s] = void
		return nil
	}
	var sl []string
	if err := json.Unmarshal(b, &sl); err == nil {
		for _, s := range sl {
			ss.values[s] = void
		}
		return nil
	}

	var il []interface{}
	if err := json.Unmarshal(b, &il); err == nil {
		for _, i := range il {
			ss.values[i.(string)] = void
		}
		return nil
	}

	return nil
}

func (ss *StringSet) Empty() bool {
	return len(ss.values) == 0
}

func (ss *StringSet) ContainsWithAny(val string) bool {
	strs := strings.Split(val, ":")
	if len(strs) > 1 {
		prefix := strs[0]
		any := prefix + ":*"
		if _, ok1 := ss.values[any]; ok1 {
			return true
		}
	}
	return ss.Contains(val)
}

func (ss *StringSet) Contains(val string) bool {
	_, ok := ss.values[val]
	return ok
}

func (ss *StringSet) ContainsRegex(val string) bool {
	if ss.Contains("*") {
		return true
	}
	if ss.Contains(val) {
		return true
	}
	for k := range ss.values {
		if patternMatch(k, val) {
			return true
		}
	}
	return false
}

func (ss *StringSet) ContainsWild(val string) bool {
	if ss.Contains("*") {
		return true
	}
	if ss.Contains(val) {
		return true
	}

	return false
}

func (ss *StringSet) Intersection(set *StringSet) bool {
	for s := range ss.values {
		if set.ContainsWild(s) {
			return true
		}
	}

	return false
}
