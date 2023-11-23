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
	"fmt"
	"net/http"
)

// String like operation. It checks whether value by Key in given
// values map is wildcard matching in condition values.
// For example,
//   - if values = ["mybucket/foo*"], at evaluate() it returns whether string
//     in value map for Key is wildcard matching in values.
type stringLikeOp struct {
	m map[Key]StringSet
}

// evaluates to check whether value by Key in given values is wildcard
// matching in condition values.
func (op stringLikeOp) evaluate(values map[string]string) bool {
	for k, v := range op.m {
		requestValue, ok := values[http.CanonicalHeaderKey(k.Name())]
		if !ok {
			requestValue = values[k.Name()]
		}
		nothingMatched := true //all values not matched
		for p := range v {
			if Match(p, requestValue) {
				nothingMatched = false
			}
		}
		if nothingMatched {
			return false
		}
	}

	return true
}

//  returns condition key which is used by this condition operation.
func (op stringLikeOp) keys() KeySet {
	keys := make(KeySet)
	for key := range op.m {
		keys.Add(key)
	}
	return keys
}

// returns "StringLike"  operator.
func (op stringLikeOp) operator() operator {
	return stringLike
}

// returns map representation of this operation.
func (op stringLikeOp) toMap() map[Key]ValueSet {
	resultMap := make(map[Key]ValueSet)
	for k, v := range op.m {
		if !k.IsValid() {
			return nil
		}
		values := NewValueSet()
		for _, value := range v.ToSlice() {
			values.Add(NewStringValue(value))
		}
		resultMap[k] = values
	}

	return resultMap
}

// returns new StringLike operation.
func newStringLikeOp(m map[Key]ValueSet) (Operation, error) {
	newMap, err := parseMap(m, stringLike)
	if err != nil {
		return nil, err
	}
	return NewStringLikeOp(newMap)
}

// NewStringLikeOp - returns new StringLike operation.
func NewStringLikeOp(m map[Key]StringSet) (Operation, error) {
	return &stringLikeOp{m: m}, nil
}

func parseMap(m map[Key]ValueSet, op operator) (map[Key]StringSet, error) {
	newMap := make(map[Key]StringSet)
	for k, v := range m {
		valueStrings, err := valuesToStringSlice(op, v)
		if err != nil {
			return nil, err
		}
		sset := CreateStringSet(valueStrings...)
		newMap[k] = sset
	}
	return newMap, nil
}

// stringNotLikeOp - String not like operation. It checks whether value by Key in given
// values map is NOT wildcard matching in condition values.
// For example,
//   - if values = ["mybucket/foo*"], at evaluate() it returns whether string
//     in value map for Key is NOT wildcard matching in values.
type stringNotLikeOp struct {
	stringLikeOp
}

// evaluates to check whether value by Key in given values is NOT wildcard
// matching in condition values.
func (op stringNotLikeOp) evaluate(values map[string]string) bool {
	return !op.stringLikeOp.evaluate(values)
}

// returns "StringNotLike" function operator.
func (op stringNotLikeOp) operator() operator {
	return stringNotLike
}

//  returns new StringNotLike operation.
func newStringNotLikeOp(m map[Key]ValueSet) (Operation, error) {
	newMap, err := parseMap(m, stringNotLike)
	if err != nil {
		return nil, err
	}

	return NewStringNotLikeOp(newMap)
}

//  returns new StringNotLike operation.
func NewStringNotLikeOp(m map[Key]StringSet) (Operation, error) {

	return &stringNotLikeOp{stringLikeOp{m}}, nil
}

func valuesToStringSlice(op operator, values ValueSet) ([]string, error) {
	var valueStrings []string

	for value := range values {
		// if AWS supports non-string values, we would need to support it.
		s, err := value.GetString()
		if err != nil {
			return nil, fmt.Errorf(invalidStringValue, op)
		}

		valueStrings = append(valueStrings, s)
	}

	return valueStrings, nil
}

func Match(pattern, name string) (matched bool) {
	if pattern == "" {
		return name == pattern
	}
	if pattern == "*" {
		return true
	}
	rname := make([]rune, 0, len(name))
	rpattern := make([]rune, 0, len(pattern))
	for _, r := range name {
		rname = append(rname, r)
	}
	for _, r := range pattern {
		rpattern = append(rpattern, r)
	}
	simple := false // Does extended wildcard '*' and '?' match.
	return deepMatchRune(rname, rpattern, simple)
}

func deepMatchRune(str, pattern []rune, simple bool) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		default:
			if len(str) == 0 || str[0] != pattern[0] {
				return false
			}
		case '?':
			if len(str) == 0 && !simple {
				return false
			}
		case '*':
			return deepMatchRune(str, pattern[1:], simple) ||
				(len(str) > 0 && deepMatchRune(str[1:], pattern, simple))
		}
		str = str[1:]
		pattern = pattern[1:]
	}
	return len(str) == 0 && len(pattern) == 0
}
