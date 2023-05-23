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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	ErrPostPolicyExpired = &ErrorCode{
		ErrorCode:    "PostPolicyExpired",
		ErrorMessage: "The policy in form upload is expired.",
		StatusCode:   http.StatusForbidden,
	}
	ErrInvalidPolicyEncoded = &ErrorCode{
		ErrorCode:    "InvalidPolicyEncoded",
		ErrorMessage: "The policy in form upload should be base64 encoded.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidJsonPolicy = &ErrorCode{
		ErrorCode:    "InvalidJsonPolicy",
		ErrorMessage: "The policy in form upload should be valid json format.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidPolicyExpiration = &ErrorCode{
		ErrorCode:    "InvalidPolicyExpiration",
		ErrorMessage: "The expiration in form policy should be ISO8601Layout and not empty.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMissingPolicyConditions = &ErrorCode{
		ErrorCode:    "MissingPolicyConditions",
		ErrorMessage: "The conditions in form policy should not be empty.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidPolicyCondType = &ErrorCode{
		ErrorCode:    "InvalidPolicyConditions",
		ErrorMessage: "The conditional field in form policy should be a valid type.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidPolicyValueType = &ErrorCode{
		ErrorCode:    "InvalidPolicyConditions",
		ErrorMessage: "The conditional field value in form policy should be a valid type.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidPolicyCondValue = &ErrorCode{
		ErrorCode:    "InvalidPolicyConditions",
		ErrorMessage: "The conditional field value in form policy should be a valid value.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMalformedPolicyCondValue = &ErrorCode{
		ErrorCode:    "InvalidPolicyConditions",
		ErrorMessage: "The conditional field value in form policy is not well-formed.",
		StatusCode:   http.StatusBadRequest,
	}
)

var startsWithSupported = map[string]bool{
	"$acl":                     true,
	"$bucket":                  false,
	"$cache-control":           true,
	"$content-type":            true,
	"$content-disposition":     true,
	"$content-encoding":        true,
	"$expires":                 true,
	"$key":                     true,
	"$redirect":                true,
	"$success_action_redirect": true,
	"$success_action_status":   false,
	"$x-amz-algorithm":         false,
	"$x-amz-credential":        false,
	"$x-amz-date":              false,
}

const (
	condEqual         = "eq"
	condStartsWith    = "starts-with"
	condContentLength = "content-length-range"
)

type PostPolicy struct {
	Expiration time.Time
	Conditions struct {
		Matches            []CondMatch
		ContentLengthRange ContentLengthRange
	}
}

type CondMatch struct {
	Operator string
	Key      string
	Value    string
}

type ContentLengthRange struct {
	Min int64
	Max int64
}

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html

func NewPostPolicy(data string) (*PostPolicy, error) {
	decodePolicy, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, ErrInvalidPolicyEncoded
	}

	var rawPolicy struct {
		Expiration string        `json:"expiration"`
		Conditions []interface{} `json:"conditions"`
	}
	dec := json.NewDecoder(bytes.NewReader(decodePolicy))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&rawPolicy); err != nil {
		return nil, ErrInvalidJsonPolicy
	}

	policy := new(PostPolicy)
	if policy.Expiration, err = time.Parse(time.RFC3339Nano, rawPolicy.Expiration); err != nil {
		return nil, ErrInvalidPolicyExpiration
	}
	if len(rawPolicy.Conditions) == 0 {
		return nil, ErrMissingPolicyConditions
	}

	for _, cond := range rawPolicy.Conditions {
		switch condVal := cond.(type) {
		case map[string]interface{}:
			if err = policy.parseMap(condVal); err != nil {
				return nil, err
			}
		case []interface{}:
			if err = policy.parseSlice(condVal); err != nil {
				return nil, err
			}
		default:
			return nil, ErrInvalidPolicyCondType
		}
	}

	return policy, nil
}

func (p *PostPolicy) parseMap(cond map[string]interface{}) error {
	for k, v := range cond {
		val, ok := v.(string)
		if !ok {
			return ErrInvalidPolicyValueType
		}
		p.Conditions.Matches = append(p.Conditions.Matches, CondMatch{
			Operator: condEqual,
			Key:      "$" + strings.ToLower(k),
			Value:    val,
		})
	}
	return nil
}

func (p *PostPolicy) parseSlice(cond []interface{}) error {
	if len(cond) != 3 {
		return ErrMalformedPolicyCondValue
	}

	cond0, ok := cond[0].(string)
	if !ok {
		return ErrInvalidPolicyValueType
	}

	switch strings.ToLower(cond0) {
	case condEqual, condStartsWith:
		for _, v := range cond {
			if _, ok = v.(string); !ok {
				return ErrInvalidPolicyValueType
			}
		}
		op, key, val := strings.ToLower(cond[0].(string)), strings.ToLower(cond[1].(string)), cond[2].(string)
		if !strings.HasPrefix(key, "$") {
			return ErrMalformedPolicyCondValue
		}
		if supported, found := startsWithSupported[key]; found {
			if op == condStartsWith && !supported {
				return &ErrorCode{
					ErrorCode:    "InvalidPolicyConditions",
					ErrorMessage: fmt.Sprintf("The conditional element %v does not support starts-with.", key),
					StatusCode:   http.StatusBadRequest,
				}
			}
		}
		p.Conditions.Matches = append(p.Conditions.Matches, CondMatch{Operator: op, Key: key, Value: val})
	case condContentLength:
		min, err := parseToInt64(cond[1])
		if err != nil || min < 0 {
			return ErrMalformedPolicyCondValue
		}
		max, err := parseToInt64(cond[2])
		if err != nil || min > max {
			return ErrMalformedPolicyCondValue
		}
		p.Conditions.ContentLengthRange = ContentLengthRange{Min: min, Max: max}
	default:
		return ErrInvalidPolicyCondValue
	}

	return nil
}

func (p *PostPolicy) Match(toMatch map[string]string) error {
	if !p.Expiration.After(time.Now().UTC()) {
		return ErrPostPolicyExpired
	}

	xAmzMeta := make(map[string]bool)
	for _, cond := range p.Conditions.Matches {
		if strings.HasPrefix(cond.Key, "$x-amz-meta-") {
			xAmzMeta[strings.TrimPrefix(cond.Key, "$")] = true
		}
	}
	for key := range toMatch {
		key = strings.ToLower(key)
		if strings.HasPrefix(key, "x-amz-meta-") && !xAmzMeta[key] {
			return &ErrorCode{
				ErrorCode:    "PolicyConditionNotMatch",
				ErrorMessage: fmt.Sprintf("The %v not match with the policy condition.", key),
				StatusCode:   http.StatusForbidden,
			}
		}
	}

	for _, cond := range p.Conditions.Matches {
		key := strings.TrimPrefix(cond.Key, "$")
		if !policyCondMatch(cond.Operator, toMatch[key], cond.Value) {
			return &ErrorCode{
				ErrorCode:    "PolicyConditionNotMatch",
				ErrorMessage: fmt.Sprintf("The %v not match with the policy condition.", key),
				StatusCode:   http.StatusForbidden,
			}
		}
	}

	if size := toMatch["content-length"]; size != "" {
		fsize, _ := strconv.ParseInt(size, 10, 64)
		if fsize < p.Conditions.ContentLengthRange.Min || fsize > p.Conditions.ContentLengthRange.Max {
			return &ErrorCode{
				ErrorCode:    "PolicyConditionNotMatch",
				ErrorMessage: "The content-length does not match the policy's content-length-range.",
				StatusCode:   http.StatusForbidden,
			}
		}
	}

	return nil
}

func policyCondMatch(op, input, val string) bool {
	switch op {
	case condEqual:
		return input == val
	case condStartsWith:
		return strings.HasPrefix(input, val)
	}
	return false
}

func parseToInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		i, err := strconv.Atoi(v)
		return int64(i), err
	default:
		return 0, errors.New("invalid number format")
	}
}
