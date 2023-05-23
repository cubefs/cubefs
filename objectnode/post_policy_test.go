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
	"encoding/base64"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPostPolicy(t *testing.T) {
	testCases := []struct {
		policy      string
		expectedErr error
	}{
		// invalid json
		{
			policy:      `"expiration":"2023-06-07T12:13:14.000Z","conditions":[["eq","$bucket","cube"]]}`,
			expectedErr: ErrInvalidJsonPolicy,
		},
		// invalid json
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":"invalid format"}`,
			expectedErr: ErrInvalidJsonPolicy,
		},
		// missing expiration
		{
			policy:      `{"conditions":[["eq","$bucket","cube"],["eq","$key","hello.txt"]]}`,
			expectedErr: ErrInvalidPolicyExpiration,
		},
		// invalid expiration
		{
			policy:      `{"expiration":"2023-06-07T15:04:05Z07:00", "conditions":[["eq","$bucket","cube"]]}`,
			expectedErr: ErrInvalidPolicyExpiration,
		},
		// missing conditions
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[]}`,
			expectedErr: ErrMissingPolicyConditions,
		},
		// invalid condition (type of value is a map or slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":["invalid format"]}`,
			expectedErr: ErrInvalidPolicyCondType,
		},
		// invalid condition (type of value is map[string]string when it's a map)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[{"bucket":["cube"]}]}`,
			expectedErr: ErrInvalidPolicyValueType,
		},
		// invalid condition (number of values must be 3 in slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["starts-with","$key","cube","fs"]]}`,
			expectedErr: ErrMalformedPolicyCondValue,
		},
		// invalid condition (the first value must be starts-with/eq/content-length-range string in slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["10086","$key","cube"]]}`,
			expectedErr: ErrInvalidPolicyCondValue,
		},
		// invalid condition (value 2 must with $ prefix in slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["starts-with","key","cube"]]}`,
			expectedErr: ErrMalformedPolicyCondValue,
		},
		// invalid condition (value must be string when the first is starts-with or eq in slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["eq","$key",10086]]}`,
			expectedErr: ErrInvalidPolicyValueType,
		},
		// invalid condition (value must be string when the first is starts-with or eq in slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["starts-with","$key",10086]]}`,
			expectedErr: ErrInvalidPolicyValueType,
		},
		// invalid condition (value 2 not supports starts-with in slice)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["starts-with","$bucket","cube"]]}`,
			expectedErr: NewError("InvalidPolicyConditions", "The conditional element $bucket does not support starts-with.", http.StatusBadRequest),
		},
		// invalid condition (min > max)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["content-length-range",10087,10086]]}`,
			expectedErr: ErrMalformedPolicyCondValue,
		},
		// invalid condition (min < 0)
		{
			policy:      `{"expiration":"2023-06-07T12:13:14.000Z","conditions":[["content-length-range",-1,10086]]}`,
			expectedErr: ErrMalformedPolicyCondValue,
		},
	}

	for _, testCase := range testCases {
		c := testCase
		t.Run("", func(t *testing.T) {
			_, err := NewPostPolicy(base64.StdEncoding.EncodeToString([]byte(c.policy)))
			require.EqualError(t, err, c.expectedErr.Error())
		})
	}
}

func TestPostPolicy_Match(t *testing.T) {
	policyStr := `{ 
		"expiration": "2222-12-09T12:13:14.000Z",
		"conditions": [
			{"bucket": "CubeFS"},
			{"acl": "public-read"},
			["starts-with", "$key", "user/cu"],
			["Starts-With", "$Content-Type", "image/"],
			{"x-amz-algorithm": "AWS4-HMAC-SHA256"},
			{"x-amz-credential": "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request"},
			{"x-amz-date": "20130728T000000Z"},
			["Starts-With", "$x-amz-meta-user", "cube"],
			["Content-Length-Range", 1048579, 10485760]
		]
	}`
	policy64 := base64.StdEncoding.EncodeToString([]byte(policyStr))
	policy, err := NewPostPolicy(policy64)
	require.NoError(t, err)

	testCases := []struct {
		bucket         string
		acl            string
		key            string
		contentType    string
		xAmzAlgorithm  string
		xAmzCredential string
		xAmzDate       string
		xAmzMetaUser   string
		contentLength  int64
		expectedErr    error
	}{
		// normal
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: nil},
		// bucket mismatch
		{bucket: "mismatch", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The bucket not match with the policy condition.", http.StatusForbidden)},
		// acl mismatch
		{bucket: "CubeFS", acl: "mismatch", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The acl not match with the policy condition.", http.StatusForbidden)},
		// key mismatch
		{bucket: "CubeFS", acl: "public-read", key: "mismatch", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The key not match with the policy condition.", http.StatusForbidden)},
		// content-type mismatch
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "mismatch", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The content-type not match with the policy condition.", http.StatusForbidden)},
		// x-amz-algorithm mismatch
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "mismatch", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The x-amz-algorithm not match with the policy condition.", http.StatusForbidden)},
		// x-amz-credential mismatch
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "mismatch", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The x-amz-credential not match with the policy condition.", http.StatusForbidden)},
		// x-amz-date mismatch
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "mismatch", xAmzMetaUser: "cube user", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The x-amz-date not match with the policy condition.", http.StatusForbidden)},
		// x-amz-meta-user mismatch
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "mismatch", contentLength: 1048579, expectedErr: NewError("PolicyConditionNotMatch", "The x-amz-meta-user not match with the policy condition.", http.StatusForbidden)},
		// content-length is out of range
		{bucket: "CubeFS", acl: "public-read", key: "user/cube", contentType: "image/png", xAmzAlgorithm: "AWS4-HMAC-SHA256", xAmzCredential: "AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request", xAmzDate: "20130728T000000Z", xAmzMetaUser: "cube user", contentLength: 1048578, expectedErr: NewError("PolicyConditionNotMatch", "The content-length does not match the policy's content-length-range.", http.StatusForbidden)},
	}

	for _, tc := range testCases {
		toMatch := map[string]string{
			"bucket":           tc.bucket,
			"acl":              tc.acl,
			"key":              tc.key,
			"content-type":     tc.contentType,
			"x-amz-algorithm":  tc.xAmzAlgorithm,
			"x-amz-credential": tc.xAmzCredential,
			"x-amz-date":       tc.xAmzDate,
			"x-amz-meta-user":  tc.xAmzMetaUser,
			"content-length":   strconv.FormatInt(tc.contentLength, 10),
		}
		require.Equal(t, tc.expectedErr, policy.Match(toMatch))
	}
}
