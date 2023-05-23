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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePolicyV2Config(t *testing.T) {
	testCases := []struct {
		policy string
		error  string
	}{
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::bucket"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::bucket/key"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::*"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/key"}]}`,
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::bucket1","arn:aws:s3:::bucket2/key"]}]}`,
		},
		{
			policy: `{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::bucket/key"}]}`,
			error:  "invalid version expecting 2012-10-17",
		},
		{
			policy: ``,
			error:  "policy should not be less than 1",
		},
		{
			policy: `{"Version":"2022-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}`,
			error:  "invalid version expecting 2012-10-17",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[]}`,
			error:  "statement cannot be empty",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
			error:  "too many policy statement",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Other","Action":"*","Resource":"*"}]}`,
			error:  "invalid effect",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Action":"*","Resource":"*"}]}`,
			error:  "invalid effect",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Resource":"*"}]}`,
			error:  "action must not be empty",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"*"}]}`,
			error:  "resource must not be empty",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"sts:*","Resource":"*"}]}`,
			error:  "invalid action",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":[],"Resource":"*"}]}`,
			error:  "invalid action",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":["sts:*"],"Resource":"*"}]}`,
			error:  "invalid action",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":{"action":"*"},"Resource":"*"}]}`,
			error:  "invalid action",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":"not s3 resource"}]}`,
			error:  "invalid resource",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::"}]}`,
			error:  "invalid resource",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::/"}]}`,
			error:  "invalid resource",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::bucket/"}]}`,
			error:  "invalid resource",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":[]}]}`,
			error:  "invalid resource",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":["arn:aws:s3:::/"]}]}`,
			error:  "invalid resource",
		},
		{
			policy: `{"Version":"2012-10-17","Statement":[{"Sid":"AddPerm","Effect":"Allow","Action":"s3:*","Resource":{"resource":"*"}}]}`,
			error:  "invalid resource",
		},
	}

	for _, testCase := range testCases {
		c := testCase
		t.Run("", func(t *testing.T) {
			if _, err := ParsePolicyV2Config(c.policy); err != nil {
				require.ErrorContains(t, err, c.error)
			}
		})
	}
}

func TestPolicyV2_IsAllow(t *testing.T) {
	policyStr := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/key*"}]}`
	policy, err := ParsePolicyV2Config(policyStr)
	require.NoError(t, err)
	require.True(t, policy.IsAllow("s3:GetObject", "bucket", "key1"))
	require.True(t, policy.IsAllow("s3:PutObject", "bucket", "key2"))
	require.False(t, policy.IsAllow("s3:DeleteObject", "bucket", "key2"))

	policyStr = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":["arn:aws:s3:::bucket1","arn:aws:s3:::bucket2/*"]}]}`
	policy, err = ParsePolicyV2Config(policyStr)
	require.NoError(t, err)
	require.True(t, policy.IsAllow("s3:ListBucket", "bucket1", ""))
	require.True(t, policy.IsAllow("s3:ListBucket", "bucket2", ""))
	require.False(t, policy.IsAllow("s3:ListBucket", "bucket3", ""))
	require.True(t, policy.IsAllow("s3:GetObject", "bucket2", "key"))
	require.False(t, policy.IsAllow("s3:GetObject", "bucket1", "key"))

	policyStr = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"arn:aws:s3:::bucket*"}]}`
	policy, err = ParsePolicyV2Config(policyStr)
	require.NoError(t, err)
	require.True(t, policy.IsAllow("s3:ListBucket", "bucket1", ""))
	require.True(t, policy.IsAllow("s3:ListBucket", "bucket2", ""))
	require.False(t, policy.IsAllow("s3:ListBucket", "other", ""))
	require.False(t, policy.IsAllow("s3:GetObject", "bucket", "key"))

	policyStr = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	policy, err = ParsePolicyV2Config(policyStr)
	require.NoError(t, err)
	require.True(t, policy.IsAllow("s3:ListBucket", "bucket", ""))
	require.True(t, policy.IsAllow("s3:GetObject", "bucket1", "key"))
	require.True(t, policy.IsAllow("s3:GetObject", "bucket2", "key"))
}

func TestIsActionValid(t *testing.T) {
	// all action
	require.True(t, isActionValid("*"))
	require.True(t, isActionValid("s3:*"))

	// normal s3 action
	require.True(t, isActionValid("s3:GetObject"))

	// not s3 action
	require.False(t, isActionValid("sts:*"))
}

func TestIsResourceValid(t *testing.T) {
	// all resource
	require.True(t, isResourceValid("*"))
	require.True(t, isResourceValid("arn:aws:s3:::*"))
	require.True(t, isResourceValid("arn:aws:s3:::*/*"))

	// not s3 resource
	require.False(t, isResourceValid("not s3 resource"))

	// invalid resource
	require.False(t, isResourceValid("arn:aws:s3:::"))
	require.False(t, isResourceValid("arn:aws:s3:::/"))
	require.False(t, isResourceValid("arn:aws:s3:::*/"))
	require.False(t, isResourceValid("arn:aws:s3:::bucket/"))

	// normal resource
	require.True(t, isResourceValid("arn:aws:s3:::bucket/key"))
}

func TestPolicyActionMatch(t *testing.T) {
	// all action
	require.True(t, policyActionMatch("*", "s3:GetObject"))
	require.True(t, policyActionMatch("s3:*", "s3:GetObject"))

	// specific action
	require.True(t, policyActionMatch("s3:PutObject", "s3:PutObject"))
	require.False(t, policyActionMatch("s3:PutObject", "s3:GetObject"))
}

func TestPolicyResourceMatch(t *testing.T) {
	// all resource
	require.True(t, policyResourceMatch("*", "", ""))
	require.True(t, policyResourceMatch("*", "bucket", ""))
	require.True(t, policyResourceMatch("*", "bucket", "key"))
	require.True(t, policyResourceMatch("arn:aws:s3:::*", "", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::*", "bucket", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::*", "bucket", "key"))
	require.True(t, policyResourceMatch("arn:aws:s3:::*/*", "", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::*/*", "bucket", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::*/*", "bucket", "key"))

	// invalid resource
	require.False(t, policyResourceMatch("not s3 arn", "", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::", "", ""))

	// normal bucket
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket", "bucket", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket", "bucket1", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket", "", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket", "bucket", "key"))

	// regular bucket
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket*", "bucket1", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket*", "bucket2", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket*", "bucket2", "key"))
	require.True(t, policyResourceMatch("arn:aws:s3:::?uck?", "ducks", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::?uck?", "bucket", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::?uck*", "bucket", ""))
	require.True(t, policyResourceMatch("arn:aws:s3:::?uck??", "bucket", ""))

	// bucket and key
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket/key", "bucket", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket/key", "bucket", "other"))
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket/key", "bucket", "key"))
	require.True(t, policyResourceMatch("arn:aws:s3:::buck*/key", "bucket", "key"))
	require.False(t, policyResourceMatch("arn:aws:s3:::buck*/key", "bucket", "other"))
	require.False(t, policyResourceMatch("arn:aws:s3:::buck?/key", "bucket", "key"))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket/?e?", "bucket", ""))
	require.False(t, policyResourceMatch("arn:aws:s3:::bucket/?e?", "bucket", "keys"))
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket/?e?", "bucket", "key"))
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket/?e?", "bucket", "bee"))
	require.True(t, policyResourceMatch("arn:aws:s3:::bucket/?e*", "bucket", "keys"))
}
