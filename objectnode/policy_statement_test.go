package objectnode

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPolicyExample(t *testing.T) {
	policyJson := `{
		"Version": "2012-10-17",
		"Id": "ExamplePolicy01",
		"Statement": [
			{
				"Sid": "ExampleStatement01",
				"Effect": "Allow",
				"Principal": {
					"AWS": ["11","22"]
				},
				"Action": [
					"s3:GetObject",
					"s3:GetBucketLocation",
					"s3:ListBucket"
				],
				"Resource": [
					"arn:aws:s3:::examplebucket/*",
					"arn:aws:s3:::examplebucket"
				],
				"Condition": {
					"StringLike": {
						"aws:Referer": [
							"*.cubefs.com"
						]
					}
				}
			}
		]
	}`
	var policy Policy
	err := json.Unmarshal([]byte(policyJson), &policy)
	require.NoError(t, err)
	out, err := json.Marshal(policy)
	err = json.Unmarshal(out, &policy)
	require.NoError(t, err)

}

func TestMissingRequiredField(t *testing.T) {
	policyJson := `{
		"Version": "2008-10-17",
		"Statement": [
			{
				"Principal": {
					"AWS": "099454345549"
				},
				"action": [
					"s3:ListBucket",
					"s3:getobject"
				],
				"Resource": [
					"arn:aws:s3:::examplebucket/*",
					"arn:aws:s3:::examplebucket"
				]
			}
		]
	}`
	var out1 Policy
	dec := json.NewDecoder(strings.NewReader(policyJson))
	dec.DisallowUnknownFields()
	err := dec.Decode(&out1)
	require.NoError(t, err)
	_, err = out1.Validate("examplebucket")
	require.EqualError(t, err, "missing Effect in policy")

	policyJson = `{
		"Version": "",
		"Statement": [
			{
				"Principal": {
					"AWS": "099454345549"
				},
				"Effect": "Allow",
				"action": [
					"s3:ListBucket",
					"s3:getobject"
				],
				"Resource": [
					"arn:aws:s3:::examplebucket/*",
					"arn:aws:s3:::examplebucket"
				]
			}
		]
	}`
	var out2 Policy
	dec = json.NewDecoder(strings.NewReader(policyJson))
	dec.DisallowUnknownFields()
	err = dec.Decode(&out2)
	require.NoError(t, err)
	_, err = out2.Validate("examplebucket")
	require.EqualError(t, err, "missing Version in policy")

	policyJson = `{
		"Version": "2008-10-17"
	}`
	var out3 Policy
	dec = json.NewDecoder(strings.NewReader(policyJson))
	dec.DisallowUnknownFields()
	err = dec.Decode(&out3)
	require.NoError(t, err)
	_, err = out3.Validate("examplebucket")
	require.EqualError(t, err, "missing Statement in policy")
}

func TestEffectFormat(t *testing.T) {
	var s Statement
	json.Unmarshal([]byte(`"dummy"`), &s.Effect)
	b := s.isEffectValid()
	require.False(t, b)

	json.Unmarshal([]byte(`"alloW"`), &s.Effect)
	b = s.isEffectValid()
	require.True(t, b)

	json.Unmarshal([]byte(`"Deny"`), &s.Effect)
	b = s.isEffectValid()
	require.True(t, b)

}

func TestResourceFormat(t *testing.T) {
	//bad: "arn:aws:s3:::bucket/" , or "arn:aws:s3:::/keyname" or "arn:aws:s3:::/" or "arn:aws:s3:::wrongbucket/key" or [ ] empty
	bucketId := "examplebucket"
	var s Statement

	validResources := []string{
		`["arn:aws:s3:::examplebucket"]`,
		`["arn:aws:s3:::examplebucket/*"]`,
		`["arn:aws:s3:::examplebucket/abc"]`,
		`["arn:aws:s3:::examplebucket/abc/*"]`,
		`"arn:aws:s3:::examplebucket/abc?d/*"`,
		`"arn:aws:s3:::examplebucket"`,
		`"arn:aws:s3:::examplebucket/*"`,
	}

	for _, r := range validResources {
		err := json.Unmarshal([]byte(r), &s.Resource)
		require.NoError(t, err)
		b := s.isResourceValid(bucketId)
		require.True(t, b)
	}

	invalidResources := []string{
		`["arn:aws:s3:::examplebucket/","arn:aws:s3:::examplebucket/key"]`, //first one is invaid
		`["arn:aws:s3:::*/keyname"]`,
		`["arn:aws:s3:::/keyname"]`,
		`["arn:aws:s3:::/"]`,
		`["arn:aws:s3:::/*"]`,
		`["arn:aws:s3:::wrongbucket/key"]`,
		`["arn:aws:s3:::wrongbucket"]`,
		`["Arn:aws:s3:::examplebucket"]`, //Arn is invalid, should be arn
		`[]`,
		`""`,
		`"arn:aws:s3:::exampl*bucket"`,
		`"arn:aws:s3:::*"`,
		`["arn:aws:s3:::*"]`,
	}

	for _, r := range invalidResources {
		err := json.Unmarshal([]byte(r), &s.Resource)
		require.NoError(t, err)
		b := s.isResourceValid(bucketId)
		require.False(t, b)
	}
}

func TestPrincipalFormat(t *testing.T) {
	var s Statement
	b := s.isPrincipalValid()
	require.False(t, b)

	validPrincipal := []string{
		`{"AWS":["11","22"]}`,
		`{"AWS":"11"}`,
		`{"AWS":"*"}`,
		`"*"`,
		`"11"`,
	}
	for _, p := range validPrincipal {
		err := json.Unmarshal([]byte(p), &s.Principal)
		require.NoError(t, err)
		b := s.isPrincipalValid()
		require.True(t, b)
	}

	invalidPrincipal := []string{
		`{}`,
		`{"aws":"11"}`, //must be "AWS", not "aws"
		`{"AWS":11}`,   //must be string, not number, 11 should be "11"
		`{"AWS":[]}`,
		`{"AWS":["11",22]}`,
		`["11"]`, //currently, aws not support [] for principal
	}
	for _, p := range invalidPrincipal {
		err := json.Unmarshal([]byte(p), &s.Principal)
		require.NoError(t, err)
		b := s.isPrincipalValid()
		require.False(t, b)
	}
}

func TestActionFormat(t *testing.T) {
	var s Statement
	validAction := []string{
		`["s3:PutObject","s3:getobject","s3:deleteObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"]`,
		`["S3:ListBucket","S3:deletebucket","s3:listbucketMultipartUploads"]`,
		`["s3:*"]`,
	}
	for _, a := range validAction {
		err := json.Unmarshal([]byte(a), &s.Action)
		require.NoError(t, err)
		b := s.isActionValid()
		require.True(t, b)
	}

	invalidAction := []string{
		`["s3:CreateBucket"]`,
		`["s3:ListAllMyBuckets"]`,
		`["s3:DeleteBucketPolicy"]`,
		`["s3:GetBucketPolicy"]`,
		`["s3:PutBucketPolicy"]`,
		`[]`,
		`["s3:PutObject","s3:invalid"]`, //second is invalid
	}
	for _, a := range invalidAction {
		err := json.Unmarshal([]byte(a), &s.Action)
		require.NoError(t, err)
		b := s.isActionValid()
		require.False(t, b)
	}
}

func TestConditionFormat(t *testing.T) {

	validCondition := []string{
		`{"IpAddress":{"aws:SourceIp":["1.1.1.1/24","1.1.1.1","fe80::45e:9d4c:20ca:20f7/64"]}}`,
		`{"NotIpAddress":{"aws:SourceIp":"1.1.1.3"}}`,
		`{"StringLike":{"aws:Referer":"http://*.example.com/*"}}`,
		`{"StringLike":{"aws:Referer":["http://*.example.com/*","http://example.com/*"]}}`,
		`{"StringLike":{"aws:Host":"http://*.example.com/*"}}`,
		`{"StringLike":{"aws:Host":["http://*.example.com/*","http://example.com/*"]}}`,
		`{"StringNotLike":{"aws:Referer":"http://*.example.com/*"}}`,
		`{"StringNotLike":{"aws:Referer":["http://*.example.com/*","http://example.com/*"]}}`,
		`{"StringNotLike":{"aws:Host":"http://*.example.com/*"}}`,
		`{"StringNotLike":{"aws:Host":["http://*.example.com/*","http://example.com/*"]}}`,
	}
	for _, c := range validCondition {
		var cond Condition
		err := json.Unmarshal([]byte(c), &cond)
		require.NoError(t, err)
	}

	invalidCondition := []string{
		`{"ipAddress":{"aws:SourceIp":["1.1.1.1/24"]}}`,          //should be "IpAddress"
		`{"notIpAddress":{"aws:SourceIp":["1.1.1.3"]}}`,          //should be "NotIpAddress"
		`{"IpAddress":{"SourceIp":["1.1.1.3"]}}`,                 //should be "aws:SourceIp"
		`{"StringLik":{"aws:Referer":"http://*.example.com/*"}}`, //should be "aws:StringLike"
		`{"StringLike":{"aws:SourceIP":"http://*.example.com/*"}}`,
		`{"StringLike":{"aws:referer":"http://*.example.com/*"}}`,
		`{"StringLike":{"aws:host":"http://*.example.com/*"}}`,
		`{"StringNotLik":{"aws:Referer":"http://*.example.com/*"}}`, //should be "aws:StringLike"
		`{"StringNotLike":{"aws:SourceIP":"http://*.example.com/*"}}`,
		`{"StringNotLike":{"aws:referer":"http://*.example.com/*"}}`,
		`{"StringNotLike":{"aws:host":"http://*.example.com/*"}}`,
	}
	for _, c := range invalidCondition {
		var cond Condition
		err := json.Unmarshal([]byte(c), &cond)
		require.Error(t, err)

	}
}

func TestActionResourceCombination(t *testing.T) {
	var s Statement
	var resource_action = []struct {
		action      string
		resource    string
		matchResult bool
	}{
		{`["s3:*"]`, `["arn:aws:s3:::examplebucket"]`, true},
		{`["s3:*"]`, `["arn:aws:s3:::examplebucket/*"]`, true},
		{`["s3:ListBucket"]`, `["arn:aws:s3:::examplebucket"]`, true},
		{`["s3:GetObject"]`, `["arn:aws:s3:::examplebucket/*"]`, true},
		{`["s3:ListBucket"]`, `["arn:aws:s3:::examplebucket","arn:aws:s3:::examplebucket/*"]`, true},
		{`["s3:GetObject"]`, `["arn:aws:s3:::examplebucket","arn:aws:s3:::examplebucket/*"]`, true},
		{`["s3:GetObject","s3:ListBucket"]`, `["arn:aws:s3:::examplebucket","arn:aws:s3:::examplebucket/*"]`, true},

		{`["s3:GetObject"]`, `["arn:aws:s3:::examplebucket"]`, false},
		{`["s3:ListBucket"]`, `["arn:aws:s3:::examplebucket/*"]`, false},
		{`["s3:GetObject","s3:ListBucket"]`, `["arn:aws:s3:::examplebucket"]`, false},
		{`["s3:GetObject","s3:ListBucket"]`, `["arn:aws:s3:::examplebucket/*"]`, false},
	}
	for _, ra := range resource_action {
		json.Unmarshal([]byte(ra.resource), &s.Resource)
		json.Unmarshal([]byte(ra.action), &s.Action)
		b := s.isValidCombination()
		require.Equal(t, ra.matchResult, b)
	}
}
