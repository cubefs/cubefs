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
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewFormAuthWithV2(t *testing.T) {
	now := time.Now().UTC()
	datetime := now.Format(ISO8601Format)
	accessKey := "AKIAIOSN7EXAMPLE"
	policyStr := `{ 
		"expiration": "2333-12-30T12:00:00.000Z",
		"conditions": [{"bucket": "cfsexamplebucket"}]
	}`
	policy := base64.StdEncoding.EncodeToString([]byte(policyStr))

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField(AWSAccessKeyID)
	fw.Write([]byte(accessKey))
	fw, _ = w.CreateFormField("key")
	fw.Write([]byte("test.txt"))
	fw, _ = w.CreateFormField("Policy")
	fw.Write([]byte(policy))
	fw, _ = w.CreateFormField(Signature)
	fw.Write([]byte("signature"))
	w.Close()

	request := httptest.NewRequest("POST", "http://s3.cubefs.com/bucket", &buf)
	request.Header.Set(ContentType, w.FormDataContentType())
	request.Header.Set(Date, datetime)
	require.NoError(t, request.ParseMultipartForm(8<<20))

	fa, err := NewFormAuth(request)
	require.NoError(t, err)
	require.False(t, fa.IsPresigned())
	require.False(t, fa.IsExpired())
	require.False(t, fa.IsSkewed())
	require.Empty(t, fa.SignedHeaders())
	require.Equal(t, "signature", fa.Signature())
	require.Equal(t, signatureV2, fa.Version())
	require.Equal(t, signV2Algorithm, fa.Algorithm())

	cred := fa.Credential()
	require.Empty(t, cred.Region)
	require.Empty(t, cred.Service)
	require.Empty(t, cred.Request)
	require.Empty(t, cred.Date)
	require.Empty(t, cred.Expires)
	require.Equal(t, datetime, cred.TimeStamp)
	require.Equal(t, accessKey, cred.AccessKey)
}

func TestNewFormAuthWithV4(t *testing.T) {
	now := time.Now().UTC()
	date := now.Format(DateLayout)
	datetime := now.Format(ISO8601Format)
	accessKey := "AKIAIOSN7EXAMPLE"
	region := "cn-south-1"
	service := "s3"
	awsRequest := "aws4_request"
	policyStr := `{ 
		"expiration": "2333-12-30T12:00:00.000Z",
		"conditions": [{"bucket": "cfsexamplebucket"}]
	}`
	policy := base64.StdEncoding.EncodeToString([]byte(policyStr))

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField(XAmzAlgorithm)
	fw.Write([]byte(signV4Algorithm))
	fw, _ = w.CreateFormField(XAmzCredential)
	fw.Write([]byte(strings.Join([]string{accessKey, date, region, service, awsRequest}, "/")))
	fw, _ = w.CreateFormField(XAmzDate)
	fw.Write([]byte(datetime))
	fw, _ = w.CreateFormField("Policy")
	fw.Write([]byte(policy))
	fw, _ = w.CreateFormField(XAmzSignature)
	fw.Write([]byte("signature"))
	w.Close()

	request := httptest.NewRequest("POST", "http://s3.cubefs.com/bucket", &buf)
	request.Header.Set(ContentType, w.FormDataContentType())
	require.NoError(t, request.ParseMultipartForm(8<<20))

	fa, err := NewFormAuth(request)
	require.NoError(t, err)
	require.False(t, fa.IsPresigned())
	require.False(t, fa.IsExpired())
	require.False(t, fa.IsSkewed())
	require.Empty(t, fa.SignedHeaders())
	require.Equal(t, "signature", fa.Signature())
	require.Equal(t, signatureV4, fa.Version())
	require.Equal(t, signV4Algorithm, fa.Algorithm())

	cred := fa.Credential()
	require.Empty(t, cred.Expires)
	require.Equal(t, region, cred.Region)
	require.Equal(t, service, cred.Service)
	require.Equal(t, awsRequest, cred.Request)
	require.Equal(t, date, cred.Date)
	require.Equal(t, datetime, cred.TimeStamp)
	require.Equal(t, accessKey, cred.AccessKey)
}

func TestFormAuthV2SignatureMatch(t *testing.T) {
	policyStr := `{ 
		"expiration": "2333-12-30T12:00:00.000Z",
		"conditions": [
			{"bucket": "cfsexamplebucket"},
			["starts-with", "$key", "user/eric/"],
			{"acl": "public-read"},
			["starts-with", "$Content-Type", "image/"],
			{"x-amz-meta-uuid": "14365123651274"},
			["starts-with", "$x-amz-meta-tag", ""]
		]
	}`
	policy := base64.StdEncoding.EncodeToString([]byte(policyStr))
	secretKey := "SKIAIOSN7EXAMPLEcfsCUBEFSEXAMPLE"
	signature := calculateSignatureV2(secretKey, policy)

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField(AWSAccessKeyID)
	fw.Write([]byte("AKIAIOSFODNN7EXAMPLE"))
	fw, _ = w.CreateFormField("Policy")
	fw.Write([]byte(policy))
	fw, _ = w.CreateFormField("key")
	fw.Write([]byte("user/eric/test.txt"))
	fw, _ = w.CreateFormField(Signature)
	fw.Write([]byte(signature))
	w.Close()

	request := httptest.NewRequest("POST", "http://s3.cubefs.com/bucket", &buf)
	request.Header.Set(ContentType, w.FormDataContentType())
	request.Header.Set(Date, time.Now().UTC().Format(ISO8601Format))
	require.NoError(t, request.ParseMultipartForm(8<<20))

	fa, err := NewFormAuth(request)
	require.NoError(t, err)
	require.True(t, fa.SignatureMatch(secretKey, nil))
}

func TestFormAuthV4SignatureMatch(t *testing.T) {
	policyStr := `{ 
		"expiration": "2333-12-30T12:00:00.000Z",
		"conditions": [
			{"bucket": "cfsexamplebucket"},
			["starts-with", "$key", "user/eric/"],
			{"acl": "public-read"},
			["starts-with", "$Content-Type", "image/"],
			{"x-amz-meta-uuid": "14365123651274"},
			["starts-with", "$x-amz-meta-tag", ""]
		]
	}`
	policy := base64.StdEncoding.EncodeToString([]byte(policyStr))
	now := time.Now().UTC()
	date := now.Format(DateLayout)
	datetime := now.Format(ISO8601Format)
	accessKey := "AKIAIOSN7EXAMPLE"
	secretKey := "SKIAIOSN7EXAMPLEcfsCUBEFSEXAMPLE"
	region := "cn-south-1"
	service := "s3"
	awsRequest := "aws4_request"
	signingKey := buildSigningKey(signatureV4, secretKey, date, region, service, awsRequest)
	signature := calculateSignature(signingKey, policy)

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField(XAmzAlgorithm)
	fw.Write([]byte(signV4Algorithm))
	fw, _ = w.CreateFormField(XAmzCredential)
	fw.Write([]byte(strings.Join([]string{accessKey, date, region, service, awsRequest}, "/")))
	fw, _ = w.CreateFormField(XAmzDate)
	fw.Write([]byte(datetime))
	fw, _ = w.CreateFormField("Policy")
	fw.Write([]byte(policy))
	fw, _ = w.CreateFormField(XAmzSignature)
	fw.Write([]byte(signature))
	w.Close()

	request := httptest.NewRequest("POST", "http://s3.cubefs.com/bucket", &buf)
	request.Header.Set(ContentType, w.FormDataContentType())
	require.NoError(t, request.ParseMultipartForm(8<<20))

	fa, err := NewFormAuth(request)
	require.NoError(t, err)
	require.True(t, fa.SignatureMatch(secretKey, nil))
}
