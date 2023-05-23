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
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewQueryAuthV2(t *testing.T) {
	expires := 1175139620
	accessKey := "AKIAIOSN7EXAMPLE"
	secretKey := "SKIAIOSFODNN7EXAMPLE"
	signature := "CzwMhRPwPmHAO/A3ZL0i+m5ibxg="
	wildcards, err := NewWildcards([]string{"s3.cubefs.com"})
	require.NoError(t, err)

	queries := url.Values{}
	queries.Set(AWSAccessKeyID, accessKey)
	queries.Set(Signature, signature)
	queries.Set(Expires, strconv.Itoa(expires))

	rawUri := "http://bucket.s3.cubefs.com/photos/puppy.jpg?" + queries.Encode()
	request := httptest.NewRequest("GET", rawUri, nil)

	qa, err := NewQueryAuth(request)
	require.NoError(t, err)
	require.False(t, qa.IsSkewed())
	require.True(t, qa.IsExpired())
	require.True(t, qa.IsPresigned())
	require.True(t, qa.SignatureMatch(secretKey, wildcards))
	require.Empty(t, qa.SignedHeaders())
	require.Equal(t, signature, qa.Signature())
	require.Equal(t, signatureV2, qa.Version())
	require.Equal(t, signV2Algorithm, qa.Algorithm())

	cred := qa.Credential()
	require.Empty(t, cred.Region)
	require.Empty(t, cred.Service)
	require.Empty(t, cred.Request)
	require.Empty(t, cred.Date)
	require.Empty(t, cred.TimeStamp)
	require.Equal(t, accessKey, cred.AccessKey)
	require.Equal(t, int64(expires), cred.Expires)
}

func TestNewQueryAuthV4(t *testing.T) {
	accessKey := "AKIAIOSFODNN7EXAMPLE"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"
	region := "us-east-1"
	service := "s3"
	awsRequest := "aws4_request"
	signature := "ea42ff6ac875745415880c39f60ae87a2e5a080cd4f477dbb934f2f296144991"
	wildcards, err := NewWildcards([]string{"s3.amazonaws.com"})
	require.NoError(t, err)

	queries := url.Values{}
	queries.Set(XAmzAlgorithm, signV4Algorithm)
	queries.Set(XAmzCredential, strings.Join([]string{accessKey, "20130524", region, service, awsRequest}, "/"))
	queries.Set(XAmzDate, "20130524T000000Z")
	queries.Set(XAmzExpires, "86400")
	queries.Set(XAmzSignedHeaders, "host")
	queries.Set(XAmzSignature, "ea42ff6ac875745415880c39f60ae87a2e5a080cd4f477dbb934f2f296144991")

	rawUri := "http://examplebucket.s3.amazonaws.com/test.txt?" + queries.Encode()
	request := httptest.NewRequest("GET", rawUri, nil)

	qa, err := NewQueryAuth(request)
	require.NoError(t, err)
	require.False(t, qa.IsSkewed())
	require.True(t, qa.IsExpired())
	require.True(t, qa.IsPresigned())
	require.True(t, qa.SignatureMatch(secretKey, wildcards))
	require.Equal(t, []string{"host"}, qa.SignedHeaders())
	require.Equal(t, signature, qa.Signature())
	require.Equal(t, signatureV4, qa.Version())
	require.Equal(t, signV4Algorithm, qa.Algorithm())

	cred := qa.Credential()
	require.Equal(t, region, cred.Region)
	require.Equal(t, service, cred.Service)
	require.Equal(t, awsRequest, cred.Request)
	require.Equal(t, "20130524", cred.Date)
	require.Equal(t, "20130524T000000Z", cred.TimeStamp)
	require.Equal(t, accessKey, cred.AccessKey)
	require.Equal(t, int64(86400), cred.Expires)
}

func TestQueryAuthV2SignatureMatch(t *testing.T) {
	// normal
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	wildcards, err := NewWildcards([]string{"s3.us-west-1.amazonaws.com", "s3.us-east-1.amazonaws.com"})
	require.NoError(t, err)
	stringToSign := "GET\n\n\n1175139620\n/awsexamplebucket1/photos/puppy.jpg"
	signature := calculateSignatureV2(secretKey, stringToSign)
	queries := url.Values{}
	queries.Set(AWSAccessKeyID, "AKIAIOSFODNN7EXAMPLE")
	queries.Set(Signature, signature)
	queries.Set(Expires, "1175139620")
	rawUri := "http://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg?" + queries.Encode()
	request := httptest.NewRequest("GET", rawUri, nil)
	qa, err := NewQueryAuth(request)
	require.NoError(t, err)
	require.True(t, qa.SignatureMatch(secretKey, wildcards))

	// with query response header
	stringToSign = "GET\n\n\n1175139620\n/awsexamplebucket1/photos/puppy.jpg?response-content-disposition=attachment; filename=database.dat&response-content-type=application/json"
	signature = calculateSignatureV2(secretKey, stringToSign)
	queries = url.Values{}
	queries.Set(AWSAccessKeyID, "AKIAIOSFODNN7EXAMPLE")
	queries.Set(Signature, signature)
	queries.Set(Expires, "1175139620")
	queries.Set("response-content-type", "application/json")
	queries.Set("response-content-disposition", "attachment; filename=database.dat")
	rawUri = "http://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg?" + queries.Encode()
	request = httptest.NewRequest("GET", rawUri, nil)
	qa, err = NewQueryAuth(request)
	require.NoError(t, err)
	require.True(t, qa.SignatureMatch(secretKey, wildcards))
}

func TestQueryAuthV4SignatureMatch(t *testing.T) {
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	wildcards, err := NewWildcards([]string{"s3.amazonaws.com", "s3.us-east-1.amazonaws.com"})
	require.NoError(t, err)

	// normal get
	queries := url.Values{}
	queries.Set(XAmzAlgorithm, "AWS4-HMAC-SHA256")
	queries.Set(XAmzCredential, "AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request")
	queries.Set(XAmzSignedHeaders, "host")
	queries.Set(XAmzDate, "20230615T024929Z")
	queries.Set(XAmzExpires, "86400")
	queries.Set(XAmzSignature, "c46996f652e38e9e6eafadb260086eb6f5e5867c6bd1c0b9dfe4f2fa13d91024")
	rawURL := "http://s3.amazonaws.com/examplebucket/test.txt?" + queries.Encode()
	request := httptest.NewRequest("GET", rawURL, nil)
	qa, err := NewQueryAuth(request)
	require.NoError(t, err)
	require.True(t, qa.SignatureMatch(secretKey, wildcards))

	// with X-Amz-Security-Token
	queries = url.Values{}
	queries.Set(XAmzAlgorithm, "AWS4-HMAC-SHA256")
	queries.Set(XAmzCredential, "AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request")
	queries.Set(XAmzSignedHeaders, "host")
	queries.Set(XAmzDate, "20230615T032225Z")
	queries.Set(XAmzExpires, "86400")
	queries.Set(XAmzSecurityToken, "IQoJb3JpZ2luX2VjEMvTOKEN")
	queries.Set(XAmzSignature, "bd569ed6d0f72ade37bc6803642e0e9506754d00e248fbaf72665204dff48588")
	rawURL = "http://examplebucket.s3.us-east-1.amazonaws.com/test.txt?" + queries.Encode()
	request = httptest.NewRequest("GET", rawURL, nil)
	qa, err = NewQueryAuth(request)
	require.NoError(t, err)
	require.True(t, qa.SignatureMatch(secretKey, wildcards))
}
