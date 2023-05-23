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
	"encoding/hex"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetHashedPayload(t *testing.T) {
	// value in header
	request := httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/test.txt", nil)
	hash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	request.Header.Set(XAmzContentSha256, hash)
	require.Equal(t, hash, getHashedPayload(request))

	// value in presigned url
	queries := url.Values{}
	queries.Set(XAmzAlgorithm, "AWS4-HMAC-SHA256")
	queries.Set(XAmzContentSha256, hash)
	request = httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/test.txt?"+queries.Encode(), nil)
	require.Equal(t, hash, getHashedPayload(request))

	// no value in presigned url
	queries = url.Values{}
	queries.Set(XAmzAlgorithm, "AWS4-HMAC-SHA256")
	request = httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/test.txt?"+queries.Encode(), nil)
	require.Equal(t, UnsignedPayload, getHashedPayload(request))

	// value is not in the header and presigned url, and no body is passed
	request = httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/test.txt", nil)
	require.Equal(t, EmptyStringSHA256, getHashedPayload(request))

	// value is not in the header and presigned url, but the body is passed
	request = httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/test.txt", strings.NewReader("body"))
	hash = hex.EncodeToString(MakeSha256([]byte("body")))
	require.Equal(t, hash, getHashedPayload(request))
}

func TestBuildCanonialQueryString(t *testing.T) {
	// must sort the parameters in the canonical query string alphabetically by key name
	request := httptest.NewRequest("GET", "http://s3.amazonaws.com/examplebucket?prefix=somePrefix&marker=someMarker&max-keys=20", nil)
	canonicalQueryString := "marker=someMarker&max-keys=20&prefix=somePrefix"
	require.Equal(t, canonicalQueryString, buildCanonialQueryString(request, false))

	// a request targets a subresource, the corresponding query parameter value will be an empty string ("")
	request = httptest.NewRequest("GET", "http://s3.amazonaws.com/examplebucket?lifecycle", nil)
	canonicalQueryString = "lifecycle="
	require.Equal(t, canonicalQueryString, buildCanonialQueryString(request, false))

	// presigned url must include all the query parameters except for X-Amz-Signature
	queries := url.Values{}
	queries.Set(XAmzAlgorithm, "AWS4-HMAC-SHA256")
	queries.Set(XAmzCredential, "AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request")
	queries.Set(XAmzSignedHeaders, "host")
	queries.Set(XAmzDate, "20230615T024929Z")
	queries.Set(XAmzExpires, "86400")
	queries.Set(XAmzSignature, "c46996f652e38e9e6eafadb260086eb6f5e5867c6bd1c0b9dfe4f2fa13d91024")
	rawURL := "http://s3.amazonaws.com/examplebucket/test.txt?" + queries.Encode()
	request = httptest.NewRequest("GET", rawURL, nil)
	canonicalQueryString = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20230615%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230615T024929Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host"
	require.Equal(t, canonicalQueryString, buildCanonialQueryString(request, true))
}

func TestBuildSignedHeaders(t *testing.T) {
	// alphabetically sorted and semicolon-separated list of lowercase
	signHeaders := []string{"host", "range", "x-amz-content-sha256", "x-amz-date"}
	require.Equal(t, "host;range;x-amz-content-sha256;x-amz-date", buildSignedHeaders(signHeaders))

	signHeaders = []string{"date", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class"}
	require.Equal(t, "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class", buildSignedHeaders(signHeaders))

	signHeaders = []string{"x-amz-content-sha256", "host", "range", "x-amz-date"}
	require.Equal(t, "host;range;x-amz-content-sha256;x-amz-date", buildSignedHeaders(signHeaders))
}

func TestBuildCanonicalHeaders(t *testing.T) {
	request := httptest.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	request.Header.Set(Range, "bytes=0-9")
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(XAmzDate, "20130524T000000Z ")
	request.Header.Set(XAmzContentSha256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	request.Host = "examplebucket.s3.amazonaws.com"
	signHeaders := []string{"host", "range", "x-amz-content-sha256", "x-amz-date"}
	canonicalHeaders := "host:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nx-amz-date:20130524T000000Z\n"
	require.Equal(t, canonicalHeaders, buildCanonicalHeaders(request, signHeaders))

	request = httptest.NewRequest("PUT", "https://examplebucket.s3.amazonaws.com/test%24file.text", nil)
	request.Header.Set(Date, "Fri, 24 May 2013 00:00:00 GMT")
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(XAmzDate, "20130524T000000Z ")
	request.Header.Set(XAmzStorageClass, "REDUCED_REDUNDANCY ")
	request.Header.Set(XAmzContentSha256, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
	request.Host = "examplebucket.s3.amazonaws.com"
	signHeaders = []string{"date", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class"}
	canonicalHeaders = "date:Fri, 24 May 2013 00:00:00 GMT\nhost:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\nx-amz-date:20130524T000000Z\nx-amz-storage-class:REDUCED_REDUNDANCY\n"
	require.Equal(t, canonicalHeaders, buildCanonicalHeaders(request, signHeaders))

	request = httptest.NewRequest("PUT", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(XAmzDate, "20130524T000000Z ")
	request.Header.Set(XAmzContentSha256, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
	request.Host = "examplebucket.s3.amazonaws.com"
	request.TransferEncoding = []string{"gzip", "chunked"}
	signHeaders = []string{"expect", "host", "transfer-encoding", "x-amz-content-sha256", "x-amz-date"}
	canonicalHeaders = "expect:100-continue\nhost:examplebucket.s3.amazonaws.com\ntransfer-encoding:gzip,chunked\nx-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\nx-amz-date:20130524T000000Z\n"
	require.Equal(t, canonicalHeaders, buildCanonicalHeaders(request, signHeaders))

	queries := url.Values{}
	queries.Set(XAmzAlgorithm, "AWS4-HMAC-SHA256")
	rawURL := "http://s3.amazonaws.com/examplebucket/test.txt?" + queries.Encode()
	request = httptest.NewRequest("GET", rawURL, nil)
	signHeaders = []string{"host"}
	canonicalHeaders = "host:s3.amazonaws.com\n"
	require.Equal(t, canonicalHeaders, buildCanonicalHeaders(request, signHeaders))
}

func TestBuildStringToSign(t *testing.T) {
	// GET Object
	canonicalRequest := "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nx-amz-date:20130524T000000Z\n\nhost;range;x-amz-content-sha256;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	signToString := "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"
	require.Equal(t, signToString, buildStringToSign("AWS4-HMAC-SHA256", "20130524T000000Z", "20130524/us-east-1/s3/aws4_request", canonicalRequest))

	// PUT Object
	canonicalRequest = "PUT\n/test%24file.text\n\ndate:Fri, 24 May 2013 00:00:00 GMT\nhost:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\nx-amz-date:20130524T000000Z\nx-amz-storage-class:REDUCED_REDUNDANCY\n\ndate;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class\n44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
	signToString = "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d"
	require.Equal(t, signToString, buildStringToSign("AWS4-HMAC-SHA256", "20130524T000000Z", "20130524/us-east-1/s3/aws4_request", canonicalRequest))

	// GET Bucket Lifecycle
	canonicalRequest = "GET\n/\nlifecycle=\nhost:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nx-amz-date:20130524T000000Z\n\nhost;x-amz-content-sha256;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	signToString = "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca"
	require.Equal(t, signToString, buildStringToSign("AWS4-HMAC-SHA256", "20130524T000000Z", "20130524/us-east-1/s3/aws4_request", canonicalRequest))
}

func TestCalculateSignature(t *testing.T) {
	// GET Object
	signingKey := buildSigningKey("AWS4", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3", "aws4_request")
	stringToSign := "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"
	require.Equal(t, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", calculateSignature(signingKey, stringToSign))

	// PUT Object
	signingKey = buildSigningKey("AWS4", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3", "aws4_request")
	stringToSign = "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d"
	require.Equal(t, "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd", calculateSignature(signingKey, stringToSign))

	// GET Bucket Lifecycle
	signingKey = buildSigningKey("AWS4", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3", "aws4_request")
	stringToSign = "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca"
	require.Equal(t, "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543", calculateSignature(signingKey, stringToSign))
}
