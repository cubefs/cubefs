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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCanonializedResourceV2(t *testing.T) {
	// resource contains only path, no query parameters
	wildcards, err := NewWildcards([]string{"s3.us-west-1.amazonaws.com"})
	require.NoError(t, err)

	// virtual hosted-style
	request := httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	require.Equal(t, "/awsexamplebucket1/photos/puppy.jpg", buildCanonicalizedResourceV2(request, wildcards))
	// path-style
	request = httptest.NewRequest("GET", "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/photos/puppy.jpg", nil)
	require.Equal(t, "/awsexamplebucket1/photos/puppy.jpg", buildCanonicalizedResourceV2(request, wildcards))
	// does not address a bucket
	request = httptest.NewRequest("GET", "https://s3.us-west-1.amazonaws.com/", nil)
	require.Equal(t, "/", buildCanonicalizedResourceV2(request, wildcards))
}

func TestCanonicalizedResourceQueryV2(t *testing.T) {
	// CanonicalizedResource with path and query parameters
	wildcards, err := NewWildcards([]string{"s3.us-west-1.amazonaws.com"})
	require.NoError(t, err)

	// with no query
	request := httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	resource := buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource := buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	require.Equal(t, "/awsexamplebucket1/photos/puppy.jpg", canonicalizedResource)
	// with a subresource
	request = httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg?acl", nil)
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	require.Equal(t, "/awsexamplebucket1/photos/puppy.jpg?acl", canonicalizedResource)
	// with response header values
	request = httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg?response-content-type=application/json&response-content-language=10&response-content-disposition=attachment%3B+filename%3Dtest.dat", nil)
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	require.Equal(t, "/awsexamplebucket1/photos/puppy.jpg?response-content-disposition=attachment; filename=test.dat&response-content-language=10&response-content-type=application/json", canonicalizedResource)
	// with other query
	request = httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/?list-type=2&delimiter=test&prefix=cube", nil)
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	require.Equal(t, "/awsexamplebucket1/", canonicalizedResource)
}

func TestCanonicalizedAmzHeadersV2(t *testing.T) {
	request := httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	// case-insensitive
	request.Header.Set(XAmzDate, "20130524T000000Z")
	request.Header.Set(XAmzAcl, "public-read")
	request.Header.Set(ContentType, "application/json")
	request.Header.Set("X-Amz-Meta-Username", "fred")
	request.Header.Add("x-amz-meta-username", "barney")
	request.Header.Add("x-Amz-meta-Username", "cube")
	request.Header.Set("X-Amz-Meta-email", "cube@cfs.com")
	expectedHeaders := "x-amz-acl:public-read\nx-amz-date:20130524T000000Z\nx-amz-meta-email:cube@cfs.com\nx-amz-meta-username:fred,barney,cube"
	require.Equal(t, expectedHeaders, buildCanonicalizedAmzHeadersV2(request.Header))

	request = httptest.NewRequest("GET", "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set("X-Amz-Meta-Username", "fred")
	request.Header.Add("x-Amz-meta-Username", "cube, fs")
	request.Header.Add("x-amz-meta-username", "barney")
	request.Header.Set(XAmzAcl, "public-read")
	expectedHeaders = "x-amz-acl:public-read\nx-amz-meta-username:fred,cube, fs,barney"
	require.Equal(t, expectedHeaders, buildCanonicalizedAmzHeadersV2(request.Header))
}

func TestGetStringToSignV2(t *testing.T) {
	wildcards, err := NewWildcards([]string{"us-west-1.s3.amazonaws.com"})
	require.NoError(t, err)

	date := "Tue, 27 Mar 2007 19:36:42 +0000"
	// Object GET
	request := httptest.NewRequest("GET", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, "AWS AKIAIOSFODNN7EXAMPLE:SIGNATUREEXAMPLE")
	request.Header.Set(Date, date)
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	resource := buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource := buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	stringToSign := "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/photos/puppy.jpg"
	require.Equal(t, stringToSign, buildStringToSignV2(request.Method, date, canonicalizedResource, request.Header))
	// Object PUT
	request = httptest.NewRequest("PUT", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, "AWS AKIAIOSFODNN7EXAMPLE:SIGNATUREEXAMPLE")
	request.Header.Set(Date, date)
	request.Header.Set(ContentType, "image/jpeg")
	request.Header.Set(ContentLength, "94328")
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	stringToSign = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/photos/puppy.jpg"
	require.Equal(t, stringToSign, buildStringToSignV2(request.Method, date, canonicalizedResource, request.Header))
	// List
	request = httptest.NewRequest("GET", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/?prefix=photos&max-keys=50&marker=puppy", nil)
	request.Header.Set(Authorization, "AWS AKIAIOSFODNN7EXAMPLE:SIGNATUREEXAMPLE")
	request.Header.Set(Date, date)
	request.Header.Set("User-Agent", "Mozilla/5.0")
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/"
	require.Equal(t, stringToSign, buildStringToSignV2(request.Method, date, canonicalizedResource, request.Header))
	// Fetch
	request = httptest.NewRequest("GET", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/?acl", nil)
	request.Header.Set(Authorization, "AWS AKIAIOSFODNN7EXAMPLE:SIGNATUREEXAMPLE")
	request.Header.Set(Date, date)
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/?acl"
	require.Equal(t, stringToSign, buildStringToSignV2(request.Method, date, canonicalizedResource, request.Header))
	// Delete
	request = httptest.NewRequest("DELETE", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, "AWS AKIAIOSFODNN7EXAMPLE:SIGNATUREEXAMPLE")
	request.Header.Set(Date, date)
	request.Header.Set(XAmzDate, "Tue, 27 Mar 2007 21:20:26 +0000")
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	stringToSign = "DELETE\n\n\n\nx-amz-date:Tue, 27 Mar 2007 21:20:26 +0000\n/awsexamplebucket1/photos/puppy.jpg"
	require.Equal(t, stringToSign, buildStringToSignV2(request.Method, "", canonicalizedResource, request.Header))
	// Upload
	request = httptest.NewRequest("PUT", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/db-backup.dat.gz", nil)
	request.Header.Set(Authorization, "AWS AKIAIOSFODNN7EXAMPLE:SIGNATUREEXAMPLE")
	request.Header.Set("User-Agent", "curl/7.15.5")
	request.Header.Set(Date, date)
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	request.Header.Set(XAmzAcl, "public-read")
	request.Header.Set(ContentType, "application/x-download")
	request.Header.Set(ContentMD5, "4gJE4saaMU4BqNR0kLY+lw==")
	request.Header.Set(ContentDisposition, "attachment; filename=database.dat")
	request.Header.Set(ContentEncoding, "gzip")
	request.Header.Set(ContentLength, "5913339")
	request.Header.Set("X-Amz-Meta-ReviewedBy", "joe@example.com")
	request.Header.Add("X-Amz-Meta-ReviewedBy", "jane@example.com")
	request.Header.Set("X-Amz-Meta-FileChecksum", "0x02661779")
	request.Header.Set("X-Amz-Meta-ChecksumAlgorithm", "crc32")
	resource = buildCanonicalizedResourceV2(request, wildcards)
	canonicalizedResource = buildCanonicalizedResourceQueryV2(resource, request.URL.Query())
	stringToSign = "PUT\n4gJE4saaMU4BqNR0kLY+lw==\napplication/x-download\nTue, 27 Mar 2007 19:36:42 +0000\nx-amz-acl:public-read\nx-amz-meta-checksumalgorithm:crc32\nx-amz-meta-filechecksum:0x02661779\nx-amz-meta-reviewedby:joe@example.com,jane@example.com\n/awsexamplebucket1/db-backup.dat.gz"
	require.Equal(t, stringToSign, buildStringToSignV2(request.Method, date, canonicalizedResource, request.Header))
}

func TestCalculateSignatureV2(t *testing.T) {
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	// Object GET
	stringToSign := "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/photos/puppy.jpg"
	require.Equal(t, "qgk2+6Sv9/oM7G3qLEjTH1a1l1g=", calculateSignatureV2(secretKey, stringToSign))

	// Object PUT
	stringToSign = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/awsexamplebucket1/photos/puppy.jpg"
	require.Equal(t, "iqRzw+ileNPu1fhspnRs8nOjjIA=", calculateSignatureV2(secretKey, stringToSign))

	// List
	stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/awsexamplebucket1/"
	require.Equal(t, "m0WP8eCtspQl5Ahe6L1SozdX9YA=", calculateSignatureV2(secretKey, stringToSign))

	// Fetch
	stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:44:46 +0000\n/awsexamplebucket1/?acl"
	require.Equal(t, "82ZHiFIjc+WbcwFKGUVEQspPn+0=", calculateSignatureV2(secretKey, stringToSign))
}
