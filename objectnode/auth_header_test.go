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
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewHeaderAuthV2(t *testing.T) {
	// normal
	now := time.Now().UTC()
	datetime := now.Format(ISO8601Format)
	accessKey := "AKIAIOSFODNN7EXAMPLE"
	signature := "qgk2+6Sv9/oM7G3qLEjTH1a1l1g="

	request := httptest.NewRequest("GET", "http://s3.cubefs.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, signature))
	request.Header.Set(Date, datetime)

	ha, err := NewHeaderAuth(request)
	require.NoError(t, err)
	require.False(t, ha.IsPresigned())
	require.False(t, ha.IsExpired())
	require.False(t, ha.IsSkewed())
	require.Empty(t, ha.SignedHeaders())
	require.Equal(t, signatureV2, ha.Version())
	require.Equal(t, signV2Algorithm, ha.Algorithm())

	cred := ha.Credential()
	require.Equal(t, accessKey, cred.AccessKey)
	require.Empty(t, cred.Request)
	require.Empty(t, cred.Date)
	require.Empty(t, cred.Region)
	require.Empty(t, cred.Service)
	require.Empty(t, cred.Expires)
	require.Equal(t, datetime, cred.TimeStamp)

	// time skewed
	datetime = now.Add(-time.Duration(MaxRequestSkewedSeconds+1) * time.Second).Format(ISO8601Format)
	request.Header.Set("X-Amz-Date", datetime)
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.IsSkewed())
}

func TestNewHeaderAuthV4(t *testing.T) {
	// normal
	now := time.Now().UTC()
	date := now.Format(DateLayout)
	datetime := now.Format(ISO8601Format)
	accessKey := "AKIAIOSFODNN7EXAMPLE"
	credential := fmt.Sprintf("%s/%s/cn-south-1/s3/aws4_request", accessKey, date)
	signedHeaders := "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
	signature := "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"

	request := httptest.NewRequest("GET", "http://s3.cubefs.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s,SignedHeaders=%s,Signature=%s", credential, signedHeaders, signature))
	request.Header.Set(Date, datetime)

	ha, err := NewHeaderAuth(request)
	require.NoError(t, err)
	require.False(t, ha.IsPresigned())
	require.False(t, ha.IsExpired())
	require.False(t, ha.IsSkewed())
	require.Equal(t, signatureV4, ha.Version())
	require.Equal(t, signV4Algorithm, ha.Algorithm())
	require.Equal(t, []string{"date", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class"}, ha.SignedHeaders())

	cred := ha.Credential()
	require.Equal(t, "cn-south-1", cred.Region)
	require.Equal(t, "s3", cred.Service)
	require.Equal(t, "aws4_request", cred.Request)
	require.Equal(t, accessKey, cred.AccessKey)
	require.Equal(t, date, cred.Date)
	require.Equal(t, datetime, cred.TimeStamp)
	require.Equal(t, accessKey, cred.AccessKey)
	require.Empty(t, cred.Expires)

	// time skewed
	datetime = now.Add(-time.Duration(MaxRequestSkewedSeconds+1) * time.Second).Format(ISO8601Format)
	request.Header.Set("X-Amz-Date", datetime)
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.IsSkewed())
}

func TestHeaderAuthV2SignatureMatch(t *testing.T) {
	accessKey := "AKIAIOSFODNN7EXAMPLE"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	wildcards, err := NewWildcards([]string{"us-west-1.s3.amazonaws.com", "s3.us-west-1.amazonaws.com"})
	require.NoError(t, err)

	// Object GET
	request := httptest.NewRequest("GET", "http://awsexamplebucket1.us-west-1.s3.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, "qgk2+6Sv9/oM7G3qLEjTH1a1l1g="))
	request.Header.Set(Date, "Tue, 27 Mar 2007 19:36:42 +0000")
	request.Header.Set(Host, "awsexamplebucket1.us-west-1.s3.amazonaws.com")
	ha, err := NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// Object PUT
	request = httptest.NewRequest("PUT", "http://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, "iqRzw+ileNPu1fhspnRs8nOjjIA="))
	request.Header.Set(Date, "Tue, 27 Mar 2007 21:15:45 +0000")
	request.Header.Set(ContentType, "image/jpeg")
	request.Header.Set(ContentLength, "94328")
	request.Header.Set(Host, "awsexamplebucket1.s3.us-west-1.amazonaws.com")
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// List
	request = httptest.NewRequest("GET", "http://awsexamplebucket1.s3.us-west-1.amazonaws.com/?prefix=photos&max-keys=50&marker=puppy", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, "m0WP8eCtspQl5Ahe6L1SozdX9YA="))
	request.Header.Set(Date, "Tue, 27 Mar 2007 19:42:41 +0000")
	request.Header.Set("User-Agent", "Mozilla/5.0")
	request.Header.Set(Host, "awsexamplebucket1.s3.us-west-1.amazonaws.com")
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// Fetch
	request = httptest.NewRequest("GET", "http://awsexamplebucket1.s3.us-west-1.amazonaws.com/?acl", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, "82ZHiFIjc+WbcwFKGUVEQspPn+0="))
	request.Header.Set(Date, "Tue, 27 Mar 2007 19:44:46 +0000")
	request.Header.Set(Host, "awsexamplebucket1.s3.us-west-1.amazonaws.com")
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// Delete
	request = httptest.NewRequest("DELETE", "http://s3.us-west-1.amazonaws.com/awsexamplebucket1/photos/puppy.jpg", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, "Ri1hpB1zpS9pGqR7y8kuNFCl4sE="))
	request.Header.Set(Date, "Tue, 27 Mar 2007 21:20:27 +0000")
	request.Header.Set(XAmzDate, "Tue, 27 Mar 2007 21:20:26 +0000")
	request.Header.Set(Host, "s3.us-west-1.amazonaws.com")
	request.Header.Set("User-Agent", "dotnet")
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// Upload
	request = httptest.NewRequest("PUT", "http://s3.us-west-1.amazonaws.com/awsexamplebucket1/db-backup.dat.gz", nil)
	request.Header.Set(Authorization, fmt.Sprintf("AWS %s:%s", accessKey, "pzpCkfxTNZ143SPzMtH8on0yBH8="))
	request.Header.Set("User-Agent", "curl/7.15.5")
	request.Header.Set(Date, "Tue, 27 Mar 2007 21:06:08 +0000")
	request.Header.Set(Host, "s3.us-west-1.amazonaws.com")
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
	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))
}

func TestHeaderAuthV4SignatureMatch(t *testing.T) {
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	wildcards, err := NewWildcards([]string{"s3.amazonaws.com"})
	require.NoError(t, err)

	// GET Object
	request := httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	request.Header.Set(Authorization, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	request.Header.Set(Range, "bytes=0-9")
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(XAmzDate, "20130524T000000Z")
	request.Header.Set(XAmzContentSha256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	ha, err := NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// PUT Object
	request = httptest.NewRequest("PUT", "http://examplebucket.s3.amazonaws.com/test%24file.text", nil)
	request.Header.Set(Authorization, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd")
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(Date, "Fri, 24 May 2013 00:00:00 GMT")
	request.Header.Set(XAmzDate, "20130524T000000Z")
	request.Header.Set(XAmzStorageClass, "REDUCED_REDUNDANCY")
	request.Header.Set(XAmzContentSha256, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")

	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// GET Bucket Lifecycle
	request = httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/?lifecycle", nil)
	request.Header.Set(Authorization, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543")
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(XAmzDate, "20130524T000000Z")
	request.Header.Set(XAmzContentSha256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// Get Bucket (List Objects)
	request = httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", nil)
	request.Header.Set(Authorization, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7")
	request.Header.Set(Host, "examplebucket.s3.amazonaws.com")
	request.Header.Set(XAmzDate, "20130524T000000Z")
	request.Header.Set(XAmzContentSha256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))

	// Chunked PUT Object
	request = httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	request.Header.Set(Authorization, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
	request.Header.Set(Host, "s3.amazonaws.com")
	request.Header.Set(ContentEncoding, "aws-chunked")
	request.Header.Set(ContentLength, "66824")
	request.Header.Set(XAmzDecodedContentLength, "66560")
	request.Header.Set(XAmzDate, "20130524T000000Z")
	request.Header.Set(XAmzStorageClass, "REDUCED_REDUNDANCY")
	request.Header.Set(XAmzContentSha256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")

	ha, err = NewHeaderAuth(request)
	require.NoError(t, err)
	require.True(t, ha.SignatureMatch(secretKey, wildcards))
}
