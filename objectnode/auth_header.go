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
	"net/http"
	"strings"
	"time"
)

type HeaderAuth struct {
	SignatureInfo

	request *http.Request
}

func NewHeaderAuth(r *http.Request) (Auther, error) {
	auth := &HeaderAuth{
		request: r,
	}
	auth.credential = new(Credential)

	authFields := strings.SplitN(strings.TrimSpace(r.Header.Get(Authorization)), " ", 2)
	if len(authFields) != 2 {
		return nil, ErrInvalidAuthHeader
	}

	switch {
	case authFields[0] == signatureV2:
		auth.version = signatureV2
		auth.algorithm = signV2Algorithm
		return auth, auth.parseSignV2(authFields[1])
	case strings.HasPrefix(authFields[0], signatureV4):
		auth.version = signatureV4
		auth.algorithm = authFields[0]
		return auth, auth.parseSignV4(authFields[1])
	}

	return nil, ErrInvalidAuthHeader
}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html#ConstructingTheAuthenticationHeader
func (auth *HeaderAuth) parseSignV2(authStr string) error {
	signFields := strings.Split(strings.TrimSpace(authStr), ":")
	if len(signFields) != 2 {
		return ErrInvalidAuthHeader
	}
	auth.credential.AccessKey = signFields[0]
	auth.signature = signFields[1]

	date := auth.request.Header.Get(XAmzDate)
	if date == "" {
		if date = auth.request.Header.Get(Date); date == "" {
			return ErrMissingDateHeader
		}
	}
	if _, err := ParseCompatibleTime(date); err != nil {
		return ErrMalformedDate
	}
	auth.credential.TimeStamp = date

	return nil
}

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sig-v4-header-based-auth.html
func (auth *HeaderAuth) parseSignV4(authStr string) error {
	for _, authField := range strings.Split(strings.TrimSpace(authStr), ",") {
		authField = strings.TrimSpace(authField)
		switch {
		case strings.HasPrefix(authField, credentialFlag):
			cred := strings.Split(strings.TrimPrefix(authField, credentialFlag), "/")
			if len(cred) != 5 {
				return ErrMalformedCredential
			}
			auth.credential.AccessKey = cred[0]
			auth.credential.Date = cred[1]
			auth.credential.Region = cred[2]
			auth.credential.Service = cred[3]
			auth.credential.Request = cred[4]
		case strings.HasPrefix(authField, signedHeadersFlag):
			auth.signedHeaders = strings.Split(strings.TrimPrefix(authField, signedHeadersFlag), ";")
		case strings.HasPrefix(authField, signatureFlag):
			auth.signature = strings.TrimPrefix(authField, signatureFlag)
		default:
			return ErrInvalidAuthHeader
		}
	}

	if len(auth.signedHeaders) == 0 {
		return ErrMissingSignedHeaders
	}

	if auth.signature == "" {
		return ErrMissingSignature
	}

	date := auth.request.Header.Get(XAmzDate)
	if date == "" {
		if date = auth.request.Header.Get(Date); date == "" {
			return ErrMissingDateHeader
		}
	}
	if _, err := time.Parse(ISO8601Format, date); err != nil {
		return ErrMalformedDate
	}
	auth.credential.TimeStamp = date

	return nil
}

func (auth *HeaderAuth) IsPresigned() bool {
	return false
}

func (auth *HeaderAuth) Version() string {
	return auth.version
}

func (auth *HeaderAuth) Algorithm() string {
	return auth.algorithm
}

func (auth *HeaderAuth) Signature() string {
	return auth.signature
}

func (auth *HeaderAuth) IsExpired() bool {
	switch {
	case auth.version == signatureV4:
		date, err := auth.credential.ParseDate()
		if err != nil {
			return true
		}
		return time.Since(date).Seconds() > MaxSignatureExpires
	default:
		return false
	}
}

func (auth *HeaderAuth) IsSkewed() bool {
	ts, err := auth.credential.ParseTimeStamp()
	if err != nil {
		return true
	}
	now := time.Now().UTC()
	maxReqSkewedDuration := MaxRequestSkewedSeconds * time.Second
	return ts.After(now.Add(maxReqSkewedDuration)) || ts.Before(now.Add(-maxReqSkewedDuration))
}

func (auth *HeaderAuth) Credential() *Credential {
	return auth.credential
}

func (auth *HeaderAuth) SignedHeaders() []string {
	return auth.signedHeaders
}

func (auth *HeaderAuth) StringToSign() string {
	return auth.stringToSign
}

func (auth *HeaderAuth) CanonicalRequest() string {
	return auth.canonicalRequest
}

func (auth *HeaderAuth) SignatureMatch(secretKey string, wildcards Wildcards) bool {
	switch auth.version {
	case signatureV2:
		return auth.signature == auth.buildSignatureV2(secretKey, wildcards)
	case signatureV4:
		var signature string
		if auth.request.Header.Get(XAmzDecodedContentLength) != "" &&
			auth.request.Header.Get(ContentEncoding) == streamingContentEncoding {
			signature = auth.buildSignatureChunk(secretKey)
		} else {
			signature = auth.buildSignatureV4(secretKey)
		}
		return auth.signature == signature
	}
	return false
}

func (auth *HeaderAuth) buildSignatureV2(secretKey string, wildcards Wildcards) string {
	req := auth.request

	canonicalResource := buildCanonicalizedResourceV2(req, wildcards)
	canonicalResourceQuery := buildCanonicalizedResourceQueryV2(canonicalResource, req.URL.Query())
	date := auth.request.Header.Get(Date)
	if auth.request.Header.Get(XAmzDate) != "" {
		date = ""
	}
	auth.stringToSign = buildStringToSignV2(req.Method, date, canonicalResourceQuery, req.Header)

	return calculateSignatureV2(secretKey, auth.stringToSign)
}

func (auth *HeaderAuth) buildSignatureV4(secretKey string) string {
	req := auth.request

	auth.canonicalRequest = buildCanonicalRequest(req, auth.signedHeaders, false)

	cred := auth.credential
	signingKey := buildSigningKey(auth.version, secretKey, cred.Date, cred.Region, cred.Service, cred.Request)
	scope := buildScope(cred.Date, cred.Region, cred.Service, cred.Request)
	auth.stringToSign = buildStringToSign(auth.algorithm, cred.TimeStamp, scope, auth.canonicalRequest)

	return calculateSignature(signingKey, auth.stringToSign)
}

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sigv4-streaming.html
func (auth *HeaderAuth) buildSignatureChunk(secretKey string) string {
	req := auth.request

	auth.canonicalRequest = buildCanonicalRequest(req, auth.signedHeaders, false)

	cred := auth.credential
	signingKey := buildSigningKey(auth.version, secretKey, cred.Date, cred.Region, cred.Service, cred.Request)
	scope := buildScope(cred.Date, cred.Region, cred.Service, cred.Request)
	auth.stringToSign = buildStringToSign(auth.algorithm, cred.TimeStamp, scope, auth.canonicalRequest)

	signature := calculateSignature(signingKey, auth.stringToSign)

	auth.request.Body = NewSignChunkedReader(auth.request.Body, signingKey, scope, cred.Date, signature)

	return signature
}
