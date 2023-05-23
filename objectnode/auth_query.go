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
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	v2PresignedQueries = []string{AWSAccessKeyID, Expires, Signature}
	v4PresignedQueries = []string{XAmzAlgorithm, XAmzCredential, XAmzDate, XAmzExpires, XAmzSignedHeaders, XAmzSignature}
)

type QueryAuth struct {
	SignatureInfo

	query   url.Values
	request *http.Request
}

func NewQueryAuth(r *http.Request) (Auther, error) {
	auth := &QueryAuth{
		request: r,
		query:   r.URL.Query(),
	}
	auth.credential = new(Credential)

	switch {
	case auth.query.Get(AWSAccessKeyID) != "":
		auth.version = signatureV2
		auth.algorithm = signV2Algorithm
		return auth, auth.parseSignV2()
	case auth.query.Get(XAmzAlgorithm) != "":
		if strings.HasPrefix(auth.query.Get(XAmzAlgorithm), signatureV4) {
			auth.version = signatureV4
			auth.algorithm = auth.query.Get(XAmzAlgorithm)
			return auth, auth.parseSignV4()
		}
		return nil, ErrInvalidAuthQueryParams
	}

	return nil, MissingSecurityElement
}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html#RESTAuthenticationQueryStringAuth
func (auth *QueryAuth) parseSignV2() error {
	for _, query := range v2PresignedQueries {
		var val string
		if val = auth.query.Get(query); val == "" {
			return ErrMissingAuthQueryParams
		}

		switch query {
		case AWSAccessKeyID:
			auth.credential.AccessKey = val
		case Expires:
			expires, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return ErrMalformedQueryExpires
			}
			auth.credential.Expires = expires
		case Signature:
			auth.signature = val
		}
	}

	return nil
}

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sigv4-query-string-auth.html
func (auth *QueryAuth) parseSignV4() error {
	for _, query := range v4PresignedQueries {
		var val string
		if val = strings.TrimSpace(auth.query.Get(query)); val == "" {
			return ErrMissingAuthQueryParams
		}

		switch query {
		case XAmzDate:
			if _, err := time.Parse(ISO8601Format, val); err != nil {
				return ErrMalformedXAmzDate
			}
			auth.credential.TimeStamp = val
		case XAmzExpires:
			expires, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return ErrMalformedQueryExpires
			}
			if expires < MinSignatureExpires || expires > MaxSignatureExpires {
				return ErrInvalidRangeExpires
			}
			auth.credential.Expires = expires
		case XAmzSignature:
			auth.signature = val
		case XAmzCredential:
			cred := strings.Split(auth.query.Get(XAmzCredential), "/")
			if len(cred) != 5 {
				return ErrMalformedCredential
			}
			auth.credential.AccessKey = cred[0]
			auth.credential.Date = cred[1]
			auth.credential.Region = cred[2]
			auth.credential.Service = cred[3]
			auth.credential.Request = cred[4]
		case XAmzSignedHeaders:
			auth.signedHeaders = strings.Split(val, ";")
		}
	}

	return nil
}

func (auth *QueryAuth) IsPresigned() bool {
	return true
}

func (auth *QueryAuth) IsExpired() bool {
	switch {
	case auth.version == signatureV2:
		return time.Now().UTC().Unix() > auth.credential.Expires
	case auth.version == signatureV4:
		ts, err := auth.credential.ParseTimeStamp()
		if err != nil {
			return true
		}
		now := time.Now().UTC()
		ltime := ts.Add(-MaxRequestSkewedSeconds * time.Second)
		rtime := ts.Add(time.Duration(auth.credential.Expires) * time.Second)
		return !(now.After(ltime) && now.Before(rtime))
	}
	return true
}

func (auth *QueryAuth) IsSkewed() bool {
	return false
}

func (auth *QueryAuth) Version() string {
	return auth.version
}

func (auth *QueryAuth) Algorithm() string {
	return auth.algorithm
}

func (auth *QueryAuth) Signature() string {
	return auth.signature
}

func (auth *QueryAuth) Credential() *Credential {
	return auth.credential
}

func (auth *QueryAuth) SignedHeaders() []string {
	return auth.signedHeaders
}

func (auth *QueryAuth) StringToSign() string {
	return auth.stringToSign
}

func (auth *QueryAuth) CanonicalRequest() string {
	return auth.canonicalRequest
}

func (auth *QueryAuth) SignatureMatch(secretKey string, wildcards Wildcards) bool {
	switch auth.version {
	case signatureV2:
		return auth.signature == auth.buildSignatureV2(secretKey, wildcards)
	case signatureV4:
		return auth.signature == auth.buildSignatureV4(secretKey)
	}
	return false
}

func (auth *QueryAuth) buildSignatureV2(secretKey string, wildcards Wildcards) string {
	req := auth.request

	canonicalResource := buildCanonicalizedResourceV2(req, wildcards)
	canonicalResourceQuery := buildCanonicalizedResourceQueryV2(canonicalResource, req.URL.Query())
	expires := strconv.FormatInt(auth.credential.Expires, 10)
	auth.stringToSign = buildStringToSignV2(req.Method, expires, canonicalResourceQuery, req.Header)

	return calculateSignatureV2(secretKey, auth.stringToSign)
}

func (auth *QueryAuth) buildSignatureV4(secretKey string) string {
	req := auth.request

	auth.canonicalRequest = buildCanonicalRequest(req, auth.signedHeaders, true)

	cred := auth.credential
	signingKey := buildSigningKey(auth.version, secretKey, cred.Date, cred.Region, cred.Service, cred.Request)
	scope := buildScope(cred.Date, cred.Region, cred.Service, cred.Request)
	auth.stringToSign = buildStringToSign(auth.algorithm, cred.TimeStamp, scope, auth.canonicalRequest)

	return calculateSignature(signingKey, auth.stringToSign)
}
