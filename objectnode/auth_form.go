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

type FormAuth struct {
	SignatureInfo

	request *http.Request
}

func NewFormAuth(r *http.Request) (Auther, error) {
	auth := &FormAuth{
		request: r,
	}
	auth.credential = new(Credential)

	fr := NewFormRequest(r)
	if err := fr.ParseMultipartForm(); err != nil {
		erc := ErrMalformedPOSTRequest
		erc.ErrorMessage += err.Error()
		return nil, erc
	}

	switch {
	case auth.FromMultipartForm(AWSAccessKeyID) != "":
		auth.version = signatureV2
		auth.algorithm = signV2Algorithm
		return auth, auth.parseSignV2()
	case strings.HasPrefix(auth.FromMultipartForm(XAmzAlgorithm), signatureV4):
		auth.version = signatureV4
		auth.algorithm = signV4Algorithm
		return auth, auth.parseSignV4()
	}

	return nil, MissingSecurityElement
}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/HTTPPOSTForms.html
func (auth *FormAuth) parseSignV2() error {
	auth.signature = strings.TrimSpace(auth.FromMultipartForm(Signature))
	policy := auth.FromMultipartForm("Policy")
	if auth.signature == "" || policy == "" {
		return ErrMissingFormFields
	}

	if _, err := NewPostPolicy(policy); err != nil {
		return err
	}

	date := auth.FromMultipartForm(XAmzDate)
	if date == "" {
		if date = auth.request.Header.Get(Date); date == "" {
			return ErrFormMissingDateParam
		}
	}
	if _, err := ParseCompatibleTime(date); err != nil {
		return ErrMalformedDate
	}

	auth.credential.TimeStamp = date
	auth.credential.AccessKey = strings.TrimSpace(auth.FromMultipartForm(AWSAccessKeyID))

	return nil
}

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sigv4-authentication-HTTPPOST.html
func (auth *FormAuth) parseSignV4() error {
	for _, key := range []string{XAmzCredential, XAmzSignature, "Policy", XAmzDate} {
		val := auth.FromMultipartForm(key)
		if val == "" {
			return ErrMissingFormFields
		}

		switch key {
		case XAmzCredential:
			cred := strings.Split(val, "/")
			if len(cred) != 5 {
				return ErrMalformedCredential
			}
			auth.credential.AccessKey = cred[0]
			auth.credential.Date = cred[1]
			auth.credential.Region = cred[2]
			auth.credential.Service = cred[3]
			auth.credential.Request = cred[4]
		case XAmzSignature:
			auth.signature = strings.TrimSpace(val)
		case "Policy":
			if _, err := NewPostPolicy(val); err != nil {
				return err
			}
		case XAmzDate:
			if _, err := time.Parse(ISO8601Format, val); err != nil {
				return ErrMalformedXAmzDate
			}
			auth.credential.TimeStamp = val
		}
	}

	return nil
}

func (auth *FormAuth) FromMultipartForm(key string) string {
	for k, v := range auth.request.MultipartForm.Value {
		if strings.ToLower(k) == strings.ToLower(key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func (auth *FormAuth) IsPresigned() bool {
	return false
}

func (auth *FormAuth) IsExpired() bool {
	// check in policy
	return false
}

func (auth *FormAuth) IsSkewed() bool {
	ts, err := auth.credential.ParseTimeStamp()
	if err != nil {
		return true
	}
	now := time.Now().UTC()
	maxReqSkewedDuration := MaxRequestSkewedSeconds * time.Second
	return ts.After(now.Add(maxReqSkewedDuration)) || ts.Before(now.Add(-maxReqSkewedDuration))
}

func (auth *FormAuth) Version() string {
	return auth.version
}

func (auth *FormAuth) Algorithm() string {
	return auth.algorithm
}

func (auth *FormAuth) Signature() string {
	return auth.signature
}

func (auth *FormAuth) SignedHeaders() []string {
	return auth.signedHeaders
}

func (auth *FormAuth) Credential() *Credential {
	return auth.credential
}

func (auth *FormAuth) StringToSign() string {
	return auth.stringToSign
}

func (auth *FormAuth) CanonicalRequest() string {
	return auth.canonicalRequest
}

func (auth *FormAuth) SignatureMatch(secretKey string, wildcards Wildcards) bool {
	auth.stringToSign = auth.FromMultipartForm("Policy")
	switch auth.version {
	case signatureV2:
		return auth.signature == calculateSignatureV2(secretKey, auth.stringToSign)
	case signatureV4:
		cred := auth.credential
		signingKey := buildSigningKey(auth.version, secretKey, cred.Date, cred.Region, cred.Service, cred.Request)
		return auth.signature == calculateSignature(signingKey, auth.stringToSign)
	default:
		return false
	}
}
