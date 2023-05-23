// Copyright 2019 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

// https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader

var (
	ErrMalformedDate = &ErrorCode{
		ErrorCode:    "AuthorizationParameterError",
		ErrorMessage: "Your request date format is invalid, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMalformedCredential = &ErrorCode{
		ErrorCode:    "AuthorizationParameterError",
		ErrorMessage: "The credential is mal-formed, expecting \"<your ak>/<yyyyMMdd>/<region>/<service>/aws4_request\".",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMalformedXAmzDate = &ErrorCode{
		ErrorCode:    "AuthorizationParameterError",
		ErrorMessage: "X-Amz-Date should be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\".",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidAuthHeader = &ErrorCode{
		ErrorCode:    "AuthorizationHeaderError",
		ErrorMessage: "The authorization header that you provided is not valid.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMissingSignedHeaders = &ErrorCode{
		ErrorCode:    "AuthorizationHeaderError",
		ErrorMessage: "SignedHeaders field is missing from the authentication header you provided.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMissingSignature = &ErrorCode{
		ErrorCode:    "AuthorizationHeaderError",
		ErrorMessage: "Signature field is missing from the authentication header you provided.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMissingDateHeader = &ErrorCode{
		ErrorCode:    "AuthorizationHeaderError",
		ErrorMessage: "Your request is missing a valid Date or X-Amz-Date parameter in header.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMalformedPOSTRequest = &ErrorCode{
		ErrorCode:    "AuthorizationFormError",
		ErrorMessage: "The body of your POST request is not well-formed multipart/form-data.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMissingFormFields = &ErrorCode{
		ErrorCode:    "AuthorizationFormError",
		ErrorMessage: "The body of your POST request is missing required authentication fields.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrFormMissingDateParam = &ErrorCode{
		ErrorCode:    "AuthorizationFormError",
		ErrorMessage: "Your request is missing a valid X-Amz-Date in body or Date in header.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMissingAuthQueryParams = &ErrorCode{
		ErrorCode:    "AuthorizationQueryError",
		ErrorMessage: "Missing required authorization fields in the query parameters.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidAuthQueryParams = &ErrorCode{
		ErrorCode:    "AuthorizationQueryError",
		ErrorMessage: "The query parameters that provide authentication information is not valid.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrMalformedQueryExpires = &ErrorCode{
		ErrorCode:    "AuthorizationQueryError",
		ErrorMessage: "The expires parameter should be a number.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidRangeExpires = &ErrorCode{
		ErrorCode:    "AuthorizationQueryError",
		ErrorMessage: "The expires parameter should be within a valid range.",
		StatusCode:   http.StatusBadRequest,
	}
)

const (
	AWSAccessKeyID           = "AWSAccessKeyId"
	signatureV2              = "AWS"
	signatureV4              = "AWS4"
	signV2Algorithm          = "HMAC-SHA1"
	signV4Algorithm          = "AWS4-HMAC-SHA256"
	streamingContentEncoding = "aws-chunked"

	credentialFlag    = "Credential="
	signatureFlag     = "Signature="
	signedHeadersFlag = "SignedHeaders="

	MaxSignatureExpires     = 7 * 24 * 60 * 60 // 7 days
	MinSignatureExpires     = 1                // 1 second
	MaxRequestSkewedSeconds = 15 * 60          // 15 min

	UnsignedPayload   = "UNSIGNED-PAYLOAD"
	EmptyStringSHA256 = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
)

type Auther interface {
	IsPresigned() bool
	IsExpired() bool
	IsSkewed() bool
	Version() string
	Algorithm() string
	Signature() string
	Credential() *Credential
	SignedHeaders() []string
	StringToSign() string
	CanonicalRequest() string
	SignatureMatch(string, Wildcards) bool
}

type SignatureInfo struct {
	credential    *Credential // parsed auth credential
	signedHeaders []string    // parsed name of signed headers, only available for AWS4
	version       string      // parsed auth version, e.g. AWS4
	algorithm     string      // parsed auth algorithm, e.g. AWS4-HMAC-SHA256
	signature     string      // requested signature

	stringToSign     string // string to sign, used to calculate the signature
	canonicalRequest string // canonical request, used to calculate the signature
}

func NewAuth(r *http.Request) (Auther, error) {
	switch {
	case r.Header.Get(Authorization) != "":
		return NewHeaderAuth(r)
	case r.URL.Query().Get(Signature) != "" || r.URL.Query().Get(XAmzSignature) != "":
		return NewQueryAuth(r)
	case r.Method == http.MethodPost && strings.Contains(r.Header.Get(ContentType), ValueMultipartFormData):
		return NewFormAuth(r)
	}

	return nil, MissingSecurityElement
}

type Credential struct {
	Region    string // e.g. cfs-dev
	Service   string // e.g. s3
	Request   string // e.g. aws4_request
	Date      string // e.g. 20060102
	TimeStamp string // e.g. 20060102T150405Z
	Expires   int64  // seconds, e.g. 86400
	AccessKey string // e.g. AKCubeFS2EXAMPLE
}

func (c *Credential) ParseTimeStamp() (time.Time, error) {
	t, err := ParseCompatibleTime(c.TimeStamp)
	if err != nil {
		return t, err
	}
	return t.UTC(), nil
}

func (c *Credential) ParseDate() (time.Time, error) {
	return time.Parse(DateLayout, c.Date)
}

func (o *ObjectNode) validateAuthInfo(r *http.Request, auth Auther) (err error) {
	param := ParseRequestParam(r)
	reqAK := auth.Credential().AccessKey

	var uid, ak, sk, token string
	if token = getSecurityToken(r); token == "" {
		ak = reqAK
		if uid, sk, err = o.getUidSecretKeyWithCheckVol(r, ak, true); err != nil {
			log.LogErrorf("validateAuthInfo: get user uid and sk fail: requestID(%v) ak(%v) err(%v)",
				GetRequestID(r), ak, err)
			return err
		}
	} else {
		if o.stsNotAllowedActions.Contains(param.action) {
			log.LogErrorf("validateAuthInfo: action not allowed by sts user: requestID(%v) action(%v)",
				GetRequestID(r), param.action)
			return AccessDeniedBySTS
		}
		sts, err := DecodeFedSessionToken(reqAK, token, o.getUserInfoByAccessKeyV2)
		if err != nil {
			log.LogErrorf("validateAuthInfo: decode session token fail: requestID(%v) ak(%v) token(%v) err(%v)",
				GetRequestID(r), reqAK, token, err)
			return err
		}
		action := "s3:" + param.apiName
		if !sts.Policy.IsAllow(action, param.bucket, param.object) {
			log.LogErrorf("validateAuthInfo: sts policy not allow: requestID(%v) policy(%v) api(%v) resource(%v)",
				GetRequestID(r), *sts.Policy, param.apiName, param.resource)
			return AccessDenied
		}
		userPolicy := sts.UserInfo.Policy
		if param.bucket != "" && userPolicy != nil && !userPolicy.IsOwn(param.bucket) {
			log.LogErrorf("validateAuthInfo: sts user access non-owner vol: requestID(%v) reqVol(%v) ownVols(%v)",
				GetRequestID(r), param.bucket, userPolicy.OwnVols)
			return AccessDenied
		}
		uid, ak, sk = sts.UserInfo.UserID, sts.UserInfo.AccessKey, sts.FedSK
	}

	mux.Vars(r)[ContextKeyUid] = uid
	mux.Vars(r)[ContextKeyAccessKey] = ak
	if !param.action.IsNone() && o.signatureIgnoredActions.Contains(param.action) {
		return nil
	}

	cred := auth.Credential()
	if auth.IsSkewed() {
		log.LogErrorf("validateAuthInfo: request skewed: requestID(%v) reqTime(%v) servTime(%v)",
			GetRequestID(r), cred.TimeStamp, time.Now().UTC().Format(ISO8601Format))
		return RequestTimeTooSkewed
	}
	if auth.IsExpired() {
		log.LogErrorf("validateAuthInfo: signature has expired: requestID(%v) servTime(%v) reqDate(%v) expires(%v)",
			GetRequestID(r), time.Now().UTC().Format(ISO8601Format), cred.Date, cred.Expires)
		return ExpiredToken
	}
	if !auth.SignatureMatch(sk, o.wildcards) {
		log.LogErrorf("validateAuthInfo: signature not match: requestID(%v) AccessKeyId(%v)\nstringToSign=(\n%v\n)\ncanonialRequest=(\n%v\n)",
			GetRequestID(r), reqAK, auth.StringToSign(), auth.CanonicalRequest())
		return SignatureDoesNotMatch
	}

	return nil
}

func (o *ObjectNode) getUidSecretKeyWithCheckVol(r *http.Request, ak string, ck bool) (uid, sk string, err error) {
	info, err := o.getUserInfoByAccessKey(ak)
	if err == nil {
		uid, sk = info.UserID, info.SecretKey
		return
	}
	if err == proto.ErrUserNotExists || err == proto.ErrAccessKeyNotExists || err == proto.ErrParamError {
		bucket := mux.Vars(r)[ContextKeyBucket]
		if len(bucket) > 0 && ck && GetActionFromContext(r) != proto.OSSCreateBucketAction {
			// In order to be directly compatible with the signature verification of version 1.5
			// (each volume has its own access key and secret key), if the user does not exist and
			// the request specifies a volume, try to use the access key and secret key bound in the
			// volume information for verification.
			var vol *Volume
			if vol, err = o.getVol(bucket); err != nil {
				return
			}
			if ossAk, ossSk := vol.OSSSecure(); ossAk == ak {
				uid, sk = vol.GetOwner(), ossSk
				return
			}
		}
		err = InvalidAccessKeyId
	}

	return
}

func getSecurityToken(r *http.Request) string {
	if token := r.Header.Get(XAmzSecurityToken); token != "" {
		return token
	}
	if token := r.URL.Query().Get(XAmzSecurityToken); token != "" {
		return token
	}
	isFormUpload := r.Method == http.MethodPost && strings.Contains(r.Header.Get(ContentType), ValueMultipartFormData)
	if !isFormUpload {
		return ""
	}
	// ParseMultipartForm is assumed to have been called
	return r.Form.Get(XAmzSecurityToken)
}
