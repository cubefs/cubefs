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
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// https://docs.aws.amazon.com/zh_cn/STS/latest/APIReference/API_GetFederationToken.html
func (o *ObjectNode) getFederationTokenHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()
	// request param check
	if token := r.Header.Get(XAmzSecurityToken); token != "" {
		ec = AccessDeniedBySTS
		return
	}
	if action := r.PostFormValue(stsActionKey); action != stsActionValue {
		log.LogErrorf("getFederationTokenHandler: sts action invalid: requestID(%v) action(%v)",
			GetRequestID(r), action)
		ec = InvalidArgument
		return
	}
	name := r.PostFormValue(stsNameKey)
	matched, _ := regexp.MatchString(`^[\w+=,.@-]*$`, name)
	if len(name) < 2 || len(name) > 32 || !matched {
		log.LogErrorf("getFederationTokenHandler: sts name invalid: requestID(%v) name(%v) err(%v)",
			GetRequestID(r), name, err)
		ec = InvalidArgument
		return
	}
	policy := r.PostFormValue(stsPolicyKey)
	if _, err = ParsePolicyV2Config(policy); err != nil {
		log.LogErrorf("getFederationTokenHandler: sts policy invalid: requestID(%v) policy(%v) err(%v)",
			GetRequestID(r), policy, err)
		ec = &ErrorCode{
			ErrorCode:    "MalformedPolicyDocument",
			ErrorMessage: fmt.Sprintf("The policy document was malformed: %v.", err.Error()),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}
	seconds := r.PostFormValue(stsDurationSecondsKey)
	durationSeconds, _ := strconv.ParseInt(seconds, 10, 64)
	if durationSeconds < 900 || durationSeconds > 129600 {
		durationSeconds = 43200
	}
	param := ParseRequestParam(r)
	user, err := o.getUserInfoByAccessKeyV2(param.AccessKey())
	if err != nil {
		log.LogErrorf("getFederationTokenHandler: get user info fail: requestID(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.AccessKey(), err)
		return
	}
	// federated ak/sk generation
	now := time.Now().UTC()
	expireUnixStr := fmt.Sprint(now.Unix() + durationSeconds)
	fedAk := stsAkPrefix + util.RandomString(13, util.Numeric|util.LowerLetter|util.UpperLetter)
	fedSk := util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	sessionToken, err := EncodeFedSessionToken(user.AccessKey, user.SecretKey, fedAk, fedSk, name, policy, expireUnixStr)
	if err != nil {
		log.LogErrorf("getFederationTokenHandler: encode session token fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}
	// response result return
	fedToken := FederationTokenResponse{
		GetFederationTokenResult: &FederationTokenResult{
			FederatedUser: &FederatedUser{
				Arn:             fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", user.UserID, name),
				FederatedUserId: fmt.Sprintf("%s:%s", user.UserID, name),
			},
			Credentials: &FederatedCredentials{
				AccessKeyId:     fedAk,
				SecretAccessKey: fedSk,
				SessionToken:    sessionToken,
				Expiration:      now.Add(time.Duration(durationSeconds) * time.Second).Format(time.RFC3339),
			},
		},
	}
	fedToken.ResponseMetadata.RequestID = GetRequestID(r)
	response, err := MarshalXMLEntity(&fedToken)
	if err != nil {
		log.LogErrorf("getFederationTokenHandler: xml marshal result fail: requestID(%v) fedToken(%v) err(%v)",
			GetRequestID(r), fedToken, err)
		return
	}
	w.Header().Set(ContentType, ValueContentTypeXML)
	w.Header().Set(ContentLength, strconv.Itoa(len(response)))
	if _, err = w.Write(response); err != nil {
		log.LogErrorf("getFederationTokenHandler: write response body fail: requestID(%v) response(%v) err(%v)",
			GetRequestID(r), string(response), err)
	}

	return
}
