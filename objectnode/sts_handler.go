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
)

// https://docs.aws.amazon.com/zh_cn/STS/latest/APIReference/API_GetFederationToken.html
func (o *ObjectNode) getFederationTokenHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetFederationToken")
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	// request param check
	if token := r.Header.Get(XAmzSecurityToken); token != "" {
		erc = AccessDeniedBySTS
		return
	}

	if action := r.PostFormValue(stsActionKey); action != stsActionValue {
		erc = InvalidArgument
		return
	}

	name := r.PostFormValue(stsNameKey)
	matched, _ := regexp.MatchString(`^[\w+=,.@-]*$`, name)
	if len(name) < 2 || len(name) > 32 || !matched {
		erc = InvalidArgument
		return
	}

	policy := r.PostFormValue(stsPolicyKey)
	if _, err = ParsePolicyV2Config(policy); err != nil {
		span.Errorf("invalid sts policy: policy(%v) err(%v)", policy, err)
		erc = &ErrorCode{
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
	user, err := o.getUserInfoByAccessKey(ctx, param.AccessKey())
	if err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}
	// federated ak/sk generation
	now := time.Now().UTC()
	expireUnixStr := fmt.Sprint(now.Unix() + durationSeconds)
	fedAk := stsAkPrefix + util.RandomString(13, util.Numeric|util.LowerLetter|util.UpperLetter)
	fedSk := util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	sessionToken, err := EncodeFedSessionToken(user.AccessKey, user.SecretKey, fedAk, fedSk, name, policy, expireUnixStr)
	if err != nil {
		span.Errorf("encode session token fail: %v", err)
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
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", fedToken, err)
		return
	}

	writeSuccessResponseXML(w, response)
}
