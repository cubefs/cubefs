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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

const (
	testUser    = "test"
	testOwnerAK = "OaAKzOwnerTtR5RQ"
	testOwnerSK = "8DmCjW94r4sOqQfaRjhTestNOhN62FSK"
)

func TestEncodeDecodeFedSessionToken(t *testing.T) {
	fedAk := stsAkPrefix + util.RandomString(13, util.Numeric|util.LowerLetter|util.UpperLetter)
	fedSk := util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	durationSeconds := 3600
	expireUnixStr := fmt.Sprint(time.Now().UTC().Unix() + int64(durationSeconds))
	policyStr := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/key"}]}`

	token, err := EncodeFedSessionToken(testOwnerAK, testOwnerSK, fedAk, fedSk, "test", policyStr, expireUnixStr)
	require.NoError(t, err)

	fed, err := DecodeFedSessionToken(fedAk, token, testGetUserInfo)
	require.NoError(t, err)
	require.Equal(t, fedSk, fed.FedSK)
	require.Equal(t, testUser, fed.UserInfo.UserID)
	require.Equal(t, testOwnerAK, fed.UserInfo.AccessKey)
	require.Equal(t, testOwnerSK, fed.UserInfo.SecretKey)

	var policy PolicyV2
	err = json.Unmarshal([]byte(policyStr), &policy)
	require.NoError(t, err)
	require.Equal(t, &policy, fed.Policy)
}

func testGetUserInfo(ak string) (*proto.UserInfo, error) {
	if ak != testOwnerAK {
		return nil, errors.New("wrong access key")
	}
	return &proto.UserInfo{UserID: testUser, AccessKey: testOwnerAK, SecretKey: testOwnerSK}, nil
}
