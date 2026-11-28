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
	"crypto/aes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// TestFedSessionTokenNotForgeableFromPublicAccessKey reproduces a federated user
// forging an admin session token using only public data: the envelope exposes
// the owner access key, and the federation access key (fedAk) is returned to the
// client and sent on every request. The token cipher must not be derivable from
// those, and tampering must be detected.
func TestFedSessionTokenNotForgeableFromPublicAccessKey(t *testing.T) {
	fedAk := stsAkPrefix + util.RandomString(13, util.Numeric|util.LowerLetter|util.UpperLetter)
	require.Len(t, fedAk, aes.BlockSize) // fedAk is exactly one AES block and is public
	fedSk := util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	expireUnixStr := fmt.Sprint(time.Now().UTC().Unix() + 3600)
	restrictedPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/key"}]}`

	token, err := EncodeFedSessionToken(testOwnerAK, testOwnerSK, fedAk, fedSk, testUser, restrictedPolicy, expireUnixStr)
	require.NoError(t, err)

	// Attacker view: the outer envelope reveals the owner access key and ciphertext.
	raw, err := base64.URLEncoding.DecodeString(token)
	require.NoError(t, err)
	envelope := strings.SplitN(string(raw), stsSep, 2)
	require.Len(t, envelope, 2)
	ownerAk, encoded := envelope[0], envelope[1]
	require.Equal(t, testOwnerAK, ownerAk)
	ciphertext, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(ciphertext), aes.BlockSize)

	// Forge an allow-all policy and a far-future expiry, keeping block 0 (= fedAk).
	forgedSk := "attackerattackerattackerattacker"
	adminPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}`
	forgedExpire := fmt.Sprint(time.Now().UTC().Unix() + 100*365*24*3600)
	forgedPlain := []byte(strings.Join([]string{fedAk, forgedSk, testUser, adminPolicy, forgedExpire}, stsSep))

	// SHA256(fedAk) keyed the old CFB cipher, so the attacker rebuilds the stream
	// block by block and reuses C0 unchanged.
	block, err := aes.NewCipher(MakeSha256([]byte(fedAk)))
	require.NoError(t, err)
	forged := make([]byte, len(forgedPlain))
	copy(forged[:aes.BlockSize], ciphertext[:aes.BlockSize])
	ks := make([]byte, aes.BlockSize)
	prev := forged[:aes.BlockSize]
	for off := aes.BlockSize; off < len(forgedPlain); off += aes.BlockSize {
		block.Encrypt(ks, prev)
		n := len(forgedPlain) - off
		if n > aes.BlockSize {
			n = aes.BlockSize
		}
		for j := 0; j < n; j++ {
			forged[off+j] = forgedPlain[off+j] ^ ks[j]
		}
		prev = forged[off : off+n]
	}
	forgedToken := base64.URLEncoding.EncodeToString(
		[]byte(ownerAk + stsSep + base64.RawURLEncoding.EncodeToString(forged)))

	_, err = DecodeFedSessionToken(fedAk, forgedToken, testGetUserInfo)
	require.Error(t, err)
}

func testGetUserInfo(ak string) (*proto.UserInfo, error) {
	if ak != testOwnerAK {
		return nil, errors.New("wrong access key")
	}
	return &proto.UserInfo{UserID: testUser, AccessKey: testOwnerAK, SecretKey: testOwnerSK}, nil
}
