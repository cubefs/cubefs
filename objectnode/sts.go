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
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	stsAkPrefix = "STS"
	stsSep      = ";;sts;;"

	stsActionKey          = "Action"
	stsActionValue        = "GetFederationToken"
	stsPolicyKey          = "Policy"
	stsNameKey            = "Name"
	stsDurationSecondsKey = "DurationSeconds"
)

type FederationTokenResponse struct {
	XMLName                  *xml.Name              `xml:"GetFederationTokenResponse"`
	GetFederationTokenResult *FederationTokenResult `xml:"GetFederationTokenResult"`
	ResponseMetadata         struct {
		RequestID string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

type FederationTokenResult struct {
	Credentials      *FederatedCredentials `xml:"Credentials"`
	FederatedUser    *FederatedUser        `xml:"FederatedUser"`
	PackedPolicySize int                   `xml:",omitempty"`
}

type FederatedUser struct {
	Arn             string `xml:"Arn"`
	FederatedUserId string `xml:"FederatedUserId"`
}

type FederatedCredentials struct {
	AccessKeyId     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	SessionToken    string `xml:"SessionToken"`
	Expiration      string `xml:"Expiration"`
}

func EncodeFedSessionToken(ownerAk, ownerSk, fedAk, fedSk, name, policy, expireUnix string) (token string, err error) {
	encoding, err := NewStsEncoding(fedAk, ownerSk)
	if err != nil {
		return
	}
	toEncrypt := strings.Join([]string{
		fedAk,
		fedSk,
		name,
		policy,
		expireUnix,
	}, stsSep)
	token = base64.URLEncoding.EncodeToString([]byte(ownerAk + stsSep + encoding.Encrypt([]byte(toEncrypt))))
	return
}

type FedDecodeResult struct {
	FedSK    string
	Policy   *PolicyV2
	UserInfo *proto.UserInfo
}

func DecodeFedSessionToken(fedAk, session string, getUserInfo func(ak string) (*proto.UserInfo, error)) (*FedDecodeResult, error) {
	if !strings.HasPrefix(fedAk, stsAkPrefix) {
		return nil, InvalidAccessKeyId
	}
	token, err := base64.URLEncoding.DecodeString(session)
	if err != nil {
		return nil, InvalidToken
	}

	tokens := strings.Split(string(token), stsSep)
	if len(tokens) != 2 {
		return nil, InvalidToken
	}
	ownerAk, encryption := tokens[0], tokens[1]
	userInfo, err := getUserInfo(ownerAk)
	if err != nil {
		return nil, InvalidToken
	}
	encoding, err := NewStsEncoding(fedAk, userInfo.SecretKey)
	if err != nil {
		return nil, InvalidToken
	}
	decryptInfo, err := encoding.Decrypt(encryption)
	if err != nil {
		return nil, InvalidToken
	}

	parts := strings.Split(string(decryptInfo), stsSep)
	if len(parts) != 5 {
		return nil, InvalidToken
	}
	fedAk1, fedSk, policyStr, expireUnixStr := parts[0], parts[1], parts[3], parts[4]
	if fedAk != fedAk1 {
		return nil, InvalidToken
	}

	expireUnix, err := strconv.ParseInt(expireUnixStr, 10, 64)
	if err != nil {
		return nil, InvalidToken
	}
	if time.Now().UTC().Unix() > expireUnix {
		return nil, ExpiredToken
	}

	var policy PolicyV2
	if err = json.Unmarshal([]byte(policyStr), &policy); err != nil {
		return nil, InvalidToken
	}

	return &FedDecodeResult{UserInfo: userInfo, FedSK: fedSk, Policy: &policy}, nil
}

func NewStsEncoding(block, key string) (*StsEncoding, error) {
	bk, err := aes.NewCipher(MakeSha256([]byte(block)))
	if err != nil {
		return nil, err
	}
	return &StsEncoding{
		encoding: base64.RawURLEncoding,
		block:    bk,
		key:      MakeMD5([]byte(key)),
	}, nil
}

type StsEncoding struct {
	encoding *base64.Encoding
	block    cipher.Block
	key      []byte
}

func (ec *StsEncoding) Encrypt(data []byte) string {
	buf := make([]byte, len(data))
	cfb := cipher.NewCFBEncrypter(ec.block, ec.key)
	cfb.XORKeyStream(buf, data)
	return ec.encoding.EncodeToString(buf)
}

func (ec *StsEncoding) Decrypt(s string) ([]byte, error) {
	data, err := ec.encoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(data))
	cfb := cipher.NewCFBDecrypter(ec.block, ec.key)
	cfb.XORKeyStream(buf, data)
	return buf, nil
}
