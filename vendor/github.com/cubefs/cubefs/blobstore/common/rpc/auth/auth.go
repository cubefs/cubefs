// Copyright 2022 The CubeFS Authors.
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

package auth

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
)

const (
	// md5 need 16 byte
	TokenKeyLenth = 16

	TokenHeaderKey = "BLOB-STORE-AUTH-TOKEN"
)

var errMismatchToken = errors.New("mismatch token")

type Config struct {
	EnableAuth bool   `json:"enable_auth"`
	Secret     string `json:"secret"`
}

// simply: use timestamp as a token calculate param
type authInfo struct {
	timestamp int64
	token     []byte
	// other auth content
	others []byte
}

func encodeAuthInfo(info *authInfo) (ret string, err error) {
	w := bytes.NewBuffer([]byte{})
	if err = binary.Write(w, binary.LittleEndian, &info.timestamp); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, &info.token); err != nil {
		return
	}
	return base64.URLEncoding.EncodeToString(w.Bytes()), nil
}

func decodeAuthInfo(encodeStr string) (info *authInfo, err error) {
	info = new(authInfo)
	b, err := base64.URLEncoding.DecodeString(encodeStr)
	if err != nil {
		return
	}

	info.token = make([]byte, TokenKeyLenth)
	r := bytes.NewBuffer(b)
	if err = binary.Read(r, binary.LittleEndian, &info.timestamp); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &info.token); err != nil {
		return
	}
	return
}

// calculate auth token with params and secret
func calculate(info *authInfo, secret []byte) (err error) {
	hash := md5.New()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(info.timestamp))
	hash.Write(info.others)
	hash.Write(b)
	hash.Write(secret)
	info.token = hash.Sum(nil)
	return
}

// verify auth token with params and secret
func verify(info *authInfo, secret []byte) (err error) {
	checkAuthInfo := &authInfo{timestamp: info.timestamp, others: info.others}
	calculate(checkAuthInfo, secret)
	if !bytes.Equal(checkAuthInfo.token, info.token) {
		return errMismatchToken
	}
	return
}

func genEncodeStr(req *http.Request) []byte {
	calStr := req.URL.Path + req.URL.RawQuery
	return []byte(calStr)
}
