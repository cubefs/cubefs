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

package proto

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
)

const (
	tokenLenth = 8 + md5.Size

	// #nosec G101
	TokenHeaderKey = "BLOB-STORE-AUTH-TOKEN"
)

var errMismatchToken = errors.New("mismatch token")

type Config struct {
	EnableAuth bool   `json:"enable_auth"`
	Secret     string `json:"secret"`
}

// token simply: use timestamp as a token calculate param
type token struct {
	param     []byte
	timestamp [8]byte
	md5sum    [md5.Size]byte
}

func Encode(timestamp int64, param, secret []byte) string {
	var t token
	t.param = param
	binary.LittleEndian.PutUint64(t.timestamp[:], uint64(timestamp))
	t.calculate(secret)
	return t.encode()
}

func Decode(tokenStr string, param, secret []byte) error {
	b, err := base64.URLEncoding.DecodeString(tokenStr)
	if err != nil {
		return err
	}
	if len(b) != tokenLenth {
		return errMismatchToken
	}

	t := new(token)
	t.param = param
	copy(t.timestamp[:], b[:8])
	t.calculate(secret)

	if !bytes.Equal(t.md5sum[:], b[8:]) {
		return errMismatchToken
	}
	return nil
}

func (t *token) encode() string {
	w := bytes.NewBuffer(make([]byte, 0, tokenLenth))
	w.Write(t.timestamp[:])
	w.Write(t.md5sum[:])
	return base64.URLEncoding.EncodeToString(w.Bytes())
}

// calculate auth token with params and secret
func (t *token) calculate(secret []byte) {
	hasher := md5.New()
	hasher.Write(t.param)
	hasher.Write(t.timestamp[:])
	hasher.Write(secret)
	copy(t.md5sum[:], hasher.Sum(nil))
}

func ParamFromRequest(req *http.Request) []byte {
	return []byte(req.URL.Path + req.URL.RawQuery)
}
