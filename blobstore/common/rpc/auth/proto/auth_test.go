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
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuth(t *testing.T) {
	secret := []byte("testSecret")
	timestamp := time.Now().Unix()

	req, err := http.NewRequest("POST", "http://127.0.0.1:80/get/name", nil)
	require.NoError(t, err)
	param := ParamFromRequest(req)

	encoded := Encode(timestamp, param, secret)
	require.NoError(t, Decode(encoded, param, secret))

	require.Error(t, Decode(encoded, nil, secret))
	require.Error(t, Decode(encoded, param, nil))
	require.Error(t, Decode(encoded[:6], param, secret))
	require.Error(t, Decode(base64.URLEncoding.EncodeToString(make([]byte, tokenLenth-1)), param, secret))
}
