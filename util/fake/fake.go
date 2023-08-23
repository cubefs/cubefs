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

package fake

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

func CreateHTTPClient(roundTripper func(*http.Request) (*http.Response, error)) *http.Client {
	return &http.Client{
		Transport: roundTripperFunc(roundTripper),
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func SuccessJsonBody(data interface{}) io.ReadCloser {
	respWrapper := struct {
		Code int32
		Msg  string
		Data interface{}
	}{
		Code: 0,
		Msg:  "success",
		Data: data,
	}

	responseBody, err := json.Marshal(respWrapper)
	if err != nil {
		panic(err)
	}
	return io.NopCloser(bytes.NewReader(responseBody))
}
