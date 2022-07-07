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

package common

import (
	"encoding/json"
	"fmt"
	"io"
)

// Marshal alias of json.Marshal
func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// NewEncoder alias of json.NewEncoder
func NewEncoder(w io.Writer) *json.Encoder {
	return json.NewEncoder(w)
}

// Unmarshal alias of json.Unmarshal
func Unmarshal(data []byte, val interface{}) error {
	return json.Unmarshal(data, val)
}

// NewDecoder alias of json.NewDecoder
func NewDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}

// RawString returns json string
func RawString(v interface{}) string {
	data, _ := Marshal(v)
	return string(data)
}

// Readable print value of json
func Readable(v interface{}) string {
	data, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return fmt.Sprint("Error: ", err)
	}
	return string(data)
}
