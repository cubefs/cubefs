// Copyright 2025 The CubeFS Authors.
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

package base

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorStats(t *testing.T) {
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	err3 := errors.New("error 3")

	es := NewErrorStats()
	for range [3]struct{}{} {
		es.AddFail(err1)
	}
	for range [5]struct{}{} {
		es.AddFail(err2)
	}
	for range [2]struct{}{} {
		es.AddFail(err3)
	}

	infos, _ := es.Stats()
	res := FormatPrint(infos)

	t.Log(res)

	p, err := json.MarshalIndent(&res, "", "\t")
	t.Logf("%v -> %s", err, p)

	es2 := NewErrorStats()
	infos, _ = es2.Stats()
	p, err = json.MarshalIndent(&infos, "", "\t")
	t.Logf("%v -> %s", err, p)
}

func TestErrStrFormat(t *testing.T) {
	err1 := errors.New("Post http://127.0.0.1:xxx/xxx: EOF")
	err2 := errors.New("fake error")
	var err3 error

	require.Equal(t, "EOF", errStrFormat(err1))
	require.Equal(t, "fake error", errStrFormat(err2))
	require.Equal(t, "", errStrFormat(err3))
}
