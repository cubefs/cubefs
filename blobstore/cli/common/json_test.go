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

package common_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

type typeT struct {
	Int    int            `json:"int"`
	Bool   bool           `json:"bool"`
	String string         `json:"string"`
	Slice  []float32      `json:"slice"`
	Map    map[int]string `json:"map"`
}

func TestCmdCommonJSONBase(t *testing.T) {
	value := typeT{
		Int:    10,
		Bool:   true,
		String: "test json",
		Slice:  []float32{0.02, 1.111},
		Map:    map[int]string{-1: "-1", 100: "100"},
	}

	data, err := common.Marshal(value)
	require.NoError(t, err)

	var val typeT
	err = common.Unmarshal(data, &val)
	require.NoError(t, err)

	require.Equal(t, value, val)

	fmt.Println("Raw      JSON:", common.RawString(value))
	fmt.Println("Readable JSON:", common.Readable(value))
}
