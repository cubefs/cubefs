// Copyright 2024 The CubeFS Authors.
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

package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKV(t *testing.T) {
	key := []byte("key")
	value := []byte("value")

	kv, err := InitKV(key, &io.LimitedReader{
		R: bytes.NewReader(value),
		N: int64(len(value)),
	})
	require.Nil(t, err)
	require.Equal(t, key, kv.Key())
	require.Equal(t, value, kv.Value())
}
