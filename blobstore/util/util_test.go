// Copyright 2023 The Cuber Authors.
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

package util

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenTmpPath(t *testing.T) {
	path, err := GenTmpPath()
	require.NoError(t, err)
	require.NotEqual(t, "", path)
}

func TestStringsToBytes(t *testing.T) {
	str := "test"
	b := StringToBytes(str)
	require.Equal(t, str, string(b))
	require.Equal(t, 0, len(StringToBytes("")))
}

func TestBytesToString(t *testing.T) {
	b := []byte("test")
	str := BytesToString(b)
	require.Equal(t, str, string(b))
	require.Equal(t, "", BytesToString(nil))
}

func TestDuration(t *testing.T) {
	{
		d := Duration{time.Second}
		b, _ := json.Marshal(d)
		require.Equal(t, `"1s"`, string(b))
		d = Duration{time.Hour + time.Second + time.Nanosecond*10}
		b, _ = json.Marshal(d)
		require.Equal(t, `"1h0m1.00000001s"`, string(b))
	}
	{
		var d Duration
		require.Error(t, json.Unmarshal([]byte("{"), &d))
		require.Error(t, json.Unmarshal([]byte("{}"), &d))
		require.NoError(t, json.Unmarshal([]byte("2e9"), &d))
		require.Equal(t, 2*time.Second, d.Duration)
		require.Error(t, json.Unmarshal([]byte(`"26hxxx"`), &d))
		require.NoError(t, json.Unmarshal([]byte(`"1h0m1.001s"`), &d))
		require.Equal(t, time.Hour+time.Second+time.Millisecond, d.Duration)
	}
}
