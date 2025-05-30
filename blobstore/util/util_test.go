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

package util

import (
	"encoding/json"
	"io"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUtilMath(t *testing.T) {
	require.True(t, Max(1, math.Inf(1)) == math.Inf(1))
	require.True(t, Max(-1, math.Inf(-1)) == -1)
	require.True(t, Max(math.Inf(-1), math.Inf(1)) == math.Inf(1))
	require.True(t, math.IsNaN(Max(1, math.NaN())))

	require.True(t, Min(1, math.Inf(1)) == 1)
	require.True(t, Min(-1, math.Inf(-1)) == math.Inf(-1))
	require.True(t, Min(math.Inf(-1), math.Inf(1)) == math.Inf(-1))
	require.True(t, math.IsNaN(Max(1, math.NaN())))

	require.True(t, Max[uint16](33, 44) == 44)
	require.True(t, Min[int8](33, 44) == 33)
	require.True(t, Max[int32](33, 33) == Min[int32](33, 33))
}

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
		require.NoError(t, json.Unmarshal([]byte(`"-1s"`), &d))
		require.Equal(t, -time.Second, d.Duration)
		require.NoError(t, json.Unmarshal([]byte("-1"), &d))
		require.Equal(t, -time.Nanosecond, d.Duration)
	}
}

func TestDiscardReader(t *testing.T) {
	buff := make([]byte, 1<<10)
	r := DiscardReader(-1)
	_, err := r.Read(buff)
	require.ErrorIs(t, err, io.EOF)

	r = DiscardReader(10)
	n, err := r.Read(buff)
	require.NoError(t, err)
	require.Equal(t, 10, n)

	r = DiscardReader(1 << 20)
	nn, err := io.Copy(io.Discard, r)
	require.NoError(t, err)
	require.Equal(t, int64(1<<20), nn)
}

func TestUtilFormatInt(t *testing.T) {
	formatInt := func(ii int64) {
		for base := 10; base <= 16; base++ {
			require.Equal(t, strconv.FormatInt(ii, base), FormatInt(ii, base))
		}
	}
	formatInt(0)
	formatInt(math.MaxInt64)
	formatInt(-math.MaxInt64)
	for range [10000]struct{}{} {
		formatInt(rand.Int63() - math.MaxInt64/2)
	}
}

func TestUtilFormatUint(t *testing.T) {
	formatInt := func(ii uint64) {
		for base := 10; base <= 16; base++ {
			require.Equal(t, strconv.FormatUint(ii, base), FormatUint(ii, base))
		}
	}
	formatInt(0)
	formatInt(math.MaxUint64)
	for range [10000]struct{}{} {
		formatInt(rand.Uint64())
	}
}

func BenchmarkUtilFormatInt(b *testing.B) {
	b.ResetTimer()
	b.Run("format", func(b *testing.B) {
		for ii := 0; ii <= b.N; ii++ {
			FormatInt(int64(ii), 11)
		}
	})
	b.ResetTimer()
	b.Run("strconv", func(b *testing.B) {
		for ii := 0; ii <= b.N; ii++ {
			strconv.FormatInt(int64(ii), 11)
		}
	})
}
