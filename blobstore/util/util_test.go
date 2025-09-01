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
	crand "crypto/rand"
	"encoding/json"
	"fmt"
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

func TestUtilSize(t *testing.T) {
	{
		s := Size(1000)
		b, err := json.Marshal(s)
		require.NoError(t, err)
		is := ISize(1000)
		ib, err := json.Marshal(is)
		require.NoError(t, err)
		exp := struct {
			V  Size
			IV ISize
		}{}
		require.NoError(t, json.Unmarshal([]byte(`{"V":`+string(b)+`,"IV":`+string(ib)+`}`), &exp))
		require.Equal(t, s, exp.V)
		require.Equal(t, is, exp.IV)
		require.Equal(t, uint64(exp.V), uint64(exp.IV))
	}
	{
		var s Size
		require.Error(t, json.Unmarshal([]byte("{"), &s))
		require.Error(t, json.Unmarshal([]byte("{}"), &s))
		require.NoError(t, json.Unmarshal([]byte("2e9"), &s))
		require.Equal(t, uint64(2e9), uint64(s))
		require.Error(t, json.Unmarshal([]byte(`"1000,000xxx"`), &s))
		require.NoError(t, json.Unmarshal([]byte(`"32MB"`), &s))
		require.Equal(t, uint64(32*1000*1000), uint64(s))
		require.NoError(t, json.Unmarshal([]byte(`"32mib"`), &s))
		require.Equal(t, uint64(32<<20), uint64(s))
	}
	{
		var s ISize
		require.Error(t, json.Unmarshal([]byte("{"), &s))
		require.Error(t, json.Unmarshal([]byte("{}"), &s))
		require.NoError(t, json.Unmarshal([]byte("2e9"), &s))
		require.Equal(t, uint64(2e9), uint64(s))
		require.Error(t, json.Unmarshal([]byte(`"1000,000xxx"`), &s))
		require.NoError(t, json.Unmarshal([]byte(`"32MB"`), &s))
		require.Equal(t, uint64(32*1000*1000), uint64(s))
		require.NoError(t, json.Unmarshal([]byte(`"32mib"`), &s))
		require.Equal(t, uint64(32<<20), uint64(s))
		require.NoError(t, json.Unmarshal([]byte(`"23,000GiB"`), &s))
		require.Equal(t, uint64((23000)<<30), uint64(s))
	}
}

func TestDiscardReader(t *testing.T) {
	buff := make([]byte, 1<<10)
	r := DiscardReader(-1)
	_, err := r.Read(buff)
	require.ErrorIs(t, err, io.EOF)

	crand.Read(buff)
	oldbuff := make([]byte, 1<<10)
	copy(oldbuff, buff)
	r = DiscardReader(10)
	n, err := r.Read(buff)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, oldbuff, buff)

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

func TestZeroReader(t *testing.T) {
	buff := make([]byte, 1<<10)
	crand.Read(buff)
	r := ZeroReader(10)
	n, err := r.Read(buff)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, make([]byte, 10), buff[:10])
}

func TestStringConvert(t *testing.T) {
	const s77 = "77"
	const s99 = "99"
	const v77 = 77
	const v99 = 99
	var (
		val                      = struct{}{}
		valStr        string     = s77
		valBool       bool       = true
		valInt        int        = v77
		valInt8       int8       = v77
		valInt16      int16      = v77
		valInt32      int32      = v77
		valInt64      int64      = v77
		valUint       uint       = v77
		valUint8      uint8      = v77
		valUint16     uint16     = v77
		valUint32     uint32     = v77
		valUint64     uint64     = v77
		valFloat32    float32    = 77.77
		valFloat64    float64    = 77.77
		valComplex64  complex64  = complex(77, 77)
		valComplex128 complex128 = complex(77, 77)
	)

	for _, cs := range []interface{}{valBool, &valBool} {
		require.Equal(t, "true", Any2String(cs))
	}
	require.NoError(t, String2Any("false", &valBool))
	require.False(t, valBool)

	for _, cs := range []interface{}{
		valStr, &valStr,
		valInt, &valInt, valInt8, &valInt8, valInt16, &valInt16,
		valInt32, &valInt32, valInt64, &valInt64,
		valUint, &valUint, valUint8, &valUint8, valUint16, &valUint16,
		valUint32, &valUint32, valUint64, &valUint64,
	} {
		require.Equal(t, s77, Any2String(cs))
	}
	require.NoError(t, String2Any(s99, &valStr))
	require.NoError(t, String2Any(s99, &valInt))
	require.NoError(t, String2Any(s99, &valInt8))
	require.NoError(t, String2Any(s99, &valInt16))
	require.NoError(t, String2Any(s99, &valInt32))
	require.NoError(t, String2Any(s99, &valInt64))
	require.NoError(t, String2Any(s99, &valUint))
	require.NoError(t, String2Any(s99, &valUint8))
	require.NoError(t, String2Any(s99, &valUint16))
	require.NoError(t, String2Any(s99, &valUint32))
	require.NoError(t, String2Any(s99, &valUint64))
	require.Equal(t, s99, valStr)
	require.Equal(t, int(v99), valInt)
	require.Equal(t, int8(v99), valInt8)
	require.Equal(t, int16(v99), valInt16)
	require.Equal(t, int32(v99), valInt32)
	require.Equal(t, int64(v99), valInt64)
	require.Equal(t, uint(v99), valUint)
	require.Equal(t, uint8(v99), valUint8)
	require.Equal(t, uint16(v99), valUint16)
	require.Equal(t, uint32(v99), valUint32)
	require.Equal(t, uint64(v99), valUint64)

	for _, cs := range []interface{}{
		valFloat32, &valFloat32, valFloat64, &valFloat64,
		valComplex64, &valComplex64, valComplex128, &valComplex128,
	} {
		t.Log(Any2String(cs))
	}
	require.NoError(t, String2Any("99.99", &valFloat32))
	require.NoError(t, String2Any("99.99", &valFloat64))
	require.NoError(t, String2Any("(99+99i)", &valComplex64))
	require.NoError(t, String2Any("(99+99i)", &valComplex128))
	t.Log(valFloat32, valFloat64, valComplex64, valComplex128)

	t.Log("unknown type", Any2String(val))
	require.Error(t, String2Any(s99, &val))
	require.Error(t, String2Any("not-bool", &valBool))
}

func BenchmarkStrFmt2String(b *testing.B) {
	var val int64 = 77
	for ii := 0; ii < b.N; ii++ {
		_ = fmt.Sprintf("%v", val)
	}
}

func BenchmarkStrAny2String(b *testing.B) {
	var val int64 = 77
	for ii := 0; ii < b.N; ii++ {
		_ = Any2String(val)
	}
}

func BenchmarkStrString2Any(b *testing.B) {
	const s = "77.77"
	var valFloat64 float64
	for ii := 0; ii < b.N; ii++ {
		_ = String2Any(s, &valFloat64)
	}
}
