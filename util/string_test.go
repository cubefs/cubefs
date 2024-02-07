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

package util_test

import (
	"fmt"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func TestRandomString(t *testing.T) {
	first := util.RandomString(256, util.UpperLetter|util.LowerLetter|util.Numeric)
	second := util.RandomString(256, util.UpperLetter|util.LowerLetter|util.Numeric)
	require.NotEqual(t, first, second)
}

func TestSubStr(t *testing.T) {
	str := "abcd"
	require.Equal(t, "b", util.SubString(str, 1, 2))
	require.Equal(t, str, util.SubString(str, -1, 100))
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
		require.Equal(t, "true", util.Any2String(cs))
	}
	require.NoError(t, util.String2Any("false", &valBool))
	require.False(t, valBool)

	for _, cs := range []interface{}{
		valStr, &valStr,
		valInt, &valInt, valInt8, &valInt8, valInt16, &valInt16,
		valInt32, &valInt32, valInt64, &valInt64,
		valUint, &valUint, valUint8, &valUint8, valUint16, &valUint16,
		valUint32, &valUint32, valUint64, &valUint64,
	} {
		require.Equal(t, s77, util.Any2String(cs))
	}
	require.NoError(t, util.String2Any(s99, &valStr))
	require.NoError(t, util.String2Any(s99, &valInt))
	require.NoError(t, util.String2Any(s99, &valInt8))
	require.NoError(t, util.String2Any(s99, &valInt16))
	require.NoError(t, util.String2Any(s99, &valInt32))
	require.NoError(t, util.String2Any(s99, &valInt64))
	require.NoError(t, util.String2Any(s99, &valUint))
	require.NoError(t, util.String2Any(s99, &valUint8))
	require.NoError(t, util.String2Any(s99, &valUint16))
	require.NoError(t, util.String2Any(s99, &valUint32))
	require.NoError(t, util.String2Any(s99, &valUint64))
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
		t.Logf(util.Any2String(cs))
	}
	require.NoError(t, util.String2Any("99.99", &valFloat32))
	require.NoError(t, util.String2Any("99.99", &valFloat64))
	require.NoError(t, util.String2Any("(99+99i)", &valComplex64))
	require.NoError(t, util.String2Any("(99+99i)", &valComplex128))
	t.Log(valFloat32, valFloat64, valComplex64, valComplex128)

	t.Log("unknown type", util.Any2String(val))
	require.Error(t, util.String2Any(s99, &val))
	require.Error(t, util.String2Any("not-bool", &valBool))
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
		_ = util.Any2String(val)
	}
}

func BenchmarkStrString2Any(b *testing.B) {
	const s = "77.77"
	var valFloat64 float64
	for ii := 0; ii < b.N; ii++ {
		_ = util.String2Any(s, &valFloat64)
	}
}
