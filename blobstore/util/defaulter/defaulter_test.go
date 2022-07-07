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

package defaulter_test

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/stretchr/testify/require"
)

func TestDefaulterString(t *testing.T) {
	for idx, cs := range []struct {
		val, def, exp string
	}{
		{"foo", "bar", "foo"},
		{"foo", "", "foo"},
		{"", "bar", "bar"},
		{"", "", ""},
	} {
		defaulter.Empty(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}

	pfoo := func() *string {
		foo := "foo"
		return &foo
	}
	pempty := func() *string {
		empty := ""
		return &empty
	}
	for idx, cs := range []struct {
		val      *string
		def, exp string
	}{
		{pfoo(), "bar", "foo"},
		{pfoo(), "", "foo"},
		{pempty(), "bar", "bar"},
		{pempty(), "", ""},
	} {
		defaulter.Empty(cs.val, cs.def)
		require.Equal(t, cs.exp, *cs.val, idx)
	}
}

func TestDefaulterBasicNotType(t *testing.T) {
	require.Panics(t, func() {
		val := int(0)
		defaulter.Equal(val, 10)
	})
	require.Panics(t, func() {
		val := int(0)
		defaulter.Equal(&val, int64(10))
	})
	require.Panics(t, func() {
		val := ""
		defaulter.Equal(&val, "def")
	})
	require.Panics(t, func() {
		type none struct{}
		defaulter.Less(&none{}, none{})
	})
	require.Panics(t, func() {
		type none struct{}
		defaulter.LessOrEqual(&none{}, none{})
	})
}

func TestDefaulterBasicEqual(t *testing.T) {
	for idx, cs := range []struct {
		val      bool
		def, exp interface{}
	}{
		{true, true, true},
		{false, true, true},
		{false, false, false},
	} {
		defaulter.Equal(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp int64
	}{
		{-1, 1, -1},
		{-1, 0, -1},
		{0, 1, 1},
		{0, 0, 0},
		{1, 2, 1},
		{1, 0, 1},
	} {
		defaulter.Equal(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp uint64
	}{
		{0, 1, 1},
		{0, 0, 0},
		{1, 2, 1},
		{1, 0, 1},
	} {
		defaulter.Equal(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp float64
	}{
		{-1.1, 0.1, -1.1},
		{-1e-10, 0.1, -1e-10},
		{0, 0.1, 0.1},
		{0, 0, 0},
		{1e-10, 0.1, 1e-10},
		{1.1, 2.1, 1.1},
		{1, 0, 1},
	} {
		defaulter.Equal(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
}

func TestDefaulterBasicLess(t *testing.T) {
	for idx, cs := range []struct {
		val, def, exp int64
	}{
		{-1, 1, 1},
		{-1, 0, 0},
		{0, 1, 0},
		{0, 0, 0},
		{1, 2, 1},
		{1, 0, 1},
	} {
		defaulter.Less(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp uint64
	}{
		{0, 1, 0},
		{0, 0, 0},
		{1, 2, 1},
	} {
		defaulter.Less(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp float64
	}{
		{-1.1, 0.1, 0.1},
		{-1e-10, 0.1, -1e-10},
		{0, 0.1, 0},
		{0, 0, 0},
		{1e-10, 0.1, 1e-10},
		{1.1, 2.1, 1.1},
		{1, 0, 1},
	} {
		defaulter.Less(&cs.val, cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
}

func TestDefaulterBasicLessOrEqual(t *testing.T) {
	for idx, cs := range []struct {
		val, def, exp int64
	}{
		{-1, 1, 1},
		{-1, 0, 0},
		{0, 1, 1},
		{0, 0, 0},
		{1, 2, 1},
		{1, 0, 1},
	} {
		defaulter.LessOrEqual(&cs.val, &cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp uint64
	}{
		{0, 1, 1},
		{0, 0, 0},
		{1, 2, 1},
	} {
		defaulter.LessOrEqual(&cs.val, &cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
	for idx, cs := range []struct {
		val, def, exp float64
	}{
		{-1.1, 0.1, 0.1},
		{-1e-10, 0.1, 0.1},
		{0, 0.1, 0.1},
		{0, 0, 0},
		{1e-10, 0.1, 0.1},
		{1e-8, 0.1, 1e-8},
		{1.1, 2.1, 1.1},
		{1, 0, 1},
	} {
		defaulter.LessOrEqual(&cs.val, &cs.def)
		require.Equal(t, cs.exp, cs.val, idx)
	}
}

func BenchmarkDefaulterString(b *testing.B) {
	val, def := "", "foo"
	for ii := 0; ii < b.N; ii++ {
		defaulter.Empty(&val, def)
	}
}

func BenchmarkDefaulterInt(b *testing.B) {
	val, def := int(0), int(1)
	for ii := 0; ii < b.N; ii++ {
		defaulter.LessOrEqual(&val, def)
	}
}

func BenchmarkDefaulterUint(b *testing.B) {
	val, def := uint(0), uint(1)
	for ii := 0; ii < b.N; ii++ {
		defaulter.LessOrEqual(&val, def)
	}
}

func BenchmarkDefaulterFloat(b *testing.B) {
	val, def := float32(0), float32(1)
	for ii := 0; ii < b.N; ii++ {
		defaulter.LessOrEqual(&val, def)
	}
}
