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

package rpc2

import (
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRpc2Header(t *testing.T) {
	var header Header
	require.False(t, header.Has("a"))

	header.Set("a", "a")
	header.Set("b", "b")
	header.Set("c", "c")
	require.True(t, header.Has("a"))

	maxBuff := make([]byte, MaxHeaderLength+1)
	maxBuff[0] = 'a'
	header.Set(string(maxBuff), "x")
	require.False(t, header.Has(string(maxBuff)))
	header.Set("x", string(maxBuff))
	require.False(t, header.Has("x"))
	for idx := range [MaxHeaders]struct{}{} {
		header.Set("id-"+strconv.Itoa(idx), "")
	}
	require.Equal(t, MaxHeaders, len(header.M))
	header.Del("a")
	require.False(t, header.Has("a"))
	header.SetStable()
	header.Set("a", "a")
	require.False(t, header.Has("a"))
	header.Del("b")
	require.True(t, header.Has("b"))

	header.Merge(header)

	header = header.Clone()
	header.Set("a", "a")
	require.True(t, header.Has("a"))

	header.Reset()
	require.Nil(t, header.M)
}

func TestRpc2FixedHeader(t *testing.T) {
	var header FixedHeader
	require.False(t, header.Has("a"))

	header.Set("a", "a")
	header.Set("b", "b")
	header.Set("c", "c")
	require.True(t, header.Has("a"))

	maxBuff := make([]byte, MaxHeaderLength+1)
	maxBuff[0] = 'a'
	header.Set(string(maxBuff), "x")
	require.False(t, header.Has(string(maxBuff)))
	header.Set("x", string(maxBuff))
	require.False(t, header.Has("x"))
	for idx := range [MaxHeaders]struct{}{} {
		header.Set("id-"+strconv.Itoa(idx), "")
	}
	require.Equal(t, MaxHeaders, len(header.M))
	header.Del("a")
	require.False(t, header.Has("a"))

	o := header.ToHeader()
	header.MergeHeader(o)

	header.SetStable()
	header.MergeHeader(o)
	header.Set("a", "a")
	require.False(t, header.Has("a"))
	header.Del("b")
	require.True(t, header.Has("b"))

	header.Reset()
	require.Nil(t, header.M)
}

func TestRpc2FixedHeaderReader(t *testing.T) {
	var header FixedHeader
	require.Equal(t, NoBody, header.Reader())

	header.Add("a", "aaaa")
	b, _ := io.ReadAll(header.Reader())
	require.Equal(t, header.Get("a"), string(b))

	header.Set("c", "cccc")
	header.SetLen("b", 8)
	header.Set("b", "123456789")

	b, _ = io.ReadAll(header.Reader())
	require.Equal(t, "aaaa12345678cccc", string(b))

	header.SetStable()
	header.Set("b", "87654321")
	header.Set("d", "dddd")
	b, _ = io.ReadAll(header.Reader())
	require.Equal(t, "aaaa87654321cccc", string(b))

	r := trailerReader{
		Fn:      func() error { return errLimitedWrite },
		Trailer: &header,
	}
	_, err := r.Read(nil)
	require.ErrorIs(t, errLimitedWrite, err)
}
