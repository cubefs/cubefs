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

package rpc

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestArgumentRegister(t *testing.T) {
	log.SetOutputLevel(log.Linfo)
	defer setLogLevel()
	defer func() {
		registeredParsers = make(map[parserKey]parserVal)
	}()
	{
		RegisterArgsParser(nil)
		RegisterArgsParser(struct{ Parser }{})
	}
	{
		type args struct{}
		require.Panics(t, func() {
			RegisterArgsParser(args{})
		})
		require.Panics(t, func() {
			RegisterArgsParser(make(map[int]int))
		})
		require.Panics(t, func() {
			var i int
			RegisterArgsParser(&i)
		})
		RegisterArgsParser(&args{})
	}
	{
		type Args struct {
			F1 string `taglv3:"f1-3" taglv2:"f1-2" taglv1:"f1-1"`
			F2 string `taglv2:"f2-2" taglv1:"f2-1"`
			F3 string `taglv1:"f3-1,omitempty"`
			F4 []byte `taglv1:"f4-1,base64"`
			F5 string
			F6 string `taglv3:"-"`
		}
		RegisterArgsParser(&Args{}, "taglv3", "taglv2", "taglv1")

		byteVal := base64.URLEncoding.EncodeToString([]byte("f4-1"))
		args := new(Args)
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "f1-3", Value: "f1-3"},
			httprouter.Param{Key: "f1-2", Value: "f1-2"},
			httprouter.Param{Key: "f1-1", Value: "f1-1"},
			httprouter.Param{Key: "f2-2", Value: "f2-2"},
			httprouter.Param{Key: "f2-1", Value: "f2-1"},
			httprouter.Param{Key: "f4-1", Value: byteVal},
			httprouter.Param{Key: "f5", Value: "f5"},
			httprouter.Param{Key: "f6", Value: "f6"},
		}
		parseArgs(c, args, OptArgsURI())

		require.Equal(t, "f1-3", args.F1)
		require.Equal(t, "f2-2", args.F2)
		require.Equal(t, "", args.F3)
		require.Equal(t, []byte("f4-1"), args.F4)
		require.Equal(t, "f5", args.F5)
		require.Equal(t, "", args.F6)
	}
}

func TestArgumentBase64(t *testing.T) {
	log.SetOutputLevel(log.Linfo)
	defer setLogLevel()
	defer func() {
		registeredParsers = make(map[parserKey]parserVal)
	}()
	type Args struct {
		Base64 []byte `tag:"base64,base64"`
	}
	RegisterArgsParser(&Args{}, "tag")

	// "ZjQtMQ==" == []byte("f4-1")
	cases := []struct {
		hasErr bool
		val    string
	}{
		{false, "ZjQtMQ=="},
		{false, "ZjQtMQ="},
		{false, "ZjQtMQ"},
		{true, "ZjQtM==="},
		{true, "ZjQtM"},
	}
	for _, cs := range cases {
		args := new(Args)
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "base64", Value: cs.val},
		}
		err := parseArgs(c, args, OptArgsURI())
		if cs.hasErr {
			require.Error(t, err)
		} else {
			require.Equal(t, []byte("f4-1"), args.Base64)
			require.NoError(t, err)
		}
	}
}

type unmarshalerArgs struct {
	I int
}

func (args *unmarshalerArgs) Unmarshal([]byte) error {
	args.I = 111
	return nil
}

type normalArgs struct {
	Bool bool

	Int   int
	Int8  int8
	Int16 int16
	Int32 int32
	Int64 int64

	Uint   uint
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64

	Float32 float32
	Float64 float64

	String  string
	Uintptr uintptr
	Ptr     *string

	Slice []byte
}

type parserArgs struct {
	getter string
	normalArgs
}

func (args *parserArgs) Parse(getter ValueGetter) error {
	args.getter = "getter"
	return nil
}

func TestArgumentParser(t *testing.T) {
	{
		err := parseArgs(nil, nil)
		require.NoError(t, err)

		c := new(Context)
		c.opts = new(serverOptions)
		err = parseArgs(c, struct{}{}, OptArgsURI())
		require.Error(t, err)
		var i int
		err = parseArgs(c, &i, OptArgsURI())
		require.Error(t, err)

		args := new(unmarshalerArgs)
		err = parseArgs(c, args, OptArgsURI())
		require.Error(t, err)
	}
	{
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "slice", Value: "[]"},
			httprouter.Param{Key: "map", Value: "{}"},
		}

		args := struct {
			Slice []string
		}{}
		err := parseArgs(c, &args, OptArgsURI())
		require.Error(t, err)

		argsx := struct {
			Map map[int]string
		}{}
		err = parseArgs(c, &argsx, OptArgsURI())
		require.Error(t, err)
	}
	{
		c := new(Context)
		c.opts = new(serverOptions)
		c.Request, _ = http.NewRequest("", "", nil)

		args := new(unmarshalerArgs)
		err := parseArgs(c, args, OptArgsBody())
		require.NoError(t, err)
		require.Equal(t, 111, args.I)

		args = new(unmarshalerArgs)
		err = parseArgs(c, args)
		require.NoError(t, err)
		require.Equal(t, 0, args.I)
	}
	{
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "bool", Value: "true"},
			httprouter.Param{Key: "int", Value: "-1111"},
			httprouter.Param{Key: "int8", Value: "-1"},
			httprouter.Param{Key: "int16", Value: "-1111"},
			httprouter.Param{Key: "int32", Value: "-1111"},
			httprouter.Param{Key: "int64", Value: "-1111"},
			httprouter.Param{Key: "uint", Value: "1111"},
			httprouter.Param{Key: "uint8", Value: "1"},
			httprouter.Param{Key: "uint16", Value: "1111"},
			httprouter.Param{Key: "uint32", Value: "1111"},
			httprouter.Param{Key: "uint64", Value: "1111"},
			httprouter.Param{Key: "float32", Value: "1e-3"},
			httprouter.Param{Key: "float64", Value: "1e-32"},
			httprouter.Param{Key: "string", Value: "string"},
			httprouter.Param{Key: "uintptr", Value: "1111"},
			httprouter.Param{Key: "ptr", Value: "string"},
			httprouter.Param{Key: "slice", Value: "slice"},
		}

		args := new(normalArgs)
		err := parseArgs(c, args, OptArgsURI())
		require.NoError(t, err)

		require.True(t, args.Bool)
		require.Equal(t, -1111, args.Int)
		require.Equal(t, int8(-1), args.Int8)
		require.Equal(t, int16(-1111), args.Int16)
		require.Equal(t, int32(-1111), args.Int32)
		require.Equal(t, int64(-1111), args.Int64)
		require.Equal(t, uint(1111), args.Uint)
		require.Equal(t, uint8(1), args.Uint8)
		require.Equal(t, uint16(1111), args.Uint16)
		require.Equal(t, uint32(1111), args.Uint32)
		require.Equal(t, uint64(1111), args.Uint64)
		require.Equal(t, float32(0.001), args.Float32)
		require.Equal(t, float64(1e-32), args.Float64)
		require.Equal(t, "string", args.String)
		require.Equal(t, uintptr(1111), args.Uintptr)
		require.Equal(t, "string", *args.Ptr)
		require.Equal(t, []byte("slice"), args.Slice)
	}
	{
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "bool", Value: "true"},
		}

		args := new(parserArgs)
		err := parseArgs(c, args, OptArgsURI())
		require.NoError(t, err)

		require.False(t, args.Bool)
		require.Equal(t, "getter", args.getter)
	}
}

func BenchmarkArgumentParse(b *testing.B) {
	b.Run("ArgsBodyJson", func(b *testing.B) {
		type jsonArgs struct {
			I int
		}
		c := new(Context)
		c.opts = new(serverOptions)
		c.Request, _ = http.NewRequest("", "", nil)
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			args := new(jsonArgs)
			parseArgs(c, args, OptArgsBody())
		}
	})
	b.Run("ArgsBodyMarshaler", func(b *testing.B) {
		c := new(Context)
		c.opts = new(serverOptions)
		c.Request, _ = http.NewRequest("", "", nil)
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			args := new(unmarshalerArgs)
			parseArgs(c, args, OptArgsBody())
		}
	})
	b.Run("ArgsURISimple", func(b *testing.B) {
		type simpleURI struct {
			Bool bool
		}
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "bool", Value: "true"},
		}
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			args := new(simpleURI)
			parseArgs(c, args, OptArgsURI())
		}
	})
	b.Run("ArgsURIComplex", func(b *testing.B) {
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "bool", Value: "true"},
			httprouter.Param{Key: "int", Value: "-1111"},
			httprouter.Param{Key: "int8", Value: "-1"},
			httprouter.Param{Key: "int16", Value: "-1111"},
			httprouter.Param{Key: "int32", Value: "-1111"},
			httprouter.Param{Key: "int64", Value: "-1111"},
			httprouter.Param{Key: "uint", Value: "1111"},
			httprouter.Param{Key: "uint8", Value: "1"},
			httprouter.Param{Key: "uint16", Value: "1111"},
			httprouter.Param{Key: "uint32", Value: "1111"},
			httprouter.Param{Key: "uint64", Value: "1111"},
			httprouter.Param{Key: "float32", Value: "1e-3"},
			httprouter.Param{Key: "float64", Value: "1e-32"},
			httprouter.Param{Key: "string", Value: "string"},
			httprouter.Param{Key: "uintptr", Value: "1111"},
			httprouter.Param{Key: "ptr", Value: "string"},
			httprouter.Param{Key: "slice", Value: "slice"},
		}
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			args := new(normalArgs)
			parseArgs(c, args, OptArgsURI())
		}
	})
	b.Run("ArgsURIParser", func(b *testing.B) {
		c := new(Context)
		c.opts = new(serverOptions)
		c.Param = httprouter.Params{
			httprouter.Param{Key: "bool", Value: "true"},
		}
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			args := new(parserArgs)
			parseArgs(c, args, OptArgsURI())
		}
	})
}
