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

package common

import (
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCmdCommonParse(t *testing.T) {
	{
		r := new(http.Request)
		r.Method = "POST"
		var b bool
		require.Error(t, ParseArguments(r, NewArgument("b", &b)))
	}

	key1 := "name"
	key2 := "id"
	r := new(http.Request)
	r.Form = make(url.Values)
	r.Form.Set(key1, "name")

	var valint int
	err := errors.New("run on")
	errEmpty := errors.New("was empty")
	errError := errors.New("has error")
	require.Error(t, ParseArguments(r, NewArgument(key2, &valint)))
	require.NoError(t, ParseArguments(r, NewArgument(key2, &valint).OmitEmpty()))
	require.ErrorIs(t, errEmpty, ParseArguments(r, NewArgument(key2, &valint).OmitEmpty().
		OnEmpty(func() error { return errEmpty })))
	require.ErrorIs(t, errEmpty, ParseArguments(r, NewArgument(key2, &valint).OmitEmpty().
		OnEmpty(func() error { return errEmpty }).
		OnValue(func() error { return err })))
	r.Form.Set(key2, "not-number")
	require.Error(t, ParseArguments(r, NewArgument(key2, &valint)))
	require.NoError(t, ParseArguments(r, NewArgument(key2, &valint).OmitError()))
	require.ErrorIs(t, errError, ParseArguments(r, NewArgument(key2, &valint).OmitError().
		OnError(func() error { return errError })))
	require.ErrorIs(t, errError, ParseArguments(r, NewArgument(key2, &valint).OmitError().
		OnError(func() error { return errError }).
		OnEmpty(func() error { return errEmpty }).
		OnValue(func() error { return err })))
	r.Form.Set(key2, "-10")

	var valstr string
	require.NoError(t, ParseArguments(r, NewArgument(key1, &valstr), NewArgument(key2, &valint)))
	require.Equal(t, "name", valstr)
	require.Equal(t, -10, valint)
	require.ErrorIs(t, err, ParseArguments(r, NewArgument(key1, &valstr),
		NewArgument(key2, &valint).OnValue(func() error { return err })))
	require.NoError(t, ParseArguments(r, NewArgument(key1, &valstr),
		NewArgument(key2, &valint).OnValue(func() error { valint = 10; return nil })))
	require.Equal(t, 10, valint)
}

func TestCmdCommonArgs(t *testing.T) {
	r := new(http.Request)
	r.Form = make(url.Values)
	r.Form.Set("bool", "1")
	r.Form.Set("int", "11")
	r.Form.Set("uint", "111")
	r.Form.Set("float", "11.11")
	r.Form.Set("string", "11111")
	r.Form.Set("id", "222")

	{
		var b Bool
		require.NoError(t, ParseArguments(r, b.Key("bool")))
		require.True(t, b.V)
		require.Error(t, ParseArguments(r, b.Enable()))
	}
	{
		var i Int
		require.NoError(t, ParseArguments(r, i.Key("int")))
		require.Equal(t, int64(11), i.V)
		require.NoError(t, ParseArguments(r, i.ID()))
		require.Equal(t, int64(222), i.V)
	}
	{
		var u Uint
		require.NoError(t, ParseArguments(r, u.Key("uint")))
		require.Equal(t, uint64(111), u.V)
		require.NoError(t, ParseArguments(r, u.ID()))
		require.Equal(t, uint64(222), u.V)
	}
	{
		var f Float
		require.NoError(t, ParseArguments(r, f.Key("float")))
		require.Less(t, float64(11), f.V)
		require.Greater(t, float64(12), f.V)
	}
	{
		var s String
		require.NoError(t, ParseArguments(r, s.Key("string")))
		require.Equal(t, "11111", s.V)
	}
}
