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

package errors_test

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestErrorBase(t *testing.T) {
	e := New("base")
	err := Base(e, "foo.Bar failed: abc", struct{ float float64 }{3.14})
	require.ErrorIs(t, e, Cause(err))
	require.ErrorIs(t, e, err.Cause())
	require.ErrorIs(t, e, err.Unwrap())

	msg := err.Details()
	t.Log(msg)
	end := "base ~ foo.Bar failed: abc {float:3.14}"
	require.Equal(t, end, msg[len(msg)-len(end):])

	// Detail with base error
	err.Detail(New("basex"))
	require.NotErrorIs(t, e, err.Unwrap())
	msg = err.Details()
	t.Log(msg)
	end = "base ~ foo.Bar failed: abc {float:3.14} --> basex"
	require.Equal(t, end, msg[len(msg)-len(end):])

	// Detail with Error
	err.Detail(Base(New("basexx"), "detail"))
	msg = err.Details()
	t.Log(msg)
	end = "basexx ~ detail"
	require.Equal(t, end, msg[len(msg)-len(end):])
}

func TestErrorDetail(t *testing.T) {
	require.Equal(t, "", Detail(nil))

	err := New("detail error")
	err = InfoEx(2, syscall.EINVAL, "TestErrorDetail failed").Detail(err)
	msg := Detail(err)
	t.Log(msg)
	end := " invalid argument ~ TestErrorDetail failed --> detail error"
	require.Equal(t, end, msg[len(msg)-len(end):])
}
