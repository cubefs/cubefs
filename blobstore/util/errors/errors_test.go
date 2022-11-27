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
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cubefs/cubefs/blobstore/util/errors"
)

type unwrap struct {
	err error
}

func (w *unwrap) Unwrap() error { return w.err }
func (w *unwrap) Error() string { return w.err.Error() }

func TestErrors(t *testing.T) {
	require.Equal(t, errors.New("foo"), New("foo"))

	require.Equal(t, errors.New("foo"), Newf("foo"))
	require.Equal(t, errors.New("foo bar"), Newf("%s %s", "foo", "bar"))

	require.Equal(t, errors.New("foo bar"), Newx("foo", "bar"))
	require.Equal(t, errors.New("foo bar"), Newx("foo", bytes.NewBuffer([]byte("bar"))))

	require.True(t, Is(nil, nil))

	require.NoError(t, Unwrap(nil))
	require.NoError(t, Unwrap(New("foo")))
	require.Error(t, Unwrap(&unwrap{New("foo")}))
}
