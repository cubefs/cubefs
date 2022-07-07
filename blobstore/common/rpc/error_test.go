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
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorBase(t *testing.T) {
	{
		err := NewError(0, "", nil)
		require.Equal(t, 0, err.StatusCode())
		require.Equal(t, "", err.ErrorCode())
		require.Equal(t, "", err.Error())
	}
	{
		err := NewError(499, "Unknown", errors.New("unknown"))
		require.Equal(t, 499, err.StatusCode())
		require.Equal(t, "Unknown", err.ErrorCode())
		require.Equal(t, "unknown", err.Error())
	}
	{
		errBase := NewError(400, "", errors.New(""))
		err := NewError(499, "Unknown", errBase)
		require.ErrorIs(t, errBase, errors.Unwrap(err))
	}

	{
		require.Equal(t, 200, DetectStatusCode(nil))
		require.Equal(t, 222, DetectStatusCode(&Error{
			Status: 222,
			Code:   "CODE",
			Err:    errors.New("error"),
		}))
		require.Equal(t, 400, DetectStatusCode(syscall.EINVAL))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Equal(t, 499, DetectStatusCode(ctx.Err()))

		require.Equal(t, 500, DetectStatusCode(errors.New("server error")))
	}
	{
		require.Equal(t, "", DetectErrorCode(nil))
		require.Equal(t, "CODE", DetectErrorCode(&Error{
			Status: 222,
			Code:   "CODE",
			Err:    errors.New("error"),
		}))
		require.Equal(t, "BadRequest", DetectErrorCode(syscall.EINVAL))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Equal(t, "Canceled", DetectErrorCode(ctx.Err()))

		require.Equal(t, "InternalServerError", DetectErrorCode(errors.New("server error")))
	}
	{
		errBase := errors.New("error")
		status, code, err := DetectError(&Error{
			Status: 222,
			Code:   "CODE",
			Err:    errBase,
		})
		require.Equal(t, 222, status)
		require.Equal(t, "CODE", code)
		require.ErrorIs(t, errBase, err)
	}

	{
		httpErr := Error2HTTPError(nil)
		require.Nil(t, httpErr)
	}
	{
		errBase := errors.New("error")
		httpErr := Error2HTTPError(&Error{
			Status: 222,
			Code:   "CODE",
			Err:    errBase,
		})
		require.Equal(t, 222, httpErr.StatusCode())
		require.Equal(t, "CODE", httpErr.ErrorCode())
		require.Equal(t, "error", httpErr.Error())
	}
	{
		errBase := errors.New("error")
		httpErr := Error2HTTPError(errBase)
		require.Equal(t, 500, httpErr.StatusCode())
		require.Equal(t, "InternalServerError", httpErr.ErrorCode())
		require.Equal(t, "error", httpErr.Error())
	}
}
