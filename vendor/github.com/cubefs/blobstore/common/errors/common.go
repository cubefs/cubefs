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

package errors

import (
	"errors"
	"net/http"

	"github.com/cubefs/blobstore/common/rpc"
)

var (
	// 2xx
	ErrExist = newError(http.StatusCreated, "Data Already Exist")

	// 4xx
	ErrIllegalArguments             = newError(http.StatusBadRequest, "Illegal Arguments")
	ErrNotFound                     = newError(http.StatusNotFound, "Not Found")
	ErrRequestTimeout               = newError(http.StatusRequestTimeout, "Request Timeout")
	ErrRequestedRangeNotSatisfiable = newError(http.StatusRequestedRangeNotSatisfiable, "Request Range Not Satisfiable")
	ErrRequestNotAllow              = newError(http.StatusBadRequest, "Request Not Allow")
	ErrReaderError                  = newError(499, "Reader Error")

	// 5xx errUnexpected - unexpected error, requires manual intervention.
	ErrUnexpected = newError(http.StatusInternalServerError, "Unexpected Error")
)

func newError(status int, msg string) *rpc.Error {
	return rpc.NewError(status, "", errors.New(msg))
}
