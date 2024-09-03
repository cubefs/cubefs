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
	"errors"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

var (
	DetectErrorCode  = rpc.DetectErrorCode
	DetectStatusCode = rpc.DetectStatusCode
)

func DetectError(err error) (int, string, error) {
	uErr := errors.Unwrap(err)
	if uErr == nil {
		uErr = err
	}
	return DetectStatusCode(err), DetectErrorCode(err), uErr
}

var _ rpc.HTTPError = (*Error)(nil)

func NewError(status int32, reason, detail string) *Error {
	return &Error{Status: status, Reason: reason, Detail: detail}
}

func (m *Error) Unwrap() error     { return errors.New(m.Error()) }
func (m *Error) StatusCode() int   { return int(m.GetStatus()) }
func (m *Error) ErrorCode() string { return m.GetReason() }
func (m *Error) Error() string     { return m.GetDetail() }

func ErrorString(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok {
		return fmt.Sprintf("[%d|%s|%s]", e.GetStatus(), e.GetReason(), e.GetDetail())
	}
	return err.Error()
}
