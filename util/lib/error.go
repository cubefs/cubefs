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

package lib

import (
	"fmt"
	"net/http"
	"syscall"
)

type Error struct {
	status int
	code   int
	msg    string
}

func newError(statusCode int, errCode int, msg string) *Error {
	return &Error{
		status: statusCode,
		code:   errCode,
		msg:    msg,
	}
}

func NewErr(statusCode int, errCode int, msg string) *Error {
	return newError(statusCode, errCode, msg)
}

// statusCode returns http status code
func (e *Error) StatusCode() int {
	return e.status
}

// ErrorCode returns special defined code
func (e *Error) ErrorCode() int {
	return e.code
}

// Error implements error
func (e *Error) Error() string {
	return e.msg
}

func (e *Error) With(err error) *Error {
	newE := &Error{
		code:   e.code,
		status: e.status,
	}
	newE.msg = fmt.Sprintf("%s, err :: %s", e.msg, err.Error())
	return newE
}

func (e *Error) WithStr(str string) *Error {
	newE := &Error{
		code:   e.code,
		status: e.status,
	}
	newE.msg = fmt.Sprintf("%s, err :: %s", e.msg, str)
	return newE
}

// func (e *Error) IsErr(code int) bool {
// 	return e.code == code
// }

func (e *Error) Same(err error) bool {
	newErr, ok := err.(*Error)
	if !ok {
		return false
	}

	return e.code == newErr.code
}

type HttpReply struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// common error
var (
	ARGS_ERR    = newError(http.StatusBadRequest, 1, "bad arguments")
	UNKNOWN_ERR = newError(http.StatusInternalServerError, 2, "unknown error")
	AUTH_FAILED = newError(http.StatusForbidden, 3, "auth failed")
)

// router err
var (
	ERR_ROUTE_NOT_EXIST = newError(http.StatusOK, 7001, "route not exist")
	ERR_ROUTE_EXIST     = newError(http.StatusOK, 7002, "route already exist")
	ERR_ROUTE_LOCKED    = newError(http.StatusOK, 7003, "route is locked")
)

// posix error
var (
	ERR_PROXY_AGAIN     = newError(http.StatusOK, 1001, "try again")
	ERR_PROXY_EEXIST    = newError(http.StatusOK, 1002, "file already exist")
	ERR_PROXY_ENOENT    = newError(http.StatusOK, 1003, "no such file or dir")
	ERR_PROXY_SUPPORT   = newError(http.StatusOK, 1004, "api not support")
	ERR_PROXY_EIO       = newError(http.StatusOK, 1005, "io error")
	ERR_PROXY_NOT_EMPTY = newError(http.StatusOK, 1006, "dir not empty")
	ERR_PROXY_NO_PERM   = newError(http.StatusOK, 1007, "no perm")
)

func SyscallToErr(err error) *Error {
	if err == nil {
		return nil
	}

	newErr, ok := err.(syscall.Errno)
	if !ok {
		return UNKNOWN_ERR
	}

	switch newErr {
	case syscall.EAGAIN:
		return ERR_PROXY_AGAIN
	case syscall.EEXIST:
		return ERR_PROXY_EEXIST
	case syscall.ENOENT:
		return ERR_PROXY_ENOENT
	case syscall.ENOTSUP:
		return ERR_PROXY_SUPPORT
	case syscall.EIO:
		return ERR_PROXY_EIO
	case syscall.ENOTEMPTY:
		return ERR_PROXY_NOT_EMPTY
	case syscall.EPERM:
		return ERR_PROXY_NO_PERM
	default:
		return UNKNOWN_ERR
	}
}
