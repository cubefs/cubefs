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
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"syscall"
)

type (
	// Error implements HTTPError
	Error struct {
		Status int    // http status code
		Code   string // error code
		Err    error  // error
	}

	// errorResponse response error with json
	// internal type between server and client
	errorResponse struct {
		Error string `json:"error"`
		Code  string `json:"code,omitempty"`
	}

	statusCoder interface {
		StatusCode() int
	}
	errorCoder interface {
		ErrorCode() string
	}
)

var _ HTTPError = &Error{}

// NewError new error with
func NewError(statusCode int, errCode string, err error) *Error {
	return &Error{
		Status: statusCode,
		Code:   errCode,
		Err:    err,
	}
}

// StatusCode returns http status code
func (e *Error) StatusCode() int {
	return e.Status
}

// ErrorCode returns special defined code
func (e *Error) ErrorCode() string {
	return e.Code
}

// Error implements error
func (e *Error) Error() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

// Unwrap errors.Is(), errors.As() and errors.Unwrap()
func (e *Error) Unwrap() error {
	return e.Err
}

// DetectStatusCode returns http status code
func DetectStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}

	var st statusCoder
	if errors.As(err, &st) {
		return st.StatusCode()
	}

	switch err {
	case syscall.EINVAL:
		return http.StatusBadRequest
	case context.Canceled:
		return 499
	}
	return http.StatusInternalServerError
}

// DetectErrorCode returns error code
func DetectErrorCode(err error) string {
	if err == nil {
		return ""
	}

	var ec errorCoder
	if errors.As(err, &ec) {
		return ec.ErrorCode()
	}

	switch err {
	case syscall.EINVAL:
		return "BadRequest"
	case context.Canceled:
		return "Canceled"
	}
	return "InternalServerError"
}

// DetectError returns status code, error code, error
func DetectError(err error) (int, string, error) {
	return DetectStatusCode(err), DetectErrorCode(err), errors.Unwrap(err)
}

// Error2HTTPError returns an interface HTTPError from an error
func Error2HTTPError(err error) HTTPError {
	if err == nil {
		return nil
	}
	if httpErr, ok := err.(HTTPError); ok {
		return httpErr
	}

	status, code, _ := DetectError(err)
	return NewError(status, code, err)
}

// ReplyErr directly reply error with response writer
func ReplyErr(w http.ResponseWriter, code int, err string) {
	msg, _ := json.Marshal(NewError(code, "", errors.New(err)))
	h := w.Header()
	h.Set("Content-Length", strconv.Itoa(len(msg)))
	h.Set("Content-Type", MIMEJSON)
	w.WriteHeader(code)
	w.Write(msg)
}

// ReplyWith directly reply body with response writer
func ReplyWith(w http.ResponseWriter, code int, bodyType string, msg []byte) {
	h := w.Header()
	h.Set("Content-Length", strconv.Itoa(len(msg)))
	h.Set("Content-Type", bodyType)
	w.WriteHeader(code)
	w.Write(msg)
}
