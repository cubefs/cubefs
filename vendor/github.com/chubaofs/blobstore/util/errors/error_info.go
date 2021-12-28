package errors

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

const prefix = " --> "

// Cause returns the cause of this error
func Cause(err error) error {
	if e, ok := err.(interface{ Cause() error }); ok {
		if diag := e.Cause(); diag != nil {
			return diag
		}
	}
	return err
}

// Detail returns detail of error, add prefix sign at the first
func Detail(err error) string {
	if err == nil {
		return ""
	}

	if e, ok := err.(interface{ Details() string }); ok {
		return e.Details()
	}

	builder := strings.Builder{}
	builder.WriteString(prefix)
	builder.WriteString(err.Error())
	return builder.String()
}

// Error error with detail
type Error struct {
	Err  error
	Why  error
	File string
	Line int
	Cmd  []interface{}
}

// Base returns a runtime.Caller(1) detail error based the error
func Base(err error, cmd ...interface{}) *Error {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "???"
	}
	return &Error{Err: Cause(err), Why: err, File: file, Line: line, Cmd: cmd}
}

// Info alias of Base, deprecated
func Info(err error, cmd ...interface{}) *Error {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "???"
	}
	return &Error{Err: Cause(err), Why: err, File: file, Line: line, Cmd: cmd}
}

// BaseEx returns a runtime.Caller(skip) detail error based the error.
// file and line tracing may have problems with go1.9,
// see related issue: https://github.com/golang/go/issues/22916
func BaseEx(skip int, err error, cmd ...interface{}) *Error {
	oldErr := err
	if e, ok := err.(*Error); ok {
		err = e.Err
	}
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file = "???"
	}
	return &Error{Err: Cause(err), Why: oldErr, File: file, Line: line, Cmd: cmd}
}

// InfoEx alias of BaseEx, deprecated
func InfoEx(skip int, err error, cmd ...interface{}) *Error {
	return BaseEx(skip, err, cmd...)
}

// Cause returns the cause of this error
func (r *Error) Cause() error {
	return r.Err
}

// Unwrap returns why of the error
func (r *Error) Unwrap() error {
	return r.Why
}

// Error returns base error Error()
func (r *Error) Error() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return ""
}

// Details returns detail message of the error
func (r *Error) Details() string {
	builder := strings.Builder{}
	builder.WriteString(prefix)
	builder.WriteString(r.File)
	builder.WriteByte(':')
	builder.WriteString(strconv.Itoa(r.Line))
	builder.WriteByte(' ')
	builder.WriteString(r.Error())
	if len(r.Cmd) > 0 {
		builder.WriteString(" ~ ")
		builder.WriteString(stringJoin(r.Cmd...))
	}
	if r.Why != nil && r.Why != r.Err {
		builder.WriteString(Detail(r.Why))
	}
	return builder.String()
}

// Detail returns Error with why
func (r *Error) Detail(err error) *Error {
	r.Why = err
	return r
}

func stringJoin(v ...interface{}) string {
	builder := strings.Builder{}
	for idx, value := range v {
		if idx > 0 {
			builder.WriteByte(' ')
		}
		builder.WriteString(fmt.Sprintf("%+v", value))
	}
	return builder.String()
}
