package errors

import (
	"errors"
	"fmt"
)

// New alias of errors.New
func New(msg string) error {
	return errors.New(msg)
}

// Newf alias of fmt.Errorf
func Newf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

// Newx returns error with multi message
func Newx(v ...interface{}) error {
	return errors.New(stringJoin(v...))
}

// As alias of errors.As
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// Is alias of errors.Is
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// Unwrap alias of errors.Unwrap
func Unwrap(err error) error {
	return errors.Unwrap(err)
}
