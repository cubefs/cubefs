package errors

import (
	"errors"
	"net/http"

	"github.com/chubaofs/blobstore/common/rpc"
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

// NewArgumentError argument with error
func NewArgumentError(err error) *rpc.Error {
	return rpc.NewError(http.StatusBadRequest, "Argument", err)
}
