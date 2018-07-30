package storage

import (
	"errors"
	"fmt"
)

// Error definitions
var (
	ErrorObjNotFound       = errors.New("object not exist")
	ErrorFileNotFound      = errors.New("file not exist")
	ErrorPartitionReadOnly = errors.New("partition readonly")
	ErrorHasDelete         = errors.New("has delete")
	ErrorParamMismatch     = errors.New("parameter mismatch error")
	ErrorNoAvaliFile       = errors.New("no avail file")
	ErrorNoUnAvaliFile     = errors.New("no Unavail file")
	ErrorNewStoreMode      = errors.New("error new store mode ")
	ErrExtentNameFormat    = errors.New("extent filePath format error")
	ErrSyscallNoSpace      = errors.New("no space left on device")
	ErrorAgain             = errors.New("try again")
	ErrorCompaction        = errors.New("compaction error")
	ErrorCommit            = errors.New("commit error")
	ErrObjectSmaller       = errors.New("object smaller error")
	ErrPkgCrcMismatch      = errors.New("pkg crc is not equal pkg data")
)

func NewParamMismatchErr(msg string) (err error) {
	err = fmt.Errorf("parameter mismatch error: %s", msg)
	return
}
