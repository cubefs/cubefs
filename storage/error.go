// Copyright 2018 The ChuBao Authors.
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
