// Copyright 2018 The Chubao Authors.
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

package ecstorage

import (
	"errors"
	"fmt"
)

var (
	ExtentHasBeenDeletedError = errors.New("extent has been deleted")
	ParameterMismatchError    = errors.New("parameter mismatch error")
	NoAvailableExtentError    = errors.New("no available extent")
	NoBrokenExtentError       = errors.New("no unavailable extent")
	NoSpaceError              = errors.New("no space left on the device")
	TryAgainError             = errors.New("try again")
	CrcMismatchError          = errors.New("packet Crc is incorrect")
	NoLeaderError             = errors.New("no raft leader")
	ExtentNotFoundError       = errors.New("extent does not exist")
	ExtentExistsError         = errors.New("extent already exists")
	ExtentIsFullError         = errors.New("extent is full")
	BrokenExtentError         = errors.New("extent has been broken")
	BrokenDiskError           = errors.New("disk has broken")
)

func NewParameterMismatchErr(msg string) (err error) {
	err = fmt.Errorf("parameter mismatch error: %s", msg)
	return
}
