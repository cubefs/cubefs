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

package raft

const (
	ErrCodeReplicaTooOld = 601 + iota
	ErrCodeRaftGroupDeleted
	ErrCodeGroupHandleRaftMessage
	ErrCodeGroupNotFound
)

var (
	ErrReplicaTooOld          = newError(ErrCodeReplicaTooOld, "replica too old")
	ErrRaftGroupDeleted       = newError(ErrCodeRaftGroupDeleted, "raft group has been deleted")
	ErrGroupHandleRaftMessage = newError(ErrCodeGroupHandleRaftMessage, "group handle raft message failed")
	ErrGroupNotFound          = newError(ErrCodeGroupNotFound, "group not found")
)

func newError(code uint32, err string) *Error {
	return &Error{
		ErrorCode: code,
		ErrorMsg:  err,
	}
}

func (m *Error) Error() string {
	return m.ErrorMsg
}
