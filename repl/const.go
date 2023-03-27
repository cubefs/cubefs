// Copyright 2018 The CubeFS Authors.
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

package repl

const (
	RequestChanSize = 1024
)

const (
	ReplProtocolError = 1
)

const (
	ActionSendToFollowers     = "ActionSendToFollowers"
	ActionReceiveFromFollower = "ActionReceiveFromFollower"
	ActionreadPkgAndPrepare   = "ActionreadPkgAndPrepare"

	ActionWriteToClient = "ActionWriteToClient"
	ActionCheckReply    = "ActionCheckReply"

	ActionPreparePkt = "ActionPreparePkt"
)

const (
	ConnIsNullErr = "ConnIsNullErr"
)

const (
	ReplRuning    = 2
	ReplExiting   = 1
	ReplHasExited = -2

	FollowerTransportRuning  = 2
	FollowerTransportExiting = 1
	FollowerTransportExited  = -1

	FollowerTransportIdleTime = 120
)

const (
	ReplProtocalServerTimeOut = 60 * 5
)
