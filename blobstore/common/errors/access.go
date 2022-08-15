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

package errors

// code for access
const (
	CodeAccessReadRequestBody  = 466 // read request body error
	CodeAccessUnexpect         = 550 // unexpect
	CodeAccessServiceDiscovery = 551 // service discovery for access api client
	CodeAccessLimited          = 552 // read write limited for access api client
	CodeAccessExceedSize       = 553 // exceed max size
)

// errro of access
var (
	ErrAccessReadRequestBody  = Error(CodeAccessReadRequestBody)
	ErrAccessUnexpect         = Error(CodeAccessUnexpect)
	ErrAccessServiceDiscovery = Error(CodeAccessServiceDiscovery)
	ErrAccessLimited          = Error(CodeAccessLimited)
	ErrAccessExceedSize       = Error(CodeAccessExceedSize)
)
