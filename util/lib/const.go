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

package lib

// header key
const (
	ReqIdKey  = "X-ReqId"
	AppIdKey  = "X-AppId"
	UserIdKey = "X-UserId"
	TokenKey  = "X-Token"
)

// client tag val
const (
	PantanalTag = "00"
	CubeKitTag  = "01"
	CubeWebTag  = "02"
	CubeInner   = "03"
)

const (
	RouteLockStatus   = 1
	RouteNormalStatus = 2
)
