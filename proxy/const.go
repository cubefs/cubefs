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

package proxy

// config key
const (
	cfgMasterAddr = "master_addr"
	cfgListenPort = "port"

	cfgRouteAddr = "routeAddr"
	cfgVolName   = "volName"

	cfgAuthAddr      = "authAddr"
	cfgAuthAccessKey = "authAccessKey"
)

// header key
const (
	deviceId = "X-DevId"

	clientTag = "X-ClientLabel"
	subUri    = "X-Uri"

	thirdAuthAccessKey = "X-AccessKey"
)

// client tag val
const (
	CubeKitTag = "01"
	CubeWebTag = "02"
	CubeInner  = "03"
	MockTag    = "04"
)
