// Copyright 2018 The Cubefs Authors.
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

package authnode

// Keys in the request
const (
	AuthService = "AuthService"
)

const (
	opSyncAddKey     uint32 = 0x01
	opSyncDeleteKey  uint32 = 0x02
	opSyncGetKey     uint32 = 0x03
	opSyncAddCaps    uint32 = 0x04
	opSyncDeleteCaps uint32 = 0x05
	opSyncGetCaps    uint32 = 0x06
)

const (
	keySeparator = "#"
	idSeparator  = "$" // To seperate ID of server that submits raft changes
	keyAcronym   = "key"
	ksPrefix     = keySeparator + keyAcronym + keySeparator

	akAcronym = "ak"
	akPrefix  = keySeparator + akAcronym + keySeparator
)
