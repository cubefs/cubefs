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

package args

import (
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// Line lines
func Line(a *grumble.Args) {
	a.Int("line", "show lines at the tail", grumble.Default(40))
}

// NodeHostRegister node host
func NodeHostRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.String("host", "node host", opts...)
}

// NodeHost returns node host
func NodeHost(a grumble.ArgMap) string {
	return a.String("host")
}

// VidRegister vid
func VidRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint64("vid", "volume id", opts...)
}

// Vid returns vid
func Vid(a grumble.ArgMap) proto.Vid {
	return proto.Vid(a.Uint64("vid"))
}

// ShardIDRegister ShardID
func ShardIDRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint64("shardID", "shard id", opts...)
}

// ShardID returns ShardID
func ShardID(a grumble.ArgMap) proto.ShardID {
	return proto.ShardID(a.Uint64("shardID"))
}

// SpaceIDRegister SpaceID
func SpaceIDRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint64("spaceID", "space id", opts...)
}

// SpaceID returns SpaceID
func SpaceID(a grumble.ArgMap) proto.SpaceID {
	return proto.SpaceID(a.Uint64("spaceID"))
}

// SpaceNameRegister Space name
func SpaceNameRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.String("spaceName", "space name", opts...)
}

// SpaceName returns SpaceName
func SpaceName(a grumble.ArgMap) string {
	return a.String("spaceName")
}

// DiskIDRegister disk id
func DiskIDRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint64("diskID", "disk id", opts...)
}

// DiskID returns disk id
func DiskID(a grumble.ArgMap) proto.DiskID {
	return proto.DiskID(a.Uint64("diskID"))
}

// VuidRegister vuid
func VuidRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint64("vuid", "vuid", opts...)
}

// Vuid returns vuid
func Vuid(a grumble.ArgMap) proto.Vuid {
	return proto.Vuid(a.Uint64("vuid"))
}

// SuidRegister suid
func SuidRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint64("suid", "suid", opts...)
}

// Suid returns suid
func Suid(a grumble.ArgMap) proto.Suid {
	return proto.Suid(a.Uint64("suid"))
}

// CodeModeRegister codeMode
func CodeModeRegister(a *grumble.Args, opts ...grumble.ArgOption) {
	a.Uint("codeMode", "codeMode", opts...)
}

// CodeMode return codeMode
func CodeMode(a grumble.ArgMap) codemode.CodeMode {
	return codemode.CodeMode(a.Uint("codeMode"))
}
