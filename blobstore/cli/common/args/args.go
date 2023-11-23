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
