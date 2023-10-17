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

package blobnode

import (
	"context"
)

// key is unexported and used for context.Context
type key int

const (
	_ioFlowStatKey key = 0
)

type IOType uint64

const (
	NormalIO     IOType = iota // From: external: user io: read/write
	BackgroundIO               // From: external: background io: shard repair;disk repair, delete, compact;balance, drop, manual migrate; internal, inspect
	IOTypeMax                  // 2
	IOTypeOldMax = 8           // For compatibility with previous versions
)

var IOtypemap = [...]string{
	"normal",
	"background",
}

var _ = IOtypemap[IOTypeMax-1]

func (it IOType) IsValid() bool {
	return it >= NormalIO && it < IOTypeOldMax
}

func (it IOType) String() string {
	return IOtypemap[it]
}

func (it IOType) IsHighLevel() bool {
	return it == NormalIO
}

func GetIoType(ctx context.Context) IOType {
	v := ctx.Value(_ioFlowStatKey)
	if v == nil {
		return NormalIO
	}
	return v.(IOType)
}

func SetIoType(ctx context.Context, iot IOType) context.Context {
	return context.WithValue(ctx, _ioFlowStatKey, iot)
}
