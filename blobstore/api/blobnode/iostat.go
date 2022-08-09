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

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// key is unexported and used for context.Context
type key int

const (
	_ioFlowStatKey key = 0
)

type IOType uint64

const (
	NormalIO   IOType = iota // From: external: user io: read/write
	RepairIO                 // From: external: repair
	MigrateIO                // From: external: chunk transfer, drop, manualMigrate
	CompactIO                // From: internal: chunk compact
	DeleteIO                 // From: external: delete io
	InternalIO               // From: internal: io, such rubbish clean, batch delete
	InspectIO                // From: internal: inspect io
	IOTypeMax
)

var IOtypemap = [...]string{
	"normal",
	"repair",
	"migrate",
	"compact",
	"delete",
	"internal",
	"inspect",
}

var _ = IOtypemap[IOTypeMax-1]

func (it IOType) IsValid() bool {
	return it >= NormalIO && it < IOTypeMax
}

func (it IOType) String() string {
	return IOtypemap[uint64(it)]
}

func Getiotype(ctx context.Context) IOType {
	v := ctx.Value(_ioFlowStatKey)
	if v == nil {
		return NormalIO
	}
	return v.(IOType)
}

func Setiotype(ctx context.Context, iot IOType) context.Context {
	return context.WithValue(ctx, _ioFlowStatKey, iot)
}

func Task2IOType(t proto.TaskType) IOType {
	switch t {
	case proto.TaskTypeDiskRepair, proto.TaskTypeShardRepair:
		return RepairIO
	default:
		return MigrateIO
	}
}
