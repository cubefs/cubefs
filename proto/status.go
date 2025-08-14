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

package proto

// The following defines the status of a disk or a partition.
const (
	Recovering  = 0
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)

var DiskStatusMap = map[int]string{
	Recovering:  "Recovering",
	ReadOnly:    "ReadOnly",
	ReadWrite:   "ReadWrite",
	Unavailable: "Unavailable",
}

// volume status
const (
	VolStatusNormal     uint8 = 0
	VolStatusMarkDelete uint8 = 1
)

// dp replica readOnly reason
const (
	ReasonNone          uint32 = 0
	DpOverCapacity      uint32 = 1 << 0
	DpExtentLimit       uint32 = 1 << 1
	DpBaseFileException uint32 = 1 << 2
	DiskReadOnly        uint32 = 1 << 3
	DpReplicaMissing    uint32 = 1 << 4
	DataNodeRdOnly      uint32 = 1 << 5
	PartitionRdOnly     uint32 = 1 << 6
)

var DpReasonMessages = map[uint32]string{
	DpOverCapacity:      "used space exceeds partition size",
	DpExtentLimit:       "extent count reached maximum limit",
	DpBaseFileException: "base file ID exception",
	DiskReadOnly:        "disk is read-only",
	DpReplicaMissing:    "replica missing",
	DataNodeRdOnly:      "dataNode is read-only",
	PartitionRdOnly:     "partition is read-only",
}

// mp readOnly reason
const (
	MpCursorOutOfRange uint32 = 1 << 0
	MetaMemUseLimit    uint32 = 1 << 1
	MetaNodeReadOnly   uint32 = 1 << 2
)

var MpReasonMessages = map[uint32]string{
	MpCursorOutOfRange: "mp cursor out of Range",
	MetaMemUseLimit:    "meta mem use reached maximum limit",
	MetaNodeReadOnly:   "MetaNode is read-only",
}
