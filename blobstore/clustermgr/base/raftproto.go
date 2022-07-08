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

package base

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
)

const (
	defaultFlushNumInterval    = uint64(10000)
	defaultFlushTimeIntervalS  = 300
	defaultFlushCheckIntervalS = 2
	defaultTruncateNumInterval = uint64(50000)
)

var (
	ApplyIndexKey = []byte("raft_apply_index")
	RaftMemberKey = []byte("#raft_members")
)

type RaftApplier interface {
	GetModuleName() string
	SetModuleName(module string)
	// Apply apply specified data into module manager
	Apply(ctx context.Context, operTypes []int32, data [][]byte, contexts []ProposeContext) error
	// Flush should flush all memory data into persistent storage ,like rocksdb etc
	Flush(ctx context.Context) error
	// NotifyLeaderChange notify leader host change
	NotifyLeaderChange(ctx context.Context, leader uint64, host string)
	// LoadData load data from applier's db
	LoadData(ctx context.Context) error
}

// ProposeContext hold propose context info during the request life cycle
type ProposeContext struct {
	ReqID string
}

func (p ProposeContext) Marshal() (ret []byte, err error) {
	w := bytes.NewBuffer(nil)
	reqIDSize := int32(len(p.ReqID))
	if err = binary.Write(w, binary.BigEndian, &reqIDSize); err != nil {
		return
	}
	if _, err = w.Write([]byte(p.ReqID)); err != nil {
		return
	}
	ret = w.Bytes()
	return
}

func (p *ProposeContext) Unmarshal(r io.Reader) (err error) {
	reqIDSize := int32(0)
	if err = binary.Read(r, binary.BigEndian, &reqIDSize); err != nil {
		return
	}
	rawReqID := make([]byte, reqIDSize)
	if _, err = r.Read(rawReqID); err != nil {
		return
	}
	p.ReqID = string(rawReqID)
	return
}

// raft propose info
type ProposeInfo struct {
	Module   string
	OperType int32
	Data     []byte
	Context  ProposeContext
}

// DecodeProposeInfo decode propose info from []byte
func DecodeProposeInfo(src []byte) *ProposeInfo {
	ret := new(ProposeInfo)
	moduleSize := int32(0)
	dataSize := int32(0)

	r := bytes.NewReader(src)
	binary.Read(r, binary.BigEndian, &moduleSize)
	module := make([]byte, moduleSize)
	r.Read(module)

	binary.Read(r, binary.BigEndian, &ret.OperType)

	binary.Read(r, binary.BigEndian, &dataSize)
	data := make([]byte, dataSize)
	r.Read(data)

	ctx := &ProposeContext{}
	if err := ctx.Unmarshal(r); err != nil {
		return nil
	}

	ret.Module = string(module)
	ret.Data = data
	ret.Context = *ctx

	return ret
}

// EncodeProposeInfo encode propose info into []byte
func EncodeProposeInfo(module string, operType int32, data []byte, ctx ProposeContext) []byte {
	w := bytes.NewBuffer(nil)
	moduleSize := int32(len(module))
	dataSize := int32(len(data))

	if err := binary.Write(w, binary.BigEndian, &moduleSize); err != nil {
		return nil
	}
	if _, err := w.Write([]byte(module)); err != nil {
		return nil
	}
	if err := binary.Write(w, binary.BigEndian, &operType); err != nil {
		return nil
	}
	if err := binary.Write(w, binary.BigEndian, &dataSize); err != nil {
		return nil
	}
	if _, err := w.Write(data); err != nil {
		return nil
	}

	b, err := ctx.Marshal()
	if err != nil {
		return nil
	}
	if _, err := w.Write(b); err != nil {
		return nil
	}

	return w.Bytes()
}
