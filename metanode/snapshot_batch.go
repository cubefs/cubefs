// Copyright 2018 The Chubao Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"
	"context"
	"github.com/chubaofs/chubaofs/util/tracing"
)

type MulItems struct {
	InodeBatches InodeBatch
	DentryBatches DentryBatch
	ExtendBatches ExtendBatch
	MultipartBatches MultipartBatch
}

// MarshalKey marshals the exporterKey to bytes.
func (i *MulItems) Marshal() (k []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	var bs []byte
	// inode: len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.InodeBatches))); err != nil {
		return
	}
	for _, inode := range i.InodeBatches {
		bs, err = inode.MarshalV2()
		if err != nil {
			return
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return
		}
		if _, err = buff.Write(bs); err != nil {
			return
		}
	}
	// dentry: len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.DentryBatches))); err != nil {
		return
	}
	for _, dentry := range i.DentryBatches {
		bs, err = dentry.MarshalV2()
		if err != nil {
			return
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return
		}
		if _, err = buff.Write(bs); err != nil {
			return
		}
	}
	// extend: len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.ExtendBatches))); err != nil {
		return
	}
	for _, extend := range i.ExtendBatches {
		bs, err = extend.Bytes()
		if err != nil {
			return
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return
		}
		if _, err = buff.Write(bs); err != nil {
			return
		}
	}
	// multipart: len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.MultipartBatches))); err != nil {
		return
	}
	for _, multipart := range i.MultipartBatches {
		bs, err = multipart.Bytes()
		if err != nil {
			return
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return
		}
		if _, err = buff.Write(bs); err != nil {
			return
		}
	}
	return buff.Bytes(), nil
}

// Unmarshal unmarshal the MulItems.
func MulItemsUnmarshal(ctx context.Context, raw []byte) (result *MulItems, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MulItemsUnmarshal")
	defer tracer.Finish()
	ctx = tracer.Context()
	buff := bytes.NewBuffer(raw)
	var (
		inodeBatchesLen     uint32
		dentryBatchLen      uint32
		extendBatchLen      uint32
		multipartBatchesLen uint32
		dataLen             uint32
		extend              *Extend
	)
	//  unmarshal the MulItems.InodeBatches
	if err = binary.Read(buff, binary.BigEndian, &inodeBatchesLen); err != nil {
		return
	}
	inodeBatchesLenResult := make(InodeBatch, 0, int(inodeBatchesLen))
	for j := 0; j < int(inodeBatchesLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		ino := NewInode(0, 0)
		if err = ino.UnmarshalV2(ctx, data); err != nil {
			return
		}
		inodeBatchesLenResult = append(inodeBatchesLenResult, ino)
	}
	//  unmarshal the MulItems.DentryBatches
	if err = binary.Read(buff, binary.BigEndian, &dentryBatchLen); err != nil {
		return
	}
	dentryBatchResult := make(DentryBatch, 0, int(dentryBatchLen))
	for j := 0; j < int(dentryBatchLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		den := &Dentry{}
		if err = den.UnmarshalV2(data); err != nil {
			return
		}
		dentryBatchResult = append(dentryBatchResult, den)
	}
	// unmarshal the MulItems.ExtendBatches
	if err = binary.Read(buff, binary.BigEndian, &extendBatchLen); err != nil {
		return
	}
	extendBatchResult := make(ExtendBatch, 0, int(extendBatchLen))
	for j := 0; j < int(extendBatchLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}

		if extend, err = NewExtendFromBytes(data); err != nil {
			return
		}
		extendBatchResult = append(extendBatchResult, extend)
	}
	// unmarshal the MulItems.MultipartBatches
	if err = binary.Read(buff, binary.BigEndian, &multipartBatchesLen); err != nil {
		return
	}
	multipartBatchesResult := make(MultipartBatch, 0, int(multipartBatchesLen))
	for j := 0; j < int(multipartBatchesLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		multipart := MultipartFromBytes(data)
		multipartBatchesResult = append(multipartBatchesResult, multipart)
	}

	result = &MulItems{
		InodeBatches:     inodeBatchesLenResult,
		DentryBatches:    dentryBatchResult,
		ExtendBatches:    extendBatchResult,
		MultipartBatches: multipartBatchesResult,
	}
	return
}