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

package metanode

import (
	"bytes"
	"context"
	"encoding/binary"

)

type MulItems struct {
	InodeBatches         InodeBatch
	DentryBatches        DentryBatch
	ExtendBatches        ExtendBatch
	MultipartBatches     MultipartBatch
	DeletedInodeBatches  DeletedINodeBatch
	DeletedDentryBatches DeletedDentryBatch
	DelExtents           DelExtentBatch
	RequestBatches       RequestInfoBatch
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
	// deleted inode:len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.DeletedInodeBatches))); err != nil {
		return
	}
	for _, delInode := range i.DeletedInodeBatches {
		bs, err = delInode.Marshal()
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
	//deleted dentry:len + data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.DeletedDentryBatches))); err != nil {
		return
	}
	for _, delDentry := range i.DeletedDentryBatches {
		bs, err = delDentry.Marshal()
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

	// del extents: len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.DelExtents))); err != nil {
		return
	}
	for _, ekInfo := range i.DelExtents {
		bs = ekInfo.Marshal()
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return
		}
		if _, err = buff.Write(bs); err != nil {
			return
		}
	}

	//request:len+data
	if err = binary.Write(buff, binary.BigEndian, uint32(len(i.RequestBatches))); err != nil {
		return
	}
	for _, reqInfo := range i.RequestBatches {
		bs = reqInfo.MarshalBinary()
		if err = binary.Write(buff,binary.BigEndian, uint32(len(bs))); err != nil {
			return
		}
		if _, err = buff.Write(bs); err != nil {
			return
		}
	}
	return buff.Bytes(), nil
}

// Unmarshal unmarshal the MulItems.
func MulItemsUnmarshal(ctx context.Context, raw []byte, snapV SnapshotVersion) (result *MulItems, err error) {
	buff := bytes.NewBuffer(raw)
	var (
		inodeBatchesLen       uint32
		dentryBatchLen        uint32
		extendBatchLen        uint32
		multipartBatchesLen   uint32
		deletedInodeBatchLen  uint32
		deletedDentryBatchLen uint32
		delExtentLen          uint32
		requestInfoLen        uint32
		dataLen               uint32
		extend                *Extend
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

	//unmarshal the deletedInode.Batches
	if err = binary.Read(buff, binary.BigEndian, &deletedInodeBatchLen); err != nil {
		return
	}
	deletedInodeBatchesResult := make(DeletedINodeBatch, 0, int(deletedInodeBatchLen))
	for j := 0; j < int(deletedInodeBatchLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		delInode := new(DeletedINode)
		if err = delInode.Unmarshal(context.Background(), data); err != nil {
			return
		}
		deletedInodeBatchesResult = append(deletedInodeBatchesResult, delInode)
	}

	//unmarshal the deletedDentry.Batches
	if err = binary.Read(buff, binary.BigEndian, &deletedDentryBatchLen); err != nil {
		return
	}
	deletedDentryBatchesResult := make(DeletedDentryBatch, 0, int(deletedDentryBatchLen))
	for j := 0; j < int(deletedDentryBatchLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		delDentry := new(DeletedDentry)
		if err = delDentry.Unmarshal(data); err != nil {
			return
		}
		deletedDentryBatchesResult = append(deletedDentryBatchesResult, delDentry)
	}

	// unmarshal the Del extents.Batches
	if err = binary.Read(buff, binary.BigEndian, &delExtentLen); err != nil {
		return
	}
	delExtentBatch := make(DelExtentBatch, 0, int(delExtentLen))
	for j := 0; j < int(delExtentLen); j++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		ekInfo := &EkData{}
		if err = ekInfo.UnMarshal(data); err != nil {
			return
		}
		delExtentBatch = append(delExtentBatch, ekInfo)
	}

	var requestInfoBatch RequestInfoBatch
	if snapV >= BatchSnapshotV3 {
		// unmarshal request info
		if err = binary.Read(buff, binary.BigEndian, &requestInfoLen); err != nil {
			return
		}
		requestInfoBatch = make(RequestInfoBatch, 0, int(requestInfoLen))
		for j := 0; j < int(requestInfoLen); j++ {
			if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
				return
			}
			data := make([]byte, int(dataLen))
			if _, err = buff.Read(data); err != nil {
				return
			}
			requestInfo := &RequestInfo{}
			if err = requestInfo.Unmarshal(data); err != nil {
				return
			}
			requestInfoBatch = append(requestInfoBatch, requestInfo)
		}
	}

	result = &MulItems{
		InodeBatches:         inodeBatchesLenResult,
		DentryBatches:        dentryBatchResult,
		ExtendBatches:        extendBatchResult,
		MultipartBatches:     multipartBatchesResult,
		DeletedInodeBatches:  deletedInodeBatchesResult,
		DeletedDentryBatches: deletedDentryBatchesResult,
		DelExtents:           delExtentBatch,
		RequestBatches:       requestInfoBatch,
	}
	return
}