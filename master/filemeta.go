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

package master

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
)

// FileMetadata defines the file metadata on a dataNode
type FileMetadata struct {
	proto.FileMetadata
	locIndex uint8
	ApplyID  uint64
}

func (fm *FileMetadata) String() (msg string) {
	msg = fmt.Sprintf("Crc[%v] LocAddr[%v] locIndex[%v]  Size[%v]",
		fm.Crc, fm.LocAddr, fm.locIndex, fm.Size)
	return
}

func (fm *FileMetadata) getLocationAddr() (loc string) {
	return fm.LocAddr
}

func (fm *FileMetadata) getFileCrc() (crc uint32) {
	return fm.Crc
}

//FileInCore define file in data partition
type FileInCore struct {
	proto.FileInCore
	MetadataArray []*FileMetadata
}

func newFileMetadata(volCrc uint32, volLoc string, volLocIndex int, size uint32, applyId uint64) (fm *FileMetadata) {
	fm = new(FileMetadata)
	fm.Crc = volCrc
	fm.LocAddr = volLoc
	fm.locIndex = uint8(volLocIndex)
	fm.Size = size
	fm.ApplyID = applyId
	return
}

func newFileInCore(name string) (fc *FileInCore) {
	fc = new(FileInCore)
	fc.Name = name
	fc.MetadataArray = make([]*FileMetadata, 0)

	return
}

func (fc FileInCore) clone() *proto.FileInCore {
	var metadataArray = make([]*proto.FileMetadata, len(fc.MetadataArray))
	for i, metadata := range fc.MetadataArray {
		metadataArray[i] = &proto.FileMetadata{
			Crc:     metadata.Crc,
			LocAddr: metadata.LocAddr,
			Size:    metadata.Size,
		}
	}

	return &proto.FileInCore{
		Name:          fc.Name,
		LastModify:    fc.LastModify,
		MetadataArray: metadataArray,
	}
}

// Use the File and the volume Location for update.
func (fc *FileInCore) updateFileInCore(volID uint64, vf *proto.File, volLoc *DataReplica, volLocIndex int) {
	if vf.Modified > fc.LastModify {
		fc.LastModify = vf.Modified
	}

	isFind := false
	for i := 0; i < len(fc.MetadataArray); i++ {
		if fc.MetadataArray[i].getLocationAddr() == volLoc.Addr {
			fc.MetadataArray[i].Crc = vf.Crc
			fc.MetadataArray[i].Size = vf.Size
			fc.MetadataArray[i].ApplyID = vf.ApplyID
			isFind = true
			break
		}
	}

	if isFind == false {
		fm := newFileMetadata(vf.Crc, volLoc.Addr, volLocIndex, vf.Size, vf.ApplyID)
		fc.MetadataArray = append(fc.MetadataArray, fm)
	}

}

func (fc *FileInCore) getFileMetaByAddr(replica *DataReplica) (fm *FileMetadata, ok bool) {
	for i := 0; i < len(fc.MetadataArray); i++ {
		fm = fc.MetadataArray[i]
		if fm.LocAddr == replica.Addr {
			ok = true
			return
		}
	}

	return
}

func (fc *FileInCore) getFileMetaAddrs() (addrs []string) {
	addrs = make([]string, 0)
	if len(fc.MetadataArray) == 0 {
		return
	}
	for _, fm := range fc.MetadataArray {
		addrs = append(addrs, fm.LocAddr)
	}
	return
}
