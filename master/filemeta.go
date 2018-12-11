// Copyright 2018 The Containerfs Authors.
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
	"github.com/tiglabs/containerfs/proto"
)

/*this struct define chunk file metadata on  dataNode */
type FileMetaOnNode struct {
	Crc      uint32
	LocAddr  string
	locIndex uint8
	Size     uint32
}

type FileInCore struct {
	Name       string
	LastModify int64
	Metas      []*FileMetaOnNode
}

func NewFileMetaOnNode(volCrc uint32, volLoc string, volLocIndex int, size uint32) (fm *FileMetaOnNode) {
	fm = new(FileMetaOnNode)
	fm.Crc = volCrc
	fm.LocAddr = volLoc
	fm.locIndex = uint8(volLocIndex)
	fm.Size = size
	return
}

func (fm *FileMetaOnNode) ToString() (msg string) {
	msg = fmt.Sprintf("Crc[%v] LocAddr[%d] locIndex[%v]  Size[%v]",
		fm.Crc, fm.LocAddr, fm.locIndex, fm.Size)
	return
}

func (fm *FileMetaOnNode) getLocationAddr() (loc string) {
	return fm.LocAddr
}

func (fm *FileMetaOnNode) getFileCrc() (crc uint32) {
	return fm.Crc
}

func NewFileInCore(name string) (fc *FileInCore) {
	fc = new(FileInCore)
	fc.Name = name
	fc.Metas = make([]*FileMetaOnNode, 0)

	return
}

/*use a File and volLocation update FileInCore,
range all FileInCore.NodeInfos,update crc and reportTime*/
func (fc *FileInCore) updateFileInCore(volID uint64, vf *proto.File, volLoc *DataReplica, volLocIndex int) {
	if vf.Modified > fc.LastModify {
		fc.LastModify = vf.Modified
	}

	isFind := false
	for i := 0; i < len(fc.Metas); i++ {
		if fc.Metas[i].getLocationAddr() == volLoc.Addr {
			fc.Metas[i].Crc = vf.Crc
			fc.Metas[i].Size = vf.Size
			isFind = true
			break
		}
	}

	if isFind == false {
		fm := NewFileMetaOnNode(vf.Crc, volLoc.Addr, volLocIndex, vf.Size)
		fc.Metas = append(fc.Metas, fm)
	}

}

func (fc *FileInCore) getFileMetaByAddr(replica *DataReplica) (fm *FileMetaOnNode, ok bool) {
	for i := 0; i < len(fc.Metas); i++ {
		fm = fc.Metas[i]
		if fm.LocAddr == replica.Addr {
			ok = true
			return
		}
	}

	return
}

func (fc *FileInCore) getFileMetaAddrs() (addrs []string) {
	addrs = make([]string, 0)
	if len(fc.Metas) == 0 {
		return
	}
	for _, fm := range fc.Metas {
		addrs = append(addrs, fm.LocAddr)
	}
	return
}
