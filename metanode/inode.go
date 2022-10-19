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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"math"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	DeleteMarkFlag = 1 << 0
	InodeDelTop    = 1 << 1
)

var (
	// InodeV1Flag uint64 = 0x01
	V2EnableColdInodeFlag uint64 = 0x02
	V3EnableSnapInodeFlag uint64 = 0x04
)

// Inode wraps necessary properties of `Inode` information in the file system.
// Marshal exporterKey:
//  +-------+-------+
//  | item  | Inode |
//  +-------+-------+
//  | bytes |   8   |
//  +-------+-------+
// Marshal value:
//  +-------+------+------+-----+----+----+----+--------+------------------+
//  | item  | Type | Size | Gen | CT | AT | MT | ExtLen | MarshaledExtents |
//  +-------+------+------+-----+----+----+----+--------+------------------+
//  | bytes |  4   |  8   |  8  | 8  | 8  | 8  |   4    |      ExtLen      |
//  +-------+------+------+-----+----+----+----+--------+------------------+
// Marshal entity:
//  +-------+-----------+--------------+-----------+--------------+
//  | item  | KeyLength | MarshaledKey | ValLength | MarshaledVal |
//  +-------+-----------+--------------+-----------+--------------+
//  | bytes |     4     |   KeyLength  |     4     |   ValLength  |
//  +-------+-----------+--------------+-----------+--------------+
type Inode struct {
	sync.RWMutex
	Inode      uint64 // Inode ID
	Type       uint32
	Uid        uint32
	Gid        uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	LinkTarget []byte // SymLink target name
	NLink      uint32 // NodeLink counts
	Flag       int32
	Reserved   uint64 // reserved space
	//Extents    *ExtentsTree
	Extents    *SortedExtents
	ObjExtents *SortedObjExtents

	// for snapshot
	verSeq        uint64 // latest version be create or modified
	multiVersions InodeBatch
	ekRefMap      *sync.Map
}

func (i *Inode) isEmptyVerList() bool {
	return len(i.multiVersions) == 0
}
func (i *Inode) isTailIndexInList(id int) bool {
	return id == len(i.multiVersions)-1
}

func (i *Inode) getTailVerInList() (verSeq uint64, found bool) {
	mLen := len(i.multiVersions)
	if mLen > 0 {
		return i.multiVersions[mLen-1].verSeq, true
	}
	return 0, false
}

type SplitExtentInfo struct {
	PartitionId uint64
	ExtentId    uint64
	refCnt      uint64
}

// freelist clean inode get all exist extents info, deal special case for split key
func (inode *Inode) GetAllExtsOfflineInode(mpID uint64) (extInfo map[uint64][]*proto.ExtentKey) {

	log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] inode.Extents %v, ino verlist %v",
		mpID, inode.Inode, inode.Extents, inode.multiVersions)
	extInfo = make(map[uint64][]*proto.ExtentKey)

	if len(inode.multiVersions) > 0 {
		log.LogWarnf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] verlist len %v should not drop",
			mpID, inode.Inode, len(inode.multiVersions))
	}

	for i := 0; i < len(inode.multiVersions)+1; i++ {
		dIno := inode
		if i > 0 {
			dIno = inode.multiVersions[i-1]
		}
		log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] dIno %v", mpID, inode.Inode, dIno)
		dIno.Extents.Range(func(ek proto.ExtentKey) bool {
			ext := &ek
			if ext.IsSplit {
				var (
					dOK  bool
					last bool
				)
				if dOK, last = dIno.DecSplitEk(ext); !dOK {
					return false
				}
				if !last {
					log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] ek %v be removed", mpID, inode.Inode, ext)
					return true
				}

				log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] ek %v be removed", mpID, inode.Inode, ext)
				ext.IsSplit = false
				ext.ExtentOffset = 0
				ext.Size = 0
			}
			extInfo[ext.PartitionId] = append(extInfo[ext.PartitionId], ext)
			log.LogWritef("GetAllExtsOfflineInode. mp(%v) ino(%v) deleteExtent(%v)", mpID, inode.Inode, ext.String())
			return true
		})
	}
	return
}

type InodeBatch []*Inode

type TxInode struct {
	Inode  *Inode
	TxInfo *proto.TransactionInfo
}

func NewTxInode(mpAddr string, ino uint64, t uint32, mpID uint64, txInfo *proto.TransactionInfo) *TxInode {
	ti := &TxInode{
		Inode:  NewInode(ino, t),
		TxInfo: txInfo,
	}

	if ti.TxInfo != nil {
		txInodeInfo := proto.NewTxInodeInfo(mpAddr, ino, mpID)
		ti.TxInfo.TxInodeInfos[txInodeInfo.GetKey()] = txInodeInfo
	}

	return ti
}

func (ti *TxInode) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))

	bs, err := ti.Inode.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}

	bs, err = ti.TxInfo.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}
	result = buff.Bytes()
	return
}

func (ti *TxInode) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)

	var dataLen uint32
	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data := make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}
	ino := NewInode(0, 0)
	if err = ino.Unmarshal(data); err != nil {
		return
	}
	ti.Inode = ino

	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data = make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}
	txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
	if err = txInfo.Unmarshal(data); err != nil {
		return
	}
	ti.TxInfo = txInfo
	return
}

func (i *InodeBatch) Clone() InodeBatch {
	var rB []*Inode
	for _, inode := range []*Inode(*i) {
		rB = append(rB, inode.Copy().(*Inode))
	}
	return rB
}

func (ino *Inode) getAllInodesInfo() (rsp []proto.InodeInfo) {
	ino.RLock()
	defer ino.RUnlock()

	for _, info := range ino.multiVersions {
		rspInodeInfo := &proto.InodeInfo{}
		replyInfoNoCheck(rspInodeInfo, info, nil)
		rsp = append(rsp, *rspInodeInfo)
	}
	return
}

func (ino *Inode) getAllLayerEks() (rsp []proto.LayerInfo) {
	ino.RLock()
	defer ino.RUnlock()
	rspInodeInfo := &proto.InodeInfo{}
	replyInfoNoCheck(rspInodeInfo, ino, nil)

	layerInfo := proto.LayerInfo{
		LayerIdx: 0,
		Info:     rspInodeInfo,
		Eks:      ino.Extents.eks,
	}
	rsp = append(rsp, layerInfo)
	for idx, info := range ino.multiVersions {
		rspInodeInfo := &proto.InodeInfo{}
		replyInfo(rspInodeInfo, info, nil)
		layerInfo := proto.LayerInfo{
			LayerIdx: uint32(idx + 1),
			Info:     rspInodeInfo,
			Eks:      info.Extents.eks,
		}
		rsp = append(rsp, layerInfo)
	}
	return
}

// String returns the string format of the inode.
func (i *Inode) String() string {
	i.RLock()
	defer i.RUnlock()
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	buff.WriteString("Inode{")
	buff.WriteString(fmt.Sprintf("Inode[%d]", i.Inode))
	buff.WriteString(fmt.Sprintf("Type[%d]", i.Type))
	buff.WriteString(fmt.Sprintf("Uid[%d]", i.Uid))
	buff.WriteString(fmt.Sprintf("Gid[%d]", i.Gid))
	buff.WriteString(fmt.Sprintf("Size[%d]", i.Size))
	buff.WriteString(fmt.Sprintf("Gen[%d]", i.Generation))
	buff.WriteString(fmt.Sprintf("CT[%d]", i.CreateTime))
	buff.WriteString(fmt.Sprintf("AT[%d]", i.AccessTime))
	buff.WriteString(fmt.Sprintf("MT[%d]", i.ModifyTime))
	buff.WriteString(fmt.Sprintf("LinkT[%s]", i.LinkTarget))
	buff.WriteString(fmt.Sprintf("NLink[%d]", i.NLink))
	buff.WriteString(fmt.Sprintf("Flag[%d]", i.Flag))
	buff.WriteString(fmt.Sprintf("Reserved[%d]", i.Reserved))
	buff.WriteString(fmt.Sprintf("Extents[%s]", i.Extents))
	buff.WriteString(fmt.Sprintf("ObjExtents[%s]", i.ObjExtents))
	buff.WriteString(fmt.Sprintf("verSeq[%v]", i.verSeq))
	buff.WriteString(fmt.Sprintf("multiVersions.len[%v]", len(i.multiVersions)))
	buff.WriteString("}")
	return buff.String()
}

// NewInode returns a new Inode instance with specified Inode ID, name and type.
// The AccessTime and ModifyTime will be set to the current time.
func NewInode(ino uint64, t uint32) *Inode {
	ts := Now.GetCurrentTimeUnix()
	i := &Inode{
		Inode:      ino,
		Type:       t,
		Generation: 1,
		CreateTime: ts,
		AccessTime: ts,
		ModifyTime: ts,
		NLink:      1,
		Extents:    NewSortedExtents(),
		ObjExtents: NewSortedObjExtents(),
	}
	if proto.IsDir(t) {
		i.NLink = 2
	}
	return i
}

// Less tests whether the current Inode item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (i *Inode) Less(than BtreeItem) bool {
	ino, ok := than.(*Inode)
	return ok && i.Inode < ino.Inode
}

// Copy returns a copy of the inode.
func (i *Inode) Copy() BtreeItem {
	newIno := NewInode(i.Inode, i.Type)
	i.RLock()
	newIno.Uid = i.Uid
	newIno.Gid = i.Gid
	newIno.Size = i.Size
	newIno.Generation = i.Generation
	newIno.CreateTime = i.CreateTime
	newIno.ModifyTime = i.ModifyTime
	newIno.AccessTime = i.AccessTime
	if size := len(i.LinkTarget); size > 0 {
		newIno.LinkTarget = make([]byte, size)
		copy(newIno.LinkTarget, i.LinkTarget)
	}
	newIno.NLink = i.NLink
	newIno.Flag = i.Flag
	newIno.Reserved = i.Reserved
	newIno.Extents = i.Extents.Clone()
	newIno.ObjExtents = i.ObjExtents.Clone()
	newIno.verSeq = i.verSeq
	newIno.multiVersions = i.multiVersions.Clone()
	newIno.ekRefMap = i.ekRefMap

	i.RUnlock()
	return newIno
}

func (i *Inode) CopyDirectly() BtreeItem {
	newIno := NewInode(i.Inode, i.Type)

	newIno.Uid = i.Uid
	newIno.Gid = i.Gid
	newIno.Size = i.Size
	newIno.Generation = i.Generation
	newIno.CreateTime = i.CreateTime
	newIno.ModifyTime = i.ModifyTime
	newIno.AccessTime = i.AccessTime
	if size := len(i.LinkTarget); size > 0 {
		newIno.LinkTarget = make([]byte, size)
		copy(newIno.LinkTarget, i.LinkTarget)
	}
	newIno.NLink = i.NLink
	newIno.Flag = i.Flag
	newIno.Reserved = i.Reserved
	newIno.Extents = i.Extents.Clone()
	newIno.ObjExtents = i.ObjExtents.Clone()
	newIno.verSeq = i.verSeq
	newIno.multiVersions = i.multiVersions.Clone()

	return newIno
}

// MarshalToJSON is the wrapper of json.Marshal.
func (i *Inode) MarshalToJSON() ([]byte, error) {
	i.RLock()
	defer i.RUnlock()
	return json.Marshal(i)
}

// Marshal marshals the inode into a byte array.
func (i *Inode) Marshal() (result []byte, err error) {
	keyBytes := i.MarshalKey()
	valBytes := i.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {
		return
	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// Unmarshal unmarshals the inode.
func (i *Inode) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = i.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = i.UnmarshalValue(valBytes)
	return
}

// Marshal marshals the inodeBatch into a byte array.
func (i InodeBatch) Marshal() ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, uint32(len(i))); err != nil {
		return nil, err
	}
	for _, inode := range i {
		bs, err := inode.Marshal()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return nil, err
		}
		if _, err := buff.Write(bs); err != nil {
			return nil, err
		}
	}
	return buff.Bytes(), nil
}

// Unmarshal unmarshals the inodeBatch.
func InodeBatchUnmarshal(raw []byte) (InodeBatch, error) {
	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(InodeBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		ino := NewInode(0, 0)
		if err := ino.Unmarshal(data); err != nil {
			return nil, err
		}
		result = append(result, ino)
	}

	return result, nil
}

// MarshalKey marshals the exporterKey to bytes.
func (i *Inode) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, i.Inode)
	return
}

// UnmarshalKey unmarshals the exporterKey from bytes.
func (i *Inode) UnmarshalKey(k []byte) (err error) {
	i.Inode = binary.BigEndian.Uint64(k)
	return
}

// MarshalValue marshals the value to bytes.
func (i *Inode) MarshalInodeValue(buff *bytes.Buffer) {
	var err error
	if err = binary.Write(buff, binary.BigEndian, &i.Type); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Uid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Gid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		panic(err)
	}
	// write SymLink
	symSize := uint32(len(i.LinkTarget))
	if err = binary.Write(buff, binary.BigEndian, &symSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(i.LinkTarget); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &i.NLink); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Flag); err != nil {
		panic(err)
	}
	if i.ObjExtents != nil && len(i.ObjExtents.eks) > 0 {
		i.Reserved |= V2EnableColdInodeFlag
	}
	i.Reserved |= V3EnableSnapInodeFlag

	log.LogInfof("action[MarshalInodeValue] inode %v Reserved %v", i.Inode, i.Reserved)
	if err = binary.Write(buff, binary.BigEndian, &i.Reserved); err != nil {
		panic(err)
	}

	// marshal ExtentsKey
	extData, err := i.Extents.MarshalBinary(true)
	if err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(extData))); err != nil {
		panic(err)
	}
	if _, err = buff.Write(extData); err != nil {
		panic(err)
	}

	if i.Reserved&V2EnableColdInodeFlag > 0 {
		// marshal ObjExtentsKey
		objExtData, err := i.ObjExtents.MarshalBinary()
		if err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(objExtData))); err != nil {
			panic(err)
		}
		if _, err = buff.Write(objExtData); err != nil {
			panic(err)
		}
	}

	if err = binary.Write(buff, binary.BigEndian, i.verSeq); err != nil {
		panic(err)
	}

	return
}

// MarshalValue marshals the value to bytes.
func (i *Inode) MarshalValue() (val []byte) {
	var err error
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)
	i.RLock()

	log.LogInfof("action[MarshalValue] inode %v current verseq %v, hist len (%v)", i.Inode, i.verSeq, len(i.multiVersions))
	i.MarshalInodeValue(buff)
	if err = binary.Write(buff, binary.BigEndian, int32(len(i.multiVersions))); err != nil {
		panic(err)
	}

	for _, ino := range i.multiVersions {
		log.LogInfof("action[MarshalValue] inode %v current verseq %v", ino.Inode, ino.verSeq)
		ino.MarshalInodeValue(buff)
	}

	val = buff.Bytes()
	i.RUnlock()
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (i *Inode) UnmarshalInodeValue(buff *bytes.Buffer) (err error) {

	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Uid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Gid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Generation); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		return
	}
	// read symLink
	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		return
	}
	if symSize > 0 {
		i.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, i.LinkTarget); err != nil {
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &i.NLink); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Flag); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Reserved); err != nil {
		return
	}

	// unmarshal ExtentsKey
	if i.Extents == nil {
		i.Extents = NewSortedExtents()
	}
	if i.ObjExtents == nil {
		i.ObjExtents = NewSortedObjExtents()
	}
	log.LogInfof("action[UnmarshalInodeValue] inode %v Reserved %v", i.Inode, i.Reserved)

	v3 := i.Reserved&V3EnableSnapInodeFlag > 0
	v2 := i.Reserved&V2EnableColdInodeFlag > 0

	if v2 || v3 {
		extSize := uint32(0)
		if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
			return
		}
		if extSize > 0 {
			extBytes := make([]byte, extSize)
			if _, err = io.ReadFull(buff, extBytes); err != nil {
				return
			}
			if err, i.ekRefMap = i.Extents.UnmarshalBinary(extBytes, v3); err != nil {
				return
			}
		}
	} else {
		if err, _ = i.Extents.UnmarshalBinary(buff.Bytes(), false); err != nil {
			return
		}
		return
	}

	if v2 {
		// unmarshal ObjExtentsKey
		ObjExtSize := uint32(0)
		if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
			return
		}
		if ObjExtSize > 0 {
			objExtBytes := make([]byte, ObjExtSize)
			if _, err = io.ReadFull(buff, objExtBytes); err != nil {
				return
			}
			if err = i.ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
				return
			}
		}
	}

	if v3 {
		if err = binary.Read(buff, binary.BigEndian, &i.verSeq); err != nil {
			return
		}
		log.LogInfof("action[UnmarshalInodeValue] verseq %v", i.verSeq)
	}

	return
}

func (i *Inode) GetSpaceSize() (extSize uint64) {
	if i.IsTempFile() {
		return
	}
	extSize += i.Extents.LayerSize()
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (i *Inode) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	i.UnmarshalInodeValue(buff)
	if i.Reserved&V3EnableSnapInodeFlag > 0 {
		var verCnt int32
		if err = binary.Read(buff, binary.BigEndian, &verCnt); err != nil {
			log.LogInfof("action[UnmarshalValue] err get ver cnt inode %v new seq %v", i.Inode, i.verSeq)
			return
		}
		log.LogInfof("action[UnmarshalValue] inode %v new seq %v verCnt %v", i.Inode, i.verSeq, verCnt)
		for idx := int32(0); idx < verCnt; idx++ {
			ino := &Inode{Inode: i.Inode}
			ino.UnmarshalInodeValue(buff)
			if ino.ekRefMap != nil {
				if i.ekRefMap == nil {
					i.ekRefMap = new(sync.Map)
				}
				log.LogDebugf("UnmarshalValue. inode %v merge top layer ekRefMap with layer %v", i.Inode, idx)
				proto.MergeSplitKey(i.Inode, i.ekRefMap, ino.ekRefMap)
			}
			log.LogInfof("action[UnmarshalValue] inode %v old seq %v hist len %v", ino.Inode, ino.verSeq, len(i.multiVersions))
			i.multiVersions = append(i.multiVersions, ino)
		}
		//		log.LogInfof("action[UnmarshalValue] inode %v old seq %v hist len %v stack(%v)", i.Inode, i.verSeq, len(i.multiVersions), string(debug.Stack()))
	}
	return
}

// AppendExtents append the extent to the btree.
func (i *Inode) AppendExtents(eks []proto.ExtentKey, ct int64, volType int) (delExtents []proto.ExtentKey) {
	if proto.IsCold(volType) {
		return
	}
	i.Lock()
	defer i.Unlock()
	for _, ek := range eks {
		delItems := i.Extents.Append(ek)
		size := i.Extents.Size()
		if i.Size < size {
			i.Size = size
		}
		delExtents = append(delExtents, delItems...)
	}
	i.Generation++
	i.ModifyTime = ct

	return
}

// AppendObjExtents append the extent to the btree.
func (i *Inode) AppendObjExtents(eks []proto.ObjExtentKey, ct int64) (err error) {
	i.Lock()
	defer i.Unlock()

	for _, ek := range eks {
		err = i.ObjExtents.Append(ek)
		if err != nil {
			return
		}
		size := i.ObjExtents.Size()
		if i.Size < size {
			i.Size = size
		}
	}
	i.Generation++
	i.ModifyTime = ct
	return
}

func (i *Inode) PrintAllVersionInfo() {
	log.LogInfof("action[PrintAllVersionInfo] inode [%v] verSeq [%v] hist len [%v]", i.Inode, i.verSeq, len(i.multiVersions))
	for id, info := range i.multiVersions {
		log.LogInfof("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode [%v]", id, info.verSeq, info)
	}
}

// clear snapshot extkey with releated verSeq
func (i *Inode) MultiLayerClearExtByVer(layer int, dVerSeq uint64) (delExtents []proto.ExtentKey) {
	var ino *Inode
	if layer == 0 {
		ino = i
	} else {
		ino = i.multiVersions[layer-1]
	}

	ino.Extents.Lock()
	defer ino.Extents.Unlock()

	for idx, ek := range ino.Extents.eks {
		if ek.VerSeq > dVerSeq {
			delExtents = append(delExtents, ek)
			ino.Extents.eks = append(ino.Extents.eks[idx:], ino.Extents.eks[:idx+1]...)
		}
	}
	return
}

func (i *Inode) mergeExtentArr(extentKeysLeft []proto.ExtentKey, extentKeysRight []proto.ExtentKey) []proto.ExtentKey {
	lCnt := len(extentKeysLeft)
	rCnt := len(extentKeysRight)
	sortMergedExts := make([]proto.ExtentKey, 0, lCnt+rCnt)
	lPos, rPos := 0, 0

	for {
		if lPos == lCnt {
			sortMergedExts = append(sortMergedExts, extentKeysRight[rPos:]...)
			break
		}
		if rPos == rCnt {
			sortMergedExts = append(sortMergedExts, extentKeysLeft[lPos:]...)
			break
		}
		mLen := len(sortMergedExts)
		if extentKeysLeft[lPos].FileOffset < extentKeysRight[rPos].FileOffset {
			if mLen > 0 && sortMergedExts[mLen-1].IsSequence(&extentKeysLeft[lPos]) {
				sortMergedExts[mLen-1].Size += extentKeysLeft[lPos].Size
				log.LogDebugf("mergeExtentArr. ek left %v right %v", sortMergedExts[mLen-1], extentKeysLeft[lPos])
				if !sortMergedExts[mLen-1].IsSplit || !extentKeysLeft[lPos].IsSplit {
					log.LogErrorf("ino %v ek merge left %v right %v not all split", i.Inode, sortMergedExts[mLen-1], extentKeysLeft[lPos])
				}
				i.DecSplitEk(&extentKeysLeft[lPos])

			} else {
				sortMergedExts = append(sortMergedExts, extentKeysLeft[lPos])
			}
			lPos++
		} else {
			if mLen > 0 && sortMergedExts[mLen-1].IsSequence(&extentKeysRight[rPos]) {
				sortMergedExts[mLen-1].Size += extentKeysRight[rPos].Size
				log.LogDebugf("mergeExtentArr. ek left %v right %v", sortMergedExts[mLen-1], extentKeysRight[rPos])
				if !sortMergedExts[mLen-1].IsSplit || !extentKeysRight[rPos].IsSplit {
					log.LogErrorf("ino %v ek merge left %v right %v not all split", i.Inode, sortMergedExts[mLen-1], extentKeysRight[rPos])
				}
				i.DecSplitEk(&extentKeysRight[rPos])
			} else {
				sortMergedExts = append(sortMergedExts, extentKeysRight[rPos])
			}
			rPos++
		}
	}

	return sortMergedExts
}

// Restore ext info to older version or deleted if no right version
// The list(multiVersions) contains all point of modification on inode, each ext must belong to one layer.
// Once the layer be deleted is top layer ver be changed to upper layer, or else the ext belongs is exclusive and can be dropped
func (i *Inode) RestoreExts2NextLayer(delExtentsOrigin []proto.ExtentKey, curVer uint64, idx int) (delExtents []proto.ExtentKey, err error) {
	log.LogInfof("action[RestoreMultiSnapExts] curVer [%v] delExtents size [%v] hist len [%v]", curVer, len(delExtentsOrigin), len(i.multiVersions))
	// no version left.all old versions be deleted
	if i.isEmptyVerList() {
		log.LogWarnf("action[RestoreMultiSnapExts] restore have no old version left")
		return delExtentsOrigin, nil
	}
	lastSeq := i.multiVersions[idx].verSeq
	specSnapExtent := make([]proto.ExtentKey, 0)

	for _, delExt := range delExtentsOrigin {
		// curr deleting delExt with a seq larger than the next version's seq, it doesn't belong to any
		// versions,so try to delete it
		log.LogDebugf("action[RestoreMultiSnapExts] ext split [%v] with seq[%v] gSeq[%v] try to del.the last seq [%v], ek details[%v]",
			delExt.IsSplit, delExt.VerSeq, curVer, lastSeq, delExt)
		if delExt.VerSeq > lastSeq {
			delExtents = append(delExtents, delExt)
		} else {
			log.LogInfof("action[RestoreMultiSnapExts] move to level 1 delExt [%v] specSnapExtent size [%v]", delExt, len(specSnapExtent))
			specSnapExtent = append(specSnapExtent, delExt)
		}
	}
	if len(specSnapExtent) == 0 {
		log.LogInfof("action[RestoreMultiSnapExts] no need to move to level 1")
		return
	}
	if len(specSnapExtent) > 0 && i.isEmptyVerList() {
		err = fmt.Errorf("inode %v error not found prev snapshot index", i.Inode)
		log.LogErrorf("action[RestoreMultiSnapExts] %v", err)
		return
	}

	i.multiVersions[idx].Extents.Lock()
	i.multiVersions[idx].Extents.eks = i.mergeExtentArr(i.multiVersions[idx].Extents.eks, specSnapExtent)
	i.multiVersions[idx].Extents.Unlock()

	return
}

func (inode *Inode) unlinkTopLayer(ino *Inode, mpVer uint64, verlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	// if there's no snapshot itself, nor have snapshot after inode's ver then need unlink directly and make no snapshot
	// just move to upper layer, the behavior looks like that the snapshot be dropped
	log.LogDebugf("action[unlinkTopLayer] mpVer %v check if have snapshot depends on the deleitng ino %v (with no snapshot itself) found seq %v, verlist %v",
		mpVer, ino, inode.verSeq, verlist)
	status = proto.OpOk

	// if topLayer verSeq is as same as mp, the current inode deletion only happen on the first layer
	// or ddelete from client do deletion at top layer which should allow delete ionde with older version contrast to mp version
	// because ddelete have two steps,1 is del dentry,2nd is unlink inode ,version may updated after 1st and before 2nd step, to
	// make sure inode be unlinked by normal deletion, sdk add filed of dentry verSeq to identify and different from other unlink actions
	if mpVer == inode.verSeq || ino.Flag&InodeDelTop > 0 {
		if len(inode.multiVersions) == 0 {
			log.LogDebugf("action[unlinkTopLayer] no snapshot available depends on ino %v not found seq %v and return, verlist %v", ino, inode.verSeq, verlist)
			inode.DecNLink()
			log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked", ino.Inode)
			// operate inode directly
			doMore = true
			return
		}

		log.LogDebugf("action[unlinkTopLayer] need restore.ino %v withSeq %v equal mp seq, verlist %v ekRefMap %v",
			ino, inode.verSeq, verlist, inode.ekRefMap)
		// need restore
		if !proto.IsDir(inode.Type) {
			if inode.NLink > 1 {
				log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, file link is %v", ino.Inode, inode.NLink)
				inode.DecNLink()
				doMore = false
				return
			}
			var dIno *Inode
			if ext2Del, dIno = inode.getAndDelVer(ino.verSeq, mpVer, verlist); dIno == nil {
				status = proto.OpNotExistErr
				log.LogDebugf("action[unlinkTopLayer] ino %v", ino)
				return
			}
			log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, File restore, ekRefMap %v", ino.Inode, inode.ekRefMap)
			dIno.DecNLink() // dIno should be inode
			doMore = true
		} else {
			log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, Dir", ino.Inode)
			inode.DecNLink()
			doMore = true
		}

		return
	}

	log.LogDebugf("action[unlinkTopLayer] need create version.ino %v withSeq %v not equal mp seq %v, verlist %v", ino, inode.verSeq, mpVer, verlist)
	if proto.IsDir(inode.Type) { // dir is whole info but inode is partition,which is quit different
		verlist.RLock()
		defer verlist.RUnlock()

		_, err := inode.getNextOlderVer(mpVer, verlist)
		if err == nil {
			log.LogDebugf("action[unlinkTopLayer] inode %v cann't get next older ver %v err %v", inode.Inode, mpVer, err)
			inode.CreateVer(mpVer)
		}
		inode.DecNLink()
		log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, Dir create ver 1st layer", ino.Inode)
		doMore = true
	} else {
		verlist.RLock()
		defer verlist.RUnlock()

		ver, err := inode.getNextOlderVer(mpVer, verlist)
		if err != nil {
			if err.Error() == "not found" {
				inode.DecNLink()
				doMore = true
			}
			log.LogErrorf("action[unlinkTopLayer] inode %v cann't get next older ver %v err %v", inode.Inode, mpVer, err)
			return
		}
		inode.CreateVer(mpVer) // protect origin version
		if inode.NLink == 1 {
			inode.CreateUnlinkVer(mpVer, ver) // create a effective top level  version
		}
		inode.DecNLink()
		log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, File create ver 1st layer", ino.Inode)
	}
	return

}

func (inode *Inode) dirUnlinkVerInlist(ino *Inode, mpVer uint64, verlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	var idxWithTopLayer int
	var dIno *Inode
	status = proto.OpOk
	if dIno, idxWithTopLayer = inode.getInoByVer(ino.verSeq, false); dIno == nil {
		log.LogDebugf("action[dirUnlinkVerInlist] ino %v not found", ino)
		return
	}
	var endSeq uint64
	if idxWithTopLayer == 0 {
		// header layer do nothing and be depends on should not be dropped
		log.LogDebugf("action[dirUnlinkVerInlist] ino %v first layer do nothing", ino)
		return
	}
	// if any alive snapshot in mp dimension exist in seq scope from dino to next ascend neighbor, dio snapshot be keep or else drop

	mIdx := idxWithTopLayer - 1
	if mIdx == 0 {
		endSeq = inode.verSeq
	} else {
		endSeq = inode.multiVersions[mIdx-1].verSeq
	}

	log.LogDebugf("action[dirUnlinkVerInlist] inode %v try drop multiVersion idx %v effective seq scope [%v,%v) ",
		inode.Inode, mIdx, dIno.verSeq, endSeq)

	doWork := func() bool {
		verlist.RLock()
		defer verlist.RUnlock()

		for vidx, info := range verlist.VerList {
			if info.Ver >= dIno.verSeq && info.Ver < endSeq {
				log.LogDebugf("action[dirUnlinkVerInlist] inode %v dir layer idx %v still have effective snapshot seq %v.so don't drop", inode.Inode, mIdx, info.Ver)
				return false
			}
			if info.Ver >= endSeq || vidx == len(verlist.VerList)-1 {
				log.LogDebugf("action[dirUnlinkVerInlist] inode %v try drop multiVersion idx %v and return", inode.Inode, mIdx)

				inode.Lock()
				inode.multiVersions = append(inode.multiVersions[:mIdx], inode.multiVersions[mIdx+1:]...)
				inode.Unlock()
				return true
			}
			log.LogDebugf("action[dirUnlinkVerInlist] inode %v try drop scope [%v, %v), mp ver %v not suitable", inode.Inode, dIno.verSeq, endSeq, info.Ver)
			return true
		}
		return true
	}
	if !doWork() {
		return
	}
	doMore = true
	dIno.DecNLink()
	return
}

func (inode *Inode) unlinkVerInList(ino *Inode, mpVer uint64, verlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	log.LogDebugf("action[unlinkVerInList] ino %v try search seq %v isdir %v", ino, ino.verSeq, proto.IsDir(inode.Type))
	if proto.IsDir(inode.Type) { // snapshot dir deletion don't take link into consider, but considers the scope of snapshot contrast to verList
		return inode.dirUnlinkVerInlist(ino, mpVer, verlist)
	}
	var dIno *Inode
	status = proto.OpOk
	// special case, snapshot is the last one and be depended by upper version,update it's version to the right one
	// ascend search util to the curr unCommit version in the verList
	if ino.verSeq == inode.verSeq || (ino.verSeq == math.MaxUint64 && inode.verSeq == 0) {
		if len(verlist.VerList) == 0 {
			status = proto.OpNotExistErr
			log.LogErrorf("action[unlinkVerInList] inode %v verlist should be larger than 0, return not found", inode.Inode)
			return
		}

		// just move to upper layer,the request snapshot be dropped
		nVerSeq, found := inode.getLastestVer(inode.verSeq, false, verlist)
		if !found {
			status = proto.OpNotExistErr
			return
		}
		log.LogDebugf("action[unlinkVerInList] inode %v update current verSeq %v to %v", inode.Inode, inode.verSeq, nVerSeq)
		inode.verSeq = nVerSeq
		return
	} else {
		// don't unlink if no version satisfied
		if ext2Del, dIno = inode.getAndDelVer(ino.verSeq, mpVer, verlist); dIno == nil {
			status = proto.OpNotExistErr
			log.LogDebugf("action[unlinkVerInList] ino %v", ino)
			return
		}
	}

	dIno.DecNLink()
	log.LogDebugf("action[unlinkVerInList] inode %v snapshot layer be unlinked", ino.Inode)
	doMore = true
	return
}

func (i *Inode) getLastestVer(reqVerSeq uint64, commit bool, verlist *proto.VolVersionInfoList) (uint64, bool) {
	verlist.RLock()
	defer verlist.RUnlock()

	if len(verlist.VerList) == 0 {
		return 0, false
	}
	for id, info := range verlist.VerList {
		if commit && id == len(verlist.VerList)-1 {
			break
		}
		if info.Ver >= reqVerSeq {
			return info.Ver, true
		}
	}

	log.LogDebugf("action[getLastestVer] inode %v reqVerSeq %v not found, the largetst one %v",
		i.Inode, reqVerSeq, verlist.VerList[len(verlist.VerList)-1].Ver)
	return 0, false
}

func (i *Inode) ShouldDelVer(mpVer uint64, delVer uint64) (ok bool, err error) {
	if i.verSeq == 0 {
		if delVer > 0 {
			if delVer == math.MaxUint64 {
				return true, nil
			}
			return false, fmt.Errorf("not found")
		} else {
			// mp ver larger than zero means snapshot happened but haven't take effect on this inode
			if mpVer > 0 {
				return false, nil
			}
			return true, nil
		}
	} else {
		if delVer > i.verSeq {
			return false, fmt.Errorf("not found")
		} else if delVer == i.verSeq {
			return true, nil
		}
	}

	if delVer == math.MaxUint64 {
		tailVer, _ := i.getTailVerInList()
		if tailVer == 0 {
			return true, nil
		}
		return false, fmt.Errorf("not found")
	}

	for _, inoVer := range i.multiVersions {
		if inoVer.verSeq == delVer {
			return true, nil
		}
		if inoVer.verSeq < delVer {
			break
		}
	}
	return false, fmt.Errorf("not found")
}

//note:search all layers.
//idx need calc include nclude top layer. index in multiVersions need add by 1
func (ino *Inode) getInoByVer(verSeq uint64, equal bool) (i *Inode, idx int) {
	ino.RLock()
	defer ino.RUnlock()

	log.LogDebugf("action[getInoByVer] ino %v verseq %v hist len %v request ino ver %v",
		ino.Inode, ino.verSeq, ino.multiVersions, verSeq)
	if verSeq == 0 || verSeq == ino.verSeq || (verSeq == math.MaxUint64 && ino.verSeq == 0) {
		return ino, 0
	}
	if verSeq == math.MaxUint64 {
		listLen := len(ino.multiVersions)
		if listLen == 0 {
			log.LogDebugf("action[getInoByVer]  ino %v no multiversion", ino.Inode)
			return
		}
		i = ino.multiVersions[listLen-1]
		if i.verSeq != 0 {
			log.LogDebugf("action[getInoByVer]  ino %v lay seq %v", ino.Inode, i.verSeq)
			return nil, 0
		}
		return i, listLen
	}
	if verSeq > 0 && ino.verSeq > verSeq {
		for id, iTmp := range ino.multiVersions {
			if verSeq == iTmp.verSeq {
				log.LogDebugf("action[getInoByVer]  ino %v get in multiversion id %v", ino.Inode, id)
				return iTmp, id + 1
			} else if verSeq > iTmp.verSeq {
				if !equal {
					log.LogDebugf("action[getInoByVer]  ino %v get in multiversion id %v, %v, %v", ino.Inode, id, verSeq, iTmp.verSeq)
					return iTmp, id + 1
				}
				log.LogDebugf("action[getInoByVer]  ino %v get in multiversion id %v", ino.Inode, id)
				return
			}
		}
	} else {
		if !equal {
			log.LogDebugf("action[getInoByVer]  ino %v", ino.Inode)
			return ino, 0
		}
	}
	return
}

// 1. check if dVer layer is the last layer of the system  1)true,drop it all 2) false goto 3
// 2. if have system layer between dVer and next older inode's layer(not exist is ok), drop dVer related exts and update ver
// 3. else Restore to next inode's Layer

func (i *Inode) getAndDelVer(dVer uint64, mpVer uint64, verlist *proto.VolVersionInfoList) (delExtents []proto.ExtentKey, ino *Inode) {
	var err error
	verlist.RLock()
	defer verlist.RUnlock()

	log.LogDebugf("action[getAndDelVer] ino %v verSeq %v request del ver %v hist len %v isTmpFile %v",
		i.Inode, i.verSeq, dVer, len(i.multiVersions), i.IsTempFile())

	// first layer need delete
	if dVer == 0 {
		if delExtents, err = i.RestoreExts2NextLayer(i.Extents.eks, mpVer, 0); err != nil {
			log.LogErrorf("action[getAndDelVer] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
			return
		}
		i.Extents.eks = i.Extents.eks[:0]
		log.LogDebugf("action[getAndDelVer] ino %v verSeq %v get del exts %v", i.Inode, i.verSeq, delExtents)
		return delExtents, i
	}
	// read inode element is fine, lock is need while write
	inoVerLen := len(i.multiVersions)
	if inoVerLen == 0 {
		log.LogDebugf("action[getAndDelVer] ino %v RestoreMultiSnapExts no left", i.Inode)
		return
	}

	tailVer, _ := i.getTailVerInList()
	// delete snapshot version
	if dVer == math.MaxUint64 || dVer == tailVer {
		if dVer == math.MaxUint64 {
			dVer = 0
		}
		inode := i.multiVersions[inoVerLen-1]
		if inode.verSeq != dVer {
			log.LogDebugf("action[getAndDelVer] ino %v idx %v is %v and cann't be dropped tail  ver %v",
				i.Inode, inoVerLen-1, inode.verSeq, tailVer)
			return
		}
		i.Lock()
		defer i.Unlock()
		i.multiVersions = i.multiVersions[:inoVerLen-1]

		log.LogDebugf("action[getAndDelVer] ino %v idx %v be dropped", i.Inode, inoVerLen)
		return inode.Extents.eks, inode
	}

	for id, mIno := range i.multiVersions {
		log.LogDebugf("action[getAndDelVer] ino %v multiVersions level %v verseq %v", i.Inode, id, mIno.verSeq)
		if mIno.verSeq < dVer {
			log.LogDebugf("action[getAndDelVer] ino %v multiVersions level %v verseq %v", i.Inode, id, mIno.verSeq)
			return
		}

		if mIno.verSeq == dVer {
			// 1.
			if i.isTailIndexInList(id) { // last layer then need delete all and unlink inode
				i.multiVersions = i.multiVersions[:id]
				return mIno.Extents.eks, mIno
			}
			log.LogDebugf("action[getAndDelVer] ino %v ver %v step 3", i.Inode, mIno.verSeq)
			// 2. get next version should according to verList but not only self multi list
			var nVerSeq uint64
			if nVerSeq, err = i.getNextOlderVer(dVer, verlist); id == -1 || err != nil {
				log.LogDebugf("action[getAndDelVer] get next version failed, err %v", err)
				return
			}

			log.LogDebugf("action[getAndDelVer] ino %v ver %v nextVerSeq %v step 3 ver ", i.Inode, mIno.verSeq, nVerSeq)
			// 2. system next layer not exist in inode ver list. update curr layer to next layer and filter out ek with verSeq
			// change id layer verSeq to neighbor layer info, omit version delete process

			if i.isTailIndexInList(id) || nVerSeq != i.multiVersions[id+1].verSeq {
				log.LogDebugf("action[getAndDelVer] ino %v  get next version in verList update ver from %v to %v.And delete exts with ver %v",
					i.Inode, i.multiVersions[id].verSeq, nVerSeq, dVer)

				i.multiVersions[id].verSeq = nVerSeq
				delExtents, ino = i.MultiLayerClearExtByVer(id, nVerSeq), i.multiVersions[id]
				if len(i.multiVersions[id].Extents.eks) != 0 {
					log.LogDebugf("action[getAndDelVer] ino %v   after clear self still have ext and left", i.Inode)
					return
				}
			} else {
				log.LogDebugf("action[getAndDelVer] ino %v ver %v nextVer %v step 3 ver ", i.Inode, mIno.verSeq, nVerSeq)
				// 3. next layer exist. the deleted version and  next version are neighbor in verlist, thus need restore and delete
				if delExtents, err = i.RestoreExts2NextLayer(mIno.Extents.eks, dVer, id+1); err != nil {
					log.LogDebugf("action[getAndDelVer] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
					return
				}
			}
			// delete layer id
			i.multiVersions = append(i.multiVersions[:id], i.multiVersions[id+1:]...)

			log.LogDebugf("action[getAndDelVer] ino %v verSeq %v get del exts %v", i.Inode, i.verSeq, delExtents)
			return delExtents, mIno
		}
	}
	return
}

func (i *Inode) getNextOlderVer(ver uint64, verlist *proto.VolVersionInfoList) (verSeq uint64, err error) {
	verlist.RLock()
	defer verlist.RUnlock()
	log.LogDebugf("getNextOlderVer inode %v ver %v", i.Inode, ver)
	for idx, info := range verlist.VerList {
		log.LogDebugf("getNextOlderVer inode %v id %v ver %v info %v", i.Inode, idx, info.Ver, info)
		if info.Ver >= ver {
			if idx == 0 {
				return 0, fmt.Errorf("not found")
			}
			return verlist.VerList[idx-1].Ver, nil
		}
	}
	log.LogErrorf("getNextOlderVer inode %v ver %v not found", i.Inode, ver)
	return 0, fmt.Errorf("version not exist")
}

func (i *Inode) CreateUnlinkVer(mpVer uint64, nVer uint64) {
	log.LogDebugf("action[CreateUnlinkVer] inode %v mpVer %v nVer %v", i.Inode, mpVer, nVer)
	//inode copy not include multi ver array
	ino := i.Copy().(*Inode)
	i.Extents = NewSortedExtents()
	i.ObjExtents = NewSortedObjExtents()
	i.SetDeleteMark()

	ino.multiVersions = nil
	ino.verSeq = nVer

	log.LogDebugf("action[CreateUnlinkVer] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, mpVer, i.verSeq, len(i.multiVersions))

	i.Lock()
	if i.multiVersions[0].verSeq == nVer {
		i.multiVersions[0] = ino
	} else {
		i.multiVersions = append([]*Inode{ino}, i.multiVersions...)
	}

	i.verSeq = mpVer
	i.Unlock()
}

func (i *Inode) CreateVer(ver uint64) {
	//inode copy not include multi ver array
	ino := i.Copy().(*Inode)
	ino.Extents = NewSortedExtents()
	ino.ObjExtents = NewSortedObjExtents()
	ino.multiVersions = nil

	i.Lock()
	defer i.Unlock()
	log.LogDebugf("action[CreateVer] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ver, i.verSeq, len(i.multiVersions))

	i.multiVersions = append([]*Inode{ino}, i.multiVersions...)
	i.verSeq = ver
}

func (i *Inode) SplitExtentWithCheck(mpVer uint64, multiVersionList *proto.VolVersionInfoList,
	reqVer uint64, ek proto.ExtentKey, ct int64, volType int) (delExtents []proto.ExtentKey, status uint8) {

	var err error
	ek.VerSeq = reqVer
	log.LogDebugf("action[SplitExtentWithCheck] inode %v,ver %v,ek %v,hist len %v", i.Inode, reqVer, ek, len(i.multiVersions))

	if reqVer != i.verSeq {
		log.LogDebugf("action[SplitExtentWithCheck] CreateVer ver %v", reqVer)
		i.CreateVer(reqVer)
	}
	i.Lock()
	defer i.Unlock()
	if i.ekRefMap == nil {
		i.ekRefMap = new(sync.Map)
	}
	delExtents, status = i.Extents.SplitWithCheck(i.Inode, ek, i.ekRefMap)
	if status != proto.OpOk {
		log.LogErrorf("action[SplitExtentWithCheck] status %v", status)
		return
	}
	if len(delExtents) == 0 {
		return
	}

	if err = i.CreateLowerVersion(i.verSeq, multiVersionList); err != nil {
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(delExtents, mpVer, 0); err != nil {
		log.LogErrorf("action[fsmAppendExtentWithCheck] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
		return
	}
	if proto.IsHot(volType) {
		i.Generation++
		i.ModifyTime = ct
	}

	return
}

// try to create version between curVer and seq of multiVersions[0] in verList
func (i *Inode) CreateLowerVersion(curVer uint64, verlist *proto.VolVersionInfoList) (err error) {
	verlist.RLock()
	defer verlist.RUnlock()

	log.LogDebugf("CreateLowerVersion inode %v curVer %v", i.Inode, curVer)
	if len(verlist.VerList) <= 1 {
		return
	}
	if i.isEmptyVerList() {
		return
	}
	var nextVer uint64
	for _, info := range verlist.VerList {
		if info.Ver < curVer {
			nextVer = info.Ver
		}
		if info.Ver >= curVer {
			break
		}
	}
	if nextVer <= i.multiVersions[0].verSeq {
		log.LogDebugf("CreateLowerVersion nextver %v layer 0 ver %v", nextVer, i.multiVersions[0].verSeq)
		return
	}

	ino := i.CopyDirectly().(*Inode)
	ino.Extents = NewSortedExtents()
	ino.ObjExtents = NewSortedObjExtents()
	ino.multiVersions = nil
	ino.verSeq = nextVer

	log.LogDebugf("action[CreateLowerVersion] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ino, i.verSeq, len(i.multiVersions))

	i.multiVersions = append([]*Inode{ino}, i.multiVersions...)

	return
}

func (i *Inode) AppendExtentWithCheck(
	mpVer uint64,
	multiVersionList *proto.VolVersionInfoList,
	reqVer uint64,
	ek proto.ExtentKey,
	ct int64,
	discardExtents []proto.ExtentKey,
	volType int) (delExtents []proto.ExtentKey, status uint8) {

	ek.VerSeq = mpVer
	log.LogDebugf("action[AppendExtentWithCheck] mpVer %v inode %v and ver %v,req ver %v,ek %v,hist len %v",
		mpVer, i.Inode, i.verSeq, reqVer, ek, len(i.multiVersions))

	if mpVer != i.verSeq {
		log.LogDebugf("action[AppendExtentWithCheck] ver %v inode ver %v", reqVer, i.verSeq)
		i.CreateVer(mpVer)
	}

	i.Lock()
	defer i.Unlock()

	delExtents, status = i.Extents.AppendWithCheck(i.Inode, ek, discardExtents)
	if status != proto.OpOk {
		log.LogErrorf("action[AppendExtentWithCheck] status %v", status)
		return
	}

	// multi version take effect
	if i.verSeq > 0 && len(delExtents) > 0 {
		var err error
		if err = i.CreateLowerVersion(i.verSeq, multiVersionList); err != nil {
			return
		}
		if delExtents, err = i.RestoreExts2NextLayer(delExtents, mpVer, 0); err != nil {
			log.LogErrorf("action[AppendExtentWithCheck] RestoreMultiSnapExts err %v", err)
			return nil, proto.OpErr
		}
	}

	if proto.IsHot(volType) {
		size := i.Extents.Size()
		if i.Size < size {
			i.Size = size
		}
		i.Generation++
		i.ModifyTime = ct
	}
	return
}

func (i *Inode) ExtentsTruncate(length uint64, ct int64, doOnLastKey func(*proto.ExtentKey)) (delExtents []proto.ExtentKey) {
	i.Lock()
	delExtents = i.Extents.Truncate(length, doOnLastKey)
	i.Size = length
	i.ModifyTime = ct
	i.Generation++
	i.Unlock()
	return
}

// IncNLink increases the nLink value by one.
func (i *Inode) IncNLink(verSeq uint64) {
	if i.verSeq < verSeq {
		i.CreateVer(verSeq)
	}
	i.Lock()
	i.NLink++
	i.Unlock()
}

// DecNLink decreases the nLink value by one.
func (i *Inode) DecNLink() {
	i.Lock()
	if proto.IsDir(i.Type) && i.NLink == 2 {
		i.NLink--
	}
	if i.NLink > 0 {
		i.NLink--
	}
	i.Unlock()
}

// DecNLink decreases the nLink value by one.
func (i *Inode) DecNLinkByVer(verSeq uint64) {
	if i.verSeq < verSeq {
		i.CreateVer(verSeq)
	}
	i.DecNLink()
}

func (i *Inode) DecSplitExts(delExtents interface{}) {
	log.LogDebugf("DecSplitExts inode %v", i.Inode)
	cnt := len(delExtents.([]proto.ExtentKey))
	for id := 0; id < cnt; id++ {
		ek := &delExtents.([]proto.ExtentKey)[id]
		if !ek.IsSplit {
			log.LogDebugf("DecSplitExts ek not split %v", ek)
			continue
		}
		if i.ekRefMap == nil {
			log.LogErrorf("DecSplitExts. ekRefMap is nil")
			return
		}

		ok, last := i.DecSplitEk(ek)
		if !ok {
			log.LogErrorf("DecSplitExts. ek %v not found!", ek)
			continue
		}
		if last {
			log.LogDebugf("DecSplitExts ek %v split flag be unset to remove all content", ek)
			ek.IsSplit = false
		}
	}
}

func (i *Inode) DecSplitEk(ext *proto.ExtentKey) (ok bool, last bool) {
	log.LogDebugf("DecSplitEk inode %v dp %v extent id %v.key %v ext %v", i.Inode, ext.PartitionId, ext.ExtentId,
		ext.PartitionId<<32|ext.ExtentId, ext)

	if val, ok := i.ekRefMap.Load(ext.PartitionId<<32 | ext.ExtentId); !ok {
		log.LogErrorf("DecSplitEk. dp %v inode [%v] ext not found", ext.PartitionId, i.Inode)
		return false, false
	} else {
		if val.(uint32) == 0 {
			log.LogErrorf("DecSplitEk. dp %v inode [%v] ek ref is zero!", ext.PartitionId, i.Inode)
			return false, false
		}
		if val.(uint32) == 1 {
			log.LogDebugf("DecSplitEk inode %v dp %v extent id %v.key %v", i.Inode, ext.PartitionId, ext.ExtentId,
				ext.PartitionId<<32|ext.ExtentId)
			i.ekRefMap.Delete(ext.PartitionId<<32 | ext.ExtentId)
			return true, true
		}
		i.ekRefMap.Store(ext.PartitionId<<32|ext.ExtentId, val.(uint32)-1)
		log.LogDebugf("DecSplitEk. mp %v inode [%v] ek %v val %v", ext.PartitionId, i.Inode, ext, val.(uint32)-1)
		return true, false
	}
}

// DecNLink decreases the nLink value by one.
func (i *Inode) GetDecNLinkResult() (nLink uint32) {
	i.Lock()
	nLink = i.NLink
	if proto.IsDir(i.Type) && nLink == 2 {
		nLink--
	}
	if nLink > 0 {
		nLink--
	}
	i.Unlock()
	return
}

// GetNLink returns the nLink value.
func (i *Inode) GetNLink() uint32 {
	i.RLock()
	defer i.RUnlock()
	return i.NLink
}

func (i *Inode) IsTempFile() bool {
	i.RLock()
	ok := i.NLink == 0 && !proto.IsDir(i.Type)
	i.RUnlock()
	return ok
}

func (i *Inode) IsEmptyDir() bool {
	i.RLock()
	ok := proto.IsDir(i.Type) && i.NLink <= 2
	i.RUnlock()
	return ok
}

func (i *Inode) IsEmptyDirAndNoSnapshot() bool {
	i.RLock()
	ok := proto.IsDir(i.Type) && i.NLink <= 2 && len(i.multiVersions) == 0
	i.RUnlock()
	return ok
}

func (i *Inode) IsTopLayerEmptyDir() bool {
	i.RLock()
	ok := proto.IsDir(i.Type) && i.NLink <= 2
	i.RUnlock()
	return ok
}

// SetDeleteMark set the deleteMark flag. TODO markDelete or deleteMark? markDelete has been used in datanode.
func (i *Inode) SetDeleteMark() {
	i.Lock()
	i.Flag |= DeleteMarkFlag
	i.Unlock()
}

// ShouldDelete returns if the inode has been marked as deleted.
func (i *Inode) ShouldDelete() (ok bool) {
	i.RLock()
	ok = i.Flag&DeleteMarkFlag == DeleteMarkFlag
	i.RUnlock()
	return
}

// inode should delay remove if as 3 conditions:
// 1. DeleteMarkFlag is unset
// 2. NLink == 0
// 3. AccessTime is 7 days ago
func (i *Inode) ShouldDelayDelete() (ok bool) {
	i.RLock()
	ok = (i.Flag&DeleteMarkFlag != DeleteMarkFlag) &&
		(i.NLink == 0) &&
		time.Now().Unix()-i.AccessTime < InodeNLink0DelayDeleteSeconds
	i.RUnlock()
	return
}

// SetAttr sets the attributes of the inode.
func (i *Inode) SetAttr(req *SetattrRequest) {
	log.LogDebugf("action[SetAttr] inode %v req seq %v inode seq %v", i.Inode, req.VerSeq, i.verSeq)

	if req.VerSeq != i.verSeq {
		i.CreateVer(req.VerSeq)
	}
	i.Lock()
	log.LogDebugf("action[SetAttr] inode %v req seq %v inode seq %v", i.Inode, req.VerSeq, i.verSeq)
	if req.Valid&proto.AttrMode != 0 {
		i.Type = req.Mode
	}
	if req.Valid&proto.AttrUid != 0 {
		i.Uid = req.Uid
	}
	if req.Valid&proto.AttrGid != 0 {
		i.Gid = req.Gid
	}
	if req.Valid&proto.AttrAccessTime != 0 {
		i.AccessTime = req.AccessTime
	}
	if req.Valid&proto.AttrModifyTime != 0 {
		i.ModifyTime = req.ModifyTime
	}

	i.Unlock()
}

func (i *Inode) DoWriteFunc(fn func()) {
	i.Lock()
	defer i.Unlock()
	fn()
}

// DoFunc executes the given function.
func (i *Inode) DoReadFunc(fn func()) {
	i.RLock()
	defer i.RUnlock()
	fn()
}

// SetMtime sets mtime to the current time.
func (i *Inode) SetMtime() {
	mtime := Now.GetCurrentTimeUnix()
	i.Lock()
	defer i.Unlock()
	i.ModifyTime = mtime
}

// EmptyExtents clean the inode's extent list.
func (i *Inode) EmptyExtents(mtime int64) (delExtents []proto.ExtentKey) {
	i.Lock()
	defer i.Unlock()
	// eks is safe because extents be reset next and eks is will not be visit except del routine
	delExtents = i.Extents.eks
	i.Extents = NewSortedExtents()

	return delExtents
}

// EmptyExtents clean the inode's extent list.
func (i *Inode) CopyTinyExtents() (delExtents []proto.ExtentKey) {
	i.RLock()
	defer i.RUnlock()
	return i.Extents.CopyTinyExtents()
}

// ReplaceExtents replace eks with curEks, delEks are which need to deleted.
// func (i *Inode) ReplaceExtents(curEks []proto.ExtentKey, mtime int64) (delEks []proto.ExtentKey) {
// 	i.Lock()
// 	defer i.Unlock()
// 	oldEks := i.Extents.eks
// 	delEks = make([]proto.ExtentKey, len(oldEks)-len(curEks))
// 	for _, key := range oldEks {
// 		exist := false
// 		for _, delKey := range curEks {
// 			if key.FileOffset == delKey.FileOffset && key.ExtentId == delKey.ExtentId &&
// 				key.ExtentOffset == delKey.ExtentOffset && key.PartitionId == delKey.PartitionId &&
// 				key.Size == delKey.Size {
// 				exist = true
// 				break
// 			}
// 		}
// 		if !exist {
// 			delEks = append(delEks, key)
// 		}
// 	}
// 	i.ModifyTime = mtime
// 	i.Generation++
// 	i.Extents.eks = curEks
// 	return
// }
