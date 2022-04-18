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
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	DeleteMarkFlag = 1 << 0
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
}

type SplitExtentInfo struct {
	PartitionId uint64
	ExtentId    uint64
	refCnt      uint64
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

	i.RUnlock()
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
	//if buff.Len() == 0 {
	//	return
	//}
	// unmarshal ExtentsKey
	if i.Extents == nil {
		i.Extents = NewSortedExtents()
	}
	log.LogInfof("action[UnmarshalInodeValue] inode %v Reserved %v", i.Inode, i.Reserved)
	v3 := i.Reserved&V3EnableSnapInodeFlag > 0
	if (i.Reserved&V2EnableColdInodeFlag > 0) || v3 {
		extSize := uint32(0)
		if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
			return
		}
		if extSize > 0 {
			extBytes := make([]byte, extSize)
			if _, err = io.ReadFull(buff, extBytes); err != nil {
				return
			}
			if err = i.Extents.UnmarshalBinary(extBytes, v3); err != nil {
				return
			}
		}
	}

	if i.Reserved&V2EnableColdInodeFlag > 0 {
		// unmarshal ObjExtentsKey
		if i.ObjExtents == nil {
			i.ObjExtents = NewSortedObjExtents()
		}
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

	if i.Reserved&V3EnableSnapInodeFlag > 0 {
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
		for verCnt > 0 {
			ino := &Inode{}
			ino.UnmarshalInodeValue(buff)
			log.LogInfof("action[UnmarshalValue] inode %v old seq %v hist len %v", ino.Inode, ino.verSeq, len(i.multiVersions))
			i.multiVersions = append(i.multiVersions, ino)
			verCnt--
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
	for idx, ek := range ino.Extents.eks {
		if ek.VerSeq == dVerSeq {
			delExtents = append(delExtents, ek)
			ino.Extents.eks = append(ino.Extents.eks[idx:], ino.Extents.eks[:idx+1]...)
		}
	}
	return
}

// restore ext info to older version or deleted if no right version
func (i *Inode) RestoreMultiSnapExts(delExtentsOrigin []proto.ExtentKey, curVer uint64, idx int) (delExtents []proto.ExtentKey, err error) {
	log.LogInfof("action[RestoreMultiSnapExts] curVer [%v] delExtents size [%v] hist len [%v]", curVer, len(delExtentsOrigin), len(i.multiVersions))
	// no version left.all old versions be deleted
	if len(i.multiVersions) == 0 {
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
		if delExt.VerSeq == curVer {
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
	if len(specSnapExtent) > 0 && len(i.multiVersions) == 0 {
		err = fmt.Errorf("inode %v error not found prev snapshot index", i.Inode)
		log.LogErrorf("action[RestoreMultiSnapExts] %v", err)
		return
	}

	i.PrintAllVersionInfo()

	i.multiVersions[idx].Extents.eks = append(i.multiVersions[idx].Extents.eks, specSnapExtent...)

	sort.SliceStable(
		i.multiVersions[idx].Extents.eks, func(w, z int) bool {
			return i.multiVersions[idx].Extents.eks[w].FileOffset < i.multiVersions[idx].Extents.eks[z].FileOffset
		})

	i.PrintAllVersionInfo()
	return
}

func (inode *Inode) unlinkVerInTopLayer(ino *Inode, mpVer uint64, verlist []*proto.VolVersionInfo) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	// if there's no snapshot itself, nor have snapshot after inode's ver then need unlink directly and make no snapshot
	// just move to upper layer, the behavior looks like that the snapshot be dropped
	log.LogDebugf("action[unlinkVerInTopLayer] check if have snapshot depends on the deleitng ino %v (with no snapshot itself) found seq %v, verlist %v",
		ino, inode.verSeq, verlist)
	_, found := inode.getLastestVer(inode.verSeq, true, verlist)
	if !found {
		if len(inode.multiVersions) == 0 {
			log.LogDebugf("action[unlinkVerInTopLayer] no snapshot available depends on ino %v not found seq %v and return, verlist %v", ino, inode.verSeq, verlist)
			inode.DecNLink()
			log.LogDebugf("action[unlinkVerInTopLayer] inode %v be unlinked", ino.Inode)
			// operate inode directly
			doMore = true
			return
		}

		log.LogDebugf("action[unlinkVerInTopLayer] need restore.ino %v withSeq %v equal mp seq, verlist %v", ino, inode.verSeq, verlist)
		// need restore
		if !proto.IsDir(inode.Type) {
			var dIno *Inode
			if ext2Del, dIno = inode.getAndDelVer(ino.verSeq, mpVer, verlist); dIno == nil {
				status = proto.OpNotExistErr
				log.LogDebugf("action[unlinkVerInTopLayer] ino %v", ino)
				return
			}
			log.LogDebugf("action[unlinkVerInTopLayer] inode %v be unlinked, File restore", ino.Inode)
			dIno.DecNLink() // dIno should be inode
			doMore = true
		} else {
			log.LogDebugf("action[unlinkVerInTopLayer] inode %v be unlinked, Dir", ino.Inode)
			inode.DecNLink()
			doMore = true
		}
		return
	}

	log.LogDebugf("action[unlinkVerInTopLayer] need create version.ino %v withSeq %v not equal mp seq %v, verlist %v", ino, inode.verSeq, mpVer, verlist)
	if proto.IsDir(inode.Type) { // dir is all info but inode is part,which is quit different
		inode.CreateVer(mpVer)
		inode.DecNLink()
		log.LogDebugf("action[unlinkVerInTopLayer] inode %v be unlinked, Dir create ver 1st layer", ino.Inode)
	} else {
		inode.CreateUnlinkVer(mpVer, verlist)
		inode.DecNLink()
		log.LogDebugf("action[unlinkVerInTopLayer] inode %v be unlinked, File create ver 1st layer", ino.Inode)
	}
	return

}

func (inode *Inode) unlinkVerInList(ino *Inode, mpVer uint64, verlist []*proto.VolVersionInfo) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	log.LogDebugf("action[unlinkVerInList] ino %v try search seq %v isdir %v", ino, ino.verSeq, proto.IsDir(inode.Type))
	var dIno *Inode
	if proto.IsDir(inode.Type) { // snapshot dir deletion don't take link into consider, but considers the scope of snapshot contrast to verList
		var idx int
		if dIno, idx = inode.getInoByVer(ino.verSeq, false); dIno == nil {
			status = proto.OpNotExistErr
			log.LogDebugf("action[unlinkVerInList] ino %v not found", ino)
			return
		}
		if idx == 0 {
			// header layer do nothing and be depends on should not be dropped
			log.LogDebugf("action[unlinkVerInList] ino %v first layer do nothing", ino)
			return
		}
		// if any alive snapshot in mp dimension exist in seq scope from dino to next ascend neighbor, dio snapshot be keep or else drop
		var endSeq uint64
		realIdx := idx - 1
		if realIdx == 0 {
			endSeq = inode.verSeq
		} else {
			endSeq = inode.multiVersions[realIdx-1].verSeq
		}

		log.LogDebugf("action[unlinkVerInList] inode %v try drop multiVersion idx %v effective seq scope [%v,%v) ",
			inode.Inode, realIdx, dIno.verSeq, endSeq)

		for vidx, info := range verlist {
			if info.Ver >= dIno.verSeq && info.Ver < endSeq {
				log.LogDebugf("action[unlinkVerInList] inode %v dir layer idx %v still have effective snapshot seq %v.so don't drop", inode.Inode, realIdx, info.Ver)
				return
			}
			if info.Ver >= endSeq || vidx == len(verlist)-1 {
				log.LogDebugf("action[unlinkVerInList] inode %v try drop multiVersion idx %v and return", inode.Inode, realIdx)
				inode.multiVersions = append(inode.multiVersions[:realIdx], inode.multiVersions[realIdx+1:]...)
				return
			}
			log.LogDebugf("action[unlinkVerInList] inode %v try drop scope [%v, %v), mp ver %v not suitable", inode.Inode, dIno.verSeq, endSeq, info.Ver)
		}
		doMore = true
	} else {
		// special case, snapshot is the last one and be depended by upper version,update it's version to the right one
		// ascend search util to the curr unCommit version in the verList
		if ino.verSeq == inode.verSeq || (ino.verSeq == math.MaxUint64 && inode.verSeq == 0) {
			if len(verlist) == 0 {
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
	}
	dIno.DecNLink()
	log.LogDebugf("action[unlinkVerInList] inode %v snapshot layer be unlinked", ino.Inode)
	doMore = true
	return
}

func (i *Inode) getLastestVer(reqVerSeq uint64, commit bool, verlist []*proto.VolVersionInfo) (uint64, bool) {
	if len(verlist) == 0 {
		return 0, false
	}
	for id, info := range verlist {
		if commit && id == len(verlist)-1 {
			break
		}
		if info.Ver >= reqVerSeq {
			return info.Ver, true
		}
	}

	log.LogErrorf("action[getLastestVer] inode %v reqVerSeq %v not found, the largetst one %v",
		i.Inode, reqVerSeq, verlist[len(verlist)-1].Ver)
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
		lenV := len(i.multiVersions)
		if i.multiVersions[lenV-1].verSeq == 0 {
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

func (ino *Inode) getInoByVer(verSeq uint64, equal bool) (i *Inode, idx int) {
	log.LogDebugf("action[getInodeByVer] ino %v verseq %v hist len %v request ino ver %v",
		ino.Inode, ino.verSeq, ino.multiVersions, verSeq)
	if verSeq == 0 || verSeq == ino.verSeq || (verSeq == math.MaxUint64 && ino.verSeq == 0) {
		return ino, 0
	}
	if verSeq == math.MaxUint64 {
		listLen := len(ino.multiVersions)
		if listLen == 0 {
			return
		}
		i = ino.multiVersions[listLen-1]
		if i.verSeq != 0 {
			return nil, 0
		}
		return
	}
	if verSeq > 0 && ino.verSeq > verSeq {
		for id, iTmp := range ino.multiVersions {
			if verSeq == iTmp.verSeq {
				return iTmp, id
			} else if verSeq > iTmp.verSeq {
				if !equal {
					return iTmp, id
				}
				return
			}

		}
	} else {
		if !equal {
			return ino, 0
		}
	}
	return
}

// only happened while data need migrate form high layer to lower one
// move data to lower layer according to verlist(current layer), delete all if lower one exceed inode scope
func (i *Inode) getAndDelVer(dVer uint64, mpVer uint64, verlist []*proto.VolVersionInfo) (delExtents []proto.ExtentKey, ino *Inode) {
	var err error

	log.LogDebugf("action[getAndDelVer] ino %v verSeq %v request del ver %v hist len %v isTmpFile %v",
		i.Inode, i.verSeq, dVer, len(i.multiVersions), i.IsTempFile())

	// first layer need delete
	if dVer == 0 {
		if delExtents, err = i.RestoreMultiSnapExts(i.Extents.eks, mpVer, 0); err != nil {
			log.LogErrorf("action[getAndDelVer] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
			return
		}
		log.LogDebugf("action[getAndDelVer] ino %v verSeq %v get del exts %v", i.Inode, i.verSeq, delExtents)
		return delExtents, i
	}

	verLen := len(i.multiVersions)
	if verLen == 0 {
		log.LogDebugf("action[getAndDelVer] ino %v RestoreMultiSnapExts no left", i.Inode)
		return
	}
	// delete snapshot version
	if dVer == math.MaxUint64 {
		inode := i.multiVersions[verLen-1]
		if inode.verSeq != 0 {
			return
		}
		i.multiVersions = i.multiVersions[:verLen-1]
		return inode.Extents.eks, inode
	}

	for id, mIno := range i.multiVersions {
		log.LogDebugf("action[getAndDelVer] ino %v multiVersions level %v verseq %v", i.Inode, id, mIno.verSeq)
		if mIno.verSeq < dVer {
			log.LogDebugf("action[getAndDelVer] ino %v multiVersions level %v verseq %v", i.Inode, id, mIno.verSeq)
			return
		}
		if mIno.verSeq == dVer {
			if id == len(i.multiVersions)-1 { // last layer then need delete all and unlink inode
				i.multiVersions = i.multiVersions[:id]
				return mIno.Extents.eks, mIno
			}

			// get next version should according to verList not self managed with hole
			var vId int
			if vId, err = i.getNextOlderVer(dVer, verlist); id == -1 || err != nil {
				log.LogErrorf("action[getAndDelVer] get next version failed, err %v", err)
				return
			}

			// next layer not exist. update curr layer to next layer and filter out ek with verSeq
			// change id layer verSeq to neighbor layer info, omit version delete process
			if verlist[vId].Ver != i.multiVersions[id+1].verSeq {
				i.multiVersions[id].verSeq = verlist[vId].Ver
				return i.MultiLayerClearExtByVer(id+1, dVer), i.multiVersions[id]
			} else {
				// next layer exist. the deleted version and  next version are neighbor in verlist, thus need restore and delete
				if delExtents, err = i.RestoreMultiSnapExts(i.Extents.eks, dVer, id+1); err != nil {
					log.LogErrorf("action[getAndDelVer] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
					return
				}
				// delete layer id
				i.multiVersions = append(i.multiVersions[:id], i.multiVersions[id+1:]...)
			}

			log.LogDebugf("action[getAndDelVer] ino %v verSeq %v get del exts %v", i.Inode, i.verSeq, delExtents)
			return delExtents, mIno
		}
	}
	return
}

func (i *Inode) getNextOlderVer(ver uint64, verlist []*proto.VolVersionInfo) (id int, err error) {
	for idx, info := range verlist {
		if info.Ver == ver {
			if idx == len(verlist)-1 {
				return -1, nil
			}
			return idx + 1, nil
		}
	}
	return -1, fmt.Errorf("not found")
}

func (i *Inode) CreateUnlinkVer(ver uint64, verlist []*proto.VolVersionInfo) {
	//inode copy not include multi ver array
	ino := i.Copy().(*Inode)
	i.Extents = NewSortedExtents()
	i.ObjExtents = NewSortedObjExtents()
	i.multiVersions = nil
	i.SetDeleteMark()

	log.LogDebugf("action[CreateVer] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ver, i.verSeq, len(i.multiVersions))

	i.Lock()
	i.multiVersions = append([]*Inode{ino}, i.multiVersions...)
	i.verSeq = ver
	i.Unlock()

	//i.IncNLink()
}

func (i *Inode) CreateVer(ver uint64) {
	//inode copy not include multi ver array
	ino := i.Copy().(*Inode)
	ino.Extents = NewSortedExtents()
	ino.ObjExtents = NewSortedObjExtents()
	ino.multiVersions = nil

	log.LogDebugf("action[CreateVer] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ver, i.verSeq, len(i.multiVersions))

	i.Lock()
	i.multiVersions = append([]*Inode{ino}, i.multiVersions...)
	i.verSeq = ver
	i.Unlock()

	// i.IncNLink()
}

func (i *Inode) SplitExtentWithCheck(gVer uint64, ver uint64, ek proto.ExtentKey) (delExtents []proto.ExtentKey, status uint8) {
	var err error
	ek.VerSeq = ver
	log.LogDebugf("action[SplitExtentWithCheck] inode %v,ver %v,ek %v,hist len %v", i.Inode, ver, ek, len(i.multiVersions))
	if ver != i.verSeq {
		log.LogDebugf("action[SplitExtentWithCheck] CreateVer ver %v", ver)
		i.CreateVer(ver)
	}

	i.Lock()
	defer i.Unlock()
	delExtents, status = i.Extents.SplitWithCheck(ek)
	if status != proto.OpOk {
		log.LogErrorf("action[SplitExtentWithCheck] status %v", status)
		return
	}

	if delExtents, err = i.RestoreMultiSnapExts(delExtents, gVer, 0); err != nil {
		log.LogErrorf("action[fsmAppendExtentWithCheck] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
		return
	}

	return
}

func (i *Inode) AppendExtentWithCheck(gVer uint64, iVer uint64, ek proto.ExtentKey, ct int64, discardExtents []proto.ExtentKey, volType int) (delExtents []proto.ExtentKey, status uint8) {
	ek.VerSeq = iVer
	log.LogDebugf("action[AppendExtentWithCheck] inode %v,ver %v,ek %v,hist len %v", i.Inode, iVer, ek, len(i.multiVersions))
	if iVer != i.verSeq {
		log.LogDebugf("action[AppendExtentWithCheck] ver %v inode ver %v", iVer, i.verSeq)
		i.CreateVer(iVer)
	}

	i.Lock()
	defer i.Unlock()

	delExtents, status = i.Extents.AppendWithCheck(ek, discardExtents)
	if status != proto.OpOk {
		log.LogErrorf("action[AppendExtentWithCheck] status %v", status)
		return
	}
	for _, ek = range i.Extents.eks {
		log.LogDebugf("action[AppendExtentWithCheck] inode %v extent %v", i.Inode, ek.String())
	}
	// multi version take effect
	if i.verSeq > 0 {
		var err error
		if delExtents, err = i.RestoreMultiSnapExts(delExtents, gVer, 0); err != nil {
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
	ok := proto.IsDir(i.Type) && i.NLink <= 2 && len(i.multiVersions) == 0
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
