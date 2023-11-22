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
	"github.com/cubefs/cubefs/util/errors"
	"io"
	syslog "log"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	DeleteMarkFlag = 1 << 0
	InodeDelTop    = 1 << 1
)

var (
	// InodeV1Flag uint64 = 0x01
	V2EnableColdInodeFlag uint64 = 0x02
	V3EnableSnapInodeFlag uint64 = 0x04
	V4EnableHybridCloud   uint64 = 0x08
	//V4CacheExtentsFlag    uint64 = 0x07
	V4ReplicaExtentsFlag uint64 = 0x10
	V4EBSExtentsFlag     uint64 = 0x20
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

type InodeMultiSnap struct {
	verSeq        uint64 // latest version be create or modified
	multiVersions InodeBatch
	ekRefMap      *sync.Map
}

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
	// Extents    *ExtentsTree
	Extents *SortedExtents

	//ObjExtents *SortedObjExtents
	// Snapshot
	multiSnap *InodeMultiSnap

	//HybridCloud
	StorageClass       uint32
	HybridCouldExtents *SortedHybridCloudExtents
	ForbiddenMigration uint32
	WriteGeneration    uint64
}

func (i *Inode) GetMultiVerString() string {
	if i.multiSnap == nil {
		return "nil"
	}

	return fmt.Sprintf("%v", i.multiSnap.multiVersions)
}

func (i *Inode) RangeMultiVer(visitor func(idx int, info *Inode) bool) {
	if i.multiSnap == nil {
		return
	}
	for k, v := range i.multiSnap.multiVersions {
		if !visitor(k, v) {
			break
		}
	}
}

func isInitSnapVer(seq uint64) bool {
	return seq == math.MaxUint64
}

func NewMultiSnap(seq uint64) *InodeMultiSnap {
	return &InodeMultiSnap{
		verSeq: seq,
	}
}

func (i *Inode) verUpdate(seq uint64) {
	if seq == 0 && i.multiSnap == nil {
		return
	}
	if i.multiSnap == nil {
		i.multiSnap = NewMultiSnap(seq)
	} else {
		i.multiSnap.verSeq = seq
	}
}

func (i *Inode) setVerNoCheck(seq uint64) {
	i.verUpdate(seq)
}

func (i *Inode) setVer(seq uint64) {
	if i.getVer() > seq {
		syslog.Println(fmt.Sprintf("inode %v old seq %v cann't use seq %v", i.getVer(), seq, string(debug.Stack())))
		log.LogFatalf("inode %v old seq %v cann't use seq %v stack %v", i.Inode, i.getVer(), seq, string(debug.Stack()))
	}
	i.verUpdate(seq)
}

func (i *Inode) insertEkRefMap(mpId uint64, ek *proto.ExtentKey) {
	if i.multiSnap == nil {
		i.multiSnap = NewMultiSnap(i.getVer())
	}
	if i.multiSnap.ekRefMap == nil {
		i.multiSnap.ekRefMap = new(sync.Map)
	}
	storeEkSplit(mpId, i.Inode, i.multiSnap.ekRefMap, ek)
}

func (i *Inode) getEkRefMap() *sync.Map {
	if i.multiSnap == nil {
		return nil
	}
	return i.multiSnap.ekRefMap
}

func (i *Inode) getVer() uint64 {
	if i.multiSnap == nil {
		return 0
	}
	return i.multiSnap.verSeq
}

func (i *Inode) getLayerLen() int {
	if i.multiSnap == nil {
		return 0
	}
	return len(i.multiSnap.multiVersions)
}

func (i *Inode) getLayerVer(layer int) uint64 {
	if i.multiSnap == nil {
		log.LogErrorf("getLayerVer. inode %v multi snap nil", i.Inode)
		return 0
	}

	if layer > i.getLayerLen()-1 {
		log.LogErrorf("getLayerVer. inode %v layer %v not exist, len %v", i.Inode, layer, i.getLayerLen())
		return 0
	}
	if i.multiSnap.multiVersions[layer] == nil {
		log.LogErrorf("getLayerVer. inode %v layer %v nil", i.Inode, layer)
		return 0
	}
	return i.multiSnap.multiVersions[layer].getVer()
}

func (i *Inode) isEmptyVerList() bool {
	return i.getLayerLen() == 0
}

func (i *Inode) isTailIndexInList(id int) bool {
	return id == i.getLayerLen()-1
}

func (i *Inode) getTailVerInList() (verSeq uint64, found bool) {
	mLen := i.getLayerLen()
	if mLen > 0 {
		return i.getLayerVer(mLen - 1), true
	}
	return 0, false
}

// freelist clean inode get all exist extents info, deal special case for split key
func (inode *Inode) GetAllExtsOfflineInode(mpID uint64, isCache bool) (extInfo map[uint64][]*proto.ExtentKey) {
	extInfo = make(map[uint64][]*proto.ExtentKey)

	if inode.getLayerLen() > 0 {
		log.LogWarnf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] verlist len %v should not drop",
			mpID, inode.Inode, inode.getLayerLen())
	}

	for i := 0; i < inode.getLayerLen()+1; i++ {
		dIno := inode
		if i > 0 {
			dIno = inode.multiSnap.multiVersions[i-1]
		}
		log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] dIno %v", mpID, inode.Inode, dIno)
		var extents = NewSortedExtents()
		if isCache {
			extents = dIno.Extents
		} else {
			if dIno.HybridCouldExtents.sortedEks != nil {
				extents = dIno.HybridCouldExtents.sortedEks.(*SortedExtents)
			}
		}
		extents.Range(func(ek proto.ExtentKey) bool {
			ext := &ek
			if ext.IsSplit() {
				var (
					dOK  bool
					last bool
				)
				log.LogDebugf("deleteMarkedInodes DecSplitEk mpID %v inode [%v]", mpID, inode.Inode)
				if dOK, last = dIno.DecSplitEk(mpID, ext); !dOK {
					return false
				}
				if !last {
					log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] ek %v be removed", mpID, inode.Inode, ext)
					return true
				}

				log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp %v inode [%v] ek %v be removed", mpID, inode.Inode, ext)
				ext.SetSplit(false)
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

func NewTxInode(ino uint64, t uint32, txInfo *proto.TransactionInfo) *TxInode {
	ti := &TxInode{
		Inode:  NewInode(ino, t),
		TxInfo: txInfo,
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

	ino.RangeMultiVer(func(idx int, info *Inode) bool {
		rspInodeInfo := &proto.InodeInfo{}
		replyInfoNoCheck(rspInodeInfo, info)
		rsp = append(rsp, *rspInodeInfo)
		return true
	})
	return
}

func (ino *Inode) getAllLayerEks() (rsp []proto.LayerInfo) {
	ino.RLock()
	defer ino.RUnlock()
	rspInodeInfo := &proto.InodeInfo{}
	replyInfoNoCheck(rspInodeInfo, ino)

	layerInfo := proto.LayerInfo{
		LayerIdx: 0,
		Info:     rspInodeInfo,
		Eks:      ino.Extents.eks,
	}
	rsp = append(rsp, layerInfo)
	//TODO:support hybrid-cloud
	ino.RangeMultiVer(func(idx int, info *Inode) bool {
		rspInodeInfo := &proto.InodeInfo{}
		replyInfo(rspInodeInfo, info, nil)
		layerInfo := proto.LayerInfo{
			LayerIdx: uint32(idx + 1),
			Info:     rspInodeInfo,
			Eks:      info.Extents.eks,
		}
		rsp = append(rsp, layerInfo)
		return true
	})

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
	buff.WriteString(fmt.Sprintf("CacheExtents[%s]", i.Extents))
	//buff.WriteString(fmt.Sprintf("ObjExtents[%s]", i.ObjExtents))
	buff.WriteString(fmt.Sprintf("verSeq[%v]", i.getVer()))
	buff.WriteString(fmt.Sprintf("multiSnap.multiVersions.len[%v]", i.getLayerLen()))
	buff.WriteString(fmt.Sprintf("StorageClass[%v]", i.StorageClass))
	if i.HybridCouldExtents.sortedEks != nil {
		if i.storeInReplicaSystem() {
			buff.WriteString(fmt.Sprintf("Extents[%s]", i.HybridCouldExtents.sortedEks.(*SortedExtents)))
		} else {
			buff.WriteString(fmt.Sprintf("Extents[%s]", i.HybridCouldExtents.sortedEks.(*SortedObjExtents)))
		}
	}
	buff.WriteString("}")
	return buff.String()
}

// NewInode returns a new Inode instance with specified Inode ID, name and type.
// The AccessTime and ModifyTime will be set to the current time.
func NewInode(ino uint64, t uint32) *Inode {
	ts := timeutil.GetCurrentTimeUnix()
	i := &Inode{
		Inode:      ino,
		Type:       t,
		Generation: 1,
		CreateTime: ts,
		AccessTime: ts,
		ModifyTime: ts,
		NLink:      1,
		Extents:    NewSortedExtents(),
		//ObjExtents:         NewSortedObjExtents(),
		multiSnap:          nil,
		StorageClass:       proto.StorageClass_Unspecified,
		HybridCouldExtents: NewSortedHybridCloudExtents(),
		ForbiddenMigration: ApproverToMigration,
		WriteGeneration:    0,
		//		HybridCouldExtentsTemp: newSortedHybridCloudExtentsTemp(),
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
	newIno.StorageClass = i.StorageClass
	newIno.Extents = i.Extents.Clone()
	//newIno.ObjExtents = i.ObjExtents.Clone()
	if i.multiSnap != nil {
		newIno.multiSnap = &InodeMultiSnap{
			verSeq:        i.getVer(),
			multiVersions: i.multiSnap.multiVersions.Clone(),
			ekRefMap:      i.multiSnap.ekRefMap,
		}
	}
	if i.HybridCouldExtents.sortedEks != nil {
		if i.storeInReplicaSystem() {
			newIno.HybridCouldExtents.sortedEks = i.HybridCouldExtents.sortedEks.(*SortedExtents).Clone()
		} else {
			newIno.HybridCouldExtents.sortedEks = i.HybridCouldExtents.sortedEks.(*SortedObjExtents).Clone()
		}
	}
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
	newIno.StorageClass = i.StorageClass
	newIno.Extents = i.Extents.Clone()
	//newIno.ObjExtents = i.ObjExtents.Clone()
	if i.HybridCouldExtents.sortedEks != nil {
		if i.storeInReplicaSystem() {
			newIno.HybridCouldExtents.sortedEks = i.HybridCouldExtents.sortedEks.(*SortedExtents).Clone()
		} else {
			newIno.HybridCouldExtents.sortedEks = i.HybridCouldExtents.sortedEks.(*SortedObjExtents).Clone()
		}
	}
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

	//if i.ObjExtents != nil && len(i.ObjExtents.eks) > 0 {
	//	i.Reserved |= V2EnableColdInodeFlag
	//}
	i.Reserved |= V3EnableSnapInodeFlag
	i.Reserved |= V4EnableHybridCloud
	//to check flag
	if i.StorageClass == proto.StorageClass_BlobStore {
		if i.HybridCouldExtents.sortedEks != nil {
			ObjExtents := i.HybridCouldExtents.sortedEks.(*SortedObjExtents)
			if ObjExtents != nil && len(ObjExtents.eks) > 0 {
				i.Reserved |= V4EBSExtentsFlag
			}
		}
	} else if i.storeInReplicaSystem() {
		if i.HybridCouldExtents.sortedEks != nil {
			replicaExtents := i.HybridCouldExtents.sortedEks.(*SortedExtents)
			if replicaExtents != nil && len(replicaExtents.eks) > 0 {
				i.Reserved |= V4ReplicaExtentsFlag
			}
		}
	} else {
		panic(errors.New(fmt.Sprintf("MarshalInodeValue failed, unsupport StorageClass %v", i.StorageClass)))
	}
	//log.LogInfof("action[MarshalInodeValue] inode %v Reserved %v", i.Inode, i.Reserved)
	if err = binary.Write(buff, binary.BigEndian, &i.Reserved); err != nil {
		panic(err)
	}
	// marshal StorageClass
	if err = binary.Write(buff, binary.BigEndian, &i.StorageClass); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ForbiddenMigration); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.WriteGeneration); err != nil {
		panic(err)
	}
	// marshal cache ExtentsKey
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

	if i.Reserved&V4EBSExtentsFlag > 0 {
		// marshal ObjExtentsKey
		ObjExtents := i.HybridCouldExtents.sortedEks.(*SortedObjExtents)
		objExtData, err := ObjExtents.MarshalBinary()
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

	if i.Reserved&V4ReplicaExtentsFlag > 0 {
		replicaExtents := i.HybridCouldExtents.sortedEks.(*SortedExtents)
		extData, err := replicaExtents.MarshalBinary(true)
		if err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(extData))); err != nil {
			panic(err)
		}
		if _, err = buff.Write(extData); err != nil {
			panic(err)
		}
	}
	//if i.Reserved&V2EnableColdInodeFlag > 0 {
	//	// marshal ObjExtentsKey
	//	objExtData, err := i.ObjExtents.MarshalBinary()
	//	if err != nil {
	//		panic(err)
	//	}
	//	if err = binary.Write(buff, binary.BigEndian, uint32(len(objExtData))); err != nil {
	//		panic(err)
	//	}
	//	if _, err = buff.Write(objExtData); err != nil {
	//		panic(err)
	//	}
	//}

	if err = binary.Write(buff, binary.BigEndian, i.getVer()); err != nil {
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
	i.MarshalInodeValue(buff)
	if i.getLayerLen() > 0 && i.getVer() == 0 {
		log.LogFatalf("action[MarshalValue] inode %v current verseq %v, hist len (%v) stack(%v)", i.Inode, i.getVer(), i.getLayerLen(), string(debug.Stack()))
	}
	if err = binary.Write(buff, binary.BigEndian, int32(i.getLayerLen())); err != nil {
		panic(err)
	}

	if i.multiSnap != nil {
		for _, ino := range i.multiSnap.multiVersions {
			ino.MarshalInodeValue(buff)
		}
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
	//if i.ObjExtents == nil {
	//	i.ObjExtents = NewSortedObjExtents()
	//}
	if i.HybridCouldExtents == nil {
		i.HybridCouldExtents = NewSortedHybridCloudExtents()
	}

	v3 := i.Reserved&V3EnableSnapInodeFlag > 0
	v2 := i.Reserved&V2EnableColdInodeFlag > 0
	v4 := i.Reserved&V4EnableHybridCloud > 0
	//hybridcloud format
	if v4 {
		if err = binary.Read(buff, binary.BigEndian, &i.StorageClass); err != nil {
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &i.ForbiddenMigration); err != nil {
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &i.WriteGeneration); err != nil {
			return
		}
		extSize := uint32(0)
		//unmarshall extents cache
		if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
			return
		}
		fmt.Printf("ino %v extSize %v\n", i.Inode, extSize)
		if extSize > 0 {
			extBytes := make([]byte, extSize)
			if _, err = io.ReadFull(buff, extBytes); err != nil {
				return
			}
			var ekRef *sync.Map
			if err, ekRef = i.Extents.UnmarshalBinary(extBytes, v3); err != nil {
				return
			}
			// log.LogDebugf("Inode %v ekRef %v", i.Inode, ekRef)
			fmt.Printf("ino %v ekRef %v\n", i.Inode, ekRef)
			if ekRef != nil {
				if i.multiSnap == nil {
					i.multiSnap = NewMultiSnap(0)
				}
				// log.LogDebugf("Inode %v ekRef %v", i.Inode, ekRef)
				i.multiSnap.ekRefMap = ekRef
			}
		}
		if i.Reserved&V4ReplicaExtentsFlag > 0 {
			extSize = uint32(0)
			if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
				return
			}
			fmt.Printf("ino %v extSize %v\n", i.Inode, extSize)
			if extSize > 0 {
				extBytes := make([]byte, extSize)
				if _, err = io.ReadFull(buff, extBytes); err != nil {
					return
				}
				var ekRef *sync.Map
				i.HybridCouldExtents.sortedEks = NewSortedExtents()
				if err, ekRef = i.HybridCouldExtents.sortedEks.(*SortedExtents).UnmarshalBinary(extBytes, v3); err != nil {
					return
				}
				// log.LogDebugf("Inode %v ekRef %v", i.Inode, ekRef)
				if ekRef != nil {
					if i.multiSnap == nil {
						i.multiSnap = NewMultiSnap(0)
					}
					// log.LogDebugf("Inode %v ekRef %v", i.Inode, ekRef)
					i.multiSnap.ekRefMap = ekRef
				}
			}
		}

		if i.Reserved&V4EBSExtentsFlag > 0 {
			ObjExtSize := uint32(0)
			if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
				return
			}
			if ObjExtSize > 0 {
				objExtBytes := make([]byte, ObjExtSize)
				if _, err = io.ReadFull(buff, objExtBytes); err != nil {
					return
				}
				ObjExtents := NewSortedObjExtents()
				if err = ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
					return
				}
				i.HybridCouldExtents.sortedEks = ObjExtents
			}
		}
		var seq uint64
		if err = binary.Read(buff, binary.BigEndian, &seq); err != nil {
			return
		}
		if seq != 0 {
			i.setVer(seq)
		}
	} else { //transform old-format inode to v4-format
		if v2 || v3 {
			if v2 {
				i.StorageClass = proto.StorageClass_BlobStore
				extSize := uint32(0)
				if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
					return
				}
				if extSize > 0 {
					extBytes := make([]byte, extSize)
					if _, err = io.ReadFull(buff, extBytes); err != nil {
						return
					}
					if i.multiSnap == nil {
						i.multiSnap = NewMultiSnap(0)
					}
					if err, i.multiSnap.ekRefMap = i.Extents.UnmarshalBinary(extBytes, v3); err != nil {
						return
					}
				}
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
					objExtents := NewSortedObjExtents()
					if err = objExtents.UnmarshalBinary(objExtBytes); err != nil {
						return
					}
					i.HybridCouldExtents.sortedEks = objExtents
				}
			} else {
				i.StorageClass = uint32(defaultMediaType)
				if i.StorageClass == proto.StorageClass_Unspecified {
					return fmt.Errorf("UnmarshalInodeValue: default media type is not specified in config")
				}
				extSize := uint32(0)
				if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
					return
				}
				if extSize > 0 {
					extBytes := make([]byte, extSize)
					if _, err = io.ReadFull(buff, extBytes); err != nil {
						return
					}
					if i.multiSnap == nil {
						i.multiSnap = NewMultiSnap(0)
					}
					extents := NewSortedExtents()
					if err, i.multiSnap.ekRefMap = extents.UnmarshalBinary(extBytes, v3); err != nil {
						return
					}
					i.HybridCouldExtents.sortedEks = extents
				}
			}

			if v3 {
				var seq uint64
				if err = binary.Read(buff, binary.BigEndian, &seq); err != nil {
					return
				}
				if seq != 0 {
					i.setVer(seq)
				}
			}
		} else {
			i.StorageClass = uint32(defaultMediaType)
			if i.StorageClass == proto.StorageClass_Unspecified {
				return fmt.Errorf("UnmarshalInodeValue: default media type is not specified in config")
			}
			extents := NewSortedExtents()
			if err, _ = extents.UnmarshalBinary(buff.Bytes(), false); err != nil {
				return
			}
			i.HybridCouldExtents.sortedEks = extents
			return
		}

	}
	//if v2 || v3 {
	//	extSize := uint32(0)
	//	if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
	//		return
	//	}
	//	if extSize > 0 {
	//		extBytes := make([]byte, extSize)
	//		if _, err = io.ReadFull(buff, extBytes); err != nil {
	//			return
	//		}
	//		if i.multiSnap == nil {
	//			i.multiSnap = NewMultiSnap(0)
	//		}
	//		if err, i.multiSnap.ekRefMap = i.Extents.UnmarshalBinary(extBytes, v3); err != nil {
	//			return
	//		}
	//	}
	//} else {
	//	if err, _ = i.Extents.UnmarshalBinary(buff.Bytes(), false); err != nil {
	//		return
	//	}
	//	return
	//}
	//
	//if v2 {
	//	// unmarshal ObjExtentsKey
	//	ObjExtSize := uint32(0)
	//	if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
	//		return
	//	}
	//	if ObjExtSize > 0 {
	//		objExtBytes := make([]byte, ObjExtSize)
	//		if _, err = io.ReadFull(buff, objExtBytes); err != nil {
	//			return
	//		}
	//		if err = i.ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
	//			return
	//		}
	//	}
	//}
	//
	//if v3 {
	//	var seq uint64
	//	if err = binary.Read(buff, binary.BigEndian, &seq); err != nil {
	//		return
	//	}
	//	if seq != 0 {
	//		i.setVer(seq)
	//	}
	//}
	return
}

func (i *Inode) GetSpaceSize() (extSize uint64) {
	if i.IsTempFile() {
		return
	}
	if i.HybridCouldExtents.sortedEks == nil {
		return
	}
	if i.storeInReplicaSystem() {
		extSize += i.HybridCouldExtents.sortedEks.(*SortedExtents).LayerSize()
	} else {
		extSize += i.HybridCouldExtents.sortedEks.(*SortedObjExtents).LayerSize()
	}
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (i *Inode) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	i.UnmarshalInodeValue(buff)
	if i.Reserved&V3EnableSnapInodeFlag > 0 {
		var verCnt int32
		if err = binary.Read(buff, binary.BigEndian, &verCnt); err != nil {
			log.LogInfof("action[UnmarshalValue] err get ver cnt inode %v new seq %v", i.Inode, i.getVer())
			return
		}
		if verCnt > 0 && i.getVer() == 0 {
			err = fmt.Errorf("inode %v verCnt %v root ver %v", i.Inode, verCnt, i.getVer())
			log.LogFatalf("UnmarshalValue. %v", err)
			return
		}
		for idx := int32(0); idx < verCnt; idx++ {
			ino := &Inode{Inode: i.Inode}
			ino.UnmarshalInodeValue(buff)
			if ino.multiSnap != nil && ino.multiSnap.ekRefMap != nil {
				if i.multiSnap.ekRefMap == nil {
					i.multiSnap.ekRefMap = new(sync.Map)
				}
				// log.LogDebugf("UnmarshalValue. inode %v merge top layer multiSnap.ekRefMap with layer %v", i.Inode, idx)
				proto.MergeSplitKey(i.Inode, i.multiSnap.ekRefMap, ino.multiSnap.ekRefMap)
			}
			if i.multiSnap == nil {
				i.multiSnap = &InodeMultiSnap{}
			}
			// log.LogDebugf("action[UnmarshalValue] inode %v old seq %v hist len %v", ino.Inode, ino.getVer(), i.getLayerLen())
			i.multiSnap.multiVersions = append(i.multiSnap.multiVersions, ino)
		}
	}
	return
}

// AppendExtents append the extent to the btree.
func (i *Inode) AppendExtents(eks []proto.ExtentKey, ct int64, volType int) (delExtents []proto.ExtentKey) {
	//if proto.IsCold(volType) {
	//	return
	//}
	if !i.storeInReplicaSystem() {
		return
	}
	i.Lock()
	defer i.Unlock()
	if i.HybridCouldExtents.sortedEks == nil {
		i.HybridCouldExtents.sortedEks = NewSortedExtents()
	}
	extents := i.HybridCouldExtents.sortedEks.(*SortedExtents)
	for _, ek := range eks {
		delItems := extents.Append(ek)
		size := extents.Size()
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
	if i.HybridCouldExtents.sortedEks == nil {
		i.HybridCouldExtents.sortedEks = NewSortedObjExtents()
	}
	for _, ek := range eks {
		err = i.HybridCouldExtents.sortedEks.(*SortedObjExtents).Append(ek)
		if err != nil {
			return
		}
		size := i.HybridCouldExtents.sortedEks.(*SortedObjExtents).Size()
		if i.Size < size {
			i.Size = size
		}
	}
	i.Generation++
	i.ModifyTime = ct
	return
}

func (i *Inode) PrintAllVersionInfo() {
	if i.multiSnap == nil {
		return
	}
	log.LogInfof("action[PrintAllVersionInfo] inode [%v] verSeq [%v] hist len [%v]", i.Inode, i.getVer(), i.getLayerLen())
	for id, info := range i.multiSnap.multiVersions {
		log.LogInfof("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode [%v]", id, info.getVer(), info)
	}
}

// clear snapshot extkey with releated verSeq
func (i *Inode) MultiLayerClearExtByVer(layer int, dVerSeq uint64) (delExtents []proto.ExtentKey) {
	var ino *Inode
	if layer == 0 {
		ino = i
	} else {
		ino = i.multiSnap.multiVersions[layer-1]
	}

	ino.Extents.Lock()
	defer ino.Extents.Unlock()

	for idx, ek := range ino.Extents.eks {
		if ek.GetSeq() > dVerSeq {
			delExtents = append(delExtents, ek)
			ino.Extents.eks = append(ino.Extents.eks[idx:], ino.Extents.eks[:idx+1]...)
		}
	}
	return
}

func (i *Inode) mergeExtentArr(mpId uint64, extentKeysLeft []proto.ExtentKey, extentKeysRight []proto.ExtentKey) []proto.ExtentKey {
	lCnt := len(extentKeysLeft)
	rCnt := len(extentKeysRight)
	sortMergedExts := make([]proto.ExtentKey, 0, lCnt+rCnt)
	lPos, rPos := 0, 0

	doWork := func(keyArr *[]proto.ExtentKey, pos int) {
		mLen := len(sortMergedExts)
		if mLen > 0 && sortMergedExts[mLen-1].IsSequence(&(*keyArr)[pos]) {
			sortMergedExts[mLen-1].Size += (*keyArr)[pos].Size
			log.LogDebugf("[mergeExtentArr] mpId[%v]. ek left %v right %v", mpId, sortMergedExts[mLen-1], (*keyArr)[pos])
			if !sortMergedExts[mLen-1].IsSplit() || !(*keyArr)[pos].IsSplit() {
				log.LogErrorf("[mergeExtentArr] mpId[%v] ino %v ek merge left %v right %v not all split", mpId, i.Inode, sortMergedExts[mLen-1], (*keyArr)[pos])
			}
			i.DecSplitEk(mpId, &(*keyArr)[pos])
		} else {
			sortMergedExts = append(sortMergedExts, (*keyArr)[pos])
		}
	}

	for {
		if lPos == lCnt {
			sortMergedExts = append(sortMergedExts, extentKeysRight[rPos:]...)
			break
		}
		if rPos == rCnt {
			sortMergedExts = append(sortMergedExts, extentKeysLeft[lPos:]...)
			break
		}

		if extentKeysLeft[lPos].FileOffset < extentKeysRight[rPos].FileOffset {
			doWork(&extentKeysLeft, lPos)
			lPos++
		} else {
			doWork(&extentKeysRight, rPos)
			rPos++
		}
	}

	return sortMergedExts
}

// Restore ext info to older version or deleted if no right version
// The list(multiSnap.multiVersions) contains all point of modification on inode, each ext must belong to one layer.
// Once the layer be deleted is top layer ver be changed to upper layer, or else the ext belongs is exclusive and can be dropped
func (i *Inode) RestoreExts2NextLayer(mpId uint64, delExtentsOrigin []proto.ExtentKey, curVer uint64, idx int) (delExtents []proto.ExtentKey, err error) {
	log.LogInfof("action[RestoreMultiSnapExts] mpId [%v] curVer [%v] delExtents size [%v] hist len [%v]", mpId, curVer, len(delExtentsOrigin), i.getLayerLen())
	// no version left.all old versions be deleted
	if i.isEmptyVerList() {
		log.LogWarnf("action[RestoreMultiSnapExts] mpId [%v] inode [%v] restore have no old version left", mpId, i.Inode)
		return delExtentsOrigin, nil
	}
	lastSeq := i.multiSnap.multiVersions[idx].getVer()
	specSnapExtent := make([]proto.ExtentKey, 0)

	for _, delExt := range delExtentsOrigin {
		// curr deleting delExt with a seq larger than the next version's seq, it doesn't belong to any
		// versions,so try to delete it
		log.LogDebugf("action[RestoreMultiSnapExts] mpId [%v] inode [%v] ext split [%v] with seq[%v] gSeq[%v] try to del.the last seq [%v], ek details[%v]",
			mpId, i.Inode, delExt.IsSplit(), delExt.GetSeq(), curVer, lastSeq, delExt)
		if delExt.GetSeq() > lastSeq {
			delExtents = append(delExtents, delExt)
		} else {
			log.LogInfof("action[RestoreMultiSnapExts] mpId [%v] inode [%v] move to level 1 delExt [%v] specSnapExtent size [%v]", mpId, i.Inode, delExt, len(specSnapExtent))
			specSnapExtent = append(specSnapExtent, delExt)
		}
	}
	if len(specSnapExtent) == 0 {
		log.LogInfof("action[RestoreMultiSnapExts] mpId [%v] inode [%v] no need to move to level 1", mpId, i.Inode)
		return
	}
	if len(specSnapExtent) > 0 && i.isEmptyVerList() {
		err = fmt.Errorf("mpId [%v] inode %v error not found prev snapshot index", mpId, i.Inode)
		log.LogErrorf("action[RestoreMultiSnapExts] mpId [%v] inode [%v] %v", mpId, i.Inode, err)
		return
	}

	i.multiSnap.multiVersions[idx].Extents.Lock()
	i.multiSnap.multiVersions[idx].Extents.eks = i.mergeExtentArr(mpId, i.multiSnap.multiVersions[idx].Extents.eks, specSnapExtent)
	i.multiSnap.multiVersions[idx].Extents.Unlock()

	return
}

func (inode *Inode) unlinkTopLayer(mpId uint64, ino *Inode, mpVer uint64, verlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	// if there's no snapshot itself, nor have snapshot after inode's ver then need unlink directly and make no snapshot
	// just move to upper layer, the behavior looks like that the snapshot be dropped
	log.LogDebugf("action[unlinkTopLayer] mpid [%v] mpVer %v check if have snapshot depends on the deleitng ino %v (with no snapshot itself) found seq %v, verlist %v",
		mpId, mpVer, ino, inode.getVer(), verlist)
	status = proto.OpOk

	delFunc := func() (done bool) {
		if inode.NLink > 1 {
			log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, file link is %v", ino.Inode, inode.NLink)
			inode.DecNLink()
			doMore = false
			return true
		}
		var dIno *Inode
		if ext2Del, dIno = inode.getAndDelVer(mpId, ino.getVer(), mpVer, verlist); dIno == nil {
			status = proto.OpNotExistErr
			log.LogDebugf("action[unlinkTopLayer] mp %v iino %v", mpId, ino)
			return true
		}
		log.LogDebugf("action[unlinkTopLayer] mp %v inode %v be unlinked", mpId, ino.Inode)
		dIno.DecNLink() // dIno should be inode
		doMore = true
		return
	}

	// if topLayer verSeq is as same as mp, the current inode deletion only happen on the first layer
	// or ddelete from client do deletion at top layer which should allow delete ionde with older version contrast to mp version
	// because ddelete have two steps,1 is del dentry,2nd is unlink inode ,version may updated after 1st and before 2nd step, to
	// make sure inode be unlinked by normal deletion, sdk add filed of dentry verSeq to identify and different from other unlink actions
	if mpVer == inode.getVer() || ino.Flag&InodeDelTop > 0 {
		if inode.getLayerLen() == 0 {
			log.LogDebugf("action[unlinkTopLayer] no snapshot available depends on ino %v not found seq %v and return, verlist %v", ino, inode.getVer(), verlist)
			inode.DecNLink()
			log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked", ino.Inode)
			// operate inode directly
			doMore = true
			return
		}

		log.LogDebugf("action[unlinkTopLayer] need restore.ino %v withSeq %v equal mp seq, verlist %v",
			ino, inode.getVer(), verlist)
		// need restore
		if !proto.IsDir(inode.Type) {
			delFunc()
			return
		} else {
			log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, Dir", ino.Inode)
			inode.DecNLink()
			doMore = true
		}
		return
	}

	log.LogDebugf("action[unlinkTopLayer] need create version.ino %v withSeq %v not equal mp seq %v, verlist %v", ino, inode.getVer(), mpVer, verlist)
	if proto.IsDir(inode.Type) { // dir is whole info but inode is partition,which is quit different
		_, err := inode.getNextOlderVer(mpVer, verlist)
		if err == nil {
			log.LogDebugf("action[unlinkTopLayer] inode %v cann't get next older ver %v err %v", inode.Inode, mpVer, err)
			inode.CreateVer(mpVer)
		}
		inode.DecNLink()
		log.LogDebugf("action[unlinkTopLayer] inode %v be unlinked, Dir create ver 1st layer", ino.Inode)
		doMore = true
	} else {
		ver, err := inode.getNextOlderVer(mpVer, verlist)
		if err != nil {
			if err.Error() == "not found" {
				delFunc()
				return
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
	if dIno, idxWithTopLayer = inode.getInoByVer(ino.getVer(), false); dIno == nil {
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
	if inode.multiSnap == nil {
		log.LogWarnf("action[dirUnlinkVerInlist] ino %v multiSnap should not be nil", inode)
		inode.multiSnap = &InodeMultiSnap{}
	}

	mIdx := idxWithTopLayer - 1
	if mIdx == 0 {
		endSeq = inode.getVer()
	} else {
		endSeq = inode.multiSnap.multiVersions[mIdx-1].getVer()
	}

	log.LogDebugf("action[dirUnlinkVerInlist] inode %v try drop multiVersion idx %v effective seq scope [%v,%v) ",
		inode.Inode, mIdx, dIno.getVer(), endSeq)

	doWork := func() bool {
		verlist.RLock()
		defer verlist.RUnlock()

		for vidx, info := range verlist.VerList {
			if info.Ver >= dIno.getVer() && info.Ver < endSeq {
				log.LogDebugf("action[dirUnlinkVerInlist] inode %v dir layer idx %v still have effective snapshot seq %v.so don't drop", inode.Inode, mIdx, info.Ver)
				return false
			}
			if info.Ver >= endSeq || vidx == len(verlist.VerList)-1 {
				log.LogDebugf("action[dirUnlinkVerInlist] inode %v try drop multiVersion idx %v and return", inode.Inode, mIdx)

				inode.Lock()
				inode.multiSnap.multiVersions = append(inode.multiSnap.multiVersions[:mIdx], inode.multiSnap.multiVersions[mIdx+1:]...)
				inode.Unlock()
				return true
			}
			log.LogDebugf("action[dirUnlinkVerInlist] inode %v try drop scope [%v, %v), mp ver %v not suitable", inode.Inode, dIno.getVer(), endSeq, info.Ver)
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

func (inode *Inode) unlinkVerInList(mpId uint64, ino *Inode, mpVer uint64, verlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	log.LogDebugf("action[unlinkVerInList] mpId [%v] ino %v try search seq %v isdir %v", mpId, ino, ino.getVer(), proto.IsDir(inode.Type))
	if proto.IsDir(inode.Type) { // snapshot dir deletion don't take link into consider, but considers the scope of snapshot contrast to verList
		return inode.dirUnlinkVerInlist(ino, mpVer, verlist)
	}
	var dIno *Inode
	status = proto.OpOk
	// special case, snapshot is the last one and be depended by upper version,update it's version to the right one
	// ascend search util to the curr unCommit version in the verList
	if ino.getVer() == inode.getVer() || (isInitSnapVer(ino.getVer()) && inode.getVer() == 0) {
		if len(verlist.VerList) == 0 {
			status = proto.OpNotExistErr
			log.LogErrorf("action[unlinkVerInList] inode %v verlist should be larger than 0, return not found", inode.Inode)
			return
		}

		// just move to upper layer,the request snapshot be dropped
		nVerSeq, found := inode.getLastestVer(inode.getVer(), false, verlist)
		if !found {
			status = proto.OpNotExistErr
			return
		}
		log.LogDebugf("action[unlinkVerInList] inode %v update current verSeq %v to %v", inode.Inode, inode.getVer(), nVerSeq)
		inode.setVer(nVerSeq)
		return
	} else {
		// don't unlink if no version satisfied
		if ext2Del, dIno = inode.getAndDelVer(mpId, ino.getVer(), mpVer, verlist); dIno == nil {
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

func (i *Inode) ShouldDelVer(delVer uint64, mpVer uint64) (ok bool, err error) {
	if i.getVer() == 0 {
		if delVer > 0 {
			if isInitSnapVer(delVer) {
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
		if delVer > i.getVer() {
			return false, fmt.Errorf("not found")
		} else if delVer == i.getVer() {
			return true, nil
		}
	}

	if isInitSnapVer(delVer) {
		tailVer, _ := i.getTailVerInList()
		if tailVer == 0 {
			return true, nil
		}
		return false, fmt.Errorf("not found")
	}
	if i.multiSnap == nil {
		return false, fmt.Errorf("not found")
	}
	for _, inoVer := range i.multiSnap.multiVersions {
		if inoVer.getVer() == delVer {
			return true, nil
		}
		if inoVer.getVer() < delVer {
			break
		}
	}
	return false, fmt.Errorf("not found")
}

// idx need calc include nclude top layer. index in multiSnap.multiVersions need add by 1
//
//note:search all layers.
func (ino *Inode) getInoByVer(verSeq uint64, equal bool) (i *Inode, idx int) {
	ino.RLock()
	defer ino.RUnlock()

	if verSeq == 0 || verSeq == ino.getVer() || (isInitSnapVer(verSeq) && ino.getVer() == 0) {
		return ino, 0
	}
	if isInitSnapVer(verSeq) {
		listLen := ino.getLayerLen()
		if listLen == 0 {
			log.LogDebugf("action[getInoByVer]  ino %v no multiversion", ino.Inode)
			return
		}
		i = ino.multiSnap.multiVersions[listLen-1]
		if i.getVer() != 0 {
			log.LogDebugf("action[getInoByVer]  ino %v lay seq %v", ino.Inode, i.getVer())
			return nil, 0
		}
		return i, listLen
	}
	if verSeq > 0 && ino.getVer() > verSeq {
		if ino.multiSnap != nil {
			for id, iTmp := range ino.multiSnap.multiVersions {
				if verSeq == iTmp.getVer() {
					log.LogDebugf("action[getInoByVer]  ino %v get in multiversion id %v", ino.Inode, id)
					return iTmp, id + 1
				} else if verSeq > iTmp.getVer() {
					if !equal {
						log.LogDebugf("action[getInoByVer]  ino %v get in multiversion id %v, %v, %v", ino.Inode, id, verSeq, iTmp.getVer())
						return iTmp, id + 1
					}
					log.LogDebugf("action[getInoByVer]  ino %v get in multiversion id %v", ino.Inode, id)
					return
				}
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

func (i *Inode) getAndDelVer(mpId uint64, dVer uint64, mpVer uint64, verlist *proto.VolVersionInfoList) (delExtents []proto.ExtentKey, ino *Inode) {
	var err error
	verlist.RLock()
	defer verlist.RUnlock()

	log.LogDebugf("action[getAndDelVer] ino %v verSeq %v request del ver %v hist len %v isTmpFile %v",
		i.Inode, i.getVer(), dVer, i.getLayerLen(), i.IsTempFile())

	// first layer need delete
	if dVer == 0 {
		if delExtents, err = i.RestoreExts2NextLayer(mpId, i.Extents.eks, mpVer, 0); err != nil {
			log.LogErrorf("action[getAndDelVer] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
			return
		}
		i.Extents.eks = i.Extents.eks[:0]
		log.LogDebugf("action[getAndDelVer] ino %v verSeq %v get del exts %v", i.Inode, i.getVer(), delExtents)
		return delExtents, i
	}
	// read inode element is fine, lock is need while write
	inoVerLen := i.getLayerLen()
	if inoVerLen == 0 {
		log.LogDebugf("action[getAndDelVer] ino %v RestoreMultiSnapExts no left", i.Inode)
		return
	}

	tailVer, _ := i.getTailVerInList()
	// delete snapshot version
	if isInitSnapVer(dVer) || dVer == tailVer {
		if isInitSnapVer(dVer) {
			dVer = 0
		}
		inode := i.multiSnap.multiVersions[inoVerLen-1]
		if inode.getVer() != dVer {
			log.LogDebugf("action[getAndDelVer] ino %v idx %v is %v and cann't be dropped tail  ver %v",
				i.Inode, inoVerLen-1, inode.getVer(), tailVer)
			return
		}
		i.Lock()
		defer i.Unlock()
		i.multiSnap.multiVersions = i.multiSnap.multiVersions[:inoVerLen-1]

		log.LogDebugf("action[getAndDelVer] ino %v idx %v be dropped", i.Inode, inoVerLen)
		return inode.Extents.eks, inode
	}

	for id, mIno := range i.multiSnap.multiVersions {
		log.LogDebugf("action[getAndDelVer] ino %v multiSnap.multiVersions level %v verseq %v", i.Inode, id, mIno.getVer())
		if mIno.getVer() < dVer {
			log.LogDebugf("action[getAndDelVer] ino %v multiSnap.multiVersions level %v verseq %v", i.Inode, id, mIno.getVer())
			return
		}

		if mIno.getVer() == dVer {
			// 1.
			if i.isTailIndexInList(id) { // last layer then need delete all and unlink inode
				i.multiSnap.multiVersions = i.multiSnap.multiVersions[:id]
				return mIno.Extents.eks, mIno
			}
			log.LogDebugf("action[getAndDelVer] ino %v ver %v step 3", i.Inode, mIno.getVer())
			// 2. get next version should according to verList but not only self multi list
			var nVerSeq uint64
			if nVerSeq, err = i.getNextOlderVer(dVer, verlist); id == -1 || err != nil {
				log.LogDebugf("action[getAndDelVer] get next version failed, err %v", err)
				return
			}

			log.LogDebugf("action[getAndDelVer] ino %v ver %v nextVerSeq %v step 3 ver ", i.Inode, mIno.getVer(), nVerSeq)
			// 2. system next layer not exist in inode ver list. update curr layer to next layer and filter out ek with verSeq
			// change id layer verSeq to neighbor layer info, omit version delete process

			if i.isTailIndexInList(id) || nVerSeq != i.multiSnap.multiVersions[id+1].getVer() {
				log.LogDebugf("action[getAndDelVer] ino %v  get next version in verList update ver from %v to %v.And delete exts with ver %v",
					i.Inode, i.multiSnap.multiVersions[id].getVer(), nVerSeq, dVer)

				i.multiSnap.multiVersions[id].setVerNoCheck(nVerSeq)
				delExtents, ino = i.MultiLayerClearExtByVer(id+1, nVerSeq), i.multiSnap.multiVersions[id]
				if len(i.multiSnap.multiVersions[id].Extents.eks) != 0 {
					log.LogDebugf("action[getAndDelVer] ino %v   after clear self still have ext and left", i.Inode)
					return
				}
			} else {
				log.LogDebugf("action[getAndDelVer] ino %v ver %v nextVer %v step 3 ver ", i.Inode, mIno.getVer(), nVerSeq)
				// 3. next layer exist. the deleted version and  next version are neighbor in verlist, thus need restore and delete
				if delExtents, err = i.RestoreExts2NextLayer(mpId, mIno.Extents.eks, dVer, id+1); err != nil {
					log.LogDebugf("action[getAndDelVer] ino %v RestoreMultiSnapExts split error %v", i.Inode, err)
					return
				}
			}
			// delete layer id
			i.multiSnap.multiVersions = append(i.multiSnap.multiVersions[:id], i.multiSnap.multiVersions[id+1:]...)

			log.LogDebugf("action[getAndDelVer] ino %v verSeq %v get del exts %v", i.Inode, i.getVer(), delExtents)
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
	ino := i.CopyDirectly().(*Inode)
	ino.setVer(nVer)

	i.Extents = NewSortedExtents()
	//i.ObjExtents = NewSortedObjExtents()
	i.HybridCouldExtents = NewSortedHybridCloudExtents()
	i.SetDeleteMark()

	log.LogDebugf("action[CreateUnlinkVer] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, mpVer, i.getVer(), i.getLayerLen())

	i.Lock()
	if i.multiSnap == nil {
		i.multiSnap = &InodeMultiSnap{}
	}
	if i.getLayerVer(0) == nVer {
		i.multiSnap.multiVersions[0] = ino
	} else {
		i.multiSnap.multiVersions = append([]*Inode{ino}, i.multiSnap.multiVersions...)
	}

	i.setVer(mpVer)
	i.Unlock()
}

func (i *Inode) CreateVer(ver uint64) {
	//inode copy not include multi ver array
	ino := i.CopyDirectly().(*Inode)
	ino.Extents = NewSortedExtents()
	//ino.ObjExtents = NewSortedObjExtents()
	ino.HybridCouldExtents = NewSortedHybridCloudExtents()
	ino.setVer(i.getVer())
	i.setVer(ver)

	i.Lock()
	defer i.Unlock()
	log.LogDebugf("action[CreateVer] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ver, i.getVer(), i.getLayerLen())

	if i.multiSnap == nil {
		i.multiSnap = &InodeMultiSnap{}
	}
	i.multiSnap.multiVersions = append([]*Inode{ino}, i.multiSnap.multiVersions...)
}
func (i *Inode) buildMultiSnap() {
	if i.multiSnap == nil {
		i.multiSnap = &InodeMultiSnap{}
	}
	if i.multiSnap.ekRefMap == nil {
		i.multiSnap.ekRefMap = new(sync.Map)
	}
}

func (i *Inode) SplitExtentWithCheck(param *AppendExtParam) (delExtents []proto.ExtentKey, status uint8) {

	var err error
	param.ek.SetSeq(param.mpVer)
	log.LogDebugf("action[SplitExtentWithCheck] mpId[%v].inode %v,ek %v,hist len %v", param.mpId, i.Inode, param.ek, i.getLayerLen())

	if param.mpVer != i.getVer() {
		log.LogDebugf("action[SplitExtentWithCheck] mpId[%v].CreateVer ver %v", param.mpId, param.mpVer)
		i.CreateVer(param.mpVer)
	}
	i.Lock()
	defer i.Unlock()
	i.buildMultiSnap()
	var extents = NewSortedExtents()
	if param.isCache {
		extents = i.Extents
	} else {
		if i.HybridCouldExtents.sortedEks != nil {
			extents = i.HybridCouldExtents.sortedEks.(*SortedExtents)
		}
	}

	delExtents, status = extents.SplitWithCheck(param.mpId, i.Inode, param.ek, i.multiSnap.ekRefMap)
	if status != proto.OpOk {
		log.LogErrorf("action[SplitExtentWithCheck] mpId[%v].status %v", param.mpId, status)
		return
	}
	if len(delExtents) == 0 {
		return
	}

	if err = i.CreateLowerVersion(i.getVer(), param.multiVersionList); err != nil {
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(param.mpId, delExtents, param.mpVer, 0); err != nil {
		log.LogErrorf("action[fsmAppendExtentWithCheck] mpId[%v].ino %v RestoreMultiSnapExts split error %v", param.mpId, i.Inode, err)
		return
	}
	if proto.IsHot(param.volType) {
		i.Generation++
		i.ModifyTime = param.ct
	}

	return
}

// try to create version between curVer and seq of multiSnap.multiVersions[0] in verList
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
	if nextVer <= i.getLayerVer(0) {
		log.LogDebugf("CreateLowerVersion nextver %v layer 0 ver %v", nextVer, i.getLayerVer(0))
		return
	}

	ino := i.CopyDirectly().(*Inode)
	ino.Extents = NewSortedExtents()
	//ino.ObjExtents = NewSortedObjExtents()
	ino.HybridCouldExtents = NewSortedHybridCloudExtents()
	ino.setVer(nextVer)

	log.LogDebugf("action[CreateLowerVersion] inode %v create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ino, i.getVer(), i.getLayerLen())
	if i.multiSnap == nil {
		i.multiSnap = &InodeMultiSnap{}
	}
	i.multiSnap.multiVersions = append([]*Inode{ino}, i.multiSnap.multiVersions...)

	return
}

type AppendExtParam struct {
	mpId             uint64
	mpVer            uint64
	multiVersionList *proto.VolVersionInfoList
	ek               proto.ExtentKey
	ct               int64
	discardExtents   []proto.ExtentKey
	volType          int
	isCache          bool
}

func (i *Inode) AppendExtentWithCheck(param *AppendExtParam) (delExtents []proto.ExtentKey, status uint8) {
	param.ek.SetSeq(param.mpVer)
	log.LogDebugf("action[AppendExtentWithCheck] mpId[%v].mpVer %v inode %v and fsm ver %v,ek %v,hist len %v",
		param.mpId, param.mpVer, i.Inode, i.getVer(), param.ek, i.getLayerLen())

	if param.mpVer != i.getVer() {
		log.LogDebugf("action[AppendExtentWithCheck] mpId[%v].inode ver %v", param.mpId, i.getVer())
		i.CreateVer(param.mpVer)
	}

	i.Lock()
	defer i.Unlock()
	var extents *SortedExtents
	if param.isCache {
		extents = i.Extents
	} else {
		if i.HybridCouldExtents.sortedEks == nil {
			i.HybridCouldExtents.sortedEks = NewSortedExtents()
		}
		extents = i.HybridCouldExtents.sortedEks.(*SortedExtents)
	}

	refFunc := func(key *proto.ExtentKey) { i.insertEkRefMap(param.mpId, key) }
	delExtents, status = extents.AppendWithCheck(i.Inode, param.ek, refFunc, param.discardExtents)
	if status != proto.OpOk {
		log.LogErrorf("action[AppendExtentWithCheck] mpId[%v].status %v", param.mpId, status)
		return
	}
	//TODO:support hybridcloud
	// multi version take effect
	if i.getVer() > 0 && len(delExtents) > 0 {
		var err error
		if err = i.CreateLowerVersion(i.getVer(), param.multiVersionList); err != nil {
			return
		}
		if delExtents, err = i.RestoreExts2NextLayer(param.mpId, delExtents, param.mpVer, 0); err != nil {
			log.LogErrorf("action[AppendExtentWithCheck] mpId[%v].RestoreMultiSnapExts err %v", param.mpId, err)
			return nil, proto.OpErr
		}
	}

	if i.storeInReplicaSystem() {
		size := extents.Size()
		if i.Size < size {
			i.Size = size
		}
		i.Generation++
		i.ModifyTime = param.ct
	}
	//if proto.IsHot(param.volType) {
	//	size := i.Extents.Size()
	//	if i.Size < size {
	//		i.Size = size
	//	}
	//	i.Generation++
	//	i.ModifyTime = param.ct
	//}
	return
}

func (i *Inode) ExtentsTruncate(length uint64, ct int64, doOnLastKey func(*proto.ExtentKey), insertRefMap func(ek *proto.ExtentKey)) (delExtents []proto.ExtentKey) {
	if i.HybridCouldExtents.sortedEks != nil {
		extents := i.HybridCouldExtents.sortedEks.(*SortedExtents)
		delExtents = extents.Truncate(length, doOnLastKey, insertRefMap)
		i.Size = length
		i.ModifyTime = ct
		i.Generation++
	}
	return
}

// IncNLink increases the nLink value by one.
func (i *Inode) IncNLink(verSeq uint64) {
	if i.getVer() < verSeq {
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
	if i.getVer() < verSeq {
		i.CreateVer(verSeq)
	}
	i.DecNLink()
}

func (i *Inode) DecSplitExts(mpId uint64, delExtents interface{}) {
	log.LogDebugf("[DecSplitExts] mpId [%v] inode %v", mpId, i.Inode)
	cnt := len(delExtents.([]proto.ExtentKey))
	for id := 0; id < cnt; id++ {
		ek := &delExtents.([]proto.ExtentKey)[id]
		if !ek.IsSplit() {
			log.LogDebugf("[DecSplitExts] mpId [%v]  ek not split %v", mpId, ek)
			continue
		}
		if i.multiSnap == nil || i.multiSnap.ekRefMap == nil {
			log.LogErrorf("[DecSplitExts] mpid [%v]. inode [%v] multiSnap.ekRefMap is nil", mpId, i.Inode)
			return
		}

		ok, last := i.DecSplitEk(mpId, ek)
		if !ok {
			log.LogErrorf("[DecSplitExts] mpid [%v]. ek %v not found!", mpId, ek)
			continue
		}
		if last {
			log.LogDebugf("[DecSplitExts] mpid [%v] ek %v split flag be unset to remove all content", mpId, ek)
			ek.SetSplit(false)
		}
	}
}

func (i *Inode) DecSplitEk(mpId uint64, ext *proto.ExtentKey) (ok bool, last bool) {
	log.LogDebugf("[DecSplitEk] mpId[%v] inode %v dp %v extent id %v.key %v ext %v", mpId, i.Inode, ext.PartitionId, ext.ExtentId,
		ext.PartitionId<<32|ext.ExtentId, ext)

	if i.multiSnap == nil || i.multiSnap.ekRefMap == nil {
		log.LogErrorf("DecSplitEk. multiSnap %v", i.multiSnap)
		return
	}
	if val, ok := i.multiSnap.ekRefMap.Load(ext.PartitionId<<32 | ext.ExtentId); !ok {
		log.LogErrorf("[DecSplitEk] mpId[%v]. dp %v inode [%v] ext not found", mpId, ext.PartitionId, i.Inode)
		return false, false
	} else {
		if val.(uint32) == 0 {
			log.LogErrorf("[DecSplitEk] mpId[%v]. dp %v inode [%v] ek ref is zero!", mpId, ext.PartitionId, i.Inode)
			return false, false
		}
		if val.(uint32) == 1 {
			log.LogDebugf("[DecSplitEk] mpId[%v] inode %v dp %v extent id %v.key %v", mpId, i.Inode, ext.PartitionId, ext.ExtentId,
				ext.PartitionId<<32|ext.ExtentId)
			i.multiSnap.ekRefMap.Delete(ext.PartitionId<<32 | ext.ExtentId)
			return true, true
		}
		i.multiSnap.ekRefMap.Store(ext.PartitionId<<32|ext.ExtentId, val.(uint32)-1)
		log.LogDebugf("[DecSplitEk] mpId[%v]. mp %v inode [%v] ek %v val %v", mpId, ext.PartitionId, i.Inode, ext, val.(uint32)-1)
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
	ok := proto.IsDir(i.Type) && i.NLink <= 2 && i.getLayerLen() == 0
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
	log.LogDebugf("action[SetAttr] inode %v req seq %v inode seq %v", i.Inode, req.VerSeq, i.getVer())

	if req.VerSeq != i.getVer() {
		i.CreateVer(req.VerSeq)
	}
	i.Lock()
	log.LogDebugf("action[SetAttr] inode %v req seq %v inode seq %v", i.Inode, req.VerSeq, i.getVer())
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
	mtime := timeutil.GetCurrentTimeUnix()
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

func (i *Inode) storeInReplicaSystem() bool {
	return i.StorageClass == proto.StorageClass_Replica_HDD || i.StorageClass == proto.StorageClass_Replica_SSD
}

func (i *Inode) updateStorageClass(storageClass uint32) error {
	i.Lock()
	defer i.Unlock()
	if i.StorageClass == proto.StorageClass_Unspecified {
		i.StorageClass = storageClass
	} else if i.StorageClass != storageClass {
		return errors.New(fmt.Sprintf("storageClass %v not equal to inode.storageClass %v",
			storageClass, i.StorageClass))
	}
	return nil
}
