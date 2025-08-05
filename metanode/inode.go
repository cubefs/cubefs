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
	"io"
	syslog "log"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/errors"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	DeleteMarkFlag               = 1 << 0
	InodeDelTop                  = 1 << 1
	DeleteMigrationExtentKeyFlag = 1 << 2 // only delete migration ek by delay
)

const (
	// InodeV1Flag uint64 = 0x01
	V2EnableEbsFlag       uint64 = 0x02
	V3EnableSnapInodeFlag uint64 = 0x04
	V4EnableHybridCloud   uint64 = 0x08
	// V4EBSExtentsFlag       uint64 = 0x20
	V4MigrationExtentsFlag uint64 = 0x40
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
	verSeq        uint64     // latest version be create or modified
	multiVersions InodeBatch // descend
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
	Extents *SortedExtents // in HybridCloud, this is for cache dp only

	// ObjExtents *SortedObjExtents
	// Snapshot
	multiSnap *InodeMultiSnap

	// HybridCloud
	StorageClass                uint32
	HybridCloudExtents          *SortedHybridCloudExtents
	HybridCloudExtentsMigration *SortedHybridCloudExtentsMigration
	ClientID                    uint32
	LeaseExpireTime             uint64
}

func (i *Inode) LeaseNotExpire() bool {
	return i.LeaseExpireTime >= uint64(timeutil.GetCurrentTimeUnix())
}

func (i *Inode) GetMultiVerString() string {
	if i.multiSnap == nil {
		return "nil"
	}

	return fmt.Sprintf("%v", i.multiSnap.multiVersions)
}

func (i *Inode) EmptyHybridExtents() bool {
	if i.HybridCloudExtents == nil {
		return true
	}
	return sortEksEmpty(i.HybridCloudExtents.sortedEks, i.StorageClass)
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

func (i *Inode) IsFile() bool {
	return proto.IsRegular(i.Type)
}

func (i *Inode) setVer(seq uint64) {
	if i.getVer() > seq {
		syslog.Printf("inode[%v] old seq [%v] cann't use seq [%v]\n", i.getVer(), seq, string(debug.Stack()))
		log.LogFatalf("inode[%v] old seq [%v] cann't use seq [%v] stack %v", i.Inode, i.getVer(), seq, string(debug.Stack()))
	}
	i.verUpdate(seq)
}

func (i *Inode) insertEkRefMap(mpId uint64, ek *proto.ExtentKey) {
	if !clusterEnableSnapshot {
		return
	}
	if i.multiSnap == nil {
		i.multiSnap = NewMultiSnap(i.getVer())
	}
	if i.multiSnap.ekRefMap == nil {
		i.multiSnap.ekRefMap = new(sync.Map)
	}
	storeEkSplit(mpId, i.Inode, i.multiSnap.ekRefMap, ek)
}

func (i *Inode) isEkInRefMap(mpId uint64, ek *proto.ExtentKey) (ok bool) {
	if i.multiSnap == nil {
		return
	}
	if i.multiSnap.ekRefMap == nil {
		log.LogErrorf("[storeEkSplit] mpId [%v] inodeID %v ekRef nil", mpId, i.Inode)
		return
	}
	log.LogDebugf("[storeEkSplit] mpId [%v] inode[%v] mp[%v] extent id[%v] ek [%v]", mpId, i.Inode, ek.PartitionId, ek.ExtentId, ek)
	id := ek.PartitionId<<32 | ek.ExtentId
	_, ok = i.multiSnap.ekRefMap.Load(id)
	return
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
		log.LogErrorf("getLayerVer. inode[%v] multi snap nil", i.Inode)
		return 0
	}

	if layer > i.getLayerLen()-1 {
		log.LogErrorf("getLayerVer. inode[%v] layer %v not exist, len %v", i.Inode, layer, i.getLayerLen())
		return 0
	}
	if i.multiSnap.multiVersions[layer] == nil {
		log.LogErrorf("getLayerVer. inode[%v] layer %v nil", i.Inode, layer)
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
func (inode *Inode) GetAllExtsOfflineInode(mpID uint64, isCache bool, isMigration bool) (extInfo map[uint64][]*proto.ExtentKey) {
	log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp[%v] inode[%v] inode.Extents: %v, ino verList: %v",
		mpID, inode.Inode, inode.Extents, inode.GetMultiVerString())

	extInfo = make(map[uint64][]*proto.ExtentKey)

	if inode.getLayerLen() > 0 {
		log.LogWarnf("deleteMarkedInodes. GetAllExtsOfflineInode.mp[%v] inode[%v] verlist len %v should not drop",
			mpID, inode.Inode, inode.getLayerLen())
	}

	for i := 0; i < inode.getLayerLen()+1; i++ {
		dIno := inode
		if i > 0 {
			dIno = inode.multiSnap.multiVersions[i-1]
		}
		log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp[%v] inode[%v] dino[%v]", mpID, inode.Inode, dIno)
		extents := NewSortedExtents()
		if isCache {
			extents = dIno.Extents
		} else if isMigration {
			if dIno.HybridCloudExtentsMigration.sortedEks != nil {
				extents = dIno.HybridCloudExtentsMigration.sortedEks.(*SortedExtents)
			}
		} else {
			if dIno.HybridCloudExtents.sortedEks != nil {
				extents = dIno.HybridCloudExtents.sortedEks.(*SortedExtents)
			}
		}
		extents.Range(func(_ int, ek proto.ExtentKey) bool {
			ext := &ek
			if ext.IsSplit() {
				var (
					dOK  bool
					last bool
				)
				log.LogDebugf("deleteMarkedInodes DecSplitEk mpID %v inode[%v]", mpID, inode.Inode)
				if dOK, last = dIno.DecSplitEk(mpID, &ek); !dOK {
					return false
				}
				if !last {
					log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp[%v] inode[%v] ek [%v] be removed", mpID, inode.Inode, ek)
					return true
				}

				log.LogDebugf("deleteMarkedInodes. GetAllExtsOfflineInode.mp[%v] inode[%v] ek [%v] be removed", mpID, inode.Inode, ek)
			}
			extInfo[ek.PartitionId] = append(extInfo[ek.PartitionId], &ek)
			// NOTE: unnecessary to set ext
			log.LogWritef("GetAllExtsOfflineInode. mp[%v] ino(%v) deleteExtent(%v)", mpID, inode.Inode, ek.String())
			return true
		})
		// NOTE: clear all extents in this layer
		// dIno.Extents = NewSortedExtents()
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
	if dataLen > proto.MaxBufferSize {
		return proto.ErrBufferSizeExceedMaximum
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
	if dataLen > proto.MaxBufferSize {
		return proto.ErrBufferSizeExceedMaximum
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
	// TODO:support hybrid-cloud
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
	buff.WriteString(fmt.Sprintf("verSeq[%v]", i.getVer()))
	buff.WriteString(fmt.Sprintf("multiSnap.multiVersions.len[%v]", i.getLayerLen()))
	buff.WriteString(fmt.Sprintf("StorageClass[%v]", i.StorageClass))
	if i.HybridCloudExtents.sortedEks != nil {
		if proto.IsStorageClassReplica(i.StorageClass) {
			buff.WriteString(fmt.Sprintf("Extents[%s]", i.HybridCloudExtents.sortedEks.(*SortedExtents)))
		} else {
			buff.WriteString(fmt.Sprintf("Extents[%s]", i.HybridCloudExtents.sortedEks.(*SortedObjExtents)))
		}
	}
	if i.HybridCloudExtentsMigration != nil {
		buff.WriteString(fmt.Sprintf("MigrationExtents[%s]", i.HybridCloudExtentsMigration))
	}
	buff.WriteString(fmt.Sprintf("ClientID[%v]", i.ClientID))
	buff.WriteString(fmt.Sprintf("LeaseExpireTime[%v]", i.LeaseExpireTime))
	buff.WriteString("}")
	return buff.String()
}

func NewSimpleInode(ino uint64) *Inode {
	return &Inode{
		Inode: ino,
	}
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
		// ObjExtents:         NewSortedObjExtents(),
		multiSnap:                   nil,
		StorageClass:                proto.StorageClass_Unspecified,
		HybridCloudExtents:          NewSortedHybridCloudExtents(),
		LeaseExpireTime:             0,
		ClientID:                    0,
		HybridCloudExtentsMigration: NewSortedHybridCloudExtentsMigration(),
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
	newIno.LeaseExpireTime = i.LeaseExpireTime
	newIno.ClientID = i.ClientID
	// newIno.ObjExtents = i.ObjExtents.Clone()
	if i.multiSnap != nil {
		newIno.multiSnap = &InodeMultiSnap{
			verSeq:        i.getVer(),
			multiVersions: i.multiSnap.multiVersions.Clone(),
			ekRefMap:      i.multiSnap.ekRefMap,
		}
	}
	if i.HybridCloudExtents.sortedEks != nil {
		if proto.IsStorageClassReplica(i.StorageClass) {
			newIno.HybridCloudExtents.sortedEks = i.HybridCloudExtents.sortedEks.(*SortedExtents).Clone()
		} else {
			newIno.HybridCloudExtents.sortedEks = i.HybridCloudExtents.sortedEks.(*SortedObjExtents).Clone()
		}
	}
	if i.HybridCloudExtentsMigration.sortedEks != nil {
		newIno.HybridCloudExtentsMigration.storageClass = i.HybridCloudExtentsMigration.storageClass
		newIno.HybridCloudExtentsMigration.expiredTime = i.HybridCloudExtentsMigration.expiredTime
		if proto.IsStorageClassReplica(i.HybridCloudExtentsMigration.storageClass) {
			newIno.HybridCloudExtentsMigration.sortedEks = i.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).Clone()
		} else {
			newIno.HybridCloudExtentsMigration.sortedEks = i.HybridCloudExtentsMigration.sortedEks.(*SortedObjExtents).Clone()
		}
	}
	i.RUnlock()
	return newIno
}

func (i *Inode) CopyInodeOnly(cInode *Inode) *Inode {
	tmpInode := cInode.CopyDirectly().(*Inode)
	tmpInode.Extents = i.Extents
	tmpInode.HybridCloudExtents.sortedEks = i.HybridCloudExtents.sortedEks
	tmpInode.multiSnap = i.multiSnap
	return tmpInode
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
	newIno.LeaseExpireTime = i.LeaseExpireTime
	newIno.ClientID = i.ClientID
	// newIno.ObjExtents = i.ObjExtents.Clone()
	if i.HybridCloudExtents.sortedEks != nil {
		if proto.IsStorageClassReplica(i.StorageClass) {
			newIno.HybridCloudExtents.sortedEks = i.HybridCloudExtents.sortedEks.(*SortedExtents).Clone()
		} else {
			newIno.HybridCloudExtents.sortedEks = i.HybridCloudExtents.sortedEks.(*SortedObjExtents).Clone()
		}
	}
	if i.HybridCloudExtentsMigration.sortedEks != nil {
		newIno.HybridCloudExtentsMigration.storageClass = i.HybridCloudExtentsMigration.storageClass
		newIno.HybridCloudExtentsMigration.expiredTime = i.HybridCloudExtentsMigration.expiredTime
		if proto.IsStorageClassReplica(i.HybridCloudExtentsMigration.storageClass) {
			newIno.HybridCloudExtentsMigration.sortedEks = i.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).Clone()
		} else {
			newIno.HybridCloudExtentsMigration.sortedEks = i.HybridCloudExtentsMigration.sortedEks.(*SortedObjExtents).Clone()
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
		err = errors.NewErrorf("[Unmarshal] read keyLen: %s", err.Error())
		return
	}
	if keyLen > proto.MaxBufferSize {
		return proto.ErrBufferSizeExceedMaximum
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		err = errors.NewErrorf("[Unmarshal] read keyBytes: %s", err.Error())
		return
	}
	if err = i.UnmarshalKey(keyBytes); err != nil {
		err = errors.NewErrorf("[Unmarshal] UnmarshalKey: %s", err.Error())
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		err = errors.NewErrorf("[Unmarshal] inode(%v) read valLen: %s", i.Inode, err.Error())
		return
	}
	if valLen > proto.MaxBufferSize {
		return proto.ErrBufferSizeExceedMaximum
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		err = errors.NewErrorf("[Unmarshal] inode(%v) read valBytes: %s", i.Inode, err.Error())
		return
	}
	err = i.UnmarshalValue(valBytes)
	if err != nil {
		err = errors.NewErrorf("[Unmarshal] inode(%v) UnmarshalValue: %s", i.Inode, err.Error())
	}

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
	log.LogDebugf("MarshalInodeValue ino(%v) storageClass(%v) Reserved(%v)", i.Inode, i.StorageClass, i.Reserved)
	// reset reserved, V4EBSExtentsFlag maybe changed after migration .eg
	reserved := uint64(0)
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("MarshalInodeValue ino(%v)  storageClass(%v) reserved(%d) Recovered from panic:%v",
				i.String(), i.StorageClass, reserved, err)
			log.LogFlush()
			panic(err)
		}
		i.Reserved = reserved
	}()

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

	enableSnapshot := false
	if i.multiSnap != nil {
		reserved |= V3EnableSnapInodeFlag
		enableSnapshot = true
	}

	reserved |= V4EnableHybridCloud
	isFile := proto.IsRegular(i.Type)
	// to check flag

	if !proto.IsValidStorageClass(i.StorageClass) && isFile && i.Size > 0 {
		panic(fmt.Sprintf("ino(%v) MarshalInodeValue failed, unsupport StorageClass %v", i.Inode, i.StorageClass))
	}

	if proto.IsStorageClassBlobStore(i.StorageClass) {
		if i.HybridCloudExtents.sortedEks != nil {
			ObjExtents := i.HybridCloudExtents.sortedEks.(*SortedObjExtents)
			if ObjExtents != nil && len(ObjExtents.eks) > 0 {
				// i.Reserved |= V4EBSExtentsFlag
				reserved |= V2EnableEbsFlag
				log.LogDebugf("MarshalInodeValue ino(%v) storageClass(%v) ?", i.Inode, i.StorageClass)
			}
		}
	}

	if i.HybridCloudExtentsMigration != nil && i.HybridCloudExtentsMigration.storageClass != proto.MediaType_Unspecified {
		reserved |= V4MigrationExtentsFlag
		log.LogDebugf("MarshalInodeValue ino(%v) V4MigrationExtentsFlag", i.Inode)
	}

	log.LogDebugf("MarshalInodeValue ino(%v) storageClass(%v) Reserved(%v) ClientID(%v) LeaseExpireTime(%v)",
		i.Inode, i.StorageClass, reserved, i.ClientID, i.LeaseExpireTime)
	if err = binary.Write(buff, binary.BigEndian, reserved); err != nil {
		panic(err)
	}

	if reserved&V2EnableEbsFlag > 0 {
		// marshal cache ExtentsKey, only use in ebs case.
		extData, err := i.Extents.MarshalBinary(false)
		if err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(extData))); err != nil {
			panic(err)
		}
		if _, err = buff.Write(extData); err != nil {
			panic(err)
		}

		ObjExtents := i.HybridCloudExtents.sortedEks.(*SortedObjExtents)
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
	} else {
		log.LogDebugf("MarshalInodeValue ino(%v) storageClass(%v) marshall HybridCloudExtents V4ReplicaExtentsFlag or empyt obj exts Reserved(%v)",
			i.Inode, i.StorageClass, reserved)
		replicaExtents := NewSortedExtents()
		if i.HybridCloudExtents.HasReplicaExts() {
			replicaExtents = i.HybridCloudExtents.sortedEks.(*SortedExtents)
		}
		extData, err := replicaExtents.MarshalBinary(enableSnapshot)
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

	if i.multiSnap != nil {
		if err = binary.Write(buff, binary.BigEndian, i.getVer()); err != nil {
			panic(err)
		}
	}

	// marshal StorageClass
	if err = binary.Write(buff, binary.BigEndian, &i.StorageClass); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ClientID); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.LeaseExpireTime); err != nil {
		panic(err)
	}

	if reserved&V4MigrationExtentsFlag > 0 {
		sem := i.HybridCloudExtentsMigration
		log.LogDebugf("MarshalInodeValue ino(%v) marshall V4MigrationExtentsFlag Reserved(%v)", i.Inode, reserved)
		if err = binary.Write(buff, binary.BigEndian, &sem.storageClass); err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, &sem.expiredTime); err != nil {
			panic(err)
		}

		if sem.Empty() {
			if err = binary.Write(buff, binary.BigEndian, uint32(0)); err != nil {
				panic(err)
			}
			return
		}

		if proto.IsStorageClassReplica(sem.storageClass) {
			log.LogDebugf("MarshalInodeValue ino(%v) migrationStorageClass(%v) marshall V4MigrationExtentsFlag SortedExtents Reserved(%v)",
				i.Inode, sem.storageClass, reserved)
			replicaExtents, ok := sem.sortedEks.(*SortedExtents)
			if !ok {
				panic(errors.New(fmt.Sprintf("MarshalInodeValue failed, inode(%v) StorageClass(%v) but type of sortedEks not match",
					i.Inode, sem.storageClass)))
			}

			extData, err := replicaExtents.MarshalBinary(enableSnapshot)
			if err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, uint32(len(extData))); err != nil {
				panic(err)
			}
			if _, err = buff.Write(extData); err != nil {
				panic(err)
			}
		} else if proto.IsStorageClassBlobStore(sem.storageClass) {
			log.LogDebugf("MarshalInodeValue ino(%v)migrationStorageClass(%v) marshall V4MigrationExtentsFlag SortedObjExtents Reserved(%v) ",
				i.Inode, sem.storageClass, reserved)
			ObjExtents := sem.sortedEks.(*SortedObjExtents)
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
		} else {
			log.LogFlush()
			panic(errors.New(fmt.Sprintf("MarshalInodeValue failed, inode(%v) unsupport migrate StorageClass(%v)",
				i.Inode, sem.storageClass)))
		}
	}
}

// MarshalValue marshals the value to bytes.
func (i *Inode) MarshalValue() (val []byte) {
	var err error
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)

	i.RLock()
	i.MarshalInodeValue(buff)

	if i.multiSnap != nil {
		if i.getLayerLen() > 0 || i.getVer() > 0 {
			// TODO:tangjingyu log for debug only
			log.LogWarnf("##### [MarshalValue] verCnt(%v) verSeq(%v) inode[%v] stack(%v)",
				i.getLayerLen(), i.getVer(), i, string(debug.Stack()))
		}
		if i.getLayerLen() > 0 && i.getVer() == 0 {
			log.LogFatalf("#### [MarshalValue] inode %v current verSeq %v, hist len (%v) stack(%v)",
				i.Inode, i.getVer(), i.getLayerLen(), string(debug.Stack()))
		}
		if err = binary.Write(buff, binary.BigEndian, int32(i.getLayerLen())); err != nil {
			i.RUnlock()
			panic(err)
		}
		for idx, ino := range i.multiSnap.multiVersions {
			// TODO:tangjingyu log for debug only
			log.LogWarnf("##### [MarshalValue] handle multiVersions idx(%v) inode[%v] ", idx, i)
			ino.MarshalInodeValue(buff)
		}
	}

	val = buff.Bytes()
	i.RUnlock()
	return
}

func UnmarshalInodeFiledError(errFieldName string, originErr error) (err error) {
	return fmt.Errorf("Unmarshal field[%v] err: %v", errFieldName, originErr.Error())
}

// UnmarshalValue unmarshals the value from bytes.
func (i *Inode) UnmarshalInodeValue(buff *bytes.Buffer) (err error) {
	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		err = UnmarshalInodeFiledError("Type", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Uid); err != nil {
		err = UnmarshalInodeFiledError("Uid", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Gid); err != nil {
		err = UnmarshalInodeFiledError("Gid", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		err = UnmarshalInodeFiledError("Size", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Generation); err != nil {
		err = UnmarshalInodeFiledError("Generation", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		err = UnmarshalInodeFiledError("CreateTime", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		err = UnmarshalInodeFiledError("AccessTime", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		err = UnmarshalInodeFiledError("ModifyTime", err)
		return
	}
	// read symLink
	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		err = UnmarshalInodeFiledError("symSize", err)
		return
	}
	if symSize > 0 {
		if symSize > proto.MaxBufferSize {
			return proto.ErrBufferSizeExceedMaximum
		}
		i.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, i.LinkTarget); err != nil {
			err = UnmarshalInodeFiledError("LinkTarget", err)
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &i.NLink); err != nil {
		err = UnmarshalInodeFiledError("NLink", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Flag); err != nil {
		err = UnmarshalInodeFiledError("Flag", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Reserved); err != nil {
		err = UnmarshalInodeFiledError("Reserved", err)
		return
	}
	// unmarshal ExtentsKey
	if i.Extents == nil {
		i.Extents = NewSortedExtents()
	}

	if i.HybridCloudExtents == nil {
		i.HybridCloudExtents = NewSortedHybridCloudExtents()
	}

	isFile := i.IsFile()
	v3 := i.Reserved&V3EnableSnapInodeFlag > 0
	v4 := i.Reserved&V4EnableHybridCloud > 0

	if i.Reserved == 0 {
		log.LogDebugf("#### [UnmarshalInodeValue] not v2 v3 V4, ino(%v), isFile %v, size %d", i.Inode, isFile, i.Size)

		extents := NewSortedExtents()
		if err, _ = extents.UnmarshalBinary(buff.Bytes(), false); err != nil {
			return fmt.Errorf("UnmarshalBinary failed, ino %d, ino %v", i.Inode, i)
		}
		if extents.Len() > 0 {
			i.HybridCloudExtents.sortedEks = extents
		}

		i.StorageClass = legacyReplicaStorageClass
		if i.StorageClass == proto.StorageClass_Unspecified && isFile && extents.Len() > 0 {
			return fmt.Errorf("UnmarshalInodeValue: legacyReplicaStorageClass not set in config, ino %d", i.Inode)
		}
		return
	}

	if i.Reserved&V2EnableEbsFlag > 0 {
		extSize := uint32(0)
		// unmarshall extents cache
		if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
			err = UnmarshalInodeFiledError("extSize(v4)", err)
			return
		}

		if extSize > 0 {
			extBytes := make([]byte, extSize)
			if _, err = io.ReadFull(buff, extBytes); err != nil {
				err = UnmarshalInodeFiledError("extBytes(v4)", err)
				return
			}

			if err, _ = i.Extents.UnmarshalBinary(extBytes, false); err != nil {
				err = UnmarshalInodeFiledError("extBytes(v4)", err)
				return
			}
		}

		ObjExtSize := uint32(0)
		if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtents.ObjExtSize(v4)", err)
			return
		}
		log.LogDebugf("UnmarshalInodeValue ino(%v) ObjExtSize(%v)", i.Inode, ObjExtSize)
		if ObjExtSize > 0 {
			objExtBytes := make([]byte, ObjExtSize)
			if _, err = io.ReadFull(buff, objExtBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.objExtBytes(v4)", err)
				return
			}
			ObjExtents := NewSortedObjExtents()
			if err = ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.ObjExtents(v4)", err)
				return
			}
			i.HybridCloudExtents.sortedEks = ObjExtents
		}
		i.StorageClass = proto.StorageClass_BlobStore
	} else {
		extSize := uint32(0)
		if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtents.extSize(v4)", err)
			return
		}
		log.LogDebugf("UnmarshalInodeValue ino(%v) extSize(%v)", i.Inode, extSize)
		if extSize > 0 {
			extBytes := make([]byte, extSize)
			if _, err = io.ReadFull(buff, extBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.extBytes(v4)", err)
				return
			}
			var ekRef *sync.Map
			eks := NewSortedExtents()
			if err, ekRef = eks.UnmarshalBinary(extBytes, v3); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.SortedExtents(v4)", err)
				return
			}
			i.HybridCloudExtents.sortedEks = eks
			if ekRef != nil {
				if i.multiSnap == nil {
					i.multiSnap = NewMultiSnap(0)
				}
				i.multiSnap.ekRefMap = ekRef
			}
		}
		i.StorageClass = legacyReplicaStorageClass
		if !proto.IsValidStorageClass(i.StorageClass) {
			i.StorageClass = proto.StorageClass_BlobStore
		}
	}

	if v3 {
		var seq uint64
		if err = binary.Read(buff, binary.BigEndian, &seq); err != nil {
			err = UnmarshalInodeFiledError("multiSnap.verSeq(v4)", err)
			log.LogWarnf("[UnmarshalInodeValue] ino(%v) err[%v]", i, err.Error())
			return
		}
		if seq != 0 {
			i.setVer(seq)
		}
	}

	log.LogDebugf("#### [UnmarshalInodeValue] v4, ino(%v)", i.Inode)

	// hybridcloud format
	if v4 {
		log.LogDebugf("#### [UnmarshalInodeValue] v4, ino(%v)", i.Inode)
		if err = binary.Read(buff, binary.BigEndian, &i.StorageClass); err != nil {
			err = UnmarshalInodeFiledError("StorageClass(v4)", err)
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &i.ClientID); err != nil {
			err = UnmarshalInodeFiledError("ForbiddenMigration(v4)", err)
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &i.LeaseExpireTime); err != nil {
			err = UnmarshalInodeFiledError("LeaseExpireTime(v4)", err)
			return
		}

		if i.StorageClass == proto.StorageClass_Unspecified && isFile {
			i.StorageClass = proto.StorageClass_BlobStore
		}

		if i.Reserved&V4MigrationExtentsFlag > 0 {
			if i.HybridCloudExtentsMigration == nil {
				i.HybridCloudExtentsMigration = NewSortedHybridCloudExtentsMigration()
			}
			if err = binary.Read(buff, binary.BigEndian, &i.HybridCloudExtentsMigration.storageClass); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.storageClass(v4)", err)
				return
			}
			if err = binary.Read(buff, binary.BigEndian, &i.HybridCloudExtentsMigration.expiredTime); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.expiredTime(v4)", err)
				return
			}
			if proto.IsStorageClassReplica(i.HybridCloudExtentsMigration.storageClass) {
				extSize := uint32(0)
				if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
					err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.extSize(v4)", err)
					return
				}
				log.LogDebugf("[UnmarshalInodeValue] ino(%v) migrateStorageClass(%v) extSize(%v)",
					i.Inode, i.HybridCloudExtentsMigration.storageClass, extSize)
				if extSize > 0 {
					extBytes := make([]byte, extSize)
					if _, err = io.ReadFull(buff, extBytes); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.extBytes(v4)", err)
						return
					}
					i.HybridCloudExtentsMigration.sortedEks = NewSortedExtents()
					if err, _ = i.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).UnmarshalBinary(extBytes, v3); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.SortedExtents(v4)", err)
						return
					}
				}

			} else if proto.IsStorageClassBlobStore(i.HybridCloudExtentsMigration.storageClass) {
				ObjExtSize := uint32(0)
				if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
					err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtSize(v4)", err)
					return
				}
				log.LogDebugf("[UnmarshalInodeValue] ino(%v) migrateStorageClass(%v) ObjExtSize(%v)",
					i.Inode, i.HybridCloudExtentsMigration.storageClass, ObjExtSize)
				if ObjExtSize > 0 {
					objExtBytes := make([]byte, ObjExtSize)
					if _, err = io.ReadFull(buff, objExtBytes); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.objExtBytes(v4)", err)
						return
					}
					ObjExtents := NewSortedObjExtents()
					if err = ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtents(v4)", err)
						return
					}
					i.HybridCloudExtentsMigration.sortedEks = ObjExtents
				}
			}
		}
	}
	return
}

func (i *Inode) GetSpaceSize() (extSize uint64) {
	if i.IsTempFile() || !i.IsFile() {
		return
	}
	if i.HybridCloudExtents.sortedEks == nil {
		return
	}
	if proto.IsStorageClassReplica(i.StorageClass) {
		extSize += i.HybridCloudExtents.sortedEks.(*SortedExtents).LayerSize()
	} else {
		extSize += i.HybridCloudExtents.sortedEks.(*SortedObjExtents).LayerSize()
	}
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (i *Inode) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)

	if err = i.UnmarshalInodeValue(buff); err != nil {
		err = fmt.Errorf("UnmarshalValue ino(%v) err: %v", i.Inode, err.Error())
		log.LogErrorf("action[UnmarshalValue] %v", err.Error())
		return
	}

	if i.Reserved&V3EnableSnapInodeFlag > 0 && clusterEnableSnapshot {
		var verCnt int32
		if err = binary.Read(buff, binary.BigEndian, &verCnt); err != nil {
			log.LogErrorf("[UnmarshalValue] inode[%v] newSeq[%v], get ver cnt err: %v", i.Inode, i.getVer(), err.Error())
			return
		}
		log.LogDebugf("####[UnmarshalValue] inode(%v) newSeq(%v), get verCnt: %v", i.Inode, i.getVer(), verCnt)
		if verCnt > 0 {
			// TODO:tangjingyu log for debug only
			log.LogWarnf("####[UnmarshalValue] inode(%v) newSeq(%v), get verCnt: %v", i.Inode, i.getVer(), verCnt)
		}

		for idx := int32(0); idx < verCnt; idx++ {
			ino := &Inode{Inode: i.Inode}
			if err = ino.UnmarshalInodeValue(buff); err != nil {
				err = fmt.Errorf("UnmarshalValue ino(%v) multiSnapIdx(%v) err: %v", i.Inode, idx, err.Error())
				log.LogErrorf("action[UnmarshalValue] verCnt(%v), %v", verCnt, err.Error())
				return
			}

			if ino.multiSnap != nil && ino.multiSnap.ekRefMap != nil {
				if i.multiSnap.ekRefMap == nil {
					i.multiSnap.ekRefMap = new(sync.Map)
				}
				// log.LogDebugf("UnmarshalValue. inode[%v] merge top layer multiSnap.ekRefMap with layer %v", i.Inode, idx)
				proto.MergeSplitKey(i.Inode, i.multiSnap.ekRefMap, ino.multiSnap.ekRefMap)
			}
			if i.multiSnap == nil {
				i.multiSnap = &InodeMultiSnap{}
			}
			// log.LogDebugf("action[UnmarshalValue] inode[%v] old seq [%v] hist len %v", ino.Inode, ino.getVer(), i.getLayerLen())
			i.multiSnap.multiVersions = append(i.multiSnap.multiVersions, ino)
		}

		multiVerLen := 0
		if i.multiSnap != nil {
			multiVerLen = len(i.multiSnap.multiVersions)
		}
		log.LogDebugf("#### [UnmarshalValue] inode(%v) newSeq(%v) verCnt(%v) multiVersions(%v)",
			i.Inode, i.getVer(), verCnt, multiVerLen)
	}
	return
}

// AppendExtents append the extent to the btree.
func (i *Inode) AppendExtents(eks []proto.ExtentKey, ct int64, volType int) (delExtents []proto.ExtentKey) {
	if proto.IsStorageClassBlobStore(i.StorageClass) {
		return
	}
	i.Lock()
	defer i.Unlock()
	if i.HybridCloudExtents.sortedEks == nil {
		i.HybridCloudExtents.sortedEks = NewSortedExtents()
	}
	extents := i.HybridCloudExtents.sortedEks.(*SortedExtents)
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
	if i.HybridCloudExtents.sortedEks == nil {
		i.HybridCloudExtents.sortedEks = NewSortedObjExtents()
	}
	for _, ek := range eks {
		err = i.HybridCloudExtents.sortedEks.(*SortedObjExtents).Append(ek)
		if err != nil {
			return
		}
		size := i.HybridCloudExtents.sortedEks.(*SortedObjExtents).Size()
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
	log.LogInfof("action[PrintAllVersionInfo] inode[%v] verSeq [%v] hist len [%v]", i.Inode, i.getVer(), i.getLayerLen())
	for id, info := range i.multiSnap.multiVersions {
		log.LogInfof("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode[%v]", id, info.getVer(), info)
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
		if mLen > 0 && sortMergedExts[mLen-1].IsSequenceWithSameSeq(&(*keyArr)[pos]) {
			sortMergedExts[mLen-1].Size += (*keyArr)[pos].Size
			log.LogDebugf("[mergeExtentArr] mpId[%v]. ek left %v right %v", mpId, sortMergedExts[mLen-1], (*keyArr)[pos])
			if !sortMergedExts[mLen-1].IsSplit() || !(*keyArr)[pos].IsSplit() {
				log.LogErrorf("[mergeExtentArr] mpId[%v] ino[%v] ek merge left %v right %v not all split", mpId, i.Inode, sortMergedExts[mLen-1], (*keyArr)[pos])
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
		log.LogInfof("action[RestoreMultiSnapExts] mpId [%v] inode[%v] restore have no old version left", mpId, i.Inode)
		return delExtentsOrigin, nil
	}
	lastSeq := i.multiSnap.multiVersions[idx].getVer()
	specSnapExtent := make([]proto.ExtentKey, 0)

	for _, delExt := range delExtentsOrigin {
		// curr deleting delExt with a seq larger than the next version's seq, it doesn't belong to any
		// versions,so try to delete it
		log.LogDebugf("action[RestoreMultiSnapExts] mpId [%v] inode[%v] ext split [%v] with seq[%v] gSeq[%v] try to del.the last seq [%v], ek details[%v]",
			mpId, i.Inode, delExt.IsSplit(), delExt.GetSeq(), curVer, lastSeq, delExt)
		if delExt.GetSeq() > lastSeq {
			delExtents = append(delExtents, delExt)
		} else {
			log.LogInfof("action[RestoreMultiSnapExts] mpId [%v] inode[%v] move to level 1 delExt [%v] specSnapExtent size [%v]", mpId, i.Inode, delExt, len(specSnapExtent))
			specSnapExtent = append(specSnapExtent, delExt)
		}
	}
	if len(specSnapExtent) == 0 {
		log.LogInfof("action[RestoreMultiSnapExts] mpId [%v] inode[%v] no need to move to level 1", mpId, i.Inode)
		return
	}
	if len(specSnapExtent) > 0 && i.isEmptyVerList() {
		err = fmt.Errorf("mpId [%v] inode[%v] error not found prev snapshot index", mpId, i.Inode)
		log.LogErrorf("action[RestoreMultiSnapExts] mpId [%v] inode[%v] %v", mpId, i.Inode, err)
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
	log.LogDebugf("action[unlinkTopLayer] mpid [%v] mpver [%v] check if have snapshot depends on the deleitng ino[%v] (with no snapshot itself) found seq [%v], verlist %v",
		mpId, mpVer, ino, inode.getVer(), verlist)
	status = proto.OpOk

	delFunc := func() (done bool) {
		if inode.NLink > 1 {
			log.LogDebugf("action[unlinkTopLayer] inode[%v] be unlinked, file link is %v", ino.Inode, inode.NLink)
			inode.DecNLink()
			doMore = false
			return true
		}
		// first layer need delete
		var err error
		if ext2Del, err = inode.RestoreExts2NextLayer(mpId, inode.Extents.eks, mpVer, 0); err != nil {
			log.LogErrorf("action[getAndDelVerInList] ino[%v] RestoreMultiSnapExts split error %v", inode.Inode, err)
			status = proto.OpNotExistErr
			log.LogDebugf("action[unlinkTopLayer] mp[%v] iino[%v]", mpId, ino)
			return
		}
		inode.Extents.eks = inode.Extents.eks[:0]
		log.LogDebugf("action[getAndDelVerInList] mp[%v] ino[%v] verseq [%v] get del exts %v", mpId, inode.Inode, inode.getVer(), ext2Del)
		inode.DecNLink() // dIno should be inode
		doMore = true
		return
	}

	// if topLayer verSeq is as same as mp, the current inode deletion only happen on the first layer
	// or ddelete from client do deletion at top layer which should allow delete ionde with older version contrast to mp version
	// because ddelete have two steps,1 is del dentry,2nd is unlink inode ,version may updated after 1st and before 2nd step, to
	// make sure inode be unlinked by normal deletion, sdk add filed of dentry verSeq to identify and different from other unlink actions
	if mpVer == inode.getVer() {
		if inode.getLayerLen() == 0 {
			log.LogDebugf("action[unlinkTopLayer] no snapshot available depends on ino[%v] not found seq[%v] and return, verlist[%v]", inode, inode.getVer(), verlist)
			inode.DecNLink()
			log.LogDebugf("action[unlinkTopLayer] inode[%v] be unlinked", ino.Inode)
			// operate inode directly
			doMore = true
			return
		}

		log.LogDebugf("action[unlinkTopLayer] need restore.ino[%v] withseq [%v] equal mp seq, verlist %v",
			ino, inode.getVer(), verlist)
		// need restore
		if !proto.IsDir(inode.Type) {
			delFunc()
			return
		}
		log.LogDebugf("action[unlinkTopLayer] inode[%v] be unlinked, Dir", ino.Inode)
		inode.DecNLink()
		doMore = true
		return
	}

	log.LogDebugf("action[unlinkTopLayer] need create version.ino[%v] withseq [%v] not equal mp seq [%v], verlist %v", ino, inode.getVer(), mpVer, verlist)
	if proto.IsDir(inode.Type) { // dir is whole info but inode is partition,which is quit different
		_, err := verlist.GetNextOlderVer(mpVer)
		if err == nil {
			log.LogDebugf("action[unlinkTopLayer] inode[%v] cann't get next older ver [%v] err %v", inode.Inode, mpVer, err)
			inode.CreateVer(mpVer)
		}
		inode.DecNLink()
		log.LogDebugf("action[unlinkTopLayer] inode[%v] be unlinked, Dir create ver 1st layer", ino.Inode)
		doMore = true
	} else {
		ver, err := verlist.GetNextOlderVer(mpVer)
		if err != nil {
			if err.Error() == "not found" {
				delFunc()
				return
			}
			log.LogErrorf("action[unlinkTopLayer] inode[%v] cann't get next older ver [%v] err %v", inode.Inode, mpVer, err)
			return
		}
		inode.CreateVer(mpVer) // protect origin version
		if inode.NLink == 1 {
			inode.CreateUnlinkVer(mpVer, ver) // create a effective top level  version
		}
		inode.DecNLink()
		log.LogDebugf("action[unlinkTopLayer] inode[%v] be unlinked, File create ver 1st layer", ino.Inode)
	}
	return
}

// mpVerlist should not include verSeq of ino,it should be deleted before in volume version deletion command
func (inode *Inode) dirUnlinkVerInlist(ino *Inode, mpVer uint64, mpVerlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	var idxWithTopLayer int
	var dIno *Inode
	status = proto.OpOk
	if dIno, idxWithTopLayer = inode.getInoByVer(ino.getVer(), false); dIno == nil {
		log.LogDebugf("action[dirUnlinkVerInlist] ino[%v] not found", ino)
		return
	}

	if idxWithTopLayer == 0 {
		// header layer do nothing and be depends on should not be dropped
		log.LogDebugf("action[dirUnlinkVerInlist] ino[%v] first layer do nothing", ino)
		return
	}
	// if any alive snapshot in mp dimension exist in seq scope from dino to next ascend neighbor, dio snapshot be keep or else drop
	if inode.multiSnap == nil {
		log.LogWarnf("action[dirUnlinkVerInlist] ino[%v] multiSnap should not be nil", inode)
		inode.multiSnap = &InodeMultiSnap{}
	}
	var (
		starSeq = dIno.getVer() // startSeq is the seq of the directory snapshot waiting to be determined whether to be deleted.
		endSeq  uint64
	)
	mIdx := idxWithTopLayer - 1
	if mIdx == 0 {
		endSeq = inode.getVer()
	} else {
		endSeq = inode.multiSnap.multiVersions[mIdx-1].getVer()
	}

	log.LogDebugf("action[dirUnlinkVerInlist] inode[%v] try drop multiVersion idx %v effective seq scope [%v,%v) ",
		inode.Inode, mIdx, starSeq, endSeq)

	// if the [startSeq, endSeq) have no effective volume version, the startSeq in dir snapshotList should be dropped
	doDelForNoDepends := func() bool {
		mpVerlist.RWLock.RLock()
		defer mpVerlist.RWLock.RUnlock()

		for id, info := range mpVerlist.VerList {
			if info.Ver >= starSeq && info.Ver < endSeq { //
				log.LogDebugf("action[dirUnlinkVerInlist] inode[%v] dir layer idx %v still have effective snapshot seq [%v].so don't drop", inode.Inode, mIdx, info.Ver)
				return false
			}
			if info.Ver >= endSeq || id == len(mpVerlist.VerList)-1 {
				log.LogDebugf("action[dirUnlinkVerInlist] inode[%v] try drop multiVersion idx %v and return", inode.Inode, mIdx)

				inode.Lock()
				inode.multiSnap.multiVersions = append(inode.multiSnap.multiVersions[:mIdx], inode.multiSnap.multiVersions[mIdx+1:]...)
				inode.Unlock()
				return true
			}
			log.LogDebugf("action[dirUnlinkVerInlist] inode[%v] try drop scope [%v, %v), mp ver [%v] not suitable", inode.Inode, dIno.getVer(), endSeq, info.Ver)
		}
		return false // should not take effect
	}
	if !doDelForNoDepends() {
		return
	}
	doMore = true
	dIno.DecNLink()
	return
}

func (inode *Inode) inodeUnlinkVerInList(mpId uint64, ino *Inode, mpVer uint64, mpVerlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	var dIno *Inode
	status = proto.OpOk
	// special case, snapshot is the last one and be depended by upper version,update it's version to the right one
	// ascend search util to the curr unCommit version in the verList
	if ino.getVer() == inode.getVer() || (isInitSnapVer(ino.getVer()) && inode.getVer() == 0) {
		if len(mpVerlist.VerList) == 0 {
			status = proto.OpNotExistErr
			log.LogErrorf("action[unlinkVerInList] inode[%v] mpVerlist should be larger than 0, return not found", inode.Inode)
			return
		}

		// just move to upper layer,the request snapshot be dropped
		nVerSeq, found := inode.getLastestVer(inode.getVer(), mpVerlist)
		if !found {
			status = proto.OpNotExistErr
			return
		}
		log.LogDebugf("action[unlinkVerInList] inode[%v] update current verseq [%v] to %v", inode.Inode, inode.getVer(), nVerSeq)
		inode.setVer(nVerSeq)
		return
	} else {
		// don't unlink if no version satisfied
		if ext2Del, dIno = inode.getAndDelVerInList(mpId, ino.getVer(), mpVer, mpVerlist); dIno == nil {
			status = proto.OpNotExistErr
			log.LogDebugf("action[unlinkVerInList] ino[%v]", ino)
			return
		}
	}

	dIno.DecNLink()
	log.LogDebugf("action[unlinkVerInList] inode[%v] snapshot layer be unlinked", ino.Inode)
	doMore = true
	return
}

func (inode *Inode) unlinkVerInList(mpId uint64, ino *Inode, mpVer uint64, mpVerlist *proto.VolVersionInfoList) (ext2Del []proto.ExtentKey, doMore bool, status uint8) {
	log.LogDebugf("action[unlinkVerInList] mpId [%v] ino[%v] try search seq [%v] isdir %v", mpId, ino, ino.getVer(), proto.IsDir(inode.Type))
	if proto.IsDir(inode.Type) { // snapshot dir deletion don't take link into consider, but considers the scope of snapshot contrast to verList
		return inode.dirUnlinkVerInlist(ino, mpVer, mpVerlist)
	}
	return inode.inodeUnlinkVerInList(mpId, ino, mpVer, mpVerlist)
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
			log.LogDebugf("action[getInoByVer]  ino[%v] no multiversion", ino.Inode)
			return
		}
		i = ino.multiSnap.multiVersions[listLen-1]
		if i.getVer() != 0 {
			log.LogDebugf("action[getInoByVer]  ino[%v] lay seq [%v]", ino.Inode, i.getVer())
			return nil, 0
		}
		return i, listLen
	}
	if verSeq > 0 && ino.getVer() > verSeq {
		if ino.multiSnap != nil {
			for id, iTmp := range ino.multiSnap.multiVersions {
				if verSeq == iTmp.getVer() {
					log.LogDebugf("action[getInoByVer]  ino[%v] get in multiversion id[%v]", ino.Inode, id)
					return iTmp, id + 1
				} else if verSeq > iTmp.getVer() {
					if !equal {
						log.LogDebugf("action[getInoByVer]  ino[%v] get in multiversion id[%v], %v, %v", ino.Inode, id, verSeq, iTmp.getVer())
						return iTmp, id + 1
					}
					log.LogDebugf("action[getInoByVer]  ino[%v] get in multiversion id[%v]", ino.Inode, id)
					return
				}
			}
		}
	} else {
		if !equal {
			log.LogDebugf("action[getInoByVer]  ino[%v]", ino.Inode)
			return ino, 0
		}
	}
	return
}

// 1. check if dVer layer is the last layer of the system  1)true,drop it all 2) false goto 3
// 2. if have system layer between dVer and next older inode's layer(not exist is ok), drop dVer related exts and update ver
// 3. else Restore to next inode's Layer

func (i *Inode) getAndDelVerInList(mpId uint64, dVer uint64, mpVer uint64, verlist *proto.VolVersionInfoList) (delExtents []proto.ExtentKey, ino *Inode) {
	var err error
	verlist.RWLock.RLock()
	defer verlist.RWLock.RUnlock()

	log.LogDebugf("action[getAndDelVerInList] ino[%v] verseq [%v] request del ver [%v] hist len %v isTmpFile %v",
		i.Inode, i.getVer(), dVer, i.getLayerLen(), i.IsTempFile())

	// read inode element is fine, lock is need while write
	inoVerLen := i.getLayerLen()
	if inoVerLen == 0 {
		log.LogDebugf("action[getAndDelVerInList] ino[%v] RestoreMultiSnapExts no left", i.Inode)
		return
	}

	// delete snapshot version
	if isInitSnapVer(dVer) {
		dVer = 0
	}
	lastVer := i.getVer()
	for id, mIno := range i.multiSnap.multiVersions {
		log.LogDebugf("action[getAndDelVerInList] ino[%v] multiSnap.multiVersions level %v verseq [%v]", i.Inode, id, mIno.getVer())
		if mIno.getVer() < dVer {
			log.LogDebugf("action[getAndDelVerInList] ino[%v] multiSnap.multiVersions level %v verseq [%v]", i.Inode, id, mIno.getVer())
			return
		}

		if mIno.getVer() == dVer {
			log.LogDebugf("action[getAndDelVerInList] ino[%v] ver [%v] step 3", i.Inode, mIno.getVer())
			// 2. get next version should according to verList but not only self multi list

			var nVerSeq uint64
			if nVerSeq, err = verlist.GetNextNewerVer(dVer); err != nil {
				log.LogDebugf("action[getAndDelVerInList] get next version failed, err %v", err)
				return
			}
			if lastVer > nVerSeq {
				mIno.setVer(nVerSeq)
				return
			}
			if i.isTailIndexInList(id) {
				i.multiSnap.multiVersions = i.multiSnap.multiVersions[:inoVerLen-1]
				log.LogDebugf("action[getAndDelVerInList] ino[%v] idx %v be dropped", i.Inode, inoVerLen)
				return mIno.Extents.eks, mIno
			}
			if nVerSeq, err = verlist.GetNextOlderVer(dVer); err != nil {
				log.LogDebugf("action[getAndDelVerInList] get next version failed, err %v", err)
				return
			}

			log.LogDebugf("action[getAndDelVerInList] ino[%v] ver [%v] nextVerseq [%v] step 3 ver ", i.Inode, mIno.getVer(), nVerSeq)
			// 2. system next layer not exist in inode ver list. update curr layer to next layer and filter out ek with verSeq
			// change id layer verSeq to neighbor layer info, omit version delete process

			if nVerSeq > i.multiSnap.multiVersions[id+1].getVer() {
				log.LogDebugf("action[getAndDelVerInList] ino[%v]  get next version in verList update ver from %v to %v.And delete exts with ver [%v]",
					i.Inode, i.multiSnap.multiVersions[id].getVer(), nVerSeq, dVer)

				i.multiSnap.multiVersions[id].setVerNoCheck(nVerSeq)
				i.multiSnap.multiVersions[id] = i.CopyInodeOnly(i.multiSnap.multiVersions[id+1])

				delExtents = i.MultiLayerClearExtByVer(id+1, dVer)
				ino = i.multiSnap.multiVersions[id]
				if len(i.multiSnap.multiVersions[id].Extents.eks) != 0 {
					log.LogDebugf("action[getAndDelVerInList] ino[%v]   after clear self still have ext and left", i.Inode)
					return
				}
			} else {
				log.LogDebugf("action[getAndDelVerInList] ino[%v] ver [%v] nextver [%v] step 3 ver ", i.Inode, mIno.getVer(), nVerSeq)
				// 3. next layer exist. the deleted version and  next version are neighbor in verlist, thus need restore and delete
				if delExtents, err = i.RestoreExts2NextLayer(mpId, mIno.Extents.eks, dVer, id+1); err != nil {
					log.LogDebugf("action[getAndDelVerInList] ino[%v] RestoreMultiSnapExts split error %v", i.Inode, err)
					return
				}
			}
			// delete layer id
			i.multiSnap.multiVersions = append(i.multiSnap.multiVersions[:id], i.multiSnap.multiVersions[id+1:]...)

			log.LogDebugf("action[getAndDelVerInList] ino[%v] verseq [%v] get del exts %v", i.Inode, i.getVer(), delExtents)
			return delExtents, mIno
		}
		lastVer = mIno.getVer()
	}
	return
}

func (i *Inode) getLastestVer(reqVerSeq uint64, verlist *proto.VolVersionInfoList) (uint64, bool) {
	verlist.RWLock.RLock()
	defer verlist.RWLock.RUnlock()

	if len(verlist.VerList) == 0 {
		return 0, false
	}
	for _, info := range verlist.VerList {
		if info.Ver > reqVerSeq {
			return info.Ver, true
		}
	}

	log.LogDebugf("action[getLastestVer] inode[%v] reqVerseq [%v] not found, the largetst one %v",
		i.Inode, reqVerSeq, verlist.VerList[len(verlist.VerList)-1].Ver)
	return 0, false
}

func (i *Inode) CreateUnlinkVer(mpVer uint64, nVer uint64) {
	log.LogDebugf("action[CreateUnlinkVer] inode[%v] mpver [%v] nver [%v]", i.Inode, mpVer, nVer)
	// inode copy not include multi ver array
	ino := i.CopyDirectly().(*Inode)
	ino.setVer(nVer)

	i.Extents = NewSortedExtents()
	// i.ObjExtents = NewSortedObjExtents()
	i.HybridCloudExtents = NewSortedHybridCloudExtents()
	i.SetDeleteMark()

	log.LogDebugf("action[CreateUnlinkVer] inode[%v] create new version [%v] and store old one [%v], hist len [%v]",
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
	if !clusterEnableSnapshot {
		return
	}
	// inode copy not include multi ver array
	ino := i.CopyDirectly().(*Inode)
	ino.Extents = NewSortedExtents()
	// ino.ObjExtents = NewSortedObjExtents()
	ino.HybridCloudExtents = NewSortedHybridCloudExtents()
	ino.setVer(i.getVer())
	i.setVer(ver)

	i.Lock()
	defer i.Unlock()
	log.LogDebugf("action[CreateVer] inode[%v] create new version [%v] and store old one [%v], hist len [%v]",
		i.Inode, ver, i.getVer(), i.getLayerLen())

	if i.multiSnap == nil {
		i.multiSnap = &InodeMultiSnap{}
	}
	i.multiSnap.multiVersions = append([]*Inode{ino}, i.multiSnap.multiVersions...)
}

func (i *Inode) buildMultiSnap() {
	if !clusterEnableSnapshot {
		return
	}
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
	log.LogDebugf("action[SplitExtentWithCheck] mpId[%v].inode[%v],ek [%v],hist len %v", param.mpId, i.Inode, param.ek, i.getLayerLen())

	if param.mpVer != i.getVer() {
		log.LogDebugf("action[SplitExtentWithCheck] mpId[%v].CreateVer ver [%v]", param.mpId, param.mpVer)
		i.CreateVer(param.mpVer)
	}
	i.Lock()
	defer i.Unlock()
	i.buildMultiSnap()
	extents := NewSortedExtents()
	if param.isCache {
		extents = i.Extents
	} else {
		if i.HybridCloudExtents.sortedEks != nil {
			extents = i.HybridCloudExtents.sortedEks.(*SortedExtents)
		}
	}

	delExtents, status = extents.SplitWithCheck(param.mpId, i.Inode, param.ek, i.multiSnap.ekRefMap)
	if status != proto.OpOk {
		log.LogErrorf("action[SplitExtentWithCheck] mpId[%v].status [%v]", param.mpId, status)
		return
	}
	if len(delExtents) == 0 {
		return
	}

	if err = i.CreateLowerVersion(i.getVer(), param.multiVersionList); err != nil {
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(param.mpId, delExtents, param.mpVer, 0); err != nil {
		log.LogErrorf("action[fsmAppendExtentWithCheck] mpId[%v].ino[%v] RestoreMultiSnapExts split error %v", param.mpId, i.Inode, err)
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
	if !clusterEnableSnapshot {
		return
	}
	verlist.RWLock.RLock()
	defer verlist.RWLock.RUnlock()

	log.LogDebugf("CreateLowerVersion inode[%v] curver [%v]", i.Inode, curVer)
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
		log.LogDebugf("CreateLowerVersion nextver [%v] layer 0 ver [%v]", nextVer, i.getLayerVer(0))
		return
	}

	ino := i.CopyDirectly().(*Inode)
	ino.Extents = NewSortedExtents()
	// ino.ObjExtents = NewSortedObjExtents()
	ino.HybridCloudExtents = NewSortedHybridCloudExtents()
	ino.setVer(nextVer)

	log.LogDebugf("action[CreateLowerVersion] inode[%v] create new version [%v] and store old one [%v], hist len [%v]",
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
	isMigration      bool
}

func (i *Inode) AppendExtentWithCheck(param *AppendExtParam) (delExtents []proto.ExtentKey, status uint8) {
	param.ek.SetSeq(param.mpVer)
	log.LogDebugf("action[AppendExtentWithCheck] mpId[%v].mpver [%v] inode[%v] and fsm ver [%v],ek [%v],hist len %v",
		param.mpId, param.mpVer, i.Inode, i.getVer(), param.ek, i.getLayerLen())

	if param.mpVer != i.getVer() {
		log.LogInfof("action[AppendExtentWithCheck] mpId[%v].inode ver [%v]", param.mpId, i.getVer())
		i.CreateVer(param.mpVer)
	}

	i.Lock()
	defer i.Unlock()
	var extents *SortedExtents
	if param.isCache {
		extents = i.Extents
	} else if param.isMigration {
		if i.HybridCloudExtentsMigration.sortedEks == nil {
			i.HybridCloudExtentsMigration.sortedEks = NewSortedExtents()
		}
		extents = i.HybridCloudExtentsMigration.sortedEks.(*SortedExtents)
	} else {
		if i.HybridCloudExtents.sortedEks == nil {
			i.HybridCloudExtents.sortedEks = NewSortedExtents()
		}
		extents = i.HybridCloudExtents.sortedEks.(*SortedExtents)
	}

	refFunc := func(key *proto.ExtentKey) { i.insertEkRefMap(param.mpId, key) }
	delExtents, status = extents.AppendWithCheck(i.Inode, param.ek, refFunc, param.discardExtents)
	if status != proto.OpOk {
		log.LogErrorf("action[AppendExtentWithCheck] mpId[%v].status [%v]", param.mpId, status)
		return
	}
	// TODO:support hybridcloud
	// multi version take effect
	if clusterEnableSnapshot && i.getVer() > 0 && len(delExtents) > 0 {
		var err error
		if err = i.CreateLowerVersion(i.getVer(), param.multiVersionList); err != nil {
			return
		}
		if delExtents, err = i.RestoreExts2NextLayer(param.mpId, delExtents, param.mpVer, 0); err != nil {
			log.LogErrorf("action[AppendExtentWithCheck] mpId[%v].RestoreMultiSnapExts err %v", param.mpId, err)
			return nil, proto.OpErr
		}
	}
	// only update size when write into replicaSystem,
	if proto.IsStorageClassReplica(i.StorageClass) && !param.isCache && !param.isMigration {
		size := extents.Size()
		if i.Size < size {
			i.Size = size
		}
		i.Generation++
		i.ModifyTime = param.ct
	}
	return
}

func (i *Inode) ExtentsTruncate(length uint64, ct int64, doOnLastKey func(*proto.ExtentKey), insertRefMap func(ek *proto.ExtentKey)) (delExtents []proto.ExtentKey) {
	if i.HybridCloudExtents.sortedEks != nil {
		extents := i.HybridCloudExtents.sortedEks.(*SortedExtents)
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
	if !clusterEnableSnapshot {
		return
	}
	log.LogDebugf("[DecSplitExts] mpId [%v] inode[%v]", mpId, i.Inode)
	cnt := len(delExtents.([]proto.ExtentKey))
	for id := 0; id < cnt; id++ {
		ek := &delExtents.([]proto.ExtentKey)[id]
		if !ek.IsSplit() {
			log.LogDebugf("[DecSplitExts] mpId [%v]  ek not split %v", mpId, ek)
			continue
		}
		if i.multiSnap == nil || i.multiSnap.ekRefMap == nil {
			log.LogErrorf("[DecSplitExts] mpid [%v]. inode[%v] multiSnap.ekRefMap is nil", mpId, i.Inode)
			return
		}

		ok, last := i.DecSplitEk(mpId, ek)
		if !ok {
			log.LogErrorf("[DecSplitExts] mpid [%v]. ek [%v] not found!", mpId, ek)
			continue
		}
		if last {
			log.LogDebugf("[DecSplitExts] mpid [%v] ek [%v] split flag be unset to remove all content", mpId, ek)
			ek.SetSplit(false)
		}
	}
}

func (i *Inode) DecSplitEk(mpId uint64, ext *proto.ExtentKey) (ok bool, last bool) {
	log.LogDebugf("[DecSplitEk] mpId[%v] inode[%v] dp [%v] extent id[%v].key %v ext %v", mpId, i.Inode, ext.PartitionId, ext.ExtentId,
		ext.PartitionId<<32|ext.ExtentId, ext)

	if i.multiSnap == nil || i.multiSnap.ekRefMap == nil {
		log.LogErrorf("DecSplitEk. multiSnap %v", i.multiSnap)
		return
	}
	if val, ok := i.multiSnap.ekRefMap.Load(ext.PartitionId<<32 | ext.ExtentId); !ok {
		log.LogErrorf("[DecSplitEk] mpId[%v]. dp [%v] inode[%v] ext not found", mpId, ext.PartitionId, i.Inode)
		return false, false
	} else {
		if val.(uint32) == 0 {
			log.LogErrorf("[DecSplitEk] mpId[%v]. dp [%v] inode[%v] ek ref is zero!", mpId, ext.PartitionId, i.Inode)
			return false, false
		}
		if val.(uint32) == 1 {
			log.LogDebugf("[DecSplitEk] mpId[%v] inode[%v] dp [%v] extent id[%v].key %v", mpId, i.Inode, ext.PartitionId, ext.ExtentId,
				ext.PartitionId<<32|ext.ExtentId)
			i.multiSnap.ekRefMap.Delete(ext.PartitionId<<32 | ext.ExtentId)
			return true, true
		}
		i.multiSnap.ekRefMap.Store(ext.PartitionId<<32|ext.ExtentId, val.(uint32)-1)
		log.LogDebugf("[DecSplitEk] mpId[%v]. extend dp [%v] inode[%v] ek [%v] val %v", mpId, ext.PartitionId, i.Inode, ext, val.(uint32)-1)
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

func (i *Inode) SetDeleteMigrationExtentKeyImmediately() {
	i.Lock()
	i.Flag |= DeleteMigrationExtentKeyFlag
	i.HybridCloudExtentsMigration.expiredTime = time.Now().Unix()
	i.Unlock()
}

func (i *Inode) NeedDeleteMigrationExtentKey() bool {
	i.Lock()
	defer i.Unlock()
	return i.Flag&DeleteMigrationExtentKeyFlag == DeleteMigrationExtentKeyFlag
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
	defer i.RUnlock()

	if i.Flag&DeleteMarkFlag == DeleteMarkFlag {
		return false
	}

	if i.Flag&DeleteMigrationExtentKeyFlag == DeleteMigrationExtentKeyFlag {
		if timeutil.GetCurrentTimeUnix() <= i.HybridCloudExtentsMigration.expiredTime {
			return true
		} else {
			return false
		}
	}

	return i.NLink == 0 && timeutil.GetCurrentTimeUnix()-i.AccessTime < InodeNLink0DelayDeleteSeconds
}

func (i *Inode) ShouldDeleteMigrationExtentKey(isMigration bool) (ok bool) {
	i.RLock()
	defer i.RUnlock()

	ok = isMigration && (i.Flag&DeleteMigrationExtentKeyFlag == DeleteMigrationExtentKeyFlag)
	return
}

// SetAttr sets the attributes of the inode.
func (i *Inode) SetAttr(req *SetattrRequest) {
	log.LogDebugf("action[SetAttr] inode[%v] req seq [%v] inode seq [%v]", i.Inode, req.VerSeq, i.getVer())

	if req.VerSeq != i.getVer() {
		i.CreateVer(req.VerSeq)
	}
	i.Lock()
	log.LogDebugf("action[SetAttr] inode[%v] req seq [%v] inode seq [%v]", i.Inode, req.VerSeq, i.getVer())
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

func (i *Inode) updateStorageClass(storageClass uint32, isCache, isMigration bool) error {
	i.Lock()
	defer i.Unlock()
	// update ek to Extents(SSD layer), no need to update storage class for Inode
	if isCache {
		return nil
	}

	if !proto.IsValidStorageClass(storageClass) {
		err := fmt.Errorf("[updateStorageClass] isCache(%v) isMigration(%v): invalid storageClass(%v)",
			isCache, isMigration, storageClass)
		panic(err.Error())
	}

	if isMigration {
		if i.HybridCloudExtentsMigration.storageClass == proto.StorageClass_Unspecified {
			i.HybridCloudExtentsMigration.storageClass = storageClass
		} else if i.HybridCloudExtentsMigration.storageClass != storageClass {
			return errors.New(fmt.Sprintf("storageClass %v not equal to HybridCloudExtentsMigration.storageClass %v",
				storageClass, i.HybridCloudExtentsMigration.storageClass))
		}
		return nil
	}

	if i.StorageClass == proto.StorageClass_Unspecified {
		i.StorageClass = storageClass
	} else if i.StorageClass == legacyReplicaStorageClass && i.Size == 0 && proto.IsStorageClassBlobStore(storageClass) {
		// empty ebs file
		i.StorageClass = storageClass
	} else if i.StorageClass != storageClass {
		return errors.New(fmt.Sprintf("req.storageClass(%v) not equal to inode.storageClass(%v)",
			storageClass, i.StorageClass))
	}

	return nil
}

func (i *Inode) SetCreateTime(req *SetCreateTimeRequest) {
	log.LogDebugf("action[SetCreateTime] inode %v req seq %v inode seq %v", i.Inode, req.VerSeq, i.getVer())

	if req.VerSeq != i.getVer() {
		i.CreateVer(req.VerSeq)
	}
	i.Lock()
	log.LogDebugf("action[SetCreateTime] inode %v req seq %v inode seq %v", i.Inode, req.VerSeq, i.getVer())

	i.CreateTime = req.CreateTime

	i.Unlock()
}

func (i *Inode) UpdateHybridCloudParams(paramIno *Inode) {
	i.Lock()
	defer i.Unlock()
	i.StorageClass = paramIno.StorageClass
	i.HybridCloudExtents.sortedEks = paramIno.HybridCloudExtents.sortedEks
	i.LeaseExpireTime = paramIno.LeaseExpireTime
	i.ClientID = paramIno.ClientID
	i.HybridCloudExtentsMigration.storageClass = paramIno.HybridCloudExtentsMigration.storageClass
	i.HybridCloudExtentsMigration.sortedEks = paramIno.HybridCloudExtentsMigration.sortedEks
	i.HybridCloudExtentsMigration.expiredTime = paramIno.HybridCloudExtentsMigration.expiredTime
	i.Type = paramIno.Type
}
