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

	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"math"
)

// Dentry wraps necessary properties of the `dentry` information in file system.
// Marshal exporterKey:
//  +-------+----------+------+
//  | item  | ParentId | Name |
//  +-------+----------+------+
//  | bytes |    8     | rest |
//  +-------+----------+------+
// Marshal value:
//  +-------+-------+------+
//  | item  | Inode | Type |
//  +-------+-------+------+
//  | bytes |   8   |   4  |
//  +-------+-------+------+
// Marshal entity:
//  +-------+-----------+--------------+-----------+--------------+
//  | item  | KeyLength | MarshaledKey | ValLength | MarshaledVal |
//  +-------+-----------+--------------+-----------+--------------+
//  | bytes |     4     |   KeyLength  |     4     |   ValLength  |
//  +-------+-----------+--------------+-----------+--------------+
type Dentry struct {
	ParentId uint64 // FileID value of the parent inode.
	Name     string // Name of the current dentry.
	Inode    uint64 // FileID value of the current inode.
	Type     uint32
	//snapshot
	VerSeq     uint64
	dentryList DentryBatch
}

func (d *Dentry) getVerSeq() (verSeq uint64) {
	return d.VerSeq & math.MaxInt64
}

func (d *Dentry) isDeleted() bool {
	return (d.VerSeq >> 63) != 0
}

func (d *Dentry) setDeleted() {
	log.LogDebugf("action[setDeleted] d %v be set deleted", d)
	d.VerSeq |= uint64(1) << 63
}

func (d *Dentry) minimizeSeq() (verSeq uint64) {
	cnt := len(d.dentryList)
	if cnt == 0 {
		return d.getVerSeq()
	}
	return d.dentryList[cnt-1].getVerSeq()
}

func (d *Dentry) isEffective(verSeq uint64) bool {
	if verSeq == 0 {
		return false
	}
	if verSeq == math.MaxUint64 {
		verSeq = 0
	}
	return verSeq >= d.minimizeSeq()
}

func (d *Dentry) getDentryFromVerList(verSeq uint64) (den *Dentry, idx int) {

	log.LogInfof("action[getDentryFromVerList] verseq %v, tmp dentry %v, inode id %v, name %v", verSeq, d.getVerSeq(), d.Inode, d.Name)
	if verSeq == 0 || (verSeq >= d.getVerSeq() && verSeq != math.MaxUint64) {
		if d.isDeleted() {
			log.LogDebugf("action[getDentryFromVerList] tmp dentry %v, is deleted, seq %v", d, d.getVerSeq())
			return
		}
		return d, 0
	}

	// read the oldest version snapshot,the oldest version is 0 should make a different with the lastest uncommit version read(with seq 0)
	if verSeq == math.MaxUint64 {
		if d.getVerSeq() == 0 {
			return d, 0
		}
		denListLen := len(d.dentryList)
		if denListLen == 0 {
			return
		}
		den = d.dentryList[denListLen-1]
		if d.dentryList[denListLen-1].getVerSeq() != 0 || d.dentryList[denListLen-1].isDeleted() {
			return nil, 0
		}
		log.LogDebugf("action[getDentryFromVerList] return dentry %v seq %v", den, den.getVerSeq())
		return
	}

	for id, lDen := range d.dentryList {
		log.LogDebugf("action[getDentryFromVerList] den in ver list %v, is delete %v, seq %v", lDen, lDen.isDeleted(), lDen.getVerSeq())
		if verSeq < lDen.getVerSeq() {
			log.LogDebugf("action[getDentryFromVerList] den in ver list %v, return nil, request seq %v, history ver seq %v", lDen, verSeq, lDen.getVerSeq())
		} else {
			if lDen.isDeleted() {
				log.LogDebugf("action[getDentryFromVerList] den in ver list %v, return nil due to latest is deleted", lDen)
				return
			}
			log.LogDebugf("action[getDentryFromVerList] den in ver list %v got", lDen)
			return lDen, id + 1
		}
	}
	log.LogDebugf("action[getDentryFromVerList] den in ver list not found right dentry with seq %v", verSeq)
	return
}

func (d *Dentry) getLastestVer(reqVerSeq uint64, commit bool, verlist []*proto.VolVersionInfo) (uint64, bool) {
	if len(verlist) == 0 {
		return 0, false
	}
	for id, info := range verlist {
		if commit && id == len(verlist)-1 {
			break
		}
		if info.Ver >= reqVerSeq { // include reqSeq itself
			return info.Ver, true
		}
	}

	log.LogDebugf("action[getLastestVer] inode %v reqVerSeq %v not found, the largetst one %v",
		d.Inode, reqVerSeq, verlist[len(verlist)-1].Ver)
	return 0, false
}

// the lastest dentry may be deleted before and set status DentryDeleted,
// the scope of  deleted happened from the DentryDeleted flag owner(include in) to the file with the same name be created is invisible,
// if create anther dentry with larger verSeq, put the deleted dentry to the history list.
// return doMore bool.True means need do next step on caller such as unlink parentIO
func (d *Dentry) deleteVerSnapshot(delVerSeq uint64, mpVerSeq uint64, verlist []*proto.VolVersionInfo) (rd *Dentry, dmore bool, clean bool) { // bool is doMore
	log.LogDebugf("action[deleteVerSnapshot] enter.dentry %v delVerSeq %v mpVer %v verList %v", d, delVerSeq, mpVerSeq, verlist)
	// create denParm version
	if delVerSeq != math.MaxUint64 && delVerSeq > mpVerSeq {
		panic(fmt.Sprintf("Dentry version %v large than mp %v", delVerSeq, mpVerSeq))
	}

	if delVerSeq == 0 {
		if d.isDeleted() {
			log.LogDebugf("action[deleteVerSnapshot.delSeq_0] do noting dentry %v seq %v be deleted before", d, delVerSeq)
			return nil, false, false
		}

		// if there's no snapshot itself, nor have snapshot after dentry's ver then need unlink directly and make no snapshot
		// just move to upper layer,the request snapshot be dropped
		if len(d.dentryList) == 0 {
			var found bool
			// no matter verSeq of dentry is larger than zero,if not be depended then dropped
			_, found = d.getLastestVer(d.getVerSeq(), true, verlist)
			if !found { // no snapshot depend on this dentry,could drop it
				// operate dentry directly
				log.LogDebugf("action[deleteVerSnapshot.delSeq_0] no snapshot depend on this dentry,could drop seq %v dentry %v", delVerSeq, d)
				return d, true, true
			}
		}
		if d.getVerSeq() < mpVerSeq {
			dn := d.Copy()
			dn.(*Dentry).dentryList = nil
			d.dentryList = append([]*Dentry{dn.(*Dentry)}, d.dentryList...)
			log.LogDebugf("action[deleteVerSnapshot.delSeq_0] create version and push to dentry list. dentry %v", dn.(*Dentry))
		}
		d.VerSeq = mpVerSeq
		d.setDeleted() // denParm create at the same version.no need to push to history list
		log.LogDebugf("action[deleteVerSnapshot.delSeq_0] den %v be set deleted at version seq %v", d, mpVerSeq)

		return d, true, false

	} else {
		var (
			idx    int
			den    *Dentry
			endSeq uint64
		)
		if den, idx = d.getDentryFromVerList(delVerSeq); den == nil {
			log.LogDebugf("action[deleteVerSnapshot.inSnapList_del_%v] den %v not found", delVerSeq, d)
			return nil, false, false
		}
		if idx == 0 { // top layer
			// header layer do nothing and be depends on should not be dropped
			log.LogDebugf("action[deleteVerSnapshot.inSnapList_del_%v] den %v first layer do nothing", delVerSeq, d)
			return d, false, false
		}
		// if any alive snapshot in mp dimension exist in seq scope from den to next ascend neighbor, dio snapshot be keep or else drop
		startSeq := den.VerSeq
		realIdx := idx - 1

		if realIdx == 0 {
			endSeq = d.getVerSeq()
		} else {
			endSeq = d.dentryList[realIdx-1].getVerSeq()
			if d.dentryList[realIdx-1].isDeleted() {
				log.LogErrorf("action[deleteVerSnapshot.inSnapList_del_%v] inode %v layer %v name %v be deleted already!", delVerSeq, d.Inode, realIdx, d.dentryList[realIdx-1].Name)
			}
		}

		log.LogDebugf("action[deleteVerSnapshot.inSnapList_del_%v] inode %v try drop multiVersion idx %v effective seq scope [%v,%v) ", delVerSeq,
			d.Inode, realIdx, den.getVerSeq(), endSeq)

		for _, info := range verlist {
			if info.Ver >= startSeq && info.Ver < endSeq { // the version itself not include in
				log.LogDebugf("action[deleteVerSnapshotInList.inSnapList_del_%v] inode %v dir layer idx %v include snapshot %v.don't drop", delVerSeq, den.Inode, realIdx, info.Ver)
				// there's some snapshot depends on the version trying to be deleted,
				// keep it,all the snapshots which depends on this version will reach here when make snapshot delete, and found the scope is minimized
				// other versions depends upon this version will be found zero finally after deletions and do clean
				return den, false, false
			}
			if info.Ver >= endSeq {
				break
			}
			log.LogDebugf("action[deleteVerSnapshotInList.inSnapList_del_%v] inode %v try drop scope [%v, %v), mp ver %v not suitable", delVerSeq, den.Inode, den.VerSeq, endSeq, info.Ver)
		}

		log.LogDebugf("action[deleteVerSnapshotInList.inSnapList_del_%v] inode %v try drop multiVersion idx %v", delVerSeq, den.Inode, realIdx)
		d.dentryList = append(d.dentryList[:realIdx], d.dentryList[realIdx+1:]...)
		return den, false, false
	}

}

func (d *Dentry) String() string {
	str := fmt.Sprintf("dentry(name:[%v],parentId:[%v],inode:[%v],type:[%v],seq:[%v],isDeleted:[%v],dentryList_len[%v])",
		d.Name, d.ParentId, d.Inode, d.Type, d.getVerSeq(), d.isDeleted(), len(d.dentryList))

	for idx, den := range d.dentryList {
		str += fmt.Sprintf("idx:%v,content(%v))", idx, den)
	}
	return str
}

type TxDentry struct {
	ParInode *Inode
	Dentry   *Dentry
	TxInfo   *proto.TransactionInfo
}

func NewTxDentry(parentID uint64, name string, ino uint64, mode uint32, parInode *Inode, txInfo *proto.TransactionInfo) *TxDentry {
	dentry := &Dentry{
		ParentId: parentID,
		Name:     name,
		Inode:    ino,
		Type:     mode,
	}

	txDentry := &TxDentry{
		ParInode: parInode,
		Dentry:   dentry,
		TxInfo:   txInfo,
	}
	return txDentry
}

func (td *TxDentry) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))

	bs, err := td.ParInode.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}

	bs, err = td.Dentry.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}

	bs, err = td.TxInfo.Marshal()
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

func (td *TxDentry) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	var dataLen uint32

	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data := make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}
	parIno := NewInode(0, 0)
	if err = parIno.Unmarshal(data); err != nil {
		return
	}
	td.ParInode = parIno

	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data = make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	dentry := &Dentry{}
	if err = dentry.Unmarshal(data); err != nil {
		return
	}
	td.Dentry = dentry

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
	td.TxInfo = txInfo
	return
}

type TxUpdateDentry struct {
	OldDentry *Dentry
	NewDentry *Dentry
	TxInfo    *proto.TransactionInfo
}

func NewTxUpdateDentry(oldDentry *Dentry, newDentry *Dentry, txInfo *proto.TransactionInfo) *TxUpdateDentry {
	txUpdateDentry := &TxUpdateDentry{
		OldDentry: oldDentry,
		NewDentry: newDentry,
		TxInfo:    txInfo,
	}
	return txUpdateDentry
}

func (td *TxUpdateDentry) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	bs, err := td.OldDentry.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}

	bs, err = td.NewDentry.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}

	bs, err = td.TxInfo.Marshal()
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

func (td *TxUpdateDentry) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	var dataLen uint32
	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data := make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	oldDentry := &Dentry{}
	if err = oldDentry.Unmarshal(data); err != nil {
		return
	}
	td.OldDentry = oldDentry

	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data = make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	newDentry := &Dentry{}
	if err = newDentry.Unmarshal(data); err != nil {
		return
	}
	td.NewDentry = newDentry

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
	td.TxInfo = txInfo
	return
}

type DentryBatch []*Dentry

// todo(leon chang), buffer need alloc first before and write directly consider the space and performance

// Marshal marshals a dentry into a byte array.
func (d *Dentry) Marshal() (result []byte, err error) {
	keyBytes := d.MarshalKey()
	valBytes := d.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(int(keyLen + valLen + 8))

	//log.LogInfof("action[dentry.Marshal] dentry name %v inode %v parent %v seq %v keyLen  %v valLen %v total len %v",
	//	d.Name, d.Inode, d.ParentId, d.VerSeq, keyLen, valLen, int(keyLen+valLen+8))

	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {
		return nil, err
	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// Unmarshal unmarshals the dentry from a byte array.
func (d *Dentry) Unmarshal(raw []byte) (err error) {
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
	if err = d.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = d.UnmarshalValue(valBytes)
	return
}

// Marshal marshals the dentryBatch into a byte array.
func (d DentryBatch) Marshal() ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, uint32(len(d))); err != nil {
		return nil, err
	}
	for _, dentry := range d {
		bs, err := dentry.Marshal()
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

// Unmarshal unmarshals the dentryBatch.
func DentryBatchUnmarshal(raw []byte) (DentryBatch, error) {
	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(DentryBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		den := &Dentry{}
		if err := den.Unmarshal(data); err != nil {
			return nil, err
		}
		result = append(result, den)
	}

	return result, nil
}

// Less tests whether the current dentry is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (d *Dentry) Less(than BtreeItem) (less bool) {
	dentry, ok := than.(*Dentry)
	less = ok && ((d.ParentId < dentry.ParentId) || ((d.ParentId == dentry.ParentId) && (d.Name < dentry.Name)))
	return
}

func (d *Dentry) Copy() BtreeItem {
	newDentry := *d
	return &newDentry
}

// MarshalKey is the bytes version of the MarshalKey method which returns the byte slice result.
func (d *Dentry) MarshalKey() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(32)
	if err := binary.Write(buff, binary.BigEndian, &d.ParentId); err != nil {
		panic(err)
	}
	buff.Write([]byte(d.Name))
	k = buff.Bytes()
	return
}

// UnmarshalKey unmarshals the exporterKey from bytes.
func (d *Dentry) UnmarshalKey(k []byte) (err error) {
	buff := bytes.NewBuffer(k)
	if err = binary.Read(buff, binary.BigEndian, &d.ParentId); err != nil {
		return
	}
	d.Name = string(buff.Bytes())
	return
}

// MarshalValue marshals the exporterKey to bytes.
func (d *Dentry) MarshalValue() (k []byte) {

	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(20 + len(d.dentryList)*20)
	if err := binary.Write(buff, binary.BigEndian, &d.Inode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.Type); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.VerSeq); err != nil {
		panic(err)
	}

	for _, dd := range d.dentryList {
		if err := binary.Write(buff, binary.BigEndian, &dd.Inode); err != nil {
			panic(err)
		}
		if err := binary.Write(buff, binary.BigEndian, &dd.Type); err != nil {
			panic(err)
		}
		if err := binary.Write(buff, binary.BigEndian, &dd.VerSeq); err != nil {
			panic(err)
		}
	}

	k = buff.Bytes()
	log.LogInfof("action[MarshalValue] dentry name %v, inode %v, parent inode %v, val len %v", d.Name, d.Inode, d.ParentId, len(k))
	return
}

func (d *Dentry) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &d.Inode); err != nil {
		return
	}
	err = binary.Read(buff, binary.BigEndian, &d.Type)
	log.LogInfof("action[UnmarshalValue] dentry name %v, inode %v, parent inode %v, val len %v", d.Name, d.Inode, d.ParentId, len(val))
	if len(val) >= 20 {
		err = binary.Read(buff, binary.BigEndian, &d.VerSeq)
		if (len(val)-20)%20 != 0 {
			return fmt.Errorf("action[UnmarshalSnapshotValue] left len %v after divide by dentry len", len(val)-20)
		}
		for i := 0; i < (len(val)-20)/20; i++ {
			//todo(leonchang) name and parentid should be removed to reduce space
			den := &Dentry{
				Name:     d.Name,
				ParentId: d.ParentId,
			}
			if err = binary.Read(buff, binary.BigEndian, &den.Inode); err != nil {
				return
			}
			if err = binary.Read(buff, binary.BigEndian, &den.Type); err != nil {
				return
			}
			if err = binary.Read(buff, binary.BigEndian, &den.VerSeq); err != nil {
				return
			}
			d.dentryList = append(d.dentryList, den)
			log.LogInfof("action[UnmarshalValue] dentry name %v, inode %v, parent inode %v, val len %v", den.Name, den.Inode, den.ParentId, len(val))
		}
	}
	return
}
