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
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/util/exporter"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type DentryResponse struct {
	Status uint8
	Msg    *Dentry
}

func NewDentryResponse() *DentryResponse {
	return &DentryResponse{
		Msg: &Dentry{},
	}
}

func (mp *metaPartition) fsmTxCreateDentry(dbHandle interface{}, txDentry *TxDentry) (status uint8, err error) {

	done, err := mp.txProcessor.txManager.txInRMDone(txDentry.TxInfo.TxID)
	if err != nil {
		return
	}
	if done {
		log.LogWarnf("fsmTxCreateDentry: tx is already finish. txId %s", txDentry.TxInfo.TxID)
		status = proto.OpTxInfoNotExistErr
		return
	}

	txDI := proto.NewTxDentryInfo("", txDentry.Dentry.ParentId, txDentry.Dentry.Name, 0)
	txDenInfo, ok := txDentry.TxInfo.TxDentryInfos[txDI.GetKey()]
	if !ok {
		status = proto.OpTxDentryInfoNotExistErr
		return
	}

	rbDentry := NewTxRollbackDentry(txDentry.Dentry, txDenInfo, TxDelete)
	status, err = mp.txProcessor.txResource.addTxRollbackDentry(dbHandle, rbDentry)
	if err != nil {
		return
	}
	if status == proto.OpExistErr {
		return proto.OpOk, nil
	}

	if status != proto.OpOk {
		return
	}

	defer func() {
		if status != proto.OpOk {
			_, err = mp.txProcessor.txResource.deleteTxRollbackDentry(dbHandle, txDenInfo.ParentId, txDenInfo.Name, txDenInfo.TxID)
			if err != nil {
				log.LogErrorf("[fsmTxCreateDentry] failed to delete rb dentry(%v), err(%v)", txDenInfo, err)
				return
			}
		}
	}()

	return mp.fsmCreateDentry(dbHandle, txDentry.Dentry, false)
}

// Insert a dentry into the dentry tree.
func (mp *metaPartition) fsmCreateDentry(dbHandle interface{}, dentry *Dentry, forceUpdate bool) (status uint8, err error) {
	status = proto.OpOk
	var (
		parIno *Inode
		d      *Dentry
		ok     bool
	)
	parIno, err = mp.inodeTree.Get(dentry.ParentId)
	if err != nil {
		status = proto.OpErr
		return
	}
	if parIno == nil {
		log.LogErrorf("action[fsmCreateDentry] mp %v ParentId [%v] not exist, dentry name [%v], inode [%v]", mp.config.PartitionId, dentry.ParentId, dentry.Name, dentry.Inode)
		status = proto.OpNotExistErr
		return
	}
	if !forceUpdate {
		if parIno.ShouldDelete() {
			log.LogErrorf("action[fsmCreateDentry] mp %v ParentId [%v] get [%v] but should del, dentry name [%v], inode [%v]", mp.config.PartitionId, dentry.ParentId, parIno, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			log.LogErrorf("action[fsmCreateDentry] mp[%v] ParentId [%v] get [%v] but should del, dentry name [%v], inode[%v]", mp.config.PartitionId, dentry.ParentId, parIno, dentry.Name, dentry.Inode)
			status = proto.OpArgMismatchErr
			return
		}
	}
	if d, ok, err = mp.dentryTree.Create(dbHandle, dentry, false); err != nil {
		status = proto.OpErr
		return
	}

	if !ok {
		//do not allow directories and files to overwrite each
		// other when renaming
		if d.isDeleted() {
			log.LogDebugf("action[fsmCreateDentry] mp[%v] newest dentry %v be set deleted flag", mp.config.PartitionId, d)
			d.Inode = dentry.Inode
			if d.getVerSeq() == dentry.getVerSeq() {
				d.setVerSeq(dentry.getSeqFiled())
			} else {
				if d.getSnapListLen() > 0 && d.multiSnap.dentryList[0].isDeleted() {
					d.setVerSeq(dentry.getSeqFiled())
				} else {
					d.addVersion(dentry.getSeqFiled())
				}
			}
			d.Type = dentry.Type
			d.ParentId = dentry.ParentId
			log.LogDebugf("action[fsmCreateDentry.ver] mp[%v] latest dentry already deleted.Now create new one [%v]", mp.config.PartitionId, dentry)

			if !forceUpdate {
				parIno.IncNLink(mp.verSeq)
				parIno.SetMtime()
				if err = mp.inodeTree.Update(dbHandle, parIno); err != nil {
					log.LogErrorf("action[fsmCreateDentry] update parent inode err:%v", err)
					status = proto.OpErr
					return
				}
			}
			return
		} else if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) && !proto.IsSymlink(dentry.Type) && !proto.IsSymlink(d.Type) {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get [%v] but should del, dentry name [%v], inode [%v], type[%v,%v],dir[%v,%v]",
				dentry.ParentId, parIno, dentry.Name, dentry.Inode, dentry.Type, d.Type, proto.IsSymlink(dentry.Type), proto.IsSymlink(d.Type))
			status = proto.OpArgMismatchErr
			return
		} else if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			log.LogDebugf("action[fsmCreateDentry.ver] mp[%v] no need repeat create new one [%v]", mp.config.PartitionId, dentry)
			return
		}
		log.LogErrorf("action[fsmCreateDentry.ver] mp[%v] dentry already exist [%v] and diff with the request [%v]", mp.config.PartitionId, d, dentry)
		status = proto.OpExistErr
		return
	}

	if !forceUpdate {
		log.LogDebugf("[fsmCreateDentry] increase parent link parent(%v)", parIno)
		parIno.IncNLink(mp.verSeq)
		parIno.SetMtime()
		if err = mp.inodeTree.Update(dbHandle, parIno); err != nil {
			log.LogErrorf("action[fsmCreateDentry] update parent inode err:%v", err)
			status = proto.OpErr
			return
		}
	}
	return
}

func (mp *metaPartition) getDentryList(dentry *Dentry) (denList []proto.DetryInfo) {
	item, err := mp.dentryTree.RefGet(dentry.ParentId, dentry.Name)
	if err != nil {
		if err == ErrRocksdbOperation {
			exporter.WarningRocksdbError(fmt.Sprintf("action[getDentry] clusterID[%s] volumeName[%s] partitionID[%v]"+
				" get dentry failed witch rocksdb error[dentry:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, dentry))
		}
		return
	}
	if item != nil {
		if item.getSnapListLen() == 0 {
			return
		}
		for _, den := range item.multiSnap.dentryList {
			denList = append(denList, proto.DetryInfo{
				Inode:  den.Inode,
				Mode:   den.Type,
				IsDel:  den.isDeleted(),
				VerSeq: den.getVerSeq(),
			})
		}
	}
	return
}

// Query a dentry from the dentry tree with specified dentry info.
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8, error) {
	status := proto.OpOk
	d, err := mp.dentryTree.RefGet(dentry.ParentId, dentry.Name)
	if err != nil {
		if err == ErrRocksdbOperation {
			exporter.WarningRocksdbError(fmt.Sprintf("action[getDentry] clusterID[%s] volumeName[%s] partitionID[%v]"+
				" get dentry failed witch rocksdb error[dentry:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, dentry))
		}
		status = proto.OpErr
		return nil, status, err
	}
	if d == nil {
		status = proto.OpNotExistErr
		return nil, status, nil
	}
	log.LogDebugf("action[getDentry] get dentry[%v] by req dentry %v", d, dentry)

	den := mp.getDentryByVerSeq(d, dentry.getSeqFiled())
	if den != nil {
		return den, proto.OpOk, nil
	}
	return den, proto.OpNotExistErr, nil
}

func (mp *metaPartition) fsmTxDeleteDentry(dbHandle interface{}, txDentry *TxDentry) (resp *DentryResponse, err error) {
	resp = NewDentryResponse()
	var (
		item *Dentry
	)
	resp.Status = proto.OpOk
	done, err := mp.txProcessor.txManager.txInRMDone(txDentry.TxInfo.TxID)
	if err != nil {
		return
	}
	if done {
		log.LogWarnf("fsmTxDeleteDentry: tx is already finish. txId %s", txDentry.TxInfo.TxID)
		resp.Status = proto.OpTxInfoNotExistErr
		return
	}

	tmpDen := txDentry.Dentry
	txDI := proto.NewTxDentryInfo("", tmpDen.ParentId, tmpDen.Name, 0)
	txDenInfo, ok := txDentry.TxInfo.TxDentryInfos[txDI.GetKey()]
	if !ok {
		resp.Status = proto.OpTxDentryInfoNotExistErr
		return
	}

	rbDentry := NewTxRollbackDentry(tmpDen, txDenInfo, TxAdd)
	resp.Status, err = mp.txProcessor.txResource.addTxRollbackDentry(dbHandle, rbDentry)
	if err != nil {
		return
	}
	if resp.Status == proto.OpExistErr {
		resp.Status = proto.OpOk
		return
	}

	if resp.Status != proto.OpOk {
		return
	}

	defer func() {
		if resp.Status != proto.OpOk {
			_, err = mp.txProcessor.txResource.deleteTxRollbackDentry(dbHandle, txDenInfo.ParentId, txDenInfo.Name, txDenInfo.TxID)
			if err != nil {
				log.LogErrorf("[fsmTxDeleteDentry] failed to delete rb dentry(%v), err(%v)", txDenInfo, err)
			}
		}
	}()

	item, err = mp.dentryTree.Get(tmpDen.ParentId, tmpDen.Name)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if item == nil || item.Inode != tmpDen.Inode {
		log.LogWarnf("fsmTxDeleteDentry: got wrong dentry, want %v, got %v", tmpDen, item)
		resp.Status = proto.OpNotExistErr
		return
	}

	if _, err = mp.dentryTree.Delete(dbHandle, tmpDen.ParentId, tmpDen.Name); err != nil {
		resp.Status = proto.OpErr
		return
	}
	// parent link count not change
	resp.Msg = item
	return
}

// Delete dentry from the dentry tree.
func (mp *metaPartition) fsmDeleteDentry(dbHandle interface{}, denParm *Dentry, checkInode bool) (resp *DentryResponse, err error) {
	log.LogDebugf("action[fsmDeleteDentry] mp [%v] delete param (%v) seq %v", mp.config.PartitionId, denParm, denParm.getSeqFiled())
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	var (
		denFound *Dentry
		den      *Dentry
		doMore   = true
		clean    bool
	)

	den, err = mp.dentryTree.Get(denParm.ParentId, denParm.Name)
	if err != nil {
		log.LogErrorf("action[fsmDeleteDentry] failed to get dentry, parent(%v) name(%v), err(%v)", denParm.ParentId, denParm.Name, err)
		return
	}

	if checkInode {
		if den.Inode != denParm.Inode {
			den = nil
		}
	}

	if den != nil {
		// NOTE: if no snapshot
		if mp.verSeq == 0 {
			denFound = den
		} else {
			denFound, doMore, clean = den.deleteVerSnapshot(denParm.getSeqFiled(), mp.verSeq, mp.GetVerList())
		}
	}

	// NOTE: dentry not found
	if denFound == nil {
		// NOTE: if enable snapshot
		if mp.verSeq != 0 {
			if doMore {
				resp.Status = proto.OpNotExistErr
				log.LogErrorf("action[fsmDeleteDentry] mp[%v] not found dentry %v", mp.config.PartitionId, denParm)
			} else if den != nil {
				// NOTE: still need to update dentry
				err = mp.dentryTree.Update(dbHandle, den)
				if err != nil {
					return
				}
			}
		}
		return
	}

	// NOTE: snapshot no enable
	if mp.verSeq == 0 {
		_, err = mp.dentryTree.Delete(dbHandle, den.ParentId, den.Name)
		if err != nil {
			return
		}
	} else if clean || (den.getSnapListLen() == 0 && den.isDeleted()) {
		// NOTE: if not other version, delete directly
		log.LogDebugf("action[fsmDeleteDentry] mp[%v] dnetry %v really be deleted", mp.config.PartitionId, den)
		_, err = mp.dentryTree.Delete(dbHandle, den.ParentId, den.Name)
		if err != nil {
			return
		}
	} else {
		// NOTE: there are other version in dentry
		// update it
		err = mp.dentryTree.Update(dbHandle, den)
		if err != nil {
			return
		}
	}

	if !doMore { // not the top layer,do nothing to parent inode
		resp.Msg = denFound
		log.LogDebugf("action[fsmDeleteDentry] mp[%v] there's nothing to do more denParm %v", mp.config.PartitionId, denParm)
		return
	} else {
		var parentIno *Inode
		parentIno, err = mp.inodeTree.Get(denParm.ParentId)
		if err != nil {
			log.LogErrorf("[fsmDeleteDentry] mp(%v) err(%v)", mp.config.PartitionId, err)
			return
		}
		if parentIno == nil {
			log.LogErrorf("[fsmDeleteDentry] mp(%v) parentIno(%v) is nil", mp.config.PartitionId, denParm.ParentId)
			return
		}
		if !parentIno.ShouldDelete() {
			log.LogDebugf("action[fsmDeleteDentry] mp %v den  %v delete parent's link", mp.config.PartitionId, denParm)
			if denParm.getSeqFiled() == 0 {
				parentIno.DecNLink()
			}
			log.LogDebugf("action[fsmDeleteDentry] mp %v inode %v be unlinked by child name %v", mp.config.PartitionId, parentIno.Inode, denParm.Name)
			parentIno.SetMtime()
			if err = mp.inodeTree.Update(dbHandle, parentIno); err != nil {
				return
			}
		}
	}
	resp.Msg = denFound
	return
}

// batch Delete dentry from the dentry tree.
func (mp *metaPartition) fsmBatchDeleteDentry(db DentryBatch) (result []*DentryResponse, err error) {
	result = make([]*DentryResponse, 0, len(db))
	var wrongIndex int
	defer func() {
		if err != nil {
			for index := wrongIndex; index < len(db); index++ {
				result = append(result, &DentryResponse{Status: proto.OpErr, Msg: db[index]})
			}
		}
	}()

	for index, dentry := range db {
		var (
			rsp           *DentryResponse
			dbWriteHandle interface{}
		)
		status, err := mp.dentryInTx(dentry.ParentId, dentry.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			result = append(result, &DentryResponse{Status: status})
			continue
		}
		dbWriteHandle, err = mp.dentryTree.CreateBatchWriteHandle()
		if err != nil {
			wrongIndex = index
			break
		}
		rsp, err = mp.fsmDeleteDentry(dbWriteHandle, dentry, false)
		if err != nil {
			_ = mp.dentryTree.ReleaseBatchWriteHandle(dbWriteHandle)
			wrongIndex = index
			break
		}
		err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, false)
		if err != nil {
			wrongIndex = index
			break
		}
		result = append(result, rsp)
	}
	return
}

func (mp *metaPartition) fsmTxUpdateDentry(dbHandle interface{}, txUpDateDentry *TxUpdateDentry) (resp *DentryResponse, err error) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	var item *Dentry

	done, err := mp.txProcessor.txManager.txInRMDone(txUpDateDentry.TxInfo.TxID)
	if err != nil {
		return
	}
	if done {
		log.LogWarnf("fsmTxUpdateDentry: tx is already finish. txId %s", txUpDateDentry.TxInfo.TxID)
		resp.Status = proto.OpTxInfoNotExistErr
		return
	}

	newDen := txUpDateDentry.NewDentry
	oldDen := txUpDateDentry.OldDentry

	txDI := proto.NewTxDentryInfo("", oldDen.ParentId, oldDen.Name, 0)
	txDenInfo, ok := txUpDateDentry.TxInfo.TxDentryInfos[txDI.GetKey()]
	if !ok {
		resp.Status = proto.OpTxDentryInfoNotExistErr
		return
	}

	item, err = mp.dentryTree.Get(oldDen.ParentId, oldDen.Name)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}

	if item == nil || item.Inode != oldDen.Inode {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmTxUpdateDentry: find dentry is not right, want %v, got %v", oldDen, item)
		return
	}

	rbDentry := NewTxRollbackDentry(txUpDateDentry.OldDentry, txDenInfo, TxUpdate)
	resp.Status, err = mp.txProcessor.txResource.addTxRollbackDentry(dbHandle, rbDentry)
	if err != nil {
		return
	}
	if resp.Status == proto.OpExistErr {
		resp.Status = proto.OpOk
		return
	}

	if resp.Status != proto.OpOk {
		return
	}

	d := item
	d.Inode, newDen.Inode = newDen.Inode, d.Inode
	if err = mp.dentryTree.Update(dbHandle, d); err != nil {
		resp.Status = proto.OpErr
		return
	}
	resp.Msg = newDen
	return
}

func (mp *metaPartition) fsmUpdateDentry(dbHandle interface{}, dentry *Dentry) (
	resp *DentryResponse, err error) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	var d *Dentry
	d, err = mp.dentryTree.Get(dentry.ParentId, dentry.Name)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}

	if d == nil {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmTxUpdateDentry: find dentry is not right, want %v, got %v", dentry, d)
		return
	}
	if d.Inode == dentry.Inode {
		return
	}

	if d.getVerSeq() < mp.GetVerSeq() {
		dn := d.CopyDirectly()
		dn.(*Dentry).setVerSeq(d.getVerSeq())
		d.setVerSeq(mp.GetVerSeq())
		d.multiSnap.dentryList = append([]*Dentry{dn.(*Dentry)}, d.multiSnap.dentryList...)
	}
	d.Inode, dentry.Inode = dentry.Inode, d.Inode
	resp.Msg = dentry
	if err = mp.dentryTree.Update(dbHandle, d); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

func (mp *metaPartition) getDentryByVerSeq(dy *Dentry, verSeq uint64) (d *Dentry) {
	d, _ = dy.getDentryFromVerList(verSeq, false)
	return
}

func (mp *metaPartition) readDirOnly(req *ReadDirOnlyReq) (resp *ReadDirOnlyResp, err error) {
	resp = &ReadDirOnlyResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}

	err = mp.dentryTree.RangeWithPrefix(&Dentry{ParentId: req.ParentID}, begDentry, endDentry, func(den *Dentry) (bool, error) {
		if proto.IsDir(den.Type) {
			d := mp.getDentryByVerSeq(den, req.VerSeq)
			if d == nil {
				return true, nil
			}
			resp.Children = append(resp.Children, proto.Dentry{
				Inode: d.Inode,
				Type:  d.Type,
				Name:  d.Name,
			})
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("readDir failed:[%s]", err.Error())
		return
	}
	return
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp, err error) {
	resp = &ReadDirResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}

	err = mp.dentryTree.RangeWithPrefix(&Dentry{ParentId: req.ParentID}, begDentry, endDentry, func(den *Dentry) (bool, error) {
		d := mp.getDentryByVerSeq(den, req.VerSeq)
		if d == nil {
			return true, nil
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true, nil
	})
	if err != nil {
		log.LogErrorf("readDir failed:[%s]", err.Error())
		return
	}
	return
}

// Read dentry from btree by limit count
// if req.Marker == "" and req.Limit == 0, it becomes readDir
// else if req.Marker != "" and req.Limit == 0, return dentries from pid:name to pid+1
// else if req.Marker == "" and req.Limit != 0, return dentries from pid with limit count
// else if req.Marker != "" and req.Limit != 0, return dentries from pid:marker to pid:xxxx with limit count
func (mp *metaPartition) readDirLimit(req *ReadDirLimitReq) (resp *ReadDirLimitResp, err error) {
	log.LogDebugf("action[readDirLimit] mp [%v] req [%v]", mp.config.PartitionId, req)
	resp = &ReadDirLimitResp{}
	startDentry := &Dentry{
		ParentId: req.ParentID,
	}
	if len(req.Marker) > 0 {
		startDentry.Name = req.Marker
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}

	err = mp.dentryTree.RangeWithPrefix(&Dentry{ParentId: req.ParentID}, startDentry, endDentry, func(den *Dentry) (bool, error) {
		if !proto.IsDir(den.Type) && (req.VerOpt&uint8(proto.FlagsSnapshotDel) > 0) {
			if req.VerOpt&uint8(proto.FlagsSnapshotDelDir) > 0 {
				return true, nil
			}
			if !den.isEffective(req.VerSeq) {
				return true, nil
			}
		}
		d := mp.getDentryByVerSeq(den, req.VerSeq)
		if d == nil {
			return true, nil
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		// Limit == 0 means no limit.
		if req.Limit > 0 && uint64(len(resp.Children)) >= req.Limit {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("readDir failed:[%s]", err.Error())
		return
	}
	log.LogDebugf("action[readDirLimit] mp[%v] resp %v", mp.config.PartitionId, resp)
	return
}
