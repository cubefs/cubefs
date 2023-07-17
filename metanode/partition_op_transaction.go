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
// permissions and limitations under the License.k

package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync"
)

func (mp *metaPartition) TxCreate(req *proto.TxCreateRequest, p *Packet) error {

	var err error
	txInfo := req.TransactionInfo.GetCopy()

	// 1. init tx in tm
	ifo, err := mp.txInit(txInfo, p)
	if err != nil || ifo == nil {
		return err
	}

	if ifo.TmID != int64(mp.config.PartitionId) {
		p.PacketOkReply()
		return nil
	}

	if ifo.State != proto.TxStatePreCommit {
		log.LogWarnf("TxCreate: tx is already init, txInfo %s", ifo.String())
		p.PacketOkReply()
		return nil
	}

	// 2. add tx to other rm
	mp.txInitToRm(ifo, p)
	if p.ResultCode != proto.OpOk {
		return nil
	}

	resp := &proto.TxCreateResponse{
		TxInfo: ifo,
	}

	status := proto.OpOk
	reply, err := json.Marshal(resp)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return nil
}

func (mp *metaPartition) txInitToRm(txInfo *proto.TransactionInfo, p *Packet) {
	mpIfos := txInfo.GroupByMp()
	statusCh := make(chan uint8, len(mpIfos))
	wg := sync.WaitGroup{}

	for mpId, ifo := range mpIfos {
		if mp.config.PartitionId == mpId {
			continue
		}

		req := &proto.TxCreateRequest{
			VolName:         mp.config.VolName,
			PartitionID:     mpId,
			TransactionInfo: txInfo,
		}

		pkt, _ := buildTxPacket(req, mpId, proto.OpMetaTxCreate)
		members := ifo.Members
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := mp.txProcessor.txManager.txSendToMpWithAddrs(members, pkt)
			if status != proto.OpOk {
				log.LogWarnf("txInitRm: send to rm failed, addr %s, pkt %s", members, string(pkt.Data))
			}
			statusCh <- status
		}()
	}

	wg.Wait()
	close(statusCh)

	for status := range statusCh {
		if !canRetry(status) {
			p.ResultCode = status
			return
		}

		if status != proto.OpOk {
			p.ResultCode = status
		}
	}

	p.ResultCode = proto.OpOk
	return
}

func canRetry(status uint8) bool {
	if status == proto.OpOk || status == proto.OpAgain || status == proto.OpErr {
		return true
	}
	return false
}

func (mp *metaPartition) txInit(txInfo *proto.TransactionInfo, p *Packet) (ifo *proto.TransactionInfo, err error) {
	if uint64(txInfo.TmID) == mp.config.PartitionId {
		mp.initTxInfo(txInfo)
	}

	val, err := txInfo.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return nil, err
	}

	status, err := mp.submit(opFSMTxInit, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return nil, err
	}

	if status.(uint8) != proto.OpOk {
		p.ResultCode = status.(uint8)
		return nil, fmt.Errorf("init tx by raft failed, %v", status)
	}

	ifo = mp.txProcessor.txManager.getTransaction(txInfo.TxID)
	if ifo == nil {
		log.LogWarnf("TxCreate: tx is still not exist, info %s", txInfo.String())
		p.ResultCode = proto.OpTxInfoNotExistErr
		return nil, nil
	}

	return ifo, nil
}

// TxCommitRM used to commit tx for single TM or RM
func (mp *metaPartition) TxCommitRM(req *proto.TxApplyRMRequest, p *Packet) error {
	txInfo := req.TransactionInfo.GetCopy()

	ifo := mp.txProcessor.txManager.getTransaction(txInfo.TxID)
	if ifo == nil {
		log.LogWarnf("TxCommitRM: can't find tx, already rollback or commit, ifo %v", req.TransactionInfo)
		p.PacketErrorWithBody(proto.OpTxInfoNotExistErr, []byte(fmt.Sprintf("tx %s is not exist", txInfo.TxID)))
		return nil
	}

	if ifo.Finish() {
		log.LogWarnf("TxCommitRM: tx already commit before in rm, tx %v", ifo)
		p.ResultCode = proto.OpOk
		return nil
	}

	val, err := ifo.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	status, err := mp.submit(opFSMTxCommitRM, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	p.ResultCode = status.(uint8)
	return nil
}

// TxRollbackRM used to rollback tx for single TM or RM
func (mp *metaPartition) TxRollbackRM(req *proto.TxApplyRMRequest, p *Packet) error {
	txInfo := req.TransactionInfo.GetCopy()

	ifo := mp.txProcessor.txManager.getTransaction(txInfo.TxID)
	if ifo == nil {
		log.LogWarnf("TxRollbackRM: can't find tx, already rollback or commit, ifo %v", req.TransactionInfo)
		p.PacketErrorWithBody(proto.OpTxInfoNotExistErr, []byte(fmt.Sprintf("tx %s is not exist", txInfo.TxID)))
		return nil
	}

	if ifo.Finish() {
		log.LogWarnf("TxRollbackRM: tx already commit before in rm, tx %v", ifo)
		p.ResultCode = proto.OpOk
		return nil
	}

	val, err := txInfo.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	status, err := mp.submit(opFSMTxRollbackRM, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	p.ResultCode = status.(uint8)
	return nil
}

func (mp *metaPartition) TxCommit(req *proto.TxApplyRequest, p *Packet) error {

	status, err := mp.txProcessor.txManager.commitTx(req.TxID, false)
	if err != nil {
		p.PacketErrorWithBody(status, []byte(err.Error()))
		return err
	}
	p.ResultCode = status
	return err
}

func (mp *metaPartition) TxRollback(req *proto.TxApplyRequest, p *Packet) error {

	status, err := mp.txProcessor.txManager.rollbackTx(req.TxID, false)
	if err != nil {
		p.PacketErrorWithBody(status, []byte(err.Error()))
		return err
	}
	p.ResultCode = status
	return err
}

func (mp *metaPartition) TxGetCnt() (uint64, uint64, uint64) {
	txCnt := mp.txProcessor.txManager.txTree.Len()
	rbInoCnt := mp.txProcessor.txResource.txRbInodeTree.Len()
	rbDenCnt := mp.txProcessor.txResource.txRbDentryTree.Len()
	return uint64(txCnt), uint64(rbInoCnt), uint64(rbDenCnt)
}

func (mp *metaPartition) TxGetTree() (*BTree, *BTree, *BTree) {
	tx := mp.txProcessor.txManager.txTree.GetTree()
	rbIno := mp.txProcessor.txResource.txRbInodeTree.GetTree()
	rbDen := mp.txProcessor.txResource.txRbDentryTree.GetTree()
	return tx, rbIno, rbDen
}

func (mp *metaPartition) TxGetInfo(req *proto.TxGetInfoRequest, p *Packet) (err error) {
	var status uint8

	txItem := proto.NewTxInfoBItem(req.TxID)
	var txInfo *proto.TransactionInfo
	if item := mp.txProcessor.txManager.txTree.Get(txItem); item != nil {
		txInfo = item.(*proto.TransactionInfo)
		status = proto.OpOk
	} else {
		status = proto.OpNotExistErr
	}

	var reply []byte
	resp := &proto.TxGetInfoResponse{
		TxInfo: txInfo,
	}
	reply, err = json.Marshal(resp)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}

	p.PacketErrorWithBody(status, reply)
	return err
}
