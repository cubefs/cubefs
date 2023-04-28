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
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) TxCommit(req *proto.TxApplyRequest, p *Packet) error {
	//val, err := json.Marshal(req)
	//if err != nil {
	//	return
	//}
	//status, err := mp.submit(opFSMTxCommit, val)
	//if err != nil {
	//	p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
	//	return
	//}
	//p.ResultCode = status.(uint8)

	//don't submit
	status, err := mp.txProcessor.txManager.commitTransaction(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status

	return err
}

func (mp *metaPartition) TxRollback(req *proto.TxApplyRequest, p *Packet) error {

	status, err := mp.txProcessor.txManager.rollbackTransaction(req, RbFromClient)
	if err != nil {
		if status == proto.OpTxInfoNotExistErr {
			p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		} else {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		}

		return err
	}
	p.ResultCode = status

	return err
}

func (mp *metaPartition) TxInodeCommit(req *proto.TxInodeApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}

	mp.txProcessor.txResource.RLock()
	rbInode := mp.txProcessor.txResource.getTxRbInode(req.Inode)
	mp.txProcessor.txResource.RUnlock()
	if rbInode == nil {
		errInfo := fmt.Sprintf("TxInodeCommit: commit inode[%v] failed, rb inode not found", req.Inode)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		p.PacketErrorWithBody(proto.OpTxRbInodeNotExistErr, []byte(err.Error()))
		return err
	}

	status, err := mp.submit(opFSMTxInodeCommit, val)
	if err != nil {
		log.LogErrorf("TxInodeCommit: commit inode[%v] failed, err: %v", req.Inode, err)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)
	//when commit successfully, return rollback item for future use in case any other item is not committed successfully.
	if p.ResultCode == proto.OpOk {

		body, err := rbInode.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpTxRollbackItemErr, []byte(err.Error()))
			return err
		}
		p.PacketOkWithBody(body)
		log.LogDebugf("TxInodeCommit: commit inode[%v] done, txinfo[%v]", req.Inode, rbInode.txInodeInfo)
	}

	return err
}

func (mp *metaPartition) TxInodeRollback(req *proto.TxInodeApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, err := mp.submit(opFSMTxInodeRollback, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}

func (mp *metaPartition) TxRestoreRollbackInode(req *proto.TxRestoreRollbackInodeRequest, p *Packet) (err error) {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}

	status, err := mp.submit(opFSMTxRestoreRollbackInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}

func (mp *metaPartition) TxDentryCommit(req *proto.TxDentryApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}

	mp.txProcessor.txResource.RLock()
	rbDentry := mp.txProcessor.txResource.getTxRbDentry(req.Pid, req.Name)
	mp.txProcessor.txResource.RUnlock()
	if rbDentry == nil {
		errInfo := fmt.Sprintf("TxDentryCommit: commit dentry[%v_%v] failed, rb dentry not found", req.Pid, req.Name)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		p.PacketErrorWithBody(proto.OpTxRbDentryNotExistErr, []byte(err.Error()))
		return err
	}

	status, err := mp.submit(opFSMTxDentryCommit, val)
	if err != nil {
		log.LogErrorf("TxDentryCommit: commit dentry[%v_%v] failed, err: %v", req.Pid, req.Name, err)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	if p.ResultCode == proto.OpOk {

		body, err := rbDentry.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpTxRollbackItemErr, []byte(err.Error()))
			return err
		}
		p.PacketOkWithBody(body)
		log.LogDebugf("TxDentryCommit: commit dentry[%v_%v] done, txinfo[%v]", req.Pid, req.Name, rbDentry.txDentryInfo)
	}
	return err
}

func (mp *metaPartition) TxDentryRollback(req *proto.TxDentryApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, err := mp.submit(opFSMTxDentryRollback, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}

func (mp *metaPartition) TxRestoreRollbackDentry(req *proto.TxRestoreRollbackDentryRequest, p *Packet) (err error) {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}

	status, err := mp.submit(opFSMTxRestoreRollbackDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}
