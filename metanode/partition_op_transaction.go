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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) TxCommit(req *proto.TxApplyRequest, p *Packet) error {

	status, err := mp.txProcessor.txManager.commitTransaction(req, false)
	if err != nil {
		p.PacketErrorWithBody(status, []byte(err.Error()))
		return err
	}
	p.ResultCode = status

	return err
}

func (mp *metaPartition) TxRollback(req *proto.TxApplyRequest, p *Packet) error {

	status, err := mp.txProcessor.txManager.rollbackTransaction(req, RbFromClient, false)
	if err != nil {
		/*if status == proto.OpTxInfoNotExistErr {
			p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		} else {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		}*/
		p.PacketErrorWithBody(status, []byte(err.Error()))
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

	status, err := mp.submit(opFSMTxInodeCommit, val)
	if err != nil {
		log.LogErrorf("TxInodeCommit: commit inode[%v] failed, err: %v", req.Inode, err)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

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

func (mp *metaPartition) TxDentryCommit(req *proto.TxDentryApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}

	status, err := mp.submit(opFSMTxDentryCommit, val)
	if err != nil {
		log.LogErrorf("TxDentryCommit: commit dentry[%v_%v] failed, err: %v", req.Pid, req.Name, err)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

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
