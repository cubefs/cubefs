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

import "github.com/cubefs/cubefs/proto"

func (mp *metaPartition) fsmTxRollback(txID string) (status uint8) {
	//status = proto.OpOk
	var err error
	status, err = mp.txProcessor.txManager.rollbackTxInfo(txID)
	if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxInfoNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}
}

func (mp *metaPartition) fsmTxInodeRollback(req *proto.TxInodeApplyRequest) (status uint8) {
	//status = proto.OpOk
	//var err error
	status, _ = mp.txProcessor.txResource.rollbackInode(req)
	return
}

func (mp *metaPartition) fsmTxDentryRollback(req *proto.TxDentryApplyRequest) (status uint8) {
	//status = proto.OpOk
	//var err error
	status, _ = mp.txProcessor.txResource.rollbackDentry(req)
	return
}

func (mp *metaPartition) fsmTxSetState(req *proto.TxSetStateRequest) (status uint8) {
	status, _ = mp.txProcessor.txManager.txSetState(req)
	return
}

func (mp *metaPartition) fsmTxCommit(txID string) (status uint8) {
	//var err error
	status, _ = mp.txProcessor.txManager.commitTxInfo(txID)
	/*if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxInfoNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}*/
	return
}

func (mp *metaPartition) fsmTxInodeCommit(txID string, inode uint64) (status uint8) {
	//var err error
	status, _ = mp.txProcessor.txResource.commitInode(txID, inode)
	/*if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxRbInodeNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}*/
	return
}

func (mp *metaPartition) fsmTxDentryCommit(txID string, pId uint64, name string) (status uint8) {
	//var err error
	status, _ = mp.txProcessor.txResource.commitDentry(txID, pId, name)
	/*if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxRbDentryNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}*/
	return
}
