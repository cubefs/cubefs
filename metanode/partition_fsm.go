// Copyright 2018 The Chubao Authors.
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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"hash/crc32"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/tracing"

	"io/ioutil"
	"os"
	"path"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

// Apply applies the given operational commands.
func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaItem{}
	defer func() {
		if err != nil {
			log.LogErrorf("Mp[%d] action[Apply] failed,index:%v,msg:%v,resp:%v", mp.config.PartitionId, index, msg, resp)
			return
		}
		mp.uploadApplyID(index)
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	var ctx = context.Background()
	if tracing.IsEnabled() {
		var tracer = tracing.NewTracer("metaPartition.Apply").SetTag("op", msg.Op)
		defer tracer.Finish()
		ctx = tracer.Context()
	}

	switch msg.Op {
	case opFSMCreateInode:
		mp.monitorData[statistics.ActionMetaCreateInode].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		resp = mp.fsmCreateInode(ino)
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp = mp.fsmUnlinkInode(ino)
	case opFSMUnlinkInodeBatch:
		inodes, err := InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmUnlinkInodeBatch(inodes)
	case opFSMExtentTruncate:
		mp.monitorData[statistics.ActionMetaTruncate].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsTruncate(ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp = mp.fsmCreateLinkInode(ino)
	case opFSMEvictInode:
		mp.monitorData[statistics.ActionMetaEvictInode].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp = mp.fsmEvictInode(ino)
	case opFSMEvictInodeBatch:
		inodes, err := InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchEvictInode(inodes)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		resp, err = mp.fsmSetAttr(req)
	case opFSMCreateDentry:
		mp.monitorData[statistics.ActionMetaCreateDentry].UpdateData(0)

		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmCreateDentry(den, false)
	case opFSMDeleteDentry:
		mp.monitorData[statistics.ActionMetaDeleteDentry].UpdateData(0)

		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmDeleteDentry(den, false)
	case opFSMDeleteDentryBatch:
		db, err := DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchDeleteDentry(db)
	case opFSMUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmUpdateDentry(den)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmUpdatePartition(req.End)
	case opFSMExtentsAdd:
		mp.monitorData[statistics.ActionMetaExtentsAdd].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtents(ctx, ino)
	case opFSMExtentsInsert:
		mp.monitorData[statistics.ActionMetaExtentsInsert].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp = mp.fsmInsertExtents(ctx, ino)
	case opFSMStoreTick:
		inodeTree := mp.getInodeTree()
		dentryTree := mp.getDentryTree()
		extendTree := mp.extendTree.GetTree()
		multipartTree := mp.multipartTree.GetTree()
		msg := &storeMsg{
			command:       opFSMStoreTick,
			applyIndex:    index,
			inodeTree:     inodeTree,
			dentryTree:    dentryTree,
			extendTree:    extendTree,
			multipartTree: multipartTree,
		}
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(msg.V)
	case opFSMCursorReset:
		resp, err = mp.internalCursorReset(msg.V)
	case opFSMInternalDeleteInodeBatch:
		err = mp.internalDeleteBatch(ctx, msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmSetXAttr(extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmRemoveXAttr(extend)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmCreateMultipart(multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmRemoveMultipart(multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmAppendMultipart(multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		if cursor > mp.config.Cursor {
			mp.config.Cursor = cursor
		}
	}

	return
}

// ApplyMemberChange  apply changes to the raft member.
func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	// change memory status
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		//updated, err = mp.confUpdateNode(req, index)
	case raftproto.ConfAddLearner:
		req := &proto.AddMetaPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddLearner(req, index)
	case raftproto.ConfPromoteLearner:
		req := &proto.PromoteMetaPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confPromoteLearner(req, index)
	}
	if err != nil {
		return
	}
	if updated {
		mp.config.sortPeers()
		if err = mp.persistMetadata(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] err[%v].", err)
			return
		}
	}
	return
}

// Snapshot returns the snapshot of the current meta partition.
func (mp *metaPartition) Snapshot() (snap raftproto.Snapshot, err error) {
	var isSnapshotBatchSend = false
	for _, peer := range mp.config.Peers {
		addr := peer.Addr
		ctx := context.Background()
		var (
			err error
			conn *net.TCPConn
		)
		if conn, err = mp.config.ConnPool.GetConnect(addr); err != nil {
			err = errors.Trace(err, "get connection failed")
			log.LogErrorf("get connection from %v failed", addr)
			goto end
		}
		defer func() {
			if err != nil {
				mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
			} else {
				mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
			}
		}()
		p := NewPacketGetMetaNodeVersionInfo(ctx)
		if err = p.WriteToConn(conn); err != nil {
			err = errors.Trace(err, "write packet to connection failed")
			log.LogErrorf("write packet to connection failed, peer addr: %v", addr)
			goto end
		}
		if err = p.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
			log.LogErrorf("readFromConn err: %v", err)
			goto end
		}
		if p.ResultCode != proto.OpOk {
			log.LogErrorf("resultCode=[%v] expectRes=[%v]",p.ResultCode, proto.OpOk)
			goto end
		}
		var metaNodeVersion MetaNodeVersion
		if err := json.Unmarshal(p.Data, &metaNodeVersion); err != nil {
			log.LogErrorf("json.Unmarshal err: %v", err)
			goto end
		}
		snapshotBatchSendSupport, err := NewMetaNodeVersion(snapshotBatchSendMinimumVersion)
		if err != nil{
			log.LogErrorf("get snapshotBatchSendSupport failed, err: %v", err)
			goto end
		}
		if metaNodeVersion.LessThan(*snapshotBatchSendSupport){
			goto end
		}
		isSnapshotBatchSend = true
	}
	if isSnapshotBatchSend {
		log.LogInfof("snapshot: use newMetaItemIteratorV2 to batch send items.")
		snap, err = newMetaItemIteratorV2(mp)
		return
	}
end:
	log.LogInfof("snapshot: use MetaItemIterator to send an item")
	snap, err = newMetaItemIterator(mp)
	return
}

// ApplySnapshot applies the given snapshots.
func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	var (
		data          []byte
		index         int
		appIndexID    uint64
		cursor        uint64
		inodeTree     = NewBtree()
		dentryTree    = NewBtree()
		extendTree    = NewBtree()
		multipartTree = NewBtree()
		wg               sync.WaitGroup
		inodeBatchCh     = make(chan InodeBatch, 128)
		dentryBatchCh    = make(chan DentryBatch, 128)
		multipartBatchCh = make(chan MultipartBatch, 128)
		extendBatchCh    = make(chan ExtendBatch, 128)
		snapshotSign = crc32.NewIEEE()
		snapshotCrcStoreSuffix = "snapshotCrc"
		leaderCrc string
	)
	defer func() {
		close(inodeBatchCh)
		close(dentryBatchCh)
		close(multipartBatchCh)
		close(extendBatchCh)
		wg.Wait()
		if err == io.EOF {
			crc := snapshotSign.Sum32()
			if len(data) >3 {
				leaderCrc = strconv.FormatUint(uint64(binary.BigEndian.Uint32(data[0:4])),10)
			}else{
				leaderCrc = " "
			}
			crcStoreFile := fmt.Sprintf("%s/%s", mp.config.RootDir, snapshotCrcStoreSuffix)
			var crcBuff = bytes.NewBuffer(make([]byte, 0, 16))
			storeStr := fmt.Sprintf("%v, %v, %v", time.Now().Format("2006-01-02 15:04:05") , leaderCrc, crc)
			crcBuff.WriteString(storeStr)
			if err = ioutil.WriteFile(crcStoreFile, crcBuff.Bytes(), 0775); err != nil {
				log.LogWarnf("write to file failed, err is :%v", err)
			}

			mp.applyID = appIndexID
			mp.inodeTree = inodeTree
			mp.dentryTree = dentryTree
			mp.extendTree = extendTree
			mp.multipartTree = multipartTree
			if cursor != 0 {
				mp.config.Cursor = cursor
			}
			err = nil
			// store message
			mp.storeChan <- &storeMsg{
				command:       opFSMStoreTick,
				applyIndex:    mp.applyID,
				inodeTree:     mp.inodeTree,
				dentryTree:    mp.dentryTree,
				extendTree:    mp.extendTree,
				multipartTree: mp.multipartTree,
			}
			mp.extReset <- struct{}{}
			log.LogDebugf("ApplySnapshot: finish with EOF: partitionID(%v) applyID(%v),cursor(%v)", mp.config.PartitionId, mp.applyID, mp.config.Cursor)
			return
		}
		log.LogErrorf("ApplySnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
	}()

	wg.Add(4)
	go func() {
		defer wg.Done()
		for inodes := range inodeBatchCh {
			for _, ino := range inodes {
				if cursor < ino.Inode {
					cursor = ino.Inode
				}
				inodeTree.ReplaceOrInsert(ino, true)
				log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for dentries := range dentryBatchCh {
			for _, dentry := range dentries {
				dentryTree.ReplaceOrInsert(dentry, true)
				log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for multiparts := range multipartBatchCh {
			for _, multipart := range multiparts {
				multipartTree.ReplaceOrInsert(multipart, true)
				log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for extends := range extendBatchCh {
			for _, extend := range extends {
				extendTree.ReplaceOrInsert(extend, true)
				log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
					mp.config.PartitionId, extend)
			}
		}
	}()

	ctx := context.Background()
	for {
		data, err = iter.Next()
		if err != nil {
			return
		}
		if index == 0 {
			appIndexID = binary.BigEndian.Uint64(data)
			lenData := make([]byte,4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil{
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			index++
			continue
		}
		snap := NewMetaItem(0, nil, nil)
		if err = snap.UnmarshalBinary(data); err != nil {
			return
		}
		index++
		switch snap.Op {
		case opFSMCreateInode:
			ino := NewInode(0, 0)

			// TODO Unhandled errors
			if mp.marshalVersion == MetaPartitionMarshVersion2 {
				if err = ino.UnmarshalV2WithKeyAndValue(ctx, snap.K, snap.V); err != nil {
					return
				}
			} else {
				if err = ino.UnmarshalKey(snap.K); err != nil {
					return
				}
				if err = ino.UnmarshalValue(ctx, snap.V); err != nil {
					return
				}
			}
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			inodeTree.ReplaceOrInsert(ino, true)
			log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)
		case opFSMCreateDentry:
			dentry := &Dentry{}
			if mp.marshalVersion == MetaPartitionMarshVersion2 {
				if err = dentry.UnmarshalV2WithKeyAndValue(snap.K, snap.V); err != nil{
					return
				}
			} else {
				if err = dentry.UnmarshalKey(snap.K); err != nil {
					return
				}
				if err = dentry.UnmarshalValue(snap.V); err != nil {
					return
				}
			}
			dentryTree.ReplaceOrInsert(dentry, true)
			log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			extendTree.ReplaceOrInsert(extend, true)
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
				mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			var multipart = MultipartFromBytes(snap.V)
			multipartTree.ReplaceOrInsert(multipart, true)
			log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opExtentFileSnapshot:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = ioutil.WriteFile(fileName, snap.V, 0644); err != nil {
				log.LogErrorf("ApplySnapshot: write snap extent delete file fail: partitionID(%v) err(%v)",
					mp.config.PartitionId, err)
			}
			log.LogDebugf("ApplySnapshot: write snap extent delete file: partitonID(%v) filename(%v).",
				mp.config.PartitionId, fileName)
			lenData := make([]byte,4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil{
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
		case opFSMBatchCreate:
			var mulItems *MulItems
			mulItems, err = MulItemsUnmarshal(ctx, snap.V)
			if err != nil{
				return
			}
			inodeBatchCh <- mulItems.InodeBatches
			dentryBatchCh <- mulItems.DentryBatches
			multipartBatchCh <- mulItems.MultipartBatches
			extendBatchCh <- mulItems.ExtendBatches
			lenData := make([]byte,4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil{
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
		default:
			err = fmt.Errorf("unknown op=%d", snap.Op)
			return
		}
	}
}

// HandleFatalEvent handles the fatal errors.
func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.Warning(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	exporter.Warning(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
	if mp.config.NodeId == leader {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", serverPort), time.Second)
		if err != nil {
			log.LogErrorf(fmt.Sprintf("HandleLeaderChange serverPort not exsit ,error %v", err))
			go mp.raftPartition.TryToLeader(mp.config.PartitionId)
			return
		}
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		go mp.initInode(ino)
	}
}

func (mp *metaPartition) submit(ctx context.Context, op uint32, from string, data []byte) (resp interface{}, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("metaPartition.submit").
		SetTag("op", op).
		SetTag("size", len(data))
	defer tracer.Finish()
	ctx = tracer.Context()

	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	snap.From = from
	snap.Timestamp = time.Now().Unix()

	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.SubmitWithCtx(ctx, cmd)

	return
}

func (mp *metaPartition) uploadApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.applyID, applyId)
}
