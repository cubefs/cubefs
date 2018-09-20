// Copyright 2018 The Containerfs Authors.
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

package datanode

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"sync"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

func (dp *dataPartition) blobRepair() {

}

type RepairChunkTask struct {
	ChunkId  int
	StartObj uint64
	EndObj   uint64
}

func (dp *dataPartition) getLocalChunkMetas(filterChunkids []int) (fileMetas *MembersFileMetas, err error) {
	var (
		chunkFiles []*storage.FileInfo
	)
	if chunkFiles, err = dp.blobStore.GetAllWatermark(); err != nil {
		return
	}
	files := make([]*storage.FileInfo, 0)
	for _, cid := range chunkFiles {
		for _, ccid := range filterChunkids {
			if cid.FileId == ccid {
				files = append(files, cid)
			}
		}
	}
	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		fileMetas.files[file.FileId] = file
	}
	return
}

func (dp *dataPartition) getRemoteChunkMetas(remote string, filterChunkids []int) (fileMetas *MembersFileMetas, err error) {
	var (
		conn *net.TCPConn
	)
	if conn, err = gConnPool.Get(remote); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition[%v] get connection", dp.partitionId)
		return
	}
	defer gConnPool.Put(conn, true)

	packet := NewExtentStoreGetAllWaterMarker(dp.partitionId)
	if err = packet.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition[%v] write to remote[%v]", dp.partitionId, remote)
		return
	}
	if err = packet.ReadFromConn(conn, 10); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition[%v] read from connection[%v]", dp.partitionId, remote)
		return
	}
	files := make([]*storage.FileInfo, 0)
	if err = json.Unmarshal(packet.Data[:packet.Size], &files); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition[%v] unmarshal packet", dp.partitionId)
		return
	}
	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		fileMetas.files[file.FileId] = file
	}
	return
}

//do stream repair chunkfile,it do on follower host
func (dp *dataPartition) doStreamBlobFixRepair(wg *sync.WaitGroup, remoteBlobFileInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairBlobObjects(remoteBlobFileInfo)
	if err != nil {
		localBlobInfo, opErr := dp.GetBlobStore().GetWatermark(uint64(remoteBlobFileInfo.FileId))
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "dataPartition[%v] remote[%v] local[%v]",
			dp.partitionId, remoteBlobFileInfo, localBlobInfo)
		log.LogError(errors.ErrorStack(err))
	}
}

//do stream repair chunkfile,it do on follower host
func (dp *dataPartition) streamRepairBlobObjects(remoteChunkInfo *storage.FileInfo) (err error) {
	store := dp.GetBlobStore()
	//1.get local chunkFile size
	localChunkInfo, err := store.GetWatermark(uint64(remoteChunkInfo.FileId))
	if err != nil {
		return errors.Annotatef(err, "streamRepairBlobObjects GetWatermark error")
	}
	//2.generator chunkRepair read packet,it contains startObj,endObj
	task := &RepairChunkTask{ChunkId: remoteChunkInfo.FileId, StartObj: localChunkInfo.Size + 1, EndObj: remoteChunkInfo.Size}
	//3.new a streamChunkRepair readPacket
	request := NewStreamChunkRepairReadPacket(dp.ID(), remoteChunkInfo.FileId)
	request.Data, _ = json.Marshal(task)
	var conn *net.TCPConn
	//4.get a connection to leader host
	conn, err = gConnPool.Get(remoteChunkInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairBlobObjects get conn from host[%v] error", remoteChunkInfo.Source)
	}
	//5.write streamChunkRepair command to leader
	err = request.WriteToConn(conn)
	if err != nil {
		gConnPool.Put(conn, true)
		return errors.Annotatef(err, "streamRepairBlobObjects send streamRead to host[%v] error", remoteChunkInfo.Source)
	}
	for {
		//for 1.get local chunkFileSize
		localChunkInfo, err := store.GetWatermark(uint64(remoteChunkInfo.FileId))
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairBlobObjects GetWatermark error")
		}
		// if local chunkfile size has great remote ,then break
		if localChunkInfo.Size >= remoteChunkInfo.Size {
			gConnPool.Put(conn, true)
			break
		}
		// read chunkStreamRepairRead response
		err = request.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			gConnPool.Put(conn, true)
			return errors.Annotatef(err, "streamRepairBlobObjects recive data error")
		}
		// get this repairPacket end oid,if oid has large,then break
		newLastOid := uint64(request.Offset)
		if newLastOid > uint64(remoteChunkInfo.FileId) {
			gConnPool.Put(conn, true)
			err = fmt.Errorf("invalid offset of OpCRepairReadResp:"+
				" %v, expect max objid is %v", newLastOid, remoteChunkInfo.FileId)
			return err
		}
		// write this blobObject to local
		err = dp.applyRepairBlobObjects(remoteChunkInfo.FileId, request.Data, newLastOid)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "streamRepairBlobObjects apply data failed")
			return err
		}
	}
	return
}

//follower recive chunkRepairReadResponse ,then write local chunkFile
func (dp *dataPartition) applyRepairBlobObjects(chunkId int, data []byte, endObjectId uint64) (err error) {
	offset := 0
	store := dp.GetBlobStore()
	var applyObjectId uint64
	dataLen := len(data)
	for {
		//if has read end,then break
		if offset+storage.ObjectHeaderSize > len(data) {
			break
		}
		//if has applyObjectId has great endObjectId,then break
		if applyObjectId >= endObjectId {
			break
		}
		o := &storage.Object{}
		o.Unmarshal(data[offset : offset+storage.ObjectHeaderSize])
		//unmarshal objectHeader,if this object has delete on leader,then ,write a deleteEntry to indexfile
		offset += storage.ObjectHeaderSize
		if o.Size == storage.MarkDeleteObject {
			err = store.WriteDeleteDentry(o.Oid, chunkId, o.Crc)
		}
		if err != nil {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] writeDeleteDentry failed", dp.ID(), chunkId, o.Oid)
		}
		//if offset +this objectSize has great 15MB,then break,donnot fix it
		if offset+int(o.Size) > dataLen {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] no body"+
				" expect[%v] actual[%v] failed", dp.ID(), chunkId, o.Oid, o.Size, dataLen-(offset))
		}
		//get this object body
		ndata := data[offset : offset+int(o.Size)]
		offset += int(o.Size)
		//generator crc
		ncrc := crc32.ChecksumIEEE(ndata)
		//check crc
		if ncrc != o.Crc {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] "+
				"repair data crc  failed,expectCrc[%v] actualCrc[%v]", dp.ID(), chunkId, o.Oid, o.Crc, ncrc)
		}
		//write local storage engine
		err = store.Write(uint32(chunkId), uint64(o.Oid), int64(o.Size), ndata, o.Crc)
		if err != nil {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] write failed", dp.ID(), chunkId, o.Oid)
		}
		//update applyObjectId
		applyObjectId = o.Oid
	}
	return nil
}

func postRepairData(pkg *Packet, lastOid uint64, data []byte, size int, conn *net.TCPConn) (err error) {
	pkg.Offset = int64(lastOid)
	pkg.ResultCode = proto.OpOk
	pkg.Size = uint32(size)
	pkg.Data = data
	pkg.Crc = crc32.ChecksumIEEE(pkg.Data)
	err = pkg.WriteToNoDeadLineConn(conn)
	log.LogWrite(pkg.ActionMsg(ActionLeaderToFollowerOpRepairReadSendPackBuffer, conn.RemoteAddr().String(), pkg.StartT, err))

	return
}

const (
	PkgRepairCReadRespMaxSize   = 10 * util.MB
	PkgRepairCReadRespLimitSize = 15 * util.MB
)

func syncData(chunkID uint32, startOid, endOid uint64, pkg *Packet, conn *net.TCPConn) error {
	var (
		err     error
		objects []*storage.Object
	)
	dataPartition := pkg.DataPartition
	objects = dataPartition.GetObjects(chunkID, startOid, endOid)
	log.LogWrite(pkg.ActionMsg(ActionLeaderToFollowerOpRepairReadPackBuffer, string(len(objects)), pkg.StartT, err))
	databuf := make([]byte, PkgRepairCReadRespMaxSize)
	pos := 0
	for i := 0; i < len(objects); i++ {
		var realSize uint32
		realSize = 0
		if objects[i].Size != storage.MarkDeleteObject {
			realSize = objects[i].Size
		}
		if pos+int(realSize)+storage.ObjectHeaderSize >= PkgRepairCReadRespLimitSize {
			if err = postRepairData(pkg, objects[i-1].Oid, databuf, pos, conn); err != nil {
				return err
			}
			databuf = make([]byte, PkgRepairCReadRespMaxSize)
			pos = 0
		}
		if dataPartition.PackObject(databuf[pos:], objects[i], chunkID); err != nil {
			return err
		}
		pos += storage.ObjectHeaderSize
		pos += int(realSize)
	}
	return postRepairData(pkg, objects[len(objects)-1].Oid, databuf, pos, conn)
}
