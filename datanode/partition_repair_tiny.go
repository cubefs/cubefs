// Copyright 2018 The ChuBao Authors.
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
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/storage"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"hash/crc32"
	"net"
	"sync"
)

type RepairChunkTask struct {
	ChunkId  int
	StartObj uint64
	EndObj   uint64
}

//do stream repair chunkfile,it do on follower host
func (dp *dataPartition) doStreamTinyFixRepair(wg *sync.WaitGroup, remoteTinyFileInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairTinyObjects(remoteTinyFileInfo)
	if err != nil {
		localTinyInfo, opErr := dp.GetTinyStore().GetWatermark(uint64(remoteTinyFileInfo.FileId))
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "dataPartition[%v] remote[%v] local[%v]",
			dp.partitionId, remoteTinyFileInfo, localTinyInfo)
		log.LogError(errors.ErrorStack(err))
	}
}

//do stream repair chunkfile,it do on follower host
func (dp *dataPartition) streamRepairTinyObjects(remoteChunkInfo *storage.FileInfo) (err error) {
	store := dp.GetTinyStore()
	//1.get local chunkFile size
	localChunkInfo, err := store.GetWatermark(uint64(remoteChunkInfo.FileId))
	if err != nil {
		return errors.Annotatef(err, "streamRepairTinyObjects GetWatermark error")
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
		return errors.Annotatef(err, "streamRepairTinyObjects get conn from host[%v] error", remoteChunkInfo.Source)
	}
	//5.write streamChunkRepair command to leader
	err = request.WriteToConn(conn)
	if err != nil {
		gConnPool.Put(conn, true)
		return errors.Annotatef(err, "streamRepairTinyObjects send streamRead to host[%v] error", remoteChunkInfo.Source)
	}
	for {
		//for 1.get local chunkFileSize
		localChunkInfo, err := store.GetWatermark(uint64(remoteChunkInfo.FileId))
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairTinyObjects GetWatermark error")
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
			return errors.Annotatef(err, "streamRepairTinyObjects recive data error")
		}
		// get this repairPacket end oid,if oid has large,then break
		newLastOid := uint64(request.Offset)
		if newLastOid > uint64(remoteChunkInfo.FileId) {
			gConnPool.Put(conn, true)
			err = fmt.Errorf("invalid offset of OpCRepairReadResp:"+
				" %v, expect max objid is %v", newLastOid, remoteChunkInfo.FileId)
			return err
		}
		// write this tinyObject to local
		err = dp.applyRepairTinyObjects(remoteChunkInfo.FileId, request.Data, newLastOid)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "streamRepairTinyObjects apply data failed")
			return err
		}
	}
	return
}

//follower recive chunkRepairReadResponse ,then write local chunkFile
func (dp *dataPartition) applyRepairTinyObjects(chunkId int, data []byte, endObjectId uint64) (err error) {
	offset := 0
	store := dp.GetTinyStore()
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
		if o.Size == storage.TombstoneFileSize {
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
		if objects[i].Size != storage.TombstoneFileSize {
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
