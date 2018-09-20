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

	"encoding/binary"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

func (dp *dataPartition) blobRepair() {

}

type RepairBlobFileTask struct {
	BlobFileId int
	StartObj   uint64
	EndObj     uint64
}

func (dp *dataPartition) getLocalBlobFileMetas(filterBlobFileids []int) (fileMetas *MembersFileMetas, err error) {
	var (
		blobFiles []*storage.FileInfo
	)
	if blobFiles, err = dp.blobStore.GetAllWatermark(); err != nil {
		return
	}
	files := make([]*storage.FileInfo, 0)
	for _, blobFile := range blobFiles {
		for _, filterBlobFileId := range filterBlobFileids {
			if blobFile.FileId == filterBlobFileId {
				files = append(files, blobFile)
			}
		}
	}
	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		fileMetas.files[file.FileId] = file
	}
	return
}

func (dp *dataPartition) getRemoteBlobFileMetas(remote string, filterBlobFileids []int) (fileMetas *MembersFileMetas, err error) {
	var (
		conn *net.TCPConn
	)
	if conn, err = gConnPool.Get(remote); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition(%v) get connection", dp.partitionId)
		return
	}
	defer func() {
		if err != nil {
			gConnPool.Put(conn, true)
		} else {
			gConnPool.Put(conn, false)
		}
	}()

	packet := NewBlobStoreGetAllWaterMarker(dp.partitionId)
	if err = packet.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition(%v) write to remote(%v)", dp.partitionId, remote)
		return
	}
	if err = packet.ReadFromConn(conn, 10); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition(%v) read from connection(%v)", dp.partitionId, remote)
		return
	}
	allFiles := make([]*storage.FileInfo, 0)
	files := make([]*storage.FileInfo, 0)
	if err = json.Unmarshal(packet.Data[:packet.Size], &allFiles); err != nil {
		err = errors.Annotatef(err, "getRemoteExtentMetas partition(%v) unmarshal packet", dp.partitionId)
		return
	}
	for _, cid := range allFiles {
		for _, ccid := range filterBlobFileids {
			if cid.FileId == ccid {
				files = append(files, cid)
			}
		}
	}
	fileMetas = NewMemberFileMetas()
	for _, file := range allFiles {
		fileMetas.files[file.FileId] = file
	}
	return
}

//generator file task
func (dp *dataPartition) generatorBlobRepairTasks(allMembers []*MembersFileMetas) {
	dp.generatorFixBlobFileSizeTasks(allMembers)
	dp.generatorBlobDeleteTasks(allMembers)

}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixBlobFileSizeTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	maxSizeExtentMap := dp.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	for fileId, leaderFile := range leader.files {
		if fileId > storage.BlobFileFileCount {
			continue
		}
		maxSizeExtentIdIndex := maxSizeExtentMap[fileId]
		maxSize := allMembers[maxSizeExtentIdIndex].files[fileId].Size
		sourceAddr := dp.replicaHosts[maxSizeExtentIdIndex]
		inode := leaderFile.Inode
		for index := 0; index < len(allMembers); index++ {
			if index == maxSizeExtentIdIndex {
				continue
			}
			extentInfo, ok := allMembers[index].files[fileId]
			if !ok {
				continue
			}
			if extentInfo.Size < maxSize {
				fixExtent := &storage.FileInfo{Source: sourceAddr, FileId: fileId, Size: maxSize, Inode: inode}
				allMembers[index].NeedFixExtentSizeTasks = append(allMembers[index].NeedFixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) fixExtent(%v).", dp.partitionId, fixExtent)
			}
		}
	}
}

//generator blobObject delete task,send leader has delete object,notify follower delete it
func (dp *dataPartition) generatorBlobDeleteTasks(allMembers []*MembersFileMetas) {
	store := dp.blobStore
	for _, blobfileInfo := range allMembers[0].files {
		blobfileId := blobfileInfo.FileId
		if blobfileId > storage.BlobFileFileCount {
			continue
		}
		deletes := store.GetDelObjects(uint32(blobfileId))
		deleteBuf := make([]byte, len(deletes)*ObjectIDSize)
		for index, deleteObject := range deletes {
			binary.BigEndian.PutUint64(deleteBuf[index*ObjectIDSize:(index+1)*ObjectIDSize], deleteObject)
		}
		for index := 0; index < len(allMembers); index++ {
			allMembers[index].NeedDeleteObjectsTasks[blobfileId] = make([]byte, len(deleteBuf))
			copy(allMembers[index].NeedDeleteObjectsTasks[blobfileId], deleteBuf)
		}
	}

}

//do stream repair blobfilefile,it do on follower host
func (dp *dataPartition) doStreamBlobFixRepair(wg *sync.WaitGroup, remoteBlobFileInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairBlobObjects(remoteBlobFileInfo)
	if err != nil {
		localBlobInfo, opErr := dp.GetBlobStore().GetWatermark(uint64(remoteBlobFileInfo.FileId))
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "dataPartition(%v) remote(%v) local(%v)",
			dp.partitionId, remoteBlobFileInfo, localBlobInfo)
		log.LogError(errors.ErrorStack(err))
	}
}

func (dp *dataPartition) getBlobRepairLogKey(blobFileId int) (s string) {
	return fmt.Sprintf("ActionBlobRepairKey(%v_%v_%v)", dp.ID(), blobFileId, dp.IsLeader())
}

//do stream repair blobfilefile,it do on follower host
func (dp *dataPartition) streamRepairBlobObjects(remoteBlobFileInfo *storage.FileInfo) (err error) {
	store := dp.GetBlobStore()
	//1.get local blobfileFile size
	localBlobFileInfo, err := store.GetWatermark(uint64(remoteBlobFileInfo.FileId))
	if err != nil {
		return errors.Annotatef(err, "%v streamRepairBlobObjects GetWatermark error",
			dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId))
	}
	log.LogWarnf("%v recive fixrepair task ,remote[%],local(%v)",
		dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId), remoteBlobFileInfo.String(), localBlobFileInfo.String())
	//2.generator blobfileRepair read packet,it contains startObj,endObj
	task := &RepairBlobFileTask{BlobFileId: remoteBlobFileInfo.FileId, StartObj: localBlobFileInfo.Size + 1, EndObj: remoteBlobFileInfo.Size}
	//3.new a streamBlobFileRepair readPacket
	request := NewStreamBlobFileRepairReadPacket(dp.ID(), remoteBlobFileInfo.FileId)
	request.Data, _ = json.Marshal(task)
	var conn *net.TCPConn
	//4.get a connection to leader host
	conn, err = gConnPool.Get(remoteBlobFileInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "%v streamRepairBlobObjects get conn from host(%v) error",
			dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId), remoteBlobFileInfo.Source)
	}
	//5.write streamBlobFileRepair command to leader
	err = request.WriteToConn(conn)
	if err != nil {
		gConnPool.Put(conn, true)
		return errors.Annotatef(err, "%v streamRepairBlobObjects send streamRead to host(%v) error",
			dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId), remoteBlobFileInfo.Source)
	}

	for {
		//for 1.get local blobfileFileSize
		localBlobFileInfo, err := store.GetWatermark(uint64(remoteBlobFileInfo.FileId))
		if err != nil {
			gConnPool.Put(conn, true)
			return errors.Annotatef(err, "%v streamRepairBlobObjects GetWatermark error",
				dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId))
		}
		// if local blobfilefile size has great remote ,then break
		if localBlobFileInfo.Size >= remoteBlobFileInfo.Size {
			gConnPool.Put(conn, true)
			break
		}
		// read blobfileStreamRepairRead response
		err = request.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			gConnPool.Put(conn, true)
			return errors.Annotatef(err, "%v streamRepairBlobObjects recive data error",
				dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId))
		}
		// get this repairPacket end oid,if oid has large,then break
		newLastOid := uint64(request.Offset)
		if newLastOid > uint64(remoteBlobFileInfo.FileId) {
			gConnPool.Put(conn, true)
			err = fmt.Errorf("%v invalid offset of OpCRepairReadResp:"+
				" %v, expect max objid is %v", dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId), newLastOid, remoteBlobFileInfo.FileId)
			return err
		}
		log.LogWarnf("%v recive repair,localOid(%v) remoteOid(%v)",
			dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId), localBlobFileInfo.Size, newLastOid)
		// write this blobObject to local
		err = dp.applyRepairBlobObjects(remoteBlobFileInfo.FileId, request.Data, newLastOid)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "%v streamRepairBlobObjects apply data failed",
				dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId))
			return err
		}
	}
	return
}

//follower recive blobfileRepairReadResponse ,then write local blobfileFile
func (dp *dataPartition) applyRepairBlobObjects(blobfileId int, data []byte, endObjectId uint64) (err error) {
	offset := 0
	store := dp.GetBlobStore()
	var applyObjectId uint64
	dataLen := len(data)
	startObjectId, _ := store.GetLastOid(uint32(blobfileId))
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
			err = store.WriteDeleteDentry(o.Oid, blobfileId, o.Crc)
		}
		if err != nil {
			return errors.Annotatef(err, "%v applyRepairBlobObjects oid(%v) writeDeleteDentry failed",
				dp.getBlobRepairLogKey(blobfileId), o.Oid)
		}
		//if offset +this objectSize has great 15MB,then break,donnot fix it
		if offset+int(o.Size) > dataLen {
			return errors.Annotatef(err, "%v applyRepairBlobObjects  oid(%v) no body"+
				" expect(%v) actual(%v) failed", dp.getBlobRepairLogKey(blobfileId), o.Oid, o.Size, dataLen-(offset))
		}
		//get this object body
		ndata := data[offset : offset+int(o.Size)]
		offset += int(o.Size)
		//generator crc
		ncrc := crc32.ChecksumIEEE(ndata)
		//check crc
		if ncrc != o.Crc {
			return errors.Annotatef(err, "%v applyRepairBlobObjects  oid(%v) "+
				"repair data crc  failed,expectCrc(%v) actualCrc(%v)", dp.getBlobRepairLogKey(blobfileId), o.Oid, o.Crc, ncrc)
		}
		//write local storage engine
		err = store.Write(uint32(blobfileId), uint64(o.Oid), int64(o.Size), ndata, o.Crc)
		if err != nil {
			return errors.Annotatef(err, "%v applyRepairBlobObjects oid(%v) write failed", dp.getBlobRepairLogKey(blobfileId), o.Oid)
		}
		//update applyObjectId
		applyObjectId = o.Oid
	}
	log.LogWarnf("%v applyRepairBlobObjects has fix start(%v) end(%v)", dp.getBlobRepairLogKey(blobfileId), startObjectId, endObjectId)
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

func syncData(blobfileID uint32, startOid, endOid uint64, pkg *Packet, conn *net.TCPConn) error {
	var (
		err     error
		objects []*storage.Object
	)
	dataPartition := pkg.DataPartition
	objects = dataPartition.GetObjects(blobfileID, startOid, endOid)
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
		if dataPartition.PackObject(databuf[pos:], objects[i], blobfileID); err != nil {
			return err
		}
		pos += storage.ObjectHeaderSize
		pos += int(realSize)
	}
	return postRepairData(pkg, objects[len(objects)-1].Oid, databuf, pos, conn)
}
