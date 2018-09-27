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
	"time"
)

func (dp *dataPartition) lauchBlobRepair() {
	ticker := time.Tick(time.Second * 20)
	dp.blobRepair()
	for {
		select {
		case <-ticker:
			dp.blobRepair()
			dp.lauchCompact()
		case <-dp.stopC:
			return
		}
	}
}

func (dp *dataPartition) repairFailedBlobFileToUnavali(unavalis []int) {
	for _, blobFile := range unavalis {
		dp.blobStore.PutUnAvailBlobFile(blobFile)
	}
}
func (dp *dataPartition) blobRepair() {
	var (
		err error
	)
	if dp.updateReplicaHosts() != nil {
		return
	}
	if !dp.isLeader {
		return
	}
	defer func() {
		log.LogInfof("%v status(%v) used(%v) avaliBlobFile(%v) unavaliBlobFile(%v)", dp.getDataPartitionLogKey(), dp.Status(),
			dp.Used(), dp.blobStore.GetAvailChanLen(), dp.blobStore.GetUnAvailChanLen())
	}()
	unavaliBlobFiles := dp.getUnavaliBlobFile()
	if len(unavaliBlobFiles) == 0 {
		return
	}
	allMembersFileMetas := make([]*MembersFileMetas, len(dp.ReplicaHosts()))
	allMembersFileMetas[0], err = dp.getLocalBlobFileMetas(unavaliBlobFiles)
	if err != nil {
		dp.repairFailedBlobFileToUnavali(unavaliBlobFiles)
		log.LogWarnf("%v blob repair GetLocalBlobFiles failed (%v)", dp.getDataPartitionLogKey(), err)
		return
	}
	for i := 1; i < len(dp.replicaHosts); i++ {
		allMembersFileMetas[i], err = dp.getRemoteBlobFileMetas(dp.replicaHosts[i], unavaliBlobFiles, i)
		if err != nil {
			dp.repairFailedBlobFileToUnavali(unavaliBlobFiles)
			log.LogWarnf("%v blob repair GetRemoteBlobFiles failed (%v)", dp.getDataPartitionLogKey(), err)
			return
		}
	}

	successBlobFiles, needRepairBlobFiles := dp.generatorBlobRepairTasks(allMembersFileMetas)
	err = dp.NotifyBlobRepair(allMembersFileMetas)
	if err != nil {
		dp.repairFailedBlobFileToUnavali(unavaliBlobFiles)
		log.LogWarnf("%v blob repair Notify failed (%v)", dp.getDataPartitionLogKey(), err)
		return
	}
	for _, success := range successBlobFiles {
		dp.blobStore.PutAvailBlobFile(success)
	}
	for _, needRepair := range needRepairBlobFiles {
		dp.blobStore.PutUnAvailBlobFile(needRepair)
	}
	dp.MergeBlobStoreRepair(allMembersFileMetas[0])

}

type RepairBlobFileTask struct {
	BlobFileId int
	StartObj   uint64
	EndObj     uint64
}

func (dp *dataPartition) getUnavaliBlobFile() (unavaliBlobFile []int) {
	unavaliBlobFile = make([]int, 0)
	maxBlobRepairCount := MaxRepairBlobFileCount
	if dp.isFirstRestart {
		maxBlobRepairCount = storage.BlobFileFileCount
		dp.isFirstRestart = false
	}
	for i := 0; i < maxBlobRepairCount; i++ {
		unavali, err := dp.blobStore.GetUnAvailBlobFile()
		if err != nil {
			break
		}
		unavaliBlobFile = append(unavaliBlobFile, unavali)
	}

	return
}

func (dp *dataPartition) getLocalBlobFileMetas(filterBlobFileids []int) (fileMetas *MembersFileMetas, err error) {
	var (
		AllblobFiles []*storage.FileInfo
	)
	if AllblobFiles, err = dp.blobStore.GetAllWatermark(); err != nil {
		return
	}
	files := make([]*storage.FileInfo, 0)
	for _, blobFile := range AllblobFiles {
		for _, filterBlobFileId := range filterBlobFileids {
			if blobFile.FileId == filterBlobFileId {
				blobFile.MemberIndex = 0
				blobFile.Source = LocalIP
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

func (dp *dataPartition) getRemoteBlobFileMetas(remote string, filterBlobFileids []int, index int) (fileMetas *MembersFileMetas, err error) {
	var (
		conn *net.TCPConn
	)
	if conn, err = gConnPool.Get(remote); err != nil {
		err = errors.Annotatef(err, "getRemoteBlobMetas partition(%v) get connection", dp.partitionId)
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
		err = errors.Annotatef(err, "getRemoteBlobMetas partition(%v) write to remote(%v)", dp.partitionId, remote)
		return
	}
	if err = packet.ReadFromConn(conn, 10); err != nil {
		err = errors.Annotatef(err, "getRemoteBlobMetas partition(%v) read from connection(%v)", dp.partitionId, remote)
		return
	}
	allBlobFiles := make([]*storage.FileInfo, 0)
	files := make([]*storage.FileInfo, 0)
	if err = json.Unmarshal(packet.Data[:packet.Size], &allBlobFiles); err != nil {
		err = errors.Annotatef(err, "getRemoteBlobMetas partition(%v) unmarshal packet", dp.partitionId)
		return
	}
	for _, cid := range allBlobFiles {
		for _, ccid := range filterBlobFileids {
			if cid.FileId == ccid {
				files = append(files, cid)
			}
		}
	}
	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		file.MemberIndex = index
		file.Source = remote
		fileMetas.files[file.FileId] = file
	}
	return
}

//generator file task
func (dp *dataPartition) generatorBlobRepairTasks(allMembers []*MembersFileMetas) (successBlobFiles, needRepairBlobFiles []int) {
	maxSizeBlobMap := dp.mapMaxSizeBlobFileToIndex(allMembers)
	successBlobFiles, needRepairBlobFiles = dp.checkNeedRepairBlobFiles(allMembers, maxSizeBlobMap)
	dp.generatorFixBlobFileSizeTasks(allMembers, maxSizeBlobMap)
	dp.generatorBlobDeleteTasks(allMembers)
	return
}

/*generator fix blob Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixBlobFileSizeTasks(allMembers []*MembersFileMetas, maxSizeBlobFileMap map[int]*storage.FileInfo) {
	leader := allMembers[0]
	for fileId, leaderFile := range leader.files {
		if fileId > storage.BlobFileFileCount {
			continue
		}
		maxSizeBlobIndex := maxSizeBlobFileMap[fileId].MemberIndex
		maxObjectId := maxSizeBlobFileMap[fileId].Size
		sourceAddr := dp.replicaHosts[maxSizeBlobIndex]
		inode := leaderFile.Inode
		for index := 0; index < len(allMembers); index++ {
			if index == maxSizeBlobIndex {
				continue
			}
			blobFileInfo, ok := allMembers[index].files[fileId]
			if !ok {
				continue
			}
			if blobFileInfo.Size < maxObjectId {
				fixBlobTask := &storage.FileInfo{Source: sourceAddr, FileId: fileId, Size: maxObjectId, Inode: inode}
				allMembers[index].NeedFixBlobFileSizeTasks = append(allMembers[index].NeedFixBlobFileSizeTasks, fixBlobTask)
				log.LogWarnf("%v action[generatorFixBlobSizeTasks] fromIndex(%v) fromAddr(%v) maxObjectId(%v)"+
					"toIndex(%v) toAddr(%v) toMaxObjectId(%v) fixBlobTask(%v).", dp.getBlobRepairLogKey(fileId), maxSizeBlobIndex,
					sourceAddr, maxObjectId, index, dp.replicaHosts[index], blobFileInfo.Size, fixBlobTask.String())
			}
		}
	}
}

func (dp *dataPartition) checkNeedRepairBlobFiles(allMembers []*MembersFileMetas,
	maxSizeBlobFileMap map[int]*storage.FileInfo) (successBlobFiles, needRepairBlobFiles []int) {
	successBlobFiles = make([]int, 0)
	needRepairBlobFiles = make([]int, 0)
	for fileId := range allMembers[0].files {
		maxSizeBlobFile := maxSizeBlobFileMap[fileId]
		if maxSizeBlobFile == nil {
			continue
		}
		isSizeEqure := true
		for _, member := range allMembers {
			if member.files[fileId] == nil || member.files[fileId].Size != maxSizeBlobFile.Size {
				isSizeEqure = false
				break
			}
		}
		if isSizeEqure {
			successBlobFiles = append(successBlobFiles, fileId)
		} else {
			needRepairBlobFiles = append(needRepairBlobFiles, fileId)
		}
	}

	for i := 0; i < len(allMembers); i++ {
		for _, success := range successBlobFiles {
			delete(allMembers[i].files, success)
		}
	}
	return
}

func (dp *dataPartition) mapMaxSizeBlobFileToIndex(allMembers []*MembersFileMetas) (maxSizeBlobMap map[int]*storage.FileInfo) {
	leader := allMembers[0]
	maxSizeBlobMap = make(map[int]*storage.FileInfo)
	for blobFileId, blobFileInfo := range leader.files { //range leader all blobFiles
		maxSizeBlobMap[blobFileId] = blobFileInfo
		var maxFileSize uint64
		for index := 0; index < len(allMembers); index++ {
			member := allMembers[index]
			_, ok := member.files[blobFileId]
			if !ok {
				continue
			}
			if maxFileSize <= member.files[blobFileId].Size {
				maxFileSize = member.files[blobFileId].Size
				maxSizeBlobMap[blobFileId] = member.files[blobFileId]
				maxSizeBlobMap[blobFileId].MemberIndex = member.files[blobFileId].MemberIndex
				log.LogWarnf("%v mapMaxSizeBlobFileToIndex maxFileSize (%v) index(%v)"+
					" replocationHost(%v)", dp.getBlobRepairLogKey(blobFileId), maxFileSize,
					member.files[blobFileId].MemberIndex, dp.replicaHosts)
			}
		}
	}
	return
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

/*notify follower to repair dataPartition blobStore*/
func (dp *dataPartition) NotifyBlobRepair(members []*MembersFileMetas) (err error) {
	var (
		errList []error
	)
	errList = make([]error, 0)
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyBlobRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.Get(target)
			if err != nil {
				errList = append(errList, err)
				return
			}
			p.Data, err = json.Marshal(members[index])
			p.Size = uint32(len(p.Data))
			err = p.WriteToConn(conn)
			if err != nil {
				gConnPool.Put(conn, true)
				errList = append(errList, err)
				return
			}
			p.ReadFromConn(conn, proto.NoReadDeadlineTime)
			gConnPool.Put(conn, true)
		}(i)
	}
	wg.Wait()
	if len(errList) > 0 {
		for _, errItem := range errList {
			err = errors.Annotate(err, errItem.Error())
		}
	}
	return
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

func (dp *dataPartition) getDataPartitionLogKey() (s string) {
	return fmt.Sprintf("RepairDataPartitionKey(%v_%v)", dp.ID(), dp.IsLeader())
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
	log.LogWarnf("%v recive fixrepair task ,remote(%v),local(%v)",
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
		err = request.ReadFromConn(conn, proto.ReadDeadlineTime*5)
		if err != nil {
			gConnPool.Put(conn, true)
			return errors.Annotatef(err, "%v streamRepairBlobObjects recive data error",
				dp.getBlobRepairLogKey(remoteBlobFileInfo.FileId))
		}
		// get this repairPacket end oid,if oid has large,then break
		newLastOid := uint64(request.Offset)
		log.LogWritef("%v recive repair,localOid(%v) remoteOid(%v)",
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
	dataPos := 0
	store := dp.GetBlobStore()
	dataLen := len(data)
	startObjectId, _ := store.GetLastOid(uint32(blobfileId))
	for startObjectId < endObjectId {
		//if has read end,then break
		if dataPos+storage.ObjectHeaderSize > len(data) {
			break
		}
		//if has applyObjectId has great endObjectId,then break
		if startObjectId >= endObjectId {
			break
		}
		o := &storage.Object{}
		o.Unmarshal(data[dataPos : dataPos+storage.ObjectHeaderSize])
		//unmarshal objectHeader,if this object has delete on leader,then ,write a deleteEntry to indexfile
		dataPos += storage.ObjectHeaderSize
		if o.Size == storage.MarkDeleteObject {
			startObjectId = o.Oid
			m := fmt.Sprintf(" %v applyRepairData write deleteEntry  "+
				"current fix Oid(%v) maxOID(%v) ", dp.getBlobRepairLogKey(blobfileId), o.Oid, endObjectId)
			err = store.WriteDeleteDentry(o.Oid, blobfileId, o.Crc)
			if err != nil {
				err = errors.Annotatef(err, "%v applyRepairBlobObjects oid(%v) writeDeleteDentry failed",
					dp.getBlobRepairLogKey(blobfileId), o.Oid)
				log.LogWarnf(err.Error())
				return err
			}
			log.LogWrite(m)
			continue
		}

		//if dataPos +this objectSize has great 15MB,then break,donnot fix it
		if dataPos+int(o.Size) > dataLen {
			return errors.Annotatef(err, "%v applyRepairBlobObjects  oid(%v) no body"+
				" expect(%v) actual(%v) failed", dp.getBlobRepairLogKey(blobfileId), o.Oid, o.Size, dataLen-(dataPos))
		}
		//get this object body
		ndata := data[dataPos : dataPos+int(o.Size)]
		dataPos += int(o.Size)
		//generator crc
		ncrc := crc32.ChecksumIEEE(ndata)
		//check crc
		if ncrc != o.Crc {
			return errors.Annotatef(err, "%v applyRepairBlobObjects  oid(%v) "+
				"repair data crc  failed,expectCrc(%v) actualCrc(%v)", dp.getBlobRepairLogKey(blobfileId), o.Oid, o.Crc, ncrc)
		}
		//write local storage engine
		log.LogWritef("%v applyRepairBlobObjects oid(%v) size(%v) crc(%v)", dp.getBlobRepairLogKey(blobfileId), o.Size, ncrc)
		err = store.Write(uint32(blobfileId), uint64(o.Oid), int64(o.Size), ndata, o.Crc)
		if err != nil {
			return errors.Annotatef(err, "%v applyRepairBlobObjects oid(%v) write failed(%v)", dp.getBlobRepairLogKey(blobfileId), o.Oid, err)
		}
		//update applyObjectId
		startObjectId = o.Oid
		if startObjectId >= endObjectId {
			break
		}
	}
	log.LogWritef("%v applyRepairBlobObjects has fix start(%v) end(%v)", dp.getBlobRepairLogKey(blobfileId), startObjectId, endObjectId)
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
	PkgRepairCReadRespMaxSize   = 15 * util.MB
	PkgRepairCReadRespLimitSize = 10 * util.MB
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
