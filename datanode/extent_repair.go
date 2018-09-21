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
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"time"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
	"hash/crc32"
)

//every  datapartion  file metas used for auto repairt
type MembersFileMetas struct {
	Index                    int                       //index on data partionGroup
	files                    map[int]*storage.FileInfo //storage file on datapartiondisk meta
	NeedDeleteExtentsTasks   []*storage.FileInfo       //generator delete extent file task
	NeedAddExtentsTasks      []*storage.FileInfo       //generator add extent file task
	NeedFixExtentSizeTasks   []*storage.FileInfo       //generator fixSize file task
	NeedDeleteObjectsTasks   map[int][]byte            //generator deleteObject on blob file task
	NeedFixBlobFileSizeTasks []*storage.FileInfo
}

func NewMemberFileMetas() (mf *MembersFileMetas) {
	mf = &MembersFileMetas{
		files: make(map[int]*storage.FileInfo),
		NeedDeleteExtentsTasks:   make([]*storage.FileInfo, 0),
		NeedAddExtentsTasks:      make([]*storage.FileInfo, 0),
		NeedFixExtentSizeTasks:   make([]*storage.FileInfo, 0),
		NeedFixBlobFileSizeTasks: make([]*storage.FileInfo, 0),
		NeedDeleteObjectsTasks:   make(map[int][]byte),
	}
	return
}

//files repair check
func (dp *dataPartition) extentFileRepair() {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[extentFileRepair] partition(%v) start.",
		dp.partitionId)

	// Get all data partition group member about file metas
	allMembers, err := dp.getAllMemberExtentMetas()
	if err != nil {
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogErrorf(errors.ErrorStack(err))
		return
	}
	dp.generatorExtentRepairTasks(allMembers) //generator file repair task
	err = dp.NotifyExtentRepair(allMembers)   //notify host to fix it
	if err != nil {
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogError(errors.ErrorStack(err))
	}
	for _, fixExtentFile := range allMembers[0].NeedFixExtentSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
	finishTime := time.Now().UnixNano()
	log.LogInfof("action[extentFileRepair] partition(%v) finish cost[%vms].",
		dp.partitionId, (finishTime-startTime)/int64(time.Millisecond))
}

// Get all data partition group ,about all files meta
func (dp *dataPartition) getAllMemberExtentMetas() (allMemberFileMetas []*MembersFileMetas, err error) {
	allMemberFileMetas = make([]*MembersFileMetas, len(dp.replicaHosts))
	var (
		extentFiles []*storage.FileInfo
	)
	files := make([]*storage.FileInfo, 0)
	// get local extent file metas
	extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberExtentMetas extent dataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	// write blob files meta to extent files meta
	files = append(files, extentFiles...)
	leaderFileMetas := NewMemberFileMetas()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	for _, fi := range files {
		leaderFileMetas.files[fi.FileId] = fi
	}
	allMemberFileMetas[0] = leaderFileMetas
	// leader files meta has ready

	// get remote files meta by opGetAllWaterMarker cmd
	p := NewExtentStoreGetAllWaterMarker(dp.partitionId)
	for i := 1; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		target := dp.replicaHosts[i]
		conn, err = gConnPool.Get(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberExtentMetas  dataPartition(%v) get host(%v) connect", dp.partitionId, target)
			return
		}
		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) write to host(%v)", dp.partitionId, target)
			return
		}
		err = p.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) read from host(%v)", dp.partitionId, target)
			return
		}
		fileInfos := make([]*storage.FileInfo, 0)
		err = json.Unmarshal(p.Data[:p.Size], &fileInfos)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) unmarshal json(%v)", dp.partitionId, string(p.Data[:p.Size]))
			return
		}
		slaverFileMetas := NewMemberFileMetas()
		for _, fileInfo := range fileInfos {
			slaverFileMetas.files[fileInfo.FileId] = fileInfo
		}
		allMemberFileMetas[i] = slaverFileMetas
		gConnPool.Put(conn, true)
	}
	return
}

//generator file task
func (dp *dataPartition) generatorExtentRepairTasks(allMembers []*MembersFileMetas) {
	dp.generatorAddExtentsTasks(allMembers) //add extentTask
	dp.generatorFixExtentSizeTasks(allMembers)
	dp.generatorDeleteExtentsTasks(allMembers)

}

/* pasre all extent,select maxExtentSize to member index map
 */
func (dp *dataPartition) mapMaxSizeExtentToIndex(allMembers []*MembersFileMetas) (maxSizeExtentMap map[int]int) {
	leader := allMembers[0]
	maxSizeExtentMap = make(map[int]int)
	for fileId := range leader.files { //range leader all extentFiles
		maxSizeExtentMap[fileId] = 0
		var maxFileSize uint64
		for index := 0; index < len(allMembers); index++ {
			member := allMembers[index]
			_, ok := member.files[fileId]
			if !ok {
				continue
			}
			if maxFileSize < member.files[fileId].Size {
				maxFileSize = member.files[fileId].Size
				maxSizeExtentMap[fileId] = index //map maxSize extentId to allMembers index
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (dp *dataPartition) generatorAddExtentsTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	leaderAddr := dp.replicaHosts[0]
	for fileId, leaderFile := range leader.files {
		if fileId <= storage.BlobFileFileCount {
			continue
		}
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[fileId]; !ok {
				addFile := &storage.FileInfo{Source: leaderAddr, FileId: fileId, Size: leaderFile.Size, Inode: leaderFile.Inode}
				follower.NeedAddExtentsTasks = append(follower.NeedAddExtentsTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition(%v) addFile(%v).", dp.partitionId, addFile)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixExtentSizeTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	maxSizeExtentMap := dp.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	for fileId, leaderFile := range leader.files {
		if fileId <= storage.BlobFileFileCount {
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

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorDeleteExtentsTasks(allMembers []*MembersFileMetas) {
	store := dp.extentStore
	deletes := store.GetDelObjects()
	leaderAddr := dp.replicaHosts[0]
	for _, deleteFileId := range deletes {
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[int(deleteFileId)]; ok {
				deleteFile := &storage.FileInfo{Source: leaderAddr, FileId: int(deleteFileId), Size: 0}
				follower.NeedDeleteExtentsTasks = append(follower.NeedDeleteExtentsTasks, deleteFile)
				log.LogInfof("action[generatorDeleteExtentsTasks] partition(%v) deleteFile(%v).", dp.partitionId, deleteFile)
			}
		}
	}
}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyExtentRepair(members []*MembersFileMetas) (err error) {
	var (
		errList []error
	)
	errList = make([]error, 0)
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyExtentRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
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

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *dataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairExtent(remoteExtentInfo)
	if err != nil {
		localExtentInfo, opErr := dp.GetExtentStore().GetWatermark(uint64(remoteExtentInfo.FileId), false)
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "partition(%v) remote(%v) local(%v)",
			dp.partitionId, remoteExtentInfo, localExtentInfo)
		log.LogErrorf("action[doStreamExtentFixRepair] err(%v).", err)
	}
}

//extent file repair function,do it on follower host
func (dp *dataPartition) streamRepairExtent(remoteExtentInfo *storage.FileInfo) (err error) {
	store := dp.GetExtentStore()
	if !store.IsExistExtent(uint64(remoteExtentInfo.FileId)) {
		return
	}

	// Get local extent file info
	localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.FileId), false)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}

	// Get need fix size for this extent file
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size

	// Create streamRead packet, it offset is local extentInfoSize, size is needFixSize
	request := NewStreamReadPacket(dp.ID(), remoteExtentInfo.FileId, int(localExtentInfo.Size), int(needFixSize))
	var conn *net.TCPConn

	// Get a connection to leader host
	conn, err = gConnPool.Get(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host(%v) error", remoteExtentInfo.Source)
	}
	defer gConnPool.Put(conn, true)

	// Write OpStreamRead command to leader
	if err = request.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "streamRepairExtent send streamRead to host(%v) error", remoteExtentInfo.Source)
		log.LogErrorf("action[streamRepairExtent] err(%v).", err)
		return
	}
	for {
		select {
		case <-dp.stopC:
			return
		default:
		}
		// Get local extentFile size
		var (
			localExtentInfo *storage.FileInfo
		)
		if localExtentInfo, err = store.GetWatermark(uint64(remoteExtentInfo.FileId), false); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent partition(%v) GetWatermark error", dp.partitionId)
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
		// If local extent size has great remoteExtent file size ,then break
		if localExtentInfo.Size >= remoteExtentInfo.Size {
			break
		}

		// Read 64k stream repair packet
		if err = request.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent receive data error")
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}
		log.LogInfof("action[streamRepairExtent] partition(%v) extent(%v) start fix from (%v)"+
			" remoteSize(%v) localSize(%v).", dp.ID(), remoteExtentInfo.FileId,
			remoteExtentInfo.Source, remoteExtentInfo.Size, localExtentInfo.Size)

		if request.Crc != crc32.ChecksumIEEE(request.Data[:request.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition(%v) extent(%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v)", dp.ID(), remoteExtentInfo.FileId,
				remoteExtentInfo.Source, remoteExtentInfo.Size, localExtentInfo.Size, request.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if err = store.Write(uint64(localExtentInfo.FileId), int64(localExtentInfo.Size), int64(request.Size), request.Data, request.Crc); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
	}
	return

}
