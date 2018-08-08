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
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/storage"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
)

//every  datapartion  file metas used for auto repairt
type MembersFileMetas struct {
	Index                  int                       //index on data partionGroup
	files                  map[int]*storage.FileInfo //storage file on datapartiondisk meta
	NeedDeleteExtentsTasks []*storage.FileInfo       //generator delete extent file task
	NeedAddExtentsTasks    []*storage.FileInfo       //generator add extent file task
	NeedFixFileSizeTasks   []*storage.FileInfo       //generator fixSize file task
	NeedDeleteObjectsTasks map[int][]byte            //generator deleteObject on tiny file task
}

func NewMemberFileMetas() (mf *MembersFileMetas) {
	mf = &MembersFileMetas{
		files: make(map[int]*storage.FileInfo),
		NeedDeleteExtentsTasks: make([]*storage.FileInfo, 0),
		NeedAddExtentsTasks:    make([]*storage.FileInfo, 0),
		NeedFixFileSizeTasks:   make([]*storage.FileInfo, 0),
		NeedDeleteObjectsTasks: make(map[int][]byte),
	}
	return
}

//files repair check
func (dp *dataPartition) fileRepair() {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[fileRepair] partition[%v] start.",
		dp.partitionId)

	// Get all data partition group member about file metas
	allMembers, err := dp.getAllMemberFileMetas()
	if err != nil {
		log.LogErrorf("action[fileRepair] partition[%v] err[%v].",
			dp.partitionId, err)
		log.LogErrorf(errors.ErrorStack(err))
		return
	}
	dp.generatorFilesRepairTasks(allMembers) //generator file repair task
	err = dp.NotifyRepair(allMembers)        //notify host to fix it
	if err != nil {
		log.LogErrorf("action[fileRepair] partition[%v] err[%v].",
			dp.partitionId, err)
		log.LogError(errors.ErrorStack(err))
	}
	for _, fixExtentFile := range allMembers[0].NeedFixFileSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
	finishTime := time.Now().UnixNano()
	log.LogInfof("action[fileRepair] partition[%v] finish cost[%vms].",
		dp.partitionId, (finishTime-startTime)/int64(time.Millisecond))
}

func (dp *dataPartition) getLocalFileMetas() (fileMetas *MembersFileMetas, err error) {
	var (
		extentFiles []*storage.FileInfo
		tinyFiles   []*storage.FileInfo
	)
	if extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter()); err != nil {
		return
	}
	if tinyFiles, err = dp.tinyStore.GetAllWatermark(); err != nil {
		return
	}
	files := make([]*storage.FileInfo, 0)
	files = append(files, extentFiles...)
	files = append(files, tinyFiles...)

	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		fileMetas.files[file.FileId] = file
	}
	return
}

func (dp *dataPartition) getRemoteFileMetas(remote string) (fileMetas *MembersFileMetas, err error) {
	var (
		conn *net.TCPConn
	)
	if conn, err = gConnPool.Get(remote); err != nil {
		err = errors.Annotatef(err, "getRemoteFileMetas partition[%v] get connection", dp.partitionId)
		return
	}
	defer gConnPool.Put(conn, true)

	packet := NewGetAllWaterMarker(dp.partitionId)
	if err = packet.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "getRemoteFileMetas partition[%v] write to remote[%v]", dp.partitionId, remote)
		return
	}
	if err = packet.ReadFromConn(conn, 10); err != nil {
		err = errors.Annotatef(err, "getRemoteFileMetas partition[%v] read from connection[%v]", dp.partitionId, remote)
		return
	}
	files := make([]*storage.FileInfo, 0)
	if err = json.Unmarshal(packet.Data[:packet.Size], &files); err != nil {
		err = errors.Annotatef(err, "getRemoteFileMetas partition[%v] unmarshal packet", dp.partitionId)
		return
	}
	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		fileMetas.files[file.FileId] = file
	}
	return
}

// Get all data partition group ,about all files meta
func (dp *dataPartition) getAllMemberFileMetas() (allMemberFileMetas []*MembersFileMetas, err error) {
	allMemberFileMetas = make([]*MembersFileMetas, len(dp.replicaHosts))
	var (
		extentFiles, tinyFiles []*storage.FileInfo
	)
	files := make([]*storage.FileInfo, 0)
	// get local extent file metas
	extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas extent dataPartition[%v] GetAllWaterMark", dp.partitionId)
		return
	}
	// get local tiny file metas
	tinyFiles, err = dp.tinyStore.GetAllWatermark()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas tiny dataPartition[%v] GetAllWaterMark", dp.partitionId)
		return
	}
	// write tiny files meta to extent files meta
	files = append(files, extentFiles...)
	files = append(files, tinyFiles...)
	leaderFileMetas := NewMemberFileMetas()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] GetAllWaterMark", dp.partitionId)
		return
	}
	for _, fi := range files {
		leaderFileMetas.files[fi.FileId] = fi
	}
	allMemberFileMetas[0] = leaderFileMetas
	// leader files meta has ready

	// get remote files meta by opGetAllWaterMarker cmd
	p := NewGetAllWaterMarker(dp.partitionId)
	for i := 1; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		target := dp.replicaHosts[i]
		conn, err = gConnPool.Get(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberFileMetas  dataPartition[%v] get host[%v] connect", dp.partitionId, target)
			return
		}
		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] write to host[%v]", dp.partitionId, target)
			return
		}
		err = p.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] read from host[%v]", dp.partitionId, target)
			return
		}
		fileInfos := make([]*storage.FileInfo, 0)
		err = json.Unmarshal(p.Data[:p.Size], &fileInfos)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] unmarshal json[%v]", dp.partitionId, string(p.Data[:p.Size]))
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
func (dp *dataPartition) generatorFilesRepairTasks(allMembers []*MembersFileMetas) {
	dp.generatorAddExtentsTasks(allMembers) //add extentTask
	dp.generatorFixFileSizeTasks(allMembers)
	dp.generatorDeleteExtentsTasks(allMembers)
	dp.generatorTinyDeleteTasks(allMembers)

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
		if fileId <= storage.TinyChunkCount {
			continue
		}
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[fileId]; !ok {
				addFile := &storage.FileInfo{Source: leaderAddr, FileId: fileId, Size: leaderFile.Size, Inode: leaderFile.Inode}
				follower.NeedAddExtentsTasks = append(follower.NeedAddExtentsTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition[%v] addFile[%v].", dp.partitionId, addFile)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixFileSizeTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	maxSizeExtentMap := dp.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	for fileId, leaderFile := range leader.files {
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
				allMembers[index].NeedFixFileSizeTasks = append(allMembers[index].NeedFixFileSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixFileSizeTasks] partition[%v] fixExtent[%v].", dp.partitionId, fixExtent)
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
				log.LogInfof("action[generatorDeleteExtentsTasks] partition[%v] deleteFile[%v].", dp.partitionId, deleteFile)
			}
		}
	}
}

//generator tinyObject delete task,send leader has delete object,notify follower delete it
func (dp *dataPartition) generatorTinyDeleteTasks(allMembers []*MembersFileMetas) {
	store := dp.tinyStore
	for _, chunkInfo := range allMembers[0].files {
		chunkId := chunkInfo.FileId
		if chunkId > storage.TinyChunkCount {
			continue
		}
		deletes := store.GetDelObjects(uint32(chunkId))
		deleteBuf := make([]byte, len(deletes)*ObjectIDSize)
		for index, deleteObject := range deletes {
			binary.BigEndian.PutUint64(deleteBuf[index*ObjectIDSize:(index+1)*ObjectIDSize], deleteObject)
		}
		for index := 0; index < len(allMembers); index++ {
			allMembers[index].NeedDeleteObjectsTasks[chunkId] = make([]byte, len(deleteBuf))
			copy(allMembers[index].NeedDeleteObjectsTasks[chunkId], deleteBuf)
		}
	}

}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyRepair(members []*MembersFileMetas) (err error) {
	var (
		errList []error
	)
	errList = make([]error, 0)
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
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
