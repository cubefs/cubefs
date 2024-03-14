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
// permissions and limitations under the License.

package metanode

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/synclist"
)

const (
	prefixDelExtent     = "EXTENT_DEL"
	prefixDelExtentV2   = "EXTENT_DEL_V2"
	prefixMultiVer      = verdataFile
	maxDeleteExtentSize = 10 * MB

	defAdjustHourMinuet = 50
	defEKDelDelaySecond = 60 * 10 //10min
)

var extentsFileHeader = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}

// TODO(NaturalSelect): remove those code

// / start metapartition delete extents work
// /
func (mp *metaPartition) startToDeleteExtents() {
	fileList := synclist.New()
	go mp.appendDelExtentsToFile(fileList)
	go mp.deleteExtentsFromList(fileList)
}

// create extent delete file
func (mp *metaPartition) createExtentDeleteFile(prefix string, idx int64, fileList *synclist.SyncList) (fp *os.File, fileName string, fileSize int64, err error) {
	fileName = fmt.Sprintf("%s_%d", prefix, idx)
	fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.LogErrorf("[metaPartition] createExtentDeletFile openFile %v %v error %v", mp.config.RootDir, fileName, err)
		return
	}
	if _, err = fp.Write(extentsFileHeader); err != nil {
		log.LogErrorf("[metaPartition] createExtentDeletFile Write %v %v error %v", mp.config.RootDir, fileName, err)
	}
	fileSize = int64(len(extentsFileHeader))
	fileList.PushBack(fileName)
	return
}

// append delete extents from extDelCh to EXTENT_DEL_N files
func (mp *metaPartition) appendDelExtentsToFile(fileList *synclist.SyncList) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf(fmt.Sprintf("[metaPartition] appendDelExtentsToFile pid(%v) panic (%v)", mp.config.PartitionId, r))
		}
	}()
	var (
		fileName string
		fileSize int64
		idx      int64
		fp       *os.File
		err      error
	)
LOOP:
	// scan existed EXTENT_DEL_* files to fill fileList
	finfos, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		panic(err)
	}

	finfos = sortDelExtFileInfo(finfos)
	for _, info := range finfos {
		fileList.PushBack(info.Name())
		fileSize = info.Size()
	}

	// check
	lastItem := fileList.Back()
	if lastItem != nil {
		fileName = lastItem.Value.(string)
	}
	if lastItem == nil || !strings.HasPrefix(fileName, prefixDelExtentV2) {
		// if no exist EXTENT_DEL_*, create one
		log.LogDebugf("action[appendDelExtentsToFile] verseq [%v]", mp.verSeq)
		fp, fileName, fileSize, err = mp.createExtentDeleteFile(prefixDelExtentV2, idx, fileList)
		log.LogDebugf("action[appendDelExtentsToFile] verseq [%v] fileName %v", mp.verSeq, fileName)
		if err != nil {
			panic(err)
		}
	} else {
		// exist, open last file
		fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			panic(err)
		}
		// continue from last item
		idx = getDelExtFileIdx(fileName)
	}

	log.LogDebugf("action[appendDelExtentsToFile] verseq [%v] fileName %v", mp.verSeq, fileName)
	// TODO Unhandled errors
	defer fp.Close()
	buf := make([]byte, 0)
	for {
		select {
		case <-mp.stopC:
			return
		case <-mp.extReset:
			// TODO Unhandled errors
			fp.Close()
			// reset fileList
			fileList.Init()
			goto LOOP
		case eks := <-mp.extDelCh:
			var data []byte
			buf = buf[:0]
			log.LogDebugf("del eks [%v]", eks)
			for _, ek := range eks {
				data, err = ek.MarshalBinaryWithCheckSum(true)
				if err != nil {
					log.LogWarnf("[appendDelExtentsToFile] partitionId=%d,"+
						" extentKey marshal: %s", mp.config.PartitionId, err.Error())
					break
				}
				buf = append(buf, data...)
			}

			if err != nil {
				err = mp.sendExtentsToChan(eks)
				if err != nil {
					log.LogErrorf("[appendDelExtentsToFile] mp[%v] sendExtentsToChan fail, err(%s)", mp.config.PartitionId, err.Error())
				}
				continue
			}
			if fileSize >= maxDeleteExtentSize {
				// TODO Unhandled errors
				// close old File
				fp.Close()
				idx += 1
				fp, fileName, fileSize, err = mp.createExtentDeleteFile(prefixDelExtentV2, idx, fileList)
				if err != nil {
					panic(err)
				}
				log.LogDebugf("appendDelExtentsToFile. volname [%v] mp[%v] createExtentDeleteFile %v",
					mp.GetVolName(), mp.config.PartitionId, fileName)
			}
			// write delete extents into file
			if _, err = fp.Write(buf); err != nil {
				panic(err)
			}
			fileSize += int64(len(buf))
			log.LogDebugf("action[appendDelExtentsToFile] filesize now %v", fileSize)
		}
	}
}

// Delete all the extents of a file.
func (mp *metaPartition) deleteExtentsFromList(fileList *synclist.SyncList) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf(fmt.Sprintf("deleteExtentsFromList(%v) deleteExtentsFromList panic (%v)", mp.config.PartitionId, r))
		}
	}()

	var (
		element  *list.Element
		fileName string
		file     string
		fileInfo os.FileInfo
		err      error
	)
	for {
		time.Sleep(time.Minute)
		select {
		case <-mp.stopC:
			return
		default:
		}
	LOOP:
		element = fileList.Front()
		if element == nil {
			continue
		}
		fileName = element.Value.(string)
		file = path.Join(mp.config.RootDir, fileName)
		if fileInfo, err = os.Stat(file); err != nil {
			fileList.Remove(element)
			goto LOOP
		}
		// if not leader, ignore delete
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deleteExtentsFromList] partitionId=%d, "+
				"not raft leader,please ignore", mp.config.PartitionId)
			continue
		}
		// leader do delete extent for EXTENT_DEL_* file

		// read delete extents from file
		buf := make([]byte, MB)
		fp, err := os.OpenFile(file, os.O_RDWR, 0o644)
		if err != nil {
			log.LogErrorf("[deleteExtentsFromList] volname [%v] mp[%v] openFile %v error: %v", mp.GetVolName(), mp.config.PartitionId, file, err)
			fileList.Remove(element)
			goto LOOP
		}

		// get delete extents cursor at file header 8 bytes
		if _, err = fp.ReadAt(buf[:8], 0); err != nil {
			log.LogWarnf("[deleteExtentsFromList] partitionId=%d, "+
				"read cursor least 8bytes, retry later", mp.config.PartitionId)
			// TODO Unhandled errors
			fp.Close()
			continue
		}
		extentV2 := false
		extentKeyLen := uint64(proto.ExtentLength)
		if strings.HasPrefix(fileName, prefixDelExtentV2) {
			extentV2 = true
			extentKeyLen = uint64(proto.ExtentV2Length)
		}
		cursor := binary.BigEndian.Uint64(buf[:8])
		stat, _ := fp.Stat()
		log.LogDebugf("[deleteExtentsFromList] volname [%v] mp[%v] o openFile %v file len %v cursor %v", mp.GetVolName(), mp.config.PartitionId, file,
			stat.Size(), cursor)

		log.LogDebugf("action[deleteExtentsFromList] get cursor %v", cursor)
		if size := uint64(fileInfo.Size()) - cursor; size < MB {
			if size <= 0 {
				size = extentKeyLen
			} else if size > 0 && size < extentKeyLen {
				errStr := fmt.Sprintf(
					"[deleteExtentsFromList] partitionId=%d, %s file corrupted!",
					mp.config.PartitionId, fileName)
				log.LogErrorf(errStr) // FIXME
				fileList.Remove(element)
				fp.Close()
				goto LOOP
			}
			buf = buf[:size]
		}
		// read extents from cursor
		rLen, err := fp.ReadAt(buf, int64(cursor))
		// TODO Unhandled errors
		fp.Close()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err == io.EOF {
			err = nil
			if fileList.Len() <= 1 {
				log.LogDebugf("[deleteExtentsFromList] partitionId=%d, %s"+
					" extents delete ok n(%d), len(%d), cursor(%d)", mp.config.PartitionId, fileName, rLen, len(buf), cursor)
			} else {
				status := mp.raftPartition.Status()
				if status.State == "StateLeader" && !status.RestoringSnapshot {
					// delete old delete extents file for metapartition
					if _, err = mp.submit(opFSMInternalDelExtentFile, []byte(fileName)); err != nil {
						log.LogErrorf(
							"[deleteExtentsFromList] partitionId=%d,"+
								"delete old file: %s,status: %s", mp.config.PartitionId,
							fileName, err.Error())
					}
					log.LogDebugf("[deleteExtentsFromList] partitionId=%d "+
						",delete old file: %s, status: %v", mp.config.PartitionId, fileName,
						err == nil)
					goto LOOP
				}
				log.LogDebugf("[deleteExtentsFromList] partitionId=%d,delete"+
					" old file status: %s", mp.config.PartitionId, status.State)
			}
			continue
		}
		buff := bytes.NewBuffer(buf)
		cursor += uint64(rLen)
		var deleteCnt uint64
		errExts := make([]proto.ExtentKey, 0)
		for {
			if buff.Len() == 0 {
				break
			}

			if uint64(buff.Len()) < extentKeyLen {
				cursor -= uint64(buff.Len())
				break
			}

			if extentV2 && uint64(buff.Len()) < uint64(proto.ExtentV3Length) {
				if r := bytes.Compare(buff.Bytes()[:4], proto.ExtentKeyHeaderV3); r == 0 {
					cursor -= uint64(buff.Len())
					break
				}
			}

			batchCount := DeleteBatchCount() * 5
			if deleteCnt%batchCount == 0 {
				DeleteWorkerSleepMs()
			}

			if len(errExts) >= int(batchCount) {
				time.Sleep(100 * time.Millisecond)
				err = mp.sendExtentsToChan(errExts)
				if err != nil {
					log.LogErrorf("deleteExtentsFromList sendExtentsToChan by raft error, mp[%v], err(%v), ek(%v)", mp.config.PartitionId, err.Error(), len(errExts))
				}

				errExts = make([]proto.ExtentKey, 0)
			}

			ek := proto.ExtentKey{}
			if extentV2 {
				if err = ek.UnmarshalBinaryWithCheckSum(buff); err != nil {
					if err == proto.InvalidKeyHeader || err == proto.InvalidKeyCheckSum {
						log.LogErrorf("[deleteExtentsFromList] invalid extent key header %v, %v, %v", fileName, mp.config.PartitionId, err)
						continue
					}
					log.LogErrorf("[deleteExtentsFromList] mp: %v Unmarshal extentkey from %v unresolved error: %v", mp.config.PartitionId, fileName, err)
					panic(err)
				}
			} else {
				// ek for del no need to get version
				if err = ek.UnmarshalBinary(buff, false); err != nil {
					panic(err)
				}
			}
			// delete dataPartition
			if err = mp.doDeleteMarkedInodes(&ek); err != nil {
				errExts = append(errExts, ek)
				log.LogWarnf("[deleteExtentsFromList] mp: %v, extent: %v, %s",
					mp.config.PartitionId, ek.String(), err.Error())
			}
			deleteCnt++
		}

		err = mp.sendExtentsToChan(errExts)
		if err != nil {
			log.LogErrorf("deleteExtentsFromList sendExtentsToChan by raft error, mp[%v], err(%v), ek(%v)", mp.config.PartitionId, err.Error(), len(errExts))
		}

		buff.Reset()
		buff.WriteString(fmt.Sprintf("%s %d", fileName, cursor))
		if _, err = mp.submit(opFSMInternalDelExtentCursor, buff.Bytes()); err != nil {
			log.LogWarnf("[deleteExtentsFromList] partitionId=%d, %s",
				mp.config.PartitionId, err.Error())
		}

		log.LogDebugf("[deleteExtentsFromList] partitionId=%d, file=%s, cursor=%d, size=%d",
			mp.config.PartitionId, fileName, cursor, len(buf))
		goto LOOP
	}
}

// func (mp *metaPartition) checkBatchDeleteExtents(allExtents map[uint64][]*proto.ExtentKey) {
// 	for partitionID, deleteExtents := range allExtents {
// 		needDeleteExtents := make([]proto.ExtentKey, len(deleteExtents))
// 		for index, ek := range deleteExtents {
// 			newEx := proto.ExtentKey{
// 				FileOffset:   ek.FileOffset,
// 				PartitionId:  ek.PartitionId,
// 				ExtentId:     ek.ExtentId,
// 				ExtentOffset: ek.ExtentOffset,
// 				Size:         ek.Size,
// 				CRC:          ek.CRC,
// 			}
// 			needDeleteExtents[index] = newEx
// 			log.LogWritef("mp[%v] deleteExtents(%v)", mp.config.PartitionId, newEx.String())
// 		}
// 		err := mp.doBatchDeleteExtentsByPartition(partitionID, deleteExtents)
// 		if err != nil {
// 			log.LogWarnf(fmt.Sprintf("metaPartition(%v) dataPartitionID(%v)"+
// 				" batchDeleteExtentsByPartition failed(%v)", mp.config.PartitionId, partitionID, err))
// 			mp.extDelCh <- needDeleteExtents
// 		}
// 		DeleteWorkerSleepMs()
// 	}
// 	return
// }

// NOTE: new api

const (
	deleteExtentsTravlerRestartTicket = 10 * time.Second
)

type DeleteExtentsFromTreeRequest struct {
	DeletedKeys []*DeletedExtentKey
}

func (r *DeleteExtentsFromTreeRequest) Marshal() (v []byte, err error) {
	buff := bytes.NewBuffer([]byte{})
	count := uint64(len(r.DeletedKeys))
	err = binary.Write(buff, binary.BigEndian, count)
	if err != nil {
		return
	}
	for _, dek := range r.DeletedKeys {
		v, err = dek.Marshal()
		if err != nil {
			return
		}
		_, err = buff.Write(v)
		if err != nil {
			return
		}
	}
	v = buff.Bytes()
	return
}

func (r *DeleteExtentsFromTreeRequest) Unmarshal(v []byte) (err error) {
	buff := bytes.NewBuffer(v)
	count := uint64(0)
	err = binary.Read(buff, binary.BigEndian, &count)
	if err != nil {
		return
	}
	r.DeletedKeys = make([]*DeletedExtentKey, 0, count)
	for i := uint64(0); i < count; i++ {
		dek := &DeletedExtentKey{}
		err = dek.UnmarshalWithBuffer(buff)
		if err != nil {
			return
		}
		r.DeletedKeys = append(r.DeletedKeys, dek)
	}
	return
}

type DeleteObjExtentsFromTreeRequest struct {
	DeletedObjKeys []*DeletedObjExtentKey
}

func (r *DeleteObjExtentsFromTreeRequest) Marshal() (v []byte, err error) {
	buff := bytes.NewBuffer([]byte{})
	count := uint64(len(r.DeletedObjKeys))
	err = binary.Write(buff, binary.BigEndian, count)
	if err != nil {
		return
	}
	for _, doek := range r.DeletedObjKeys {
		v, err = doek.Marshal()
		if err != nil {
			return
		}
		_, err = buff.Write(v)
		if err != nil {
			return
		}
	}
	v = buff.Bytes()
	return
}

func (r *DeleteObjExtentsFromTreeRequest) Unmarshal(v []byte) (err error) {
	buff := bytes.NewBuffer(v)
	count := uint64(0)
	err = binary.Read(buff, binary.BigEndian, &count)
	if err != nil {
		return
	}
	r.DeletedObjKeys = make([]*DeletedObjExtentKey, 0, count)
	for i := uint64(0); i < count; i++ {
		doek := &DeletedObjExtentKey{}
		err = doek.UnmarshalWithBuffer(buff)
		if err != nil {
			return
		}
		r.DeletedObjKeys = append(r.DeletedObjKeys, doek)
	}
	return
}

var (
	ErrDelExtentFileBroken      = errors.New("delete extent file broken")
	ErrDataPartitionNotFound    = errors.New("data partition not found")
	ErrInvalidDataPartition     = errors.New("invalid data partition")
	ErrDataPartitionUnreachable = errors.New("data partition network error")
	ErrFailedToDeleteExtents    = errors.New("failed to delete extents")
)

func (mp *metaPartition) batchDeleteExtentsHotVol(dpId uint64, deks []*DeletedExtentKey) (err error) {
	log.LogDebugf("[batchDeleteExtentsHotVol] mp(%v) delete dp(%v) extents count(%v)", mp.config.PartitionId, dpId, len(deks))
	// get the data node view
	dp := mp.vol.GetPartition(dpId)
	if dp == nil {
		err = ErrDataPartitionNotFound
		return
	}

	// delete the data node
	if len(dp.Hosts) < 1 {
		log.LogErrorf("[batchDeleteExtentsHotVol] dp id(%v) is invalid, detail[%v]", dpId, dp)
		err = ErrInvalidDataPartition
		return
	}
	addr := util.ShiftAddrPort(dp.Hosts[0], smuxPortShift)
	conn, err := smuxPool.GetConnect(addr)
	log.LogInfof("[batchDeleteExtentsHotVol] mp (%v) GetConnect (%v)", mp.config.PartitionId, addr)

	resultCode := proto.OpOk

	defer func() {
		smuxPool.PutConnect(conn, ForceClosedConnect)
		log.LogInfof("[batchDeleteExtentsHotVol] mp (%v) PutConnect (%v)", mp.config.PartitionId, addr)
	}()

	if err != nil {
		err = ErrDataPartitionUnreachable
		return
	}
	eks := make([]*proto.ExtentKey, 0, len(deks))
	for _, dek := range deks {
		eks = append(eks, &dek.ExtentKey)
	}
	p := NewPacketToBatchDeleteExtent(dp, eks)
	if err = p.WriteToConn(conn); err != nil {
		err = ErrDataPartitionUnreachable
		return
	}
	if err = p.ReadFromConnWithVer(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		err = ErrDataPartitionUnreachable
		return
	}

	resultCode = p.ResultCode

	if resultCode == proto.OpTryOtherAddr && proto.IsCold(mp.volType) {
		log.LogInfof("[batchDeleteExtentsHotVol] deleteOp retrun tryOtherAddr code means dp is deleted for LF vol, dp(%d)", dpId)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = ErrFailedToDeleteExtents
		log.LogErrorf("[batchDeleteExtentsHotVol] failed to delete dp(%v) extents, result code(%v)", dpId, p.ResultCode)
		return
	}
	return
}

const maxDelCntOnce = 512

func (mp *metaPartition) batchDeleteExtentsColdVol(doeks []*DeletedObjExtentKey) (err error) {
	total := len(doeks)
	oeks := make([]proto.ObjExtentKey, maxDelCntOnce)
	for i := 0; i < total; i += maxDelCntOnce {
		max := util.Min(i+maxDelCntOnce, total)
		length := max - i
		for j := 0; j < length; j++ {
			oeks[j] = doeks[i+j].ObjExtentKey
		}
		err = mp.ebsClient.Delete(oeks[0:length])
		if err != nil {
			log.LogErrorf("[batchDeleteExtentsColdVol] delete ebs eks fail, cnt(%d), err(%s)", max-i, err.Error())
			return err
		}
	}
	return
}

func (mp *metaPartition) startDeleteExtentsTraveler() {
	go func() {
		for {
			select {
			case <-mp.stopC:
				return
			default:
				err := mp.deletedExtentsTreeTraveler()
				if err != nil {
					log.LogErrorf("[startDeleteExtent] mp(%v) traveler exits, restart after %v, err(%v)", mp.config.PartitionId, deleteExtentsTravlerRestartTicket, err)
					time.Sleep(deleteExtentsTravlerRestartTicket)
					continue
				}
			}
		}
	}()
}

func (mp *metaPartition) startDeleteObjExtentsTraveler() {
	go func() {
		for {
			select {
			case <-mp.stopC:
				return
			default:
				err := mp.deletedObjExtentsTreeTravler()
				if err != nil {
					log.LogErrorf("[startDeleteObjExtentsTraveler] mp(%v) traveler exits, restart after %v, err(%v)", mp.config.PartitionId, deleteExtentsTravlerRestartTicket, err)
					time.Sleep(deleteExtentsTravlerRestartTicket)
					continue
				}
			}
		}
	}()
}

func (mp *metaPartition) skipDeleteExtentKey(dek *DeletedExtentKey) (ok bool, err error) {
	inTx, _, err := mp.txProcessor.txResource.isInodeInTransction(&Inode{Inode: dek.Inode})
	if err != nil {
		return false, err
	}
	// TODO(NaturalSelect): consider discard flag
	ok = inTx
	return
}

func (mp *metaPartition) skipDeleteObjExtentKey(doek *DeletedObjExtentKey) (ok bool, err error) {
	inTx, _, err := mp.txProcessor.txResource.isInodeInTransction(&Inode{Inode: doek.Inode})
	if err != nil {
		return false, err
	}
	ok = inTx
	return
}

func (mp *metaPartition) deletedExtentsTreeTraveler() (err error) {
	sleepDuration := getDeleteWorkerSleepMs()
	batchSize := DeleteBatchCount()
	timer := time.NewTimer(sleepDuration)
	defer timer.Stop()
	var snap Snapshot
	for {
		select {
		case <-mp.stopC:
			return
		case <-timer.C:
			if sleepDuration != getDeleteWorkerSleepMs() {
				timer.Reset(sleepDuration)
			}
			if batchSize != DeleteBatchCount() {
				batchSize = DeleteBatchCount()
			}
		}
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deletedExtentTreeTraveler] mp(%v) is not leader sleep", mp.config.PartitionId)
			continue
		}
		func() {
			log.LogDebugf("[deletedExtentTreeTraveler] mp(%v) start travel", mp.config.PartitionId)
			snap, err = mp.GetSnapShot()
			if err != nil {
				log.LogErrorf("[deletedExtentTreeTraveler] mp(%v) failed to open snapshot, err(%v)", mp.config.PartitionId, err)
				return
			}
			defer snap.Close()
			count := uint64(0)
			dekMap := make(map[uint64][]*DeletedExtentKey)
			err = snap.Range(DeletedExtentsType, func(item interface{}) (bool, error) {
				dek := item.(*DeletedExtentKey)
				skip, err := mp.skipDeleteExtentKey(dek)
				if err != nil {
					return false, err
				}
				if skip {
					return true, nil
				}
				count++
				dpId := dek.ExtentKey.PartitionId
				deks, ok := dekMap[dpId]
				if !ok {
					deks = make([]*DeletedExtentKey, 0)
				}
				deks = append(deks, dek)
				dekMap[dpId] = deks
				if count >= batchSize {
					return false, nil
				}
				return true, nil
			})
			if err != nil || count == 0 {
				return
			}
			for dpId, deks := range dekMap {
				err = mp.batchDeleteExtentsHotVol(dpId, deks)
				if err != nil {
					log.LogErrorf("[deletedExtentTreeTraveler] failed to delete ek from dp(%v), err(%v)", dpId, err)
					err = nil
					continue
				}

				request := &DeleteExtentsFromTreeRequest{
					DeletedKeys: deks,
				}

				var v []byte
				v, err = request.Marshal()
				if err != nil {
					log.LogErrorf("[deletedExtentTreeTraveler] failed to marshal remove request, err(%v)", err)
					err = nil
					continue
				}

				log.LogDebugf("[deletedExtentTreeTraveler] mp(%v) delete deks cnt(%v)", mp.config.PartitionId, len(deks))
				_, err = mp.submit(opFSMDeleteExtentFromTree, v)
				if err != nil {
					log.LogErrorf("[deletedExtentTreeTraveler] mp(%v) failed to remove deks, err(%v)", mp.config.PartitionId, err)
					err = nil
					continue
				}
			}
		}()
		// NOTE: err may modified in lambda
		if err != nil {
			return
		}
	}
}

func (mp *metaPartition) deletedObjExtentsTreeTravler() (err error) {
	sleepDuration := getDeleteWorkerSleepMs()
	batchSize := DeleteBatchCount()
	timer := time.NewTimer(sleepDuration)
	defer timer.Stop()
	var snap Snapshot
	doeks := make([]*DeletedObjExtentKey, batchSize)
	for {
		select {
		case <-mp.stopC:
			return
		case <-timer.C:
			if sleepDuration != getDeleteWorkerSleepMs() {
				timer.Reset(sleepDuration)
			}
			if batchSize != DeleteBatchCount() {
				batchSize = DeleteBatchCount()
			}
		}
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deletedObjExtentsTreeTravler] mp(%v) is not leader sleep", mp.config.PartitionId)
			continue
		}
		func() {
			log.LogDebugf("[deletedObjExtentsTreeTravler] mp(%v) start travel", mp.config.PartitionId)
			snap, err = mp.GetSnapShot()
			if err != nil {
				log.LogErrorf("[deletedObjExtentsTreeTravler] mp(%v) failed to open snapshot, err(%v)", mp.config.PartitionId, err)
				return
			}
			defer snap.Close()
			count := uint64(0)
			err = snap.Range(DeletedObjExtentsType, func(item interface{}) (bool, error) {
				doek := item.(*DeletedObjExtentKey)
				skip, err := mp.skipDeleteObjExtentKey(doek)
				if err != nil {
					return false, err
				}
				if skip {
					return true, nil
				}
				doeks[count] = doek
				count++
				if count >= batchSize {
					return false, nil
				}
				return true, nil
			})
			if err != nil || count == 0 {
				return
			}
			err = mp.batchDeleteExtentsColdVol(doeks[:count])
			if err != nil {
				log.LogErrorf("[deletedObjExtentsTreeTravler] failed to delete ek from cid(%v), err(%v)", doeks[0].ObjExtentKey.Cid, err)
				return
			}
			request := &DeleteObjExtentsFromTreeRequest{
				DeletedObjKeys: doeks[:count],
			}
			var v []byte
			v, err = request.Marshal()
			if err != nil {
				log.LogErrorf("[deletedObjExtentsTreeTravler] failed to marshal remove request, err(%v)", err)
				return
			}
			log.LogErrorf("[deletedObjExtentsTreeTravler] mp(%v) delete doeks cnt(%v)", mp.config.PartitionId, count)
			_, err = mp.submit(opFSMDeleteObjExtentFromTree, v)
			if err != nil {
				log.LogErrorf("[deletedObjExtentTreeTraveler] mp(%v) failed to remove doeks, err(%v)", mp.config.PartitionId, err)
				return
			}
		}()
		// NOTE: err may modified in lambda
		if err != nil {
			return
		}
	}
}

func (mp *metaPartition) sortExtentsForMove(eks []*proto.ExtentKey) {
	// NOTE: sort it to keep consistent
	sort.Slice(eks, func(i, j int) (ok bool) {
		// NOTE: partition id
		if eks[i].PartitionId != eks[j].PartitionId {
			return eks[i].PartitionId < eks[j].PartitionId
		}
		// NOTE: extent id
		if eks[i].ExtentId != eks[j].ExtentId {
			return eks[i].ExtentId < eks[j].ExtentOffset
		}
		// NOTE: extent offset
		return eks[i].ExtentOffset < eks[j].ExtentOffset
	})
}

func (mp *metaPartition) moveDelExtentToTree(dbHandle interface{}) (files []string, err error) {
	files, err = mp.getDelExtentFiles()
	if err != nil {
		return
	}
	eks := make([]*proto.ExtentKey, 0)
	for _, file := range files {
		eks, err = mp.getAllDeltedExtentsFromList(eks, file)
		if err != nil {
			return
		}
	}

	mp.sortExtentsForMove(eks)
	for _, ek := range eks {
		dek := NewDeletedExtentKey(ek, 0, mp.AllocDeletedExtentId())
		err = mp.deletedExtentsTree.Put(dbHandle, dek)
		if err != nil {
			return
		}
	}
	return
}

func (mp *metaPartition) getDelExtentFiles() (files []string, err error) {
	files = make([]string, 0)
	dentries, err := os.ReadDir(mp.config.RootDir)
	if err != nil {
		return
	}

	for _, dentry := range dentries {
		if dentry.Type().IsRegular() && strings.HasPrefix(dentry.Name(), prefixDelExtent) {
			files = append(files, dentry.Name())
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return getDelExtFileIdx(files[i]) < getDelExtFileIdx(files[j])
	})
	return
}

func (mp *metaPartition) getAllDeltedExtentsFromList(oldEks []*proto.ExtentKey, fileName string) (eks []*proto.ExtentKey, err error) {
	eks = oldEks
	if eks == nil {
		eks = make([]*proto.ExtentKey, 0)
	}
	var (
		fp       *os.File
		fileInfo os.FileInfo
	)
	file := path.Join(mp.config.RootDir, fileName)
	if fileInfo, err = os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		log.LogErrorf("[getAllDeltedExtentsFromList] mp(%v) deleted extents file stat error, disk broken? err(%v)", mp.config.PartitionId, err)
		return
	}

	// NOTE: get version
	extentV2 := false
	extentKeyLen := uint64(proto.ExtentLength)
	if strings.HasPrefix(fileName, prefixDelExtentV2) {
		extentV2 = true
		extentKeyLen = uint64(proto.ExtentV2Length)
	}

	// read delete extents from file
	var cursor uint64
	fp, err = os.OpenFile(file, os.O_RDWR, 0o644)
	if err != nil {
		log.LogErrorf("[getAllDeltedExtentsFromList] volname [%v] mp[%v] openFile %v error: %v", mp.GetVolName(), mp.config.PartitionId, file, err)
		return
	}
	defer fp.Close()

	// get delete extents cursor at file header 8 bytes
	if err = binary.Read(fp, binary.BigEndian, &cursor); err != nil {
		log.LogWarnf("[getAllDeltedExtentsFromList] partitionId(%v), failed to read cursor", mp.config.PartitionId)
		return
	}
	log.LogDebugf("action[getAllDeltedExtentsFromList] get cursor %v", cursor)
	fileSize := uint64(fileInfo.Size())

	// NOTE: >= size, return
	if cursor >= fileSize {
		return
	}

	if fileSize-cursor < extentKeyLen {
		log.LogErrorf("[getAllDeltedExtentsFromList] partitionId(%v), %v file corrupted!", mp.config.PartitionId, fileName)
		err = ErrDelExtentFileBroken
		return
	}

	readLen := 0
	headerBuffer := make([]byte, proto.ExtentKeyHeaderSize)
	ekBuffer := make([]byte, proto.ExtentV3Length)
	for cursor < fileSize {
		// NOTE: read header to check ek version
		ekSize := extentKeyLen
		if extentV2 {
			// NOTE: check extent V3
			readLen, err = fp.ReadAt(headerBuffer, int64(cursor))
			if err != nil {
				return
			}
			if readLen != len(headerBuffer) {
				log.LogErrorf("[getAllDeltedExtentsFromList] partitionId(%v), %v file corrupted!", mp.config.PartitionId, fileName)
				err = ErrDelExtentFileBroken
				return
			}
			if extentV2 && bytes.Equal(headerBuffer, proto.ExtentKeyHeaderV3) {
				// NOTE: is extent v3
				ekSize = uint64(proto.ExtentV3Length)
			}
		}
		// NOTE: read extent key
		readLen, err = fp.ReadAt(ekBuffer, int64(cursor))
		if err != nil {
			return
		}
		if readLen != int(ekSize) {
			log.LogErrorf("[getAllDeltedExtentsFromList] partitionId(%v), %v file corrupted!", mp.config.PartitionId, fileName)
			return
		}
		cursor += uint64(readLen)
		// NOTE: unmarshal extents
		ek := &proto.ExtentKey{}
		buff := bytes.NewBuffer(ekBuffer)
		if extentV2 {
			if err = ek.UnmarshalBinaryWithCheckSum(buff); err != nil {
				if err == proto.InvalidKeyHeader || err == proto.InvalidKeyCheckSum {
					log.LogErrorf("[getAllDeltedExtentsFromList] invalid extent key header %v, %v, %v", fileName, mp.config.PartitionId, err)
					continue
				}
				log.LogErrorf("[getAllDeltedExtentsFromList] mp: %v Unmarshal extentkey from %v unresolved error: %v", mp.config.PartitionId, fileName, err)
				return
			}
		} else {
			// ek for del no need to get version
			if err = ek.UnmarshalBinary(buff, false); err != nil {
				return
			}
		}
		// NOTE: append to result set
		eks = append(eks, ek)
	}
	return
}
