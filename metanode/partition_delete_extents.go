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
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/util/unit"
	"github.com/spf13/cast"
	"go.uber.org/atomic"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/synclist"
)

const (
	prefixDelExtent     = "EXTENT_DEL"
	maxDeleteExtentSize = 10 * MB
	oldDBExtentKeySize  = 32 // 1 +   5   +   2    +   8  +  8  +   4  +    4
	//type  time   reversed  pid   exid   offset  size
	//time:YY YY MM DD HH

	dbExtentKeySize = 48 // 1 +   5   +   2    +  8   +   8  +  8  +   8  +    4    + 4
	//type  time   reversed   foff    pid   ekid   ekoff  size    reversed2
	//time:YY YY MM DD HH
	centuryKeyIndex = 1
	yearKeyIndex    = 2
	monthKeyIndex   = 3
	dayKeyIndex     = 4
	hourKeyIndex    = 5

	maxItemsPerBatch = 100
	maxRetryCnt      = 1000

	leaderDelTimerValue   = 1 * time.Minute
	followerDelTimerValue = 5 * time.Minute

	defMaxDelEKRecord = 100000

	delExtentKeyList             = "deleteExtentList.tmp"
	prefixDelExtentKeyListBackup = "deleteExtentList."

	InodeDelExtentKeyList             = "inodeDeleteExtentList.tmp"
	PrefixInodeDelExtentKeyListBackup = "inodeDeleteExtentList."

	defDeleteEKRecordFilesMaxTotalSize = 60 * unit.MB

	MaxMetaDataDiskUsedFactor = 0.5
)

var extentsFileHeader = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}
var writeDeleteExtentsLock = &sync.Mutex{}
var DeleteEKRecordFilesMaxTotalSize = atomic.NewUint64(defDeleteEKRecordFilesMaxTotalSize)

func (mp *metaPartition) initResouce() {
	mp.addBatchKey = make([]byte, dbExtentKeySize*maxItemsPerBatch)
	mp.delBatchKey = make([]byte, dbExtentKeySize*maxItemsPerBatch)
}

func (mp *metaPartition) startToDeleteExtents() {
	mp.addBatchKey = make([]byte, dbExtentKeySize*maxItemsPerBatch)
	mp.delBatchKey = make([]byte, dbExtentKeySize*maxItemsPerBatch)

	go mp.appendDelExtentsToDb()
	go mp.deleteExtentsFromDb()
}

func updateKeyToNow(data []byte) {
	now := time.Now()
	now.Add(24 * time.Hour)
	//YY YY MM DD HH
	data[0] = byte(ExtentDelTable)
	data[centuryKeyIndex] = (byte)(now.Year() / 100)
	data[yearKeyIndex] = (byte)(now.Year() % 100)
	data[monthKeyIndex] = (byte)(now.Month())
	data[dayKeyIndex] = (byte)(now.Day())
	data[hourKeyIndex] = (byte)(now.Hour())
	return
}

func updateKeyToDate(key []byte, date uint64) {
	key[0] = byte(ExtentDelTable)

	for i := hourKeyIndex; i >= centuryKeyIndex; i-- {
		key[i] = byte(date % 100)
		date /= 100
	}
	return
}

func generalDateKey() uint64 {
	now := time.Now()
	curDateKey := uint64(now.Year()/100)*100000000 + uint64(now.Year()%100)*1000000 + uint64(now.Month())*10000 + uint64(now.Day())*100 + uint64(now.Hour())
	return curDateKey
}

func getDateInKey(k []byte) uint64 {
	dateInfo := uint64(k[centuryKeyIndex])*100000000 + uint64(k[yearKeyIndex])*1000000 + uint64(k[monthKeyIndex])*10000 + uint64(k[dayKeyIndex])*100 + uint64(k[hourKeyIndex])
	return dateInfo
}

func (mp *metaPartition) addDelExtentToDb(key []byte, eks []proto.MetaDelExtentKey) (err error) {
	log.LogInfof("Mp[%d] add delete extent, date:%d, count:%d, success", mp.config.PartitionId, getDateInKey(key), len(eks))
	data := make([]byte, 1)
	var handle interface{}
	cnt := 0

	defer func() {
		if err != nil {
			log.LogWarnf("Mp[%d] add delete extent, date:%d, count:%d, failed:%s", mp.config.PartitionId, getDateInKey(key), len(eks), err.Error())
			return
		}
		log.LogDebugf("Mp[%d] add delete extent, date:%d, count:%d, success", mp.config.PartitionId, getDateInKey(key), len(eks))
	}()

	if handle, err = mp.db.CreateBatchHandler(); err != nil {
		log.LogErrorf("[addDelExtentToDb] partition[%v] create batch handler failed:%v", mp.config.PartitionId, err)
		return err
	}

	for _, ek := range eks {
		var ekInfo []byte
		valueBuff := make([]byte, proto.ExtentValueLen)
		ek.MarshDelEkValue(valueBuff)

		if ekInfo, err = ek.MarshalDbKey(); err != nil {
			log.LogWarnf("[addDelExtentToDb] partitionId=%d,"+
				" extentKey[%v] marshal err: %s", mp.config.PartitionId, ek, err.Error())
			goto errOut
		}

		copy(key[8:], ekInfo)
		log.LogDebugf("add del extent key:%v, len:%v, data:%v, len:%v, cnt:%v, inode:%d\n", key, len(key), data, len(data), cnt, ek.FileOffset)

		if cnt != 0 && cnt%maxItemsPerBatch == 0 {
			if err = mp.db.CommitBatchAndRelease(handle); err != nil {
				log.LogWarnf("[addDelExtentToDb] partitionId=%d,"+
					" extentKey marshal: %s", mp.config.PartitionId, err.Error())
				goto errOut
			}
			if handle, err = mp.db.CreateBatchHandler(); err != nil {
				log.LogErrorf("[addDelExtentToDb] partition[%v] create batch handle failed:%v", mp.config.PartitionId, err)
				goto errOut
			}
		}
		keyOffset := (cnt % maxItemsPerBatch) * dbExtentKeySize
		copy(mp.addBatchKey[keyOffset:keyOffset+8], key[0:8])
		copy(mp.addBatchKey[keyOffset+8:keyOffset+dbExtentKeySize], ekInfo)
		if err = mp.db.AddItemToBatch(handle, mp.addBatchKey[keyOffset:keyOffset+dbExtentKeySize], valueBuff); err != nil {
			log.LogErrorf("[addDelExtentToDb] partition[%v] add item to batch handle failed:%v", mp.config.PartitionId, err)
			goto errOut
		}

		cnt++
	}

	err = mp.db.CommitBatchAndRelease(handle)
	if err != nil {
		log.LogErrorf("[addDelExtentToDb] partition[%v] commit batch handle and release failed:%v", mp.config.PartitionId, err)
	}

	return

errOut:
	if handle != nil {
		mp.db.ReleaseBatchHandle(handle)
	}
	return
}

func (mp *metaPartition) appendDelExtentsToDb() {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("appendDelExtentsToDb: partition(%v) err(%v)", mp.config.PartitionId, err)
			log.LogFlush()
			panic(err)
		}
	}()
	commitCheckTimer := time.NewTimer(time.Minute * 5)
	extDeleteCursor := uint64(0)
	for {
		select {
		case <-mp.stopC:
			commitCheckTimer.Stop()
			return
		case <-mp.extReset:
			// nothing to do; already clean
		case <-commitCheckTimer.C:
			commitCheckTimer.Reset(time.Minute * 5)
			if _, ok := mp.IsLeader(); !ok {
				continue
			}

			curDate := generalDateKey()
			if extDeleteCursor == 0 {
				extDeleteCursor = curDate
			} else if extDeleteCursor != curDate {
				log.LogInfof("Mp[%d] hour changed, notify sync to follower, old:%d, new:%d", mp.config.PartitionId, extDeleteCursor, curDate)
				extDeleteCursor = curDate
				select {
				case mp.extDelCursor <- extDeleteCursor:
				default:
				}
			}
		case eks := <-mp.extDelCh:
			key := make([]byte, dbExtentKeySize)
			updateKeyToNow(key)
			if err := mp.addDelExtentToDb(key, eks); err != nil {
				select {
				case mp.extDelCh <- eks:
				default:
					log.LogWarnf("appendDelExtentsToDb mp[%v] reput deleted eks(%v) to channel failed", mp.config.PartitionId, eks)
				}
			}
		}
	}
}

func (mp *metaPartition) fsmSyncDelExtents(data []byte) {
	eks := make([]proto.MetaDelExtentKey, 0)
	key := make([]byte, dbExtentKeySize)
	extDeleteCursor := binary.BigEndian.Uint64(data)
	buff := bytes.NewBuffer(data[8:])
	log.LogInfof("Mp[%d] follower recv sync extent delete info, date:%d, data:%v", mp.config.PartitionId, extDeleteCursor, data)
	for {
		if buff.Len() == 0 {
			break
		}

		if buff.Len() < 24 {
			log.LogWarnf("Mp[%d] follower recv sync extent delete info, date:%d; recv err packet; broken ek record, buff len:%d",
				mp.config.PartitionId, extDeleteCursor, buff.Len())
			return
		}

		ek := proto.ExtentKey{}
		if err := ek.UnmarshalDbKeyByBuffer(buff); err != nil {
			log.LogWarnf("Mp[%d] follower recv sync extent delete info, date:%d; recv err packet; unmarshal failed:%v",
				mp.config.PartitionId, extDeleteCursor, err.Error())
			return
		}
		if proto.IsTinyExtent(ek.ExtentId) {
			continue
		}

		eks = append(eks, *ek.ConvertToMetaDelEk(0, 0, 0))
	}

	updateKeyToDate(key, extDeleteCursor)
	//Update the key to next day. ignore day 32 item as it will be deleted by next month
	key[dayKeyIndex] += 1

	log.LogInfof("Mp[%d] follower recv sync extent delete info, date:%d, retry date:%d", mp.config.PartitionId, extDeleteCursor, getDateInKey(key))
	if err := mp.addDelExtentToDb(key, eks); err != nil {
		log.LogWarnf("Mp[%d] follower recv sync extent delete info, date:%d; commit retry eks failed:%s", mp.config.PartitionId, extDeleteCursor, err.Error())
	}

	if _, ok := mp.IsLeader(); !ok {
		select {
		case mp.extDelCursor <- extDeleteCursor:
		default:
		}
	}
	log.LogInfof("Mp[%d] follower recv sync extent delete info, date:%d, err count:%d finished", mp.config.PartitionId, extDeleteCursor, len(eks))
	return
}

func (mp *metaPartition) fsmSyncDelExtentsV2(data []byte) {
	eks := make([]proto.MetaDelExtentKey, 0)
	key := make([]byte, dbExtentKeySize)
	extDeleteCursor := binary.BigEndian.Uint64(data)
	buff := bytes.NewBuffer(data[8:])
	log.LogInfof("Mp[%d] follower recv sync extent delete info, date:%d, data:%v", mp.config.PartitionId, extDeleteCursor, data)
	for {
		if buff.Len() == 0 {
			break
		}

		if buff.Len() < proto.ExtentDbKeyLengthWithIno {
			log.LogErrorf("Mp[%d] follower recv sync extent delete info, date:%d; recv err packet; broken ek record, buff len:%d",
				mp.config.PartitionId, extDeleteCursor, buff.Len())
			return
		}

		ek := proto.MetaDelExtentKey{}
		if err := ek.UnmarshalDbKeyByBuffer(buff); err != nil {
			log.LogErrorf("Mp[%d] follower recv sync extent delete info, date:%d; recv err packet; unmarshal failed:%v",
				mp.config.PartitionId, extDeleteCursor, err.Error())
			return
		}
		eks = append(eks, ek)
	}

	updateKeyToDate(key, extDeleteCursor)

	if extDeleteCursor == 0 {
		updateKeyToNow(key)
	}
	//Update the key to next day. ignore day 32 item as it will be deleted by next month
	key[dayKeyIndex] += 1

	log.LogInfof("Mp[%d] follower recv sync extent delete info, date:%d, retry date:%d", mp.config.PartitionId, extDeleteCursor, getDateInKey(key))
	if err := mp.addDelExtentToDb(key, eks); err != nil {
		log.LogWarnf("Mp[%d] follower recv sync extent delete info, date:%d; commit retry eks failed:%s", mp.config.PartitionId, extDeleteCursor, err.Error())
	}

	if extDeleteCursor != 0 {
		//0 just sync failed ek, ignore clean local
		if _, ok := mp.IsLeader(); !ok {
			select {
			case mp.extDelCursor <- extDeleteCursor:
			default:
			}
		}
	}

	log.LogInfof("Mp[%d] follower recv sync extent delete info, date:%d, err count:%d finished", mp.config.PartitionId, extDeleteCursor, len(eks))
	return
}

func (mp *metaPartition) syncDelExtentsToFollowers(extDeletedCursor uint64, retryList *list.List) (err error) {
	log.LogInfof("Mp[%d] leader sync delete extent info to followers, date:%d, err count:%v", mp.config.PartitionId, extDeletedCursor, retryList.Len())

	buf := bytes.NewBuffer(make([]byte, 0, retryList.Len()*proto.ExtentDbKeyLengthWithIno+8))

	defer func() {
		if err != nil {
			log.LogWarnf("Mp[%d] leader sync delete extent info to followers, date:%d, err count:%v, failed:%s", mp.config.PartitionId, extDeletedCursor, retryList.Len(), err.Error())
			return
		}
		log.LogInfof("Mp[%d] leader sync delete extent info to followers, date:%d, err count:%v, success, data:%v", mp.config.PartitionId, extDeletedCursor, retryList.Len(), buf.Bytes())
	}()

	if err = binary.Write(buf, binary.BigEndian, extDeletedCursor); err != nil {
		return err
	}

	for elem := retryList.Front(); elem != nil; elem = elem.Next() {
		ek := elem.Value.(*proto.MetaDelExtentKey)
		log.LogInfof("Mp[%d] add del to followers ek:%v\n", mp.config.PartitionId, ek)
		if err = binary.Write(buf, binary.BigEndian, ek.FileOffset); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.PartitionId); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.ExtentId); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.ExtentOffset); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.Size); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.CRC); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.InodeId); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.TimeStamp); err != nil {
			return err
		}
		if err = binary.Write(buf, binary.BigEndian, ek.SrcType); err != nil {
			return err
		}
	}
	if _, err = mp.submit(context.Background(), opFSMExtentDelSyncV2, "", buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func (mp *metaPartition) cleanExpiredExtents(retryList *list.List) (delCursor uint64, err error) {
	stKey := make([]byte, 1)
	endKey := make([]byte, 1)
	cur := generalDateKey()
	cnt := 0
	var delHandle interface{}

	delCursor = cur
	delHandle, err = mp.db.CreateBatchHandler()
	if err != nil {
		log.LogErrorf("[cleanExpiredExtents] partition[%v] create batch handler failed:%v", mp.config.PartitionId, err)
		return
	}

	stKey[0] = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)

	log.LogInfof("Mp[%d] leader run once clean extent, now err extents:%v", mp.config.PartitionId, retryList.Len())

	defer func() {
		if retryList.Len() < maxRetryCnt {
			delCursor = cur
		}

		if err != nil {
			log.LogWarnf("Mp[%d] leader run once clean extent, now err extents:%v, failed:%s", mp.config.PartitionId, retryList.Len(), err.Error())
			return
		}
	}()


	handleItemFunc := func(k, v []byte) (bool, error) {

		needDel := true
		if retryList.Len() >= maxRetryCnt {
			//next term
			return false, nil
		}

		ek := &proto.MetaDelExtentKey{}
		ino := uint64(0)
		if len(v) > 1 {
			ek.UnMarshDelEkValue(v)
		}

		ekDelTimeStamp := getDateInKey(k)
		if k[0] != byte(ExtentDelTable) || ekDelTimeStamp > cur {
			return false, nil
		}
		delCursor = ekDelTimeStamp

		if err = ek.UnmarshalDbKey(k[8:]); err != nil {
			return false, err
		}

		if proto.IsTinyExtent(ek.ExtentId) && len(k) < dbExtentKeySize {
			log.LogErrorf("may be error ek, do not send del")
			needDel = false
		}

		if needDel {
			if err = mp.doDeleteMarkedInodes(context.Background(), &ek.ExtentKey); err != nil {
				retryList.PushBack(ek)
				log.LogWarnf("[cleanExpiredExtents] partitionId=%d, %s",
					mp.config.PartitionId, err.Error())
			}
		}

		mp.recordDeleteEkInfo(ino, ek)

		if cnt != 0 && cnt%maxItemsPerBatch == 0 {
			if err = mp.db.CommitBatchAndRelease(delHandle); err != nil {
				log.LogWarnf("[cleanExpiredExtents] partitionId=%d,"+
					" extentKey marshal: %s", mp.config.PartitionId, err.Error())
				return false, err
			}
			if delHandle, err = mp.db.CreateBatchHandler(); err != nil {
				log.LogErrorf("Mp[%d] leader cleanExpiredExtents create batch handle failed:%v", mp.config.PartitionId, err)
				return false, err
			}

		}

		keyOffset := (cnt % maxItemsPerBatch) * dbExtentKeySize
		copy(mp.delBatchKey[keyOffset:keyOffset+dbExtentKeySize], k)
		if err = mp.db.DelItemToBatch(delHandle, mp.delBatchKey[keyOffset:keyOffset+dbExtentKeySize]); err != nil {
			log.LogErrorf("Mp[%d] leader cleanExpiredExtents rocksdb handle DelItemToBatch failed:%v", mp.config.PartitionId, err)
			return false, err
		}
		cnt++
		return true, nil
	}

	if err = mp.db.Range(stKey, endKey, handleItemFunc); err != nil {
		log.LogErrorf("Mp[%d] leader cleanExpiredExtents handle item failed:%v", mp.config.PartitionId, err)
		if delHandle != nil {
			_ = mp.db.ReleaseBatchHandle(delHandle)
		}
		return
	}

	if err = mp.db.CommitBatchAndRelease(delHandle); err != nil {
		_ = mp.db.ReleaseBatchHandle(delHandle)
		log.LogErrorf("[cleanExpiredExtents] partitionId=%d, commit batch and release handle failed: %s", mp.config.PartitionId, err.Error())
		return
	}
	log.LogInfof("Mp[%d] leader clean expired extents[%d], now err extents:%v, success", mp.config.PartitionId, cnt, retryList.Len())
	return
}

func (mp *metaPartition) renameDeleteEKRecordFile(curFileName string, prefixName string) {
	delExtentListDir := path.Join(mp.config.RootDir, curFileName)
	_, err := os.Stat(path.Join(delExtentListDir))
	if err == nil {
		backupDir := path.Join(mp.config.RootDir, prefixName+time.Now().Format(proto.TimeFormat2))
		err = os.Rename(delExtentListDir, backupDir)
		if err != nil {
			log.LogErrorf("[renameDeleteEKRecordFile] rename %s to %s failed:%v", delExtentListDir, backupDir, err)
			return
		}
	}

	if err != nil && !os.IsNotExist(err) {
		log.LogErrorf("[renameDeleteEKRecordFile] stat delExtentListDir(%s) failed:%v", delExtentListDir, err)
		return
	}
	mp.removeOldDeleteEKRecordFile(curFileName, prefixName, false)
	return
}

func (mp *metaPartition) removeOldDeleteEKRecordFile(curFileName, prefixName string, forceRemove bool) {
	var metaDataDiskUsedRatio float64
	if metaDataDisk, ok := mp.manager.metaNode.disks[mp.manager.metaNode.metadataDir]; ok {
		metaDataDiskUsedRatio = metaDataDisk.Used/metaDataDisk.Total
	}
	if metaDataDiskUsedRatio < MaxMetaDataDiskUsedFactor && !forceRemove {
		log.LogDebugf("[removeOldDeleteEKRecordFile] meta data disk used ratio:%v", metaDataDiskUsedRatio)
		return
	}

	deleteEKRecordFilesMaxTotalSize := DeleteEKRecordFilesMaxTotalSize.Load()
	filesInfo, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		log.LogErrorf("[removeOldDeleteEKRecordFile] read root dir %s failed:%v", mp.config.RootDir, err)
		return
	}
	totalSize := int64(0)
	canDelFiles := make([]fs.FileInfo, 0, len(filesInfo))
	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			continue
		}
		if !strings.HasPrefix(fileInfo.Name(), prefixName) {
			continue
		}
		if fileInfo.Name() == curFileName {
			continue
		}
		canDelFiles = append(canDelFiles, fileInfo)
		totalSize += fileInfo.Size()
	}

	if uint64(totalSize) <= deleteEKRecordFilesMaxTotalSize {
		log.LogDebugf("[removeOldDeleteEKRecordFile] mp(%v) prefixName(%s) no need remove old file, total size:%v",
			mp.config.PartitionId, prefixName, totalSize)
		return
	}

	sort.Slice(canDelFiles, func(i, j int) bool {
		return canDelFiles[i].ModTime().Before(canDelFiles[j].ModTime())
	})

	delSize := int64(0)
	for _, canDelFile := range canDelFiles {
		if err = os.Remove(path.Join(mp.config.RootDir, canDelFile.Name())); err != nil {
			log.LogErrorf("failed delete log file %s", canDelFile.Name())
			continue
		}
		delSize += canDelFile.Size()
		if totalSize - delSize < int64(deleteEKRecordFilesMaxTotalSize) {
			break
		}
	}
	log.LogDebugf("[removeOldDeleteEKRecordFile] mp(%v) prefixName(%s) file total size after remove:%v",
		mp.config.PartitionId, prefixName, totalSize-delSize)
	return
}

func (mp *metaPartition) recordDeleteEkInfo(ino uint64, ek *proto.MetaDelExtentKey) {
	log.LogDebugf("[recordDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] record", mp.config.PartitionId, ino, ek)
	var (
		data []byte
		err  error
	)
	delExtentListDir := path.Join(mp.config.RootDir, delExtentKeyList)
	if mp.deleteEKRecordCount >= defMaxDelEKRecord {
		_ = mp.delEKFd.Close()
		mp.delEKFd = nil
		mp.renameDeleteEKRecordFile(delExtentKeyList, prefixDelExtentKeyListBackup)
		mp.deleteEKRecordCount = 0
	}
	data = make([]byte, proto.ExtentDbKeyLengthWithIno)
	ek.MarshalDeleteEKRecord(data)

	mp.deleteEKRecordCount++

	if mp.delEKFd == nil {
		if mp.delEKFd, err = os.OpenFile(delExtentListDir, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			log.LogErrorf("[recordDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] create delEKListFile(%s) failed:%v",
				mp.config.PartitionId, ino, ek, delExtentListDir, err)
			return
		}
	}

	defer func() {
		if err != nil {
			_ = mp.delEKFd.Close()
			mp.delEKFd = nil
		}
	}()

	if _, err = mp.delEKFd.Write(data); err != nil {
		log.LogErrorf("[recordDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] write file(%s) failed:%v",
			mp.config.PartitionId, ino, ek, delExtentListDir, err)
		return
	}
	log.LogDebugf("[recordDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] record success", mp.config.PartitionId, ino, ek)
	return
}

func (mp *metaPartition) followerCleanDeletedExtents(delCommitDate uint64) (err error) {
	stKey := make([]byte, 1)
	endKey := make([]byte, 1)
	var delHandle interface{}
	delHandle, err = mp.db.CreateBatchHandler()
	if err != nil {
		log.LogErrorf("[followerCleanDeletedExtents] partition[%v] create batch handler failed:%v", mp.config.PartitionId, err)
		return
	}

	stKey[0] = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)

	log.LogInfof("Mp[%d] follower sync clean extents before:%v", mp.config.PartitionId, delCommitDate)
	defer func() {
		if err != nil {
			log.LogWarnf("Mp[%d] follower sync clean extents before:%v, failed:%s", mp.config.PartitionId, delCommitDate, err.Error())
			return
		}
	}()

	cnt := 0
	handleItemFunc := func(k, v []byte) (bool, error) {
		if k[0] != byte(ExtentDelTable) {
			return false, nil
		}

		ekDate := getDateInKey(k)
		if ekDate >= delCommitDate {
			log.LogInfof("Clean expired extents finished, curKey:%v, curSor:%v", ekDate, delCommitDate)
			return false, nil
		}

		if cnt != 0 && cnt%maxItemsPerBatch == 0 {
			if err = mp.db.CommitBatchAndRelease(delHandle); err != nil {
				log.LogWarnf("[cleanExpiredExtents] partitionId=%d,"+
					" extentKey marshal: %s", mp.config.PartitionId, err.Error())
				return false, err
			}
			if delHandle, err = mp.db.CreateBatchHandler(); err != nil {
				log.LogErrorf("Mp[%d] followerCleanDeletedExtents create batch handle failed:%v", mp.config.PartitionId, err)
				return false, err
			}

		}

		keyOffset := (cnt % maxItemsPerBatch) * dbExtentKeySize
		copy(mp.delBatchKey[keyOffset:keyOffset+dbExtentKeySize], k)
		//log.LogInfof("MP[%v] clean del extent: %v, cnt:%v", mp.config.PartitionId, k, cnt)
		if err = mp.db.DelItemToBatch(delHandle, mp.delBatchKey[keyOffset:keyOffset+dbExtentKeySize]); err != nil {
			log.LogErrorf("Mp[%d] followerCleanDeletedExtents rocksdb handle DelItemToBatch failed:%v", mp.config.PartitionId, err)
			return false, err
		}

		cnt++
		return true, nil
	}

	if err = mp.db.Range(stKey, endKey, handleItemFunc); err != nil {
		log.LogErrorf("Mp[%d] follower clean deleted extents handleItem failed:%v", mp.config.PartitionId, err)
		if delHandle != nil {
			_ = mp.db.ReleaseBatchHandle(delHandle)
		}
		return err
	}

	if err = mp.db.CommitBatchAndRelease(delHandle); err != nil {
		_ = mp.db.ReleaseBatchHandle(delHandle)
		log.LogErrorf("[followerCleanDeletedExtents] partitionId=%d, commit batch and release handle failed: %s", mp.config.PartitionId, err.Error())
		return err
	}
	log.LogInfof("Mp[%d] follower sync clean extents(%d) before:%v, success", mp.config.PartitionId, cnt, delCommitDate)
	return nil
}

func (mp *metaPartition) deleteExtentsFromDb() {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("deleteExtentsFromDb: partition(%v) err(%v)", mp.config.PartitionId, err)
			log.LogFlush()
			panic(err)
		}
	}()
	retryList := list.New()
	delTimer := time.NewTimer(time.Minute * 1)
	for {
		select {
		case <-mp.stopC:
			delTimer.Stop()
			if mp.delEKFd != nil {
				mp.delEKFd.Sync()
				mp.delEKFd.Close()
			}
			return

		case extDeletedCursor := <-mp.extDelCursor:
			if _, ok := mp.IsLeader(); !ok {
				retryList = list.New()
				_ = mp.followerCleanDeletedExtents(extDeletedCursor)
				continue
			}

			log.LogInfof("Mp[%d] leader clean extents before:%v", mp.config.PartitionId, extDeletedCursor)
			delCursor, err := mp.cleanExpiredExtents(retryList)
			if err != nil {
				continue
			}
			if err := mp.syncDelExtentsToFollowers(delCursor, retryList); err != nil {
				continue
			}

			retryList = list.New()

		case <-delTimer.C:
			if _, ok := mp.IsLeader(); !ok {
				retryList = list.New()
				delTimer.Reset(followerDelTimerValue)
				continue
			}

			if _, err := mp.cleanExpiredExtents(retryList); err != nil {
				log.LogWarnf("Mp[%d] del extent failed:%s", mp.config.PartitionId, err.Error())
			}

			if retryList.Len() > (maxRetryCnt / 2) {
				if err := mp.syncDelExtentsToFollowers(0, retryList); err == nil {
					retryList = list.New()
				}
			}
			delTimer.Reset(leaderDelTimerValue)
		}
	}
}

func (mp *metaPartition) appendDelExtentsToFile(fileList *synclist.SyncList) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf(fmt.Sprintf("appendDelExtentsToFile(%v) appendDelExtentsToFile panic (%v)", mp.config.PartitionId, r))
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
	allFileInfos, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		panic(err)
	}

	finfos := make([]os.FileInfo, 0, len(allFileInfos))
	for _, info := range allFileInfos {
		if strings.HasPrefix(info.Name(), prefixDelExtent) && !info.IsDir() {
			if _, err := delExtNameID(info.Name()); err != nil {
				log.LogErrorf("partition:[%d] load file:[%s] format err so skip", mp.config.PartitionId, info.Name())
				continue
			}
			finfos = append(finfos, info)
		}
	}

	sort.Slice(finfos, func(i, j int) bool {
		in, _ := delExtNameID(finfos[i].Name())
		jn, _ := delExtNameID(finfos[j].Name())
		return jn > in
	})

	for _, info := range finfos {
		if strings.HasPrefix(info.Name(), prefixDelExtent) {
			log.LogInfof("path:[%s] append file:[%s] to fileList ", mp.config.RootDir, info.Name())
			fileList.PushBack(info.Name())
			fileSize = info.Size()
		}
	}

	lastItem := fileList.Back()
	if lastItem == nil {
		fileName = fmt.Sprintf("%s_%d", prefixDelExtent, idx)
		fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		// TODO Unhandled errors
		fp.Write(extentsFileHeader)
		fileList.PushBack(fileName)
		log.LogInfof("partition:[%d] startToDeleteExtents star idx:[%d]", mp.config.PartitionId, idx)
	} else {
		fileName = lastItem.Value.(string)
		if id, err := delExtNameID(fileName); err != nil {
			log.LogErrorf("del extent partition:[%d] Name:[%s] to id has err:[%s] list size:[%d]", mp.config.PartitionId, fileName, err.Error(), fileList.Len())
		} else {
			idx = int64(id)
		}
		log.LogInfof("partition:[%d] startToDeleteExtents star idx:[%d]", mp.config.PartitionId, idx)
		fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	}

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
			for _, ek := range eks {
				data, err = ek.MarshalBinary()
				if err != nil {
					log.LogWarnf("[appendDelExtentsToFile] partitionId=%d,"+
						" extentKey marshal: %s", mp.config.PartitionId, err.Error())
					break
				}
				buf = append(buf, data...)
			}
			if err != nil {
				mp.extDelCh <- eks
				continue
			}
			if info, err := fp.Stat(); err != nil {
				panic(err)
			} else {
				fileSize = info.Size()
			}

			if fileSize >= maxDeleteExtentSize || (int(fileSize)-len(extentsFileHeader))%proto.ExtentLength != 0 {
				// TODO Unhandled errors
				// close old File
				fp.Close()

				if (int(fileSize)-len(extentsFileHeader))%proto.ExtentLength != 0 {
					log.LogErrorf("delete extents partition:[%d] fileName:[%s] fileSize:[%d] size not a multiple of ExtentLength", mp.config.PartitionId, fileName, fileSize)
				}

				idx += 1
				fileName = fmt.Sprintf("%s_%d", prefixDelExtent, idx)
				fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
					os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
				if _, err = fp.Write(extentsFileHeader); err != nil {
					panic(err)
				}
				fileSize = 8
				fileList.PushBack(fileName)
			}
			// write file
			writeDeleteExtentsLock.Lock()
			if _, err = fp.Write(buf); err != nil {
				writeDeleteExtentsLock.Unlock()
				panic(err)
			}
			writeDeleteExtentsLock.Unlock()

			fileSize += int64(len(buf))
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
		time.Sleep(10 * time.Minute)
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
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deleteExtentsFromList] partitionId=%d, "+
				"not raft leader,please ignore", mp.config.PartitionId)
			continue
		}
		buf := make([]byte, MB)
		fp, err := os.OpenFile(file, os.O_RDWR, 0644)
		if err != nil {
			log.LogErrorf("partition:[%d] [deleteExtentsFromList] openFile %v error: %v", mp.config.PartitionId, file, err)
			fileList.Remove(element)
			goto LOOP
		}

		if _, err = fp.ReadAt(buf[:8], 0); err != nil {
			log.LogWarnf("[deleteExtentsFromList] partitionId=%d, "+
				"read cursor least 8bytes, retry later", mp.config.PartitionId)
			// TODO Unhandled errors
			fp.Close()
			continue
		}
		cursor := binary.BigEndian.Uint64(buf[:8])

		writeDeleteExtentsLock.Lock()
		fileSize := fileInfo.Size()
		writeDeleteExtentsLock.Unlock()

		if size := fileSize - int64(cursor); size < MB {
			if size <= 0 {
				size = int64(proto.ExtentLength)
			} else if size > 0 && size < int64(proto.ExtentLength) {
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
		n, err := fp.ReadAt(buf, int64(cursor))
		// TODO Unhandled errors
		fp.Close()
		if err != nil {
			if err == io.EOF {
				err = nil
				if fileList.Len() > 1 {
					status := mp.raftPartition.Status()
					if status.State == "StateLeader" && !status.
						RestoringSnapshot {
						if _, err = mp.submit(context.Background(), opFSMInternalDelExtentFile, "", []byte(fileName)); err != nil {
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
				} else {
					log.LogDebugf("[deleteExtentsFromList] partitionId=%d, %s"+
						" extents delete ok", mp.config.PartitionId, fileName)
				}
				continue
			}
			panic(err)
		}
		buff := bytes.NewBuffer(buf)
		cursor += uint64(n)
		for {
			if buff.Len() == 0 {
				break
			}
			if buff.Len() < proto.ExtentLength {
				cursor -= uint64(buff.Len())
				break
			}
			ek := proto.ExtentKey{}
			if err = ek.UnmarshalBinary(buff); err != nil {
				panic(err)
			}
			// delete dataPartition
			if err = mp.doDeleteMarkedInodes(context.Background(), &ek); err != nil {
				eks := make([]proto.MetaDelExtentKey, 0)
				eks = append(eks, *ek.ConvertToMetaDelEk(0, 0, 0))
				if !strings.Contains(err.Error(), "NotExistErr") {
					mp.extDelCh <- eks
				}
				log.LogWarnf("[deleteExtentsFromList] partitionId=%d, %s",
					mp.config.PartitionId, err.Error())
			}
		}
		buff.Reset()
		buff.WriteString(fmt.Sprintf("%s %d", fileName, cursor))
		if _, err = mp.submit(context.Background(), opFSMInternalDelExtentCursor, "", buff.Bytes()); err != nil {
			log.LogWarnf("[deleteExtentsFromList] partitionId=%d, %s",
				mp.config.PartitionId, err.Error())
		}
		log.LogDebugf("[deleteExtentsFromList] partitionId=%d, file=%s, cursor=%d",
			mp.config.PartitionId, fileName, cursor)
		goto LOOP
	}
}

func delExtNameID(name string) (id uint64, err error) {
	id, err = cast.ToUint64E(strings.ReplaceAll(name, prefixDelExtent+"_", ""))
	return
}
