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
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/synclist"
)

const (
	prefixDelExtent     = "EXTENT_DEL"
	prefixDelExtentV2   = "EXTENT_DEL_V2"
	maxDeleteExtentSize = 10 * MB
)

var extentsFileHeader = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}

/// start metapartition delete extents work
///
func (mp *metaPartition) startToDeleteExtents() {
	fileList := synclist.New()
	go mp.appendDelExtentsToFile(fileList)
	go mp.deleteExtentsFromList(fileList)
}

// create extent delete file
func (mp *metaPartition) createExtentDeleteFile(prefix string, idx int64, fileList *synclist.SyncList) (fp *os.File, fileName string, fileSize int64, err error) {
	fileName = fmt.Sprintf("%s_%d", prefix, idx)
	fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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

//append delete extents from extDelCh to EXTENT_DEL_N files
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
	if lastItem == nil {
		//if no exist EXTENT_DEL_*, create one
		fp, fileName, fileSize, err = mp.createExtentDeleteFile(prefixDelExtentV2, idx, fileList)
		if err != nil {
			panic(err)
		}
	} else {
		//exist, open last file
		fileName = lastItem.Value.(string)
		fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		// continue from last item
		idx = getDelExtFileIdx(fileName)
	}

	extentV2 := false
	if strings.HasPrefix(fileName, prefixDelExtentV2) {
		extentV2 = true
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
				if extentV2 {
					data, err = ek.MarshalBinaryWithCheckSum()
				} else {
					data, err = ek.MarshalBinary()
				}
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
					log.LogErrorf("[appendDelExtentsToFile] mp(%d) sendExtentsToChan fail, err(%s)", mp.config.PartitionId, err.Error())
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
			}
			// write delete extents into file
			if _, err = fp.Write(buf); err != nil {
				panic(err)
			}
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
		//if not leader, ignore delete
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deleteExtentsFromList] partitionId=%d, "+
				"not raft leader,please ignore", mp.config.PartitionId)
			continue
		}
		//leader do delete extent for EXTENT_DEL_* file

		// read delete extents from file
		buf := make([]byte, MB)
		fp, err := os.OpenFile(file, os.O_RDWR, 0644)
		if err != nil {
			log.LogErrorf("[deleteExtentsFromList] openFile %v error: %v", file, err)
			fileList.Remove(element)
			goto LOOP
		}

		//get delete extents cursor at file header 8 bytes
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
		//read extents from cursor
		n, err := fp.ReadAt(buf, int64(cursor))
		// TODO Unhandled errors
		fp.Close()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err == io.EOF {
			err = nil
			if fileList.Len() <= 1 {
				log.LogDebugf("[deleteExtentsFromList] partitionId=%d, %s"+
					" extents delete ok n(%d), len(%d), cursor(%d)", mp.config.PartitionId, fileName, n, len(buf), cursor)
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
		cursor += uint64(n)
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
			batchCount := DeleteBatchCount() * 5
			if deleteCnt%batchCount == 0 {
				DeleteWorkerSleepMs()
			}

			if len(errExts) >= int(batchCount) {
				time.Sleep(100 * time.Millisecond)
				err = mp.sendExtentsToChan(errExts)
				if err != nil {
					log.LogErrorf("deleteExtentsFromList sendExtentsToChan by raft error, mp(%d), err(%s), ek(%s)", mp.config.PartitionId, err.Error(), len(errExts))
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
				if err = ek.UnmarshalBinary(buff); err != nil {
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
			log.LogErrorf("deleteExtentsFromList sendExtentsToChan by raft error, mp(%d), err(%s), ek(%s)", mp.config.PartitionId, err.Error(), len(errExts))
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
// 			log.LogWritef("mp(%v) deleteExtents(%v)", mp.config.PartitionId, newEx.String())
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
