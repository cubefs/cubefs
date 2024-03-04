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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/synclist"
)

const (
	prefixDelExtent     = "EXTENT_DEL"
	prefixDelExtentV2   = "EXTENT_DEL_V2"
	prefixMultiVer      = verdataFile
	maxDeleteExtentSize = 10 * MB
)

var extentsFileHeader = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}

// start metapartition delete extents work
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
			if len(eks) == 0 {
				goto LOOP
			}
			log.LogDebugf("[appendDelExtentsToFile] mp(%v) del eks [%v]", mp.config.PartitionId, eks)
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

func (mp *metaPartition) batchDeleteExtentsByDp(dpId uint64, extents []*proto.ExtentKey) (err error) {
	dp := mp.vol.GetPartition(dpId)
	if dp == nil {
		log.LogErrorf("[batchDeleteExtentsByDp] mp(%v) dp(%v) not found", mp.config.PartitionId, dpId)
		err = fmt.Errorf("dp %v is not found", dpId)
		return
	}
	if dp.IsDiscard {
		log.LogDebugf("[batchDeleteExtentsByDp] mp(%v) dp(%v) is discard", mp.config.PartitionId, dpId)
		return
	}
	log.LogDebugf("[batchDeleteExtentsByDp] mp(%v) delete eks from dp(%v)", mp.config.PartitionId, dpId)
	err = mp.doBatchDeleteExtentsByPartition(dpId, extents)
	return
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
		// DeleteWorkerSleepMs()
		time.Sleep(1 * time.Minute)
		select {
		case <-mp.stopC:
			return
		default:
		}
		element = fileList.Front()
		if element == nil {
			continue
		}
		fileName = element.Value.(string)
		file = path.Join(mp.config.RootDir, fileName)
		if fileInfo, err = os.Stat(file); err != nil {
			log.LogDebugf("[deleteExtentsFromList] mp(%v) skip file(%v)", mp.config.PartitionId, fileName)
			fileList.Remove(element)
			continue
		}
		log.LogDebugf("[deleteExtentsFromList] mp(%v) reading file(%v)", mp.config.PartitionId, fileName)
		// if not leader, ignore delete
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deleteExtentsFromList] partitionId=%d, "+
				"not raft leader,please ignore", mp.config.PartitionId)
			continue
		}
		// leader do delete extent for EXTENT_DEL_* file

		// read delete extents from file
		buf := make([]byte, 8)
		fp, err := os.OpenFile(file, os.O_RDWR, 0o644)
		if err != nil {
			if !os.IsNotExist(err) {
				log.LogErrorf("[deleteExtentsFromList] volname [%v] mp[%v] openFile %v error: %v", mp.GetVolName(), mp.config.PartitionId, file, err)
			} else {
				log.LogDebugf("[deleteExtentsFromList] mp(%v) delete extents file(%v) deleted", mp.config.PartitionId, fileName)
			}
			fileList.Remove(element)
			continue
		}

		// get delete extents cursor at file header 8 bytes
		if _, err = fp.ReadAt(buf, 0); err != nil {
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
		cursor := binary.BigEndian.Uint64(buf)
		stat, err := fp.Stat()
		if err != nil {
			log.LogErrorf("[deleteExtentsFromList] mp(%v) stat file(%v) err(%v)", mp.config.PartitionId, fileName, err)
			continue
		}
		log.LogDebugf("[deleteExtentsFromList] volname [%v] mp[%v] o openFile %v file len %v cursor %v", mp.GetVolName(), mp.config.PartitionId, file,
			stat.Size(), cursor)

		log.LogDebugf("action[deleteExtentsFromList] get cursor %v", cursor)
		if fileInfo.Size() == int64(cursor) {
			log.LogDebugf("[deleteExtentsFromList] mp(%v) reach the end of file(%v), sleep", mp.config.PartitionId, fileName)
			fp.Close()
			continue
		} else if fileInfo.Size() > int64(cursor) && fileInfo.Size() < int64(cursor)+int64(extentKeyLen) {
			log.LogErrorf("[deleteExtentsFromList] mp(%d), file(%v) corrupted!", mp.config.PartitionId, fileName)
			fileList.Remove(element)
			fp.Close()
			continue
		}

		var deleteCnt uint64
		errExts := make([]proto.ExtentKey, 0)
		needDeleteExtents := make(map[uint64][]*proto.ExtentKey)
		buf = make([]byte, util.MB)
		err = func() (err error) {
			// read extents from cursor
			defer fp.Close()
			// NOTE: read 1 MB at once
			rLen, err := fp.ReadAt(buf, int64(cursor))
			log.LogDebugf("[deleteExtentsFromList] mp(%v) read len(%v) cursor(%v), err(%v)", mp.config.PartitionId, rLen, cursor, err)
			if err != nil {
				if err == io.EOF {
					err = nil
					if rLen == 0 {
						log.LogDebugf("[deleteExtentsFromList] mp(%v) file list cnt(%v)", mp.config.PartitionId, fileList.Len())
						if fileList.Len() <= 1 {
							log.LogDebugf("[deleteExtentsFromList] mp(%v) skip delete file(%v), free list count(%v)", mp.config.PartitionId, fileName, fileList.Len())
							return
						}
						status := mp.raftPartition.Status()
						_, isLeader := mp.IsLeader()
						if isLeader && !status.RestoringSnapshot {
							// delete old delete extents file for metapartition
							if _, err = mp.submit(opFSMInternalDelExtentFile, []byte(fileName)); err != nil {
								log.LogErrorf("[deleteExtentsFromList] mp(%v), delete old file(%v), err(%v)", mp.config.PartitionId, fileName, err)
								return
							}
							log.LogDebugf("[deleteExtentsFromList] mp(%v), delete old file(%v)", mp.config.PartitionId, fileName)
							return
						}
						log.LogDebugf("[deleteExtentsFromList] partitionId=%d,delete"+
							" old file status: %s", mp.config.PartitionId, status.State)
					}
				} else {
					log.LogErrorf("[deleteExtentsFromList] mp(%v) failed to read file(%v), err(%v)", mp.config.PartitionId, fileName, err)
					return
				}
			}
			cursor += uint64(rLen)
			buff := bytes.NewBuffer(buf[:rLen])
			batchCount := DeleteBatchCount() * 5
			for buff.Len() != 0 && deleteCnt < batchCount {
				lastUnread := buff.Len()
				// NOTE: audjust cursor
				if uint64(buff.Len()) < extentKeyLen {
					cursor -= uint64(lastUnread)
					break
				}
				if extentV2 && uint64(buff.Len()) < uint64(proto.ExtentV3Length) {
					if r := bytes.Compare(buff.Bytes()[:4], proto.ExtentKeyHeaderV3); r == 0 {
						cursor -= uint64(lastUnread)
						break
					}
				}
				// NOTE: read ek
				ek := proto.ExtentKey{}
				if extentV2 {
					if err = ek.UnmarshalBinaryWithCheckSum(buff); err != nil {
						if err == proto.InvalidKeyHeader || err == proto.InvalidKeyCheckSum {
							log.LogErrorf("[deleteExtentsFromList] invalid extent key header %v, %v, %v", fileName, mp.config.PartitionId, err)
							return
						}
						log.LogErrorf("[deleteExtentsFromList] mp: %v Unmarshal extentkey from %v unresolved error: %v", mp.config.PartitionId, fileName, err)
						return
					}
				} else {
					// ek for del no need to get version
					if err = ek.UnmarshalBinary(buff, false); err != nil {
						log.LogErrorf("[deleteExtentsFromList] mp(%v) failed to unmarshal extent", mp.config.PartitionId)
						return
					}
				}

				// NOTE: add to current batch
				dpId := ek.PartitionId
				eks := needDeleteExtents[dpId]
				if eks == nil {
					eks = make([]*proto.ExtentKey, 0)
				}
				eks = append(eks, &ek)
				needDeleteExtents[dpId] = eks

				// NOTE: limit batch count
				deleteCnt++
				log.LogDebugf("[deleteExtentsFromList] mp(%v) append extent(%v) to batch, count limit(%v), cnt(%v)", mp.config.PartitionId, ek, batchCount, deleteCnt)
			}
			log.LogDebugf("[deleteExtentsFromList] mp(%v) reach the end of buffer", mp.config.PartitionId)
			return
		}()

		if err != nil {
			log.LogErrorf("[deleteExtentsFromList] mp(%v) failed to read delete file(%v), err(%v)", mp.config.PartitionId, fileName, err)
			continue
		}

		if deleteCnt == 0 {
			log.LogDebugf("[deleteExtentsFromList] mp(%v) delete cnt is 0, sleep", mp.config.PartitionId)
			continue
		}

		successCnt := 0

		for dpId, eks := range needDeleteExtents {
			log.LogDebugf("[deleteExtentsFromList] mp(%v) delete dp(%v) eks count(%v)", mp.config.PartitionId, dpId, len(eks))
			err = mp.batchDeleteExtentsByDp(dpId, eks)
			if err != nil {
				log.LogErrorf("[deleteExtentsFromList] mp(%v) failed to delete dp(%v) extents", mp.config.PartitionId, dpId)
				err = nil
				for _, ek := range eks {
					errExts = append(errExts, *ek)
				}
			} else {
				successCnt += len(eks)
			}
		}

		log.LogDebugf("[deleteExtentsFromList] mp(%v) delete success cnt(%v), err cnt(%v)", mp.config.PartitionId, successCnt, len(errExts))

		if successCnt == 0 {
			log.LogErrorf("[deleteExtentsFromList] no extents delete successfully, sleep")
			continue
		}

		if len(errExts) != 0 {
			log.LogDebugf("[deleteExtentsFromList] mp(%v) sync errExts(%v)", mp.config.PartitionId, errExts)
			err = mp.sendExtentsToChan(errExts)
			if err != nil {
				log.LogErrorf("[deleteExtentsFromList] sendExtentsToChan by raft error, mp[%v], err(%v), ek(%v)", mp.config.PartitionId, err, len(errExts))
			}
		}

		buff := bytes.NewBuffer([]byte{})
		buff.WriteString(fmt.Sprintf("%s %d", fileName, cursor))
		log.LogDebugf("[deleteExtentsFromList] mp(%v) delete eks(%v) from file(%v)", mp.config.PartitionId, deleteCnt, fileName)
		if _, err = mp.submit(opFSMInternalDelExtentCursor, buff.Bytes()); err != nil {
			log.LogWarnf("[deleteExtentsFromList] partitionId=%d, %s",
				mp.config.PartitionId, err.Error())
		}

		log.LogDebugf("[deleteExtentsFromList] mp(%v) file(%v), cursor(%v), size(%v)", mp.config.PartitionId, fileName, cursor, len(buf))
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
