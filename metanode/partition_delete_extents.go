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
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
func (mp *metaPartition) createExtentDeleteFile(ctx context.Context, prefix string, idx int64, fileList *synclist.SyncList) (
	fp *os.File, fileName string, fileSize int64, err error,
) {
	span := getSpan(ctx)
	fileName = fmt.Sprintf("%s_%d", prefix, idx)
	fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		span.Errorf("OpenFile %v %v error %v", mp.config.RootDir, fileName, err)
		return
	}
	if _, err = fp.Write(extentsFileHeader); err != nil {
		span.Errorf("Write %v %v error %v", mp.config.RootDir, fileName, err)
		// TODO ???: return
	}
	fileSize = int64(len(extentsFileHeader))
	fileList.PushBack(fileName)
	return
}

// append delete extents from extDelCh to EXTENT_DEL_N files
func (mp *metaPartition) appendDelExtentsToFile(fileList *synclist.SyncList) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("partition(%d) append panic %v", mp.config.PartitionId, r)
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
	entries, err := os.ReadDir(mp.config.RootDir)
	if err != nil {
		panic(err)
	}

	finfos := sortDelExtFileInfo(entries)
	for _, info := range finfos {
		fileList.PushBack(info.Name())
		fileSize = info.Size()
	}

	span, ctx := spanContext()
	// check
	lastItem := fileList.Back()
	if lastItem != nil {
		fileName = lastItem.Value.(string)
	}
	if lastItem == nil || !strings.HasPrefix(fileName, prefixDelExtentV2) {
		// if no exist EXTENT_DEL_*, create one
		span.Debugf("verseq [%v]", mp.verSeq)
		fp, fileName, fileSize, err = mp.createExtentDeleteFile(ctx, prefixDelExtentV2, idx, fileList)
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
	span.Debugf("verseq [%v] filename %v", mp.verSeq, fileName)

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
			span, ctx = spanContext()
			span = span.WithOperation(fmt.Sprintf("appendDelExtentsToFile-vol(%s)mp(%d)",
				mp.GetVolName(), mp.config.PartitionId))

			var data []byte
			buf = buf[:0]
			if len(eks) == 0 {
				goto LOOP
			}
			span.Debugf("to delete eks [%v]", eks)
			for _, ek := range eks {
				data, err = ek.MarshalBinaryWithCheckSum(true)
				if err != nil {
					span.Warnf("extentKey [%v] marshal %s", ek, err.Error())
					break
				}
				buf = append(buf, data...)
			}

			if err != nil {
				err = mp.sendExtentsToChan(ctx, eks)
				if err != nil {
					span.Errorf("sendExtentsToChan err(%s)", err.Error())
				}
				continue
			}
			if fileSize >= maxDeleteExtentSize {
				// TODO Unhandled errors
				// close old File
				fp.Close()
				idx += 1
				fp, fileName, fileSize, err = mp.createExtentDeleteFile(ctx, prefixDelExtentV2, idx, fileList)
				if err != nil {
					panic(err)
				}
				span.Debug("createExtentDeleteFile", fileName)
			}
			// write delete extents into file
			if _, err = fp.Write(buf); err != nil {
				panic(err)
			}
			fileSize += int64(len(buf))
			span.Debugf("filesize now %d", fileSize)
		}
	}
}

func (mp *metaPartition) batchDeleteExtentsByDp(ctx context.Context, dpId uint64, extents []*proto.ExtentKey) (err error) {
	span := getSpan(ctx).WithOperation("batchDeleteExtentsByDp")
	dp := mp.vol.GetPartition(dpId)
	if dp == nil {
		span.Errorf("mp(%v) dp(%v) not found", mp.config.PartitionId, dpId)
		err = fmt.Errorf("dp %v is not found", dpId)
		return
	}
	if dp.IsDiscard {
		span.Debugf("mp(%v) dp(%v) is discard", mp.config.PartitionId, dpId)
		return
	}
	span.Debugf("mp(%v) delete eks from dp(%v)", mp.config.PartitionId, dpId)
	err = mp.doBatchDeleteExtentsByPartition(ctx, dpId, extents)
	return
}

// Delete all the extents of a file.
func (mp *metaPartition) deleteExtentsFromList(fileList *synclist.SyncList) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("partition(%d) delete panic %v", mp.config.PartitionId, r)
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

		span, ctx := spanContext()
		span = span.WithOperation(fmt.Sprintf("deleteExtentsFromList-vol(%s)mp(%d)",
			mp.GetVolName(), mp.config.PartitionId))

		fileName = element.Value.(string)
		file = path.Join(mp.config.RootDir, fileName)
		if fileInfo, err = os.Stat(file); err != nil {
			span.Debugf("mp(%v) skip file(%v)", mp.config.PartitionId, fileName)
			fileList.Remove(element)
			continue
		}
		span.Debugf("mp(%v) reading file(%v)", mp.config.PartitionId, fileName)

		// if not leader, ignore delete
		if _, ok := mp.IsLeader(); !ok {
			span.Debug("is not raft leader, ignore")
			continue
		}
		// leader do delete extent for EXTENT_DEL_* file

		// read delete extents from file
		buf := make([]byte, 8)
		fp, err := os.OpenFile(file, os.O_RDWR, 0o644)
		if err != nil {
			if !os.IsNotExist(err) {
				span.Errorf("openFile %v error: %v", file, err)
			} else {
				span.Debugf("delete extents file(%v) deleted", fileName)
			}
			fileList.Remove(element)
			continue
		}

		// get delete extents cursor at file header 8 bytes
		if _, err = fp.ReadAt(buf, 0); err != nil {
			span.Warn("read cursor least 8bytes, retry later")
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
			span.Errorf("stat file(%v) err(%v)", fileName, err)
			continue
		}
		span.Debugf("openFile %v file len %v cursor %v", file, stat.Size(), cursor)

		if fileInfo.Size() == int64(cursor) {
			span.Debugf("reach the end of file(%v), sleep", fileName)
			fp.Close()
			continue
		} else if fileInfo.Size() > int64(cursor) && fileInfo.Size() < int64(cursor)+int64(extentKeyLen) {
			span.Errorf("file(%v) corrupted!", fileName)
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
			span.Debugf("read len(%v) cursor(%v), err(%v)", rLen, cursor, err)
			if err != nil {
				if err == io.EOF {
					err = nil
					if rLen == 0 {
						span.Debugf("file list cnt(%v)", fileList.Len())
						if fileList.Len() <= 1 {
							span.Debugf("skip delete file(%v), free list count(%v)", fileName, fileList.Len())
							return
						}
						status := mp.raftPartition.Status()
						_, isLeader := mp.IsLeader()
						if isLeader && !status.RestoringSnapshot {
							// delete old delete extents file for metapartition
							if _, err = mp.submit(ctx, opFSMInternalDelExtentFile, []byte(fileName)); err != nil {
								span.Errorf("delete old file(%v), err(%v)", fileName, err)
								return
							}
							span.Debugf("delete old file(%v)", fileName)
							return
						}
						span.Debugf("delete old file status: %s", status.State)
					}
				} else {
					span.Errorf("failed to read file(%v), err(%v)", fileName, err)
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
							span.Errorf("invalid extent key header %v, %v, %v", fileName, mp.config.PartitionId, err)
							return
						}
						span.Errorf("Unmarshal extentkey from %v unresolved error: %v", fileName, err)
						return
					}
				} else {
					// ek for del no need to get version
					if err = ek.UnmarshalBinary(buff, false); err != nil {
						span.Error("failed to unmarshal extent")
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
				span.Debugf("append extent(%v) to batch, count limit(%v), cnt(%v)", ek, batchCount, deleteCnt)
			}
			span.Debug("reach the end of buffer")
			return
		}()

		if err != nil {
			span.Errorf("failed to read delete file(%v), err(%v)", fileName, err)
			continue
		}

		if deleteCnt == 0 {
			span.Debug("delete cnt is 0, sleep")
			continue
		}

		successCnt := 0

		for dpId, eks := range needDeleteExtents {
			span.Debugf("delete dp(%v) eks count(%v)", dpId, len(eks))
			err = mp.batchDeleteExtentsByDp(ctx, dpId, eks)
			if err != nil {
				span.Errorf("failed to delete dp(%v) extents", dpId)
				err = nil
				for _, ek := range eks {
					errExts = append(errExts, *ek)
				}
			} else {
				successCnt += len(eks)
			}
		}

		span.Debugf("delete success cnt(%v), err cnt(%v)", successCnt, len(errExts))

		if successCnt == 0 {
			span.Error("no extents delete successfully, sleep")
			continue
		}

		if len(errExts) != 0 {
			span.Debugf("sync errExts(%v)", errExts)
			err = mp.sendExtentsToChan(ctx, errExts)
			if err != nil {
				span.Errorf("sendExtentsToChan by raft error, mp[%v], err(%v), ek(%v)", mp.config.PartitionId, err, len(errExts))
			}
		}

		buff := bytes.NewBuffer([]byte{})
		buff.WriteString(fmt.Sprintf("%s %d", fileName, cursor))
		span.Debugf("delete eks(%v) from file(%v)", deleteCnt, fileName)
		if _, err = mp.submit(ctx, opFSMInternalDelExtentCursor, buff.Bytes()); err != nil {
			span.Warn(err)
		}

		span.Debugf("file(%v), cursor(%v), size(%v)", fileName, cursor, len(buf))
	}
}
