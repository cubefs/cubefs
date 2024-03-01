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

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
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
	span := trace.SpanFromContextSafe(ctx)
	fileName = fmt.Sprintf("%s_%d", prefix, idx)
	fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		span.Errorf("OpenFile %v %v error %v", mp.config.RootDir, fileName, err)
		return
	}
	if _, err = fp.Write(extentsFileHeader); err != nil {
		span.Errorf("Write %v %v error %v", mp.config.RootDir, fileName, err)
		// TODO: return ???
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

	span, ctx := trace.StartSpanFromContext(context.Background(), "")
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
			span, ctx = trace.StartSpanFromContext(context.Background(), "")
			span = span.WithOperation(fmt.Sprintf("vol(%s)pid(%d)", mp.GetVolName(), mp.config.PartitionId))

			var data []byte
			buf = buf[:0]
			span.Debug("to delete eks:", eks)
			for _, ek := range eks {
				data, err = ek.MarshalBinaryWithCheckSum(true)
				if err != nil {
					span.Warn("extentKey marshal ", ek, err.Error())
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

		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		span = span.WithOperation(fmt.Sprintf("vol(%s)pid(%d)", mp.GetVolName(), mp.config.PartitionId))
		// if not leader, ignore delete
		if _, ok := mp.IsLeader(); !ok {
			span.Debug("is not raft leader, ignore")
			continue
		}
		// leader do delete extent for EXTENT_DEL_* file

		// read delete extents from file
		buf := make([]byte, MB)
		fp, err := os.OpenFile(file, os.O_RDWR, 0o644)
		if err != nil {
			span.Errorf("openFile %v error: %v", file, err)
			fileList.Remove(element)
			goto LOOP
		}

		// get delete extents cursor at file header 8 bytes
		if _, err = fp.ReadAt(buf[:8], 0); err != nil {
			span.Warn("read cursor least 8 bytes, retry later", err)
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
		span.Debugf("openFile %v file len %v cursor %v", file, stat.Size(), cursor)
		if size := uint64(fileInfo.Size()) - cursor; size < MB {
			if size <= 0 {
				size = extentKeyLen
			} else if size > 0 && size < extentKeyLen {
				span.Errorf("%s file corrupted", fileName)
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
				span.Debugf("%s extents delete ok n(%d), len(%d), cursor(%d)",
					fileName, rLen, len(buf), cursor)
			} else {
				status := mp.raftPartition.Status()
				if status.State == "StateLeader" && !status.RestoringSnapshot {
					// delete old delete extents file for metapartition
					if _, err = mp.submit(ctx, opFSMInternalDelExtentFile, []byte(fileName)); err != nil {
						span.Errorf("delete old file: %s, status: %s", fileName, err.Error())
					}
					span.Debugf("delete old file: %s ok", fileName)
					goto LOOP
				}
				span.Debugf("delete old file status: %s", status.State)
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
				err = mp.sendExtentsToChan(ctx, errExts)
				if err != nil {
					span.Errorf("sendExtentsToChan by raft error, ek(%d) err(%v)", len(errExts), err)
				}

				errExts = make([]proto.ExtentKey, 0)
			}

			ek := proto.ExtentKey{}
			if extentV2 {
				if err = ek.UnmarshalBinaryWithCheckSum(buff); err != nil {
					if err == proto.InvalidKeyHeader || err == proto.InvalidKeyCheckSum {
						span.Errorf("invalid extent key header %v, %v", fileName, err)
						continue
					}
					span.Errorf("Unmarshal extentkey from %v unresolved error: %v", fileName, err)
					panic(err)
				}
			} else {
				// ek for del no need to get version
				if err = ek.UnmarshalBinary(buff, false); err != nil {
					panic(err)
				}
			}
			// delete extent from dataPartition
			dp := mp.vol.GetPartition(ek.PartitionId)
			if !dp.IsDiscard {
				if err = mp.doDeleteMarkedInodes(&ek); err != nil {
					errExts = append(errExts, ek)
					span.Warnf("extent: %v, %s", ek.String(), err.Error())
				}
			} else {
				span.Warnf("dp(%v) is discard, skip extent(%v)", ek.PartitionId, ek.ExtentId)
			}
			deleteCnt++
		}

		if err = mp.sendExtentsToChan(ctx, errExts); err != nil {
			span.Errorf("sendExtentsToChan by raft error, ek(%d) err(%v)", len(errExts), err)
		}

		buff.Reset()
		buff.WriteString(fmt.Sprintf("%s %d", fileName, cursor))
		if _, err = mp.submit(ctx, opFSMInternalDelExtentCursor, buff.Bytes()); err != nil {
			span.Warn(err)
		}

		span.Debugf("file=%s, cursor=%d, size=%d", fileName, cursor, len(buf))
		goto LOOP
	}
}
