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
	"github.com/spf13/cast"
	"io"
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
)

var extentsFileHeader = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}
var writeDeleteExtentsLock = &sync.Mutex{}

func (mp *metaPartition) startToDeleteExtents() {
	fileList := synclist.New()
	go mp.appendDelExtentsToFile(fileList)
	go mp.deleteExtentsFromList(fileList)
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
				eks := make([]proto.ExtentKey, 0)
				eks = append(eks, ek)
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
