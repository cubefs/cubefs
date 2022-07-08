// Copyright 2022 The CubeFS Authors.
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

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"reflect"
	"syscall"
	"unsafe"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

var metaFiles = [2]string{"meta01", "meta02"}

const metaSize = 52 //|version(8 bytes)|HardState(24 bytes)|wal Snapshot(16 bytes)|crc(4 bytes)|

type metaData struct {
	ver       uint64
	term      uint64
	vote      uint64
	commit    uint64
	snapTerm  uint64
	snapIndex uint64
}

type meta struct {
	curIdx  int
	ver     uint64
	files   [2]*os.File
	dataRef [2][]byte
	mdatas  [2]metaData
}

func (mt *metaData) decode(data []byte) error {
	if len(data) != metaSize {
		return fmt.Errorf("Invalid meta data len(%d)", len(data))
	}
	// check crc
	crc := binary.BigEndian.Uint32(data[metaSize-4:])
	if crc32.ChecksumIEEE(data[0:metaSize-4]) != crc {
		return fmt.Errorf("meta data has invalid crc(%d)", crc)
	}
	mt.ver = binary.BigEndian.Uint64(data[0:8])
	mt.term = binary.BigEndian.Uint64(data[8:16])
	mt.vote = binary.BigEndian.Uint64(data[16:24])
	mt.commit = binary.BigEndian.Uint64(data[24:32])
	mt.snapTerm = binary.BigEndian.Uint64(data[32:40])
	mt.snapIndex = binary.BigEndian.Uint64(data[40:48])
	return nil
}

func (mt *metaData) encodeTo(data []byte) {
	binary.BigEndian.PutUint64(data[0:8], mt.ver)
	binary.BigEndian.PutUint64(data[8:16], mt.term)
	binary.BigEndian.PutUint64(data[16:24], mt.vote)
	binary.BigEndian.PutUint64(data[24:32], mt.commit)
	binary.BigEndian.PutUint64(data[32:40], mt.snapTerm)
	binary.BigEndian.PutUint64(data[40:48], mt.snapIndex)
	crc := crc32.ChecksumIEEE(data[0:48])
	binary.BigEndian.PutUint32(data[48:], crc)
}

func NewMeta(dir string) (mt *meta, st Snapshot, hs pb.HardState, err error) {
	var (
		curIdx       int
		files        [2]*os.File
		dataRef      [2][]byte
		mdatas       [2]metaData
		decodeErrCnt int
		info         os.FileInfo
	)

	for i := 0; i < 2; i++ {
		files[i], err = os.OpenFile(path.Join(dir, metaFiles[i]), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			break
		}
		info, err = files[i].Stat()
		if err != nil {
			break
		}
		fSize := info.Size()
		if fSize != 0 && fSize != metaSize {
			err = fmt.Errorf("Invalid meta file(%s) size(%d)", path.Join(dir, metaFiles[i]), fSize)
			break
		}
		if err = files[i].Truncate(metaSize); err != nil {
			break
		}
		dataRef[i], err = syscall.Mmap(int(files[i].Fd()), 0, metaSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			break
		}
		if fSize == 0 {
			mdatas[i].encodeTo(dataRef[i])
			continue
		}
		var mdata metaData
		if err = mdata.decode(dataRef[i]); err != nil {
			decodeErrCnt++
			if decodeErrCnt < 2 {
				err = nil
			}
		} else {
			mdatas[i] = mdata
		}
	}
	if err != nil {
		for i := 0; i < 2; i++ {
			if files[i] != nil {
				files[i].Close()
			}
			if dataRef[i] != nil {
				syscall.Munmap(dataRef[i])
			}
		}
		return nil, st, hs, err
	}
	if mdatas[1].ver > mdatas[0].ver {
		curIdx = 1
	}
	hs.Term = mdatas[curIdx].term
	hs.Vote = mdatas[curIdx].vote
	hs.Commit = mdatas[curIdx].commit
	st.Index = mdatas[curIdx].snapIndex
	st.Term = mdatas[curIdx].snapTerm
	mt = &meta{curIdx, mdatas[curIdx].ver, files, dataRef, mdatas}
	return mt, st, hs, nil
}

func (mt *meta) changeIndex() {
	next := (mt.curIdx + 1) % 2
	mt.mdatas[next] = mt.mdatas[mt.curIdx]
	mt.curIdx = next
	mt.ver++
	mt.mdatas[mt.curIdx].ver = mt.ver
}

func (mt *meta) SaveSnapshot(st Snapshot) {
	mt.changeIndex()
	mt.mdatas[mt.curIdx].snapIndex = st.Index
	mt.mdatas[mt.curIdx].snapTerm = st.Term
	mt.mdatas[mt.curIdx].encodeTo(mt.dataRef[mt.curIdx])
}

func (mt *meta) SaveHardState(hs pb.HardState) {
	mt.changeIndex()
	mt.mdatas[mt.curIdx].term = hs.Term
	mt.mdatas[mt.curIdx].vote = hs.Vote
	mt.mdatas[mt.curIdx].commit = hs.Commit
	mt.mdatas[mt.curIdx].encodeTo(mt.dataRef[mt.curIdx])
}

func (mt *meta) SaveAll(st Snapshot, hs pb.HardState) {
	mt.changeIndex()
	mt.mdatas[mt.curIdx].snapIndex = st.Index
	mt.mdatas[mt.curIdx].snapTerm = st.Term
	mt.mdatas[mt.curIdx].term = hs.Term
	mt.mdatas[mt.curIdx].vote = hs.Vote
	mt.mdatas[mt.curIdx].commit = hs.Commit
	mt.mdatas[mt.curIdx].encodeTo(mt.dataRef[mt.curIdx])
}

func (mt *meta) Sync() error {
	ptr := (*reflect.SliceHeader)(unsafe.Pointer(&mt.dataRef[mt.curIdx]))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, uintptr(ptr.Data), uintptr(ptr.Len), uintptr(syscall.MS_SYNC))
	if err != 0 {
		return err
	}
	return nil
}

func (mt *meta) Close() error {
	if mt == nil {
		return nil
	}
	for i := 0; i < 2; i++ {
		if mt.dataRef[i] != nil {
			syscall.Munmap(mt.dataRef[i])
		}
		if mt.files[i] != nil {
			mt.files[i].Close()
		}
	}
	return nil
}
