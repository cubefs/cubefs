// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strings"

	"github.com/tiglabs/raft/proto"
)

func (file *LogFile) loadLogFileMetaHead(buf []byte) error {
	if len(buf) < Log_MetaHeadSize {
		return Err_BufferSizeNotEnough
	}
	if binary.LittleEndian.Uint32(buf) != Log_Magic {
		return Err_BadMagic
	}
	file.shardId = binary.LittleEndian.Uint64(buf[8:])
	file.cap = binary.LittleEndian.Uint32(buf[16:])
	file.entryNum = int(binary.LittleEndian.Uint32(buf[20:]))
	if file.entryNum < 0 || file.entryNum > Log_MaxEntryNum {
		return Err_BadMeta
	}
	return nil
}
func (file *LogFile) loadLogFileMeta(buf []byte) error {
	if len(buf) < Log_FileMetaSize {
		return Err_BufferSizeNotEnough
	}
	e := file.loadLogFileMetaHead(buf)
	if e != nil {
		return e
	}
	crc := binary.LittleEndian.Uint32(buf[4:])
	n := Log_MetaHeadSize + file.entryNum*Log_FileMetaEntrySize
	if cc := crc32.ChecksumIEEE(buf[8:n]); cc != crc {
		return Err_CrcNotMatch
	}
	for i := 0; i < file.entryNum; i++ {
		off := Log_MetaHeadSize + i*Log_FileMetaEntrySize
		file.entryMeta[i].Index = binary.LittleEndian.Uint64(buf[off:])
		file.entryMeta[i].Term = binary.LittleEndian.Uint64(buf[off+8:])
		file.entryMeta[i].Offset = int64(binary.LittleEndian.Uint64(buf[off+16:]))
	}
	return nil
}
func (file *LogFile) flushMeta() (int, error) {
	buf := metaBufferPool.GetBuffer()
	defer metaBufferPool.PutBuffer(buf)

	binary.LittleEndian.PutUint32(buf[0:], Log_Magic)
	binary.LittleEndian.PutUint64(buf[8:], file.shardId)
	binary.LittleEndian.PutUint32(buf[16:], file.cap)
	binary.LittleEndian.PutUint32(buf[20:], uint32(file.entryNum))
	for i := 0; i < file.entryNum; i++ {
		off := Log_MetaHeadSize + i*Log_FileMetaEntrySize
		binary.LittleEndian.PutUint64(buf[off:], file.entryMeta[i].Index)
		binary.LittleEndian.PutUint64(buf[off+8:], file.entryMeta[i].Term)
		binary.LittleEndian.PutUint64(buf[off+16:], uint64(file.entryMeta[i].Offset))
	}
	n := Log_MetaHeadSize + file.entryNum*Log_FileMetaEntrySize
	binary.LittleEndian.PutUint32(buf[4:], crc32.ChecksumIEEE(buf[8:n]))

	wn, e := file.f.WriteAt(buf[:n], 0)
	if e != nil {
		return wn, e
	}
	return n, nil
}
func (file *LogFile) getEntriesPos(lo, hi uint64) ([]EntryMeta, error) {
	if file.entryNum == 0 {
		return nil, Err_LogFileEmpty
	}
	if lo < file.entryMeta[0].Index || hi > file.entryMeta[file.entryNum-1].Index {
		return nil, Err_IndexOutOfFileRange
	}
	return file.entryMeta[lo-file.entryMeta[0].Index : hi-file.entryMeta[0].Index+1], nil
}
func (file *LogFile) getFirstIndex() uint64 {
	if file.entryNum > 0 {
		return file.entryMeta[0].Index
	}
	return 0
}
func (file *LogFile) getLastIndex() uint64 {
	if file.entryNum > 0 {
		return file.entryMeta[file.entryNum-1].Index
	}
	return 0
}
func (file *LogFile) getEntryMetaObj(pos int) (*EntryMeta, error) {
	if pos >= 0 && pos < file.entryNum {
		return file.entryMeta[pos].Clone(), nil
	} else {
		return nil, Err_IndexOutOfFileRange
	}
}
func (file *LogFile) getEntryMeta(pos int) (index, term uint64, offset int64, e error) {
	if pos >= 0 && pos < file.entryNum {
		index, term, offset = file.entryMeta[pos].Index, file.entryMeta[pos].Term, file.entryMeta[pos].Offset
	} else {
		e = Err_IndexOutOfFileRange
	}
	return
}
func (file *LogFile) setEntryMeta(index, term uint64, offset int64, pos int) error {
	if pos < 0 && pos >= Log_MaxEntryNum {
		return Err_IndexOutOfFileRange
	}
	file.entryMeta[pos].Index, file.entryMeta[pos].Term, file.entryMeta[pos].Offset = index, term, offset
	return nil
}

type HardStateFile struct {
	shardId uint64
	seqId   uint64

	hs        proto.HardState
	hsValue   bool
	snap      proto.SnapshotMeta
	snapValue bool

	dir string
}

func LoadHardStateFile(dir string, shardId uint64) (*HardStateFile, error) {
	fis, e := ioutil.ReadDir(dir)
	if e != nil {
		return nil, e
	}
	var (
		info []string
	)
	hf := &HardStateFile{shardId: shardId, dir: dir}
	hs := new(proto.HardState)
	snap := new(proto.SnapshotMeta)
	prefix := fmt.Sprintf("hs_%d.", shardId)
	for _, fi := range fis {
		if fi.IsDir() || !strings.HasPrefix(fi.Name(), prefix) {
			continue
		}
		b, e := ioutil.ReadFile(dir + "/" + fi.Name())
		if e != nil {
			info = append(info, fmt.Sprintf("read %v failed, %v", fi.Name(), e))
			continue
		}
		if len(b) < Log_HsHeadSize {
			info = append(info, fmt.Sprintf("read size too small, %d", len(b)))
			continue
		}
		if binary.LittleEndian.Uint32(b[0:]) != Log_Magic {
			info = append(info, fmt.Sprintf("read %v bad magic", fi.Name()))
			continue
		}
		if sid := binary.LittleEndian.Uint64(b[8:]); sid != shardId {
			info = append(info, fmt.Sprintf("read %v shard id not match %d != %d", fi.Name(), sid, shardId))
			continue
		}
		seqId := binary.LittleEndian.Uint64(b[16:])
		if seqId < hf.seqId {
			info = append(info, fmt.Sprintf("read %v smaller logic id %d", fi.Name(), seqId))
			continue
		}
		hsLen := int(binary.LittleEndian.Uint32(b[24:]))
		snapLen := int(binary.LittleEndian.Uint32(b[28:]))
		if Log_HsHeadSize+hsLen+snapLen != len(b) {
			info = append(info, fmt.Sprintf("read %v short of data, %d, %d", fi.Name(), Log_HsHeadSize+hsLen+snapLen, len(b)))
			continue
		}
		if e = json.Unmarshal(b[Log_HsHeadSize:Log_HsHeadSize+hsLen], hs); e != nil {
			info = append(info, fmt.Sprintf("read %v Unmarshal hs failed, %v", fi.Name(), e))
			continue
		}
		if e = json.Unmarshal(b[Log_HsHeadSize+hsLen:], snap); e != nil {
			info = append(info, fmt.Sprintf("read %v Unmarshal snap failed, %v", fi.Name(), e))
			continue
		}
		hf.seqId = seqId
		hf.setMeta(hs, snap)
	}
	if hf.hsValue == false && hf.snapValue == false {
		if len(info) == 0 {
			return hf, Err_NoHardStateFile
		}
		return nil, fmt.Errorf("%v", info)
	}
	return hf, nil
}
func NewHardStateFile(dir string, shardId uint64, hs *proto.HardState, snap *proto.SnapshotMeta) (hf *HardStateFile, e error) {
	hf = &HardStateFile{shardId: shardId, seqId: 0, dir: dir}
	if hs != nil || snap != nil {
		if e = hf.write(hs, snap); e == nil {
			hf.setMeta(hs, snap)
		} else {
			return nil, e
		}
	}
	return hf, nil
}
func (hf *HardStateFile) write(hs *proto.HardState, snap *proto.SnapshotMeta) error {
	if hs == nil {
		hs = &hf.hs
	}
	if snap == nil {
		snap = &hf.snap
	}
	hsData, e := json.Marshal(hs)
	if e != nil {
		return e
	}
	snapData, e := json.Marshal(snap)
	if e != nil {
		return e
	}
	co := crc32.NewIEEE()
	head := hsHeadBufferPool.GetBuffer()
	defer hsHeadBufferPool.PutBuffer(head)

	binary.LittleEndian.PutUint32(head[0:], Log_Magic)
	binary.LittleEndian.PutUint64(head[8:], hf.shardId)
	binary.LittleEndian.PutUint64(head[16:], hf.seqId+1)
	binary.LittleEndian.PutUint32(head[24:], uint32(len(hsData)))
	binary.LittleEndian.PutUint32(head[28:], uint32(len(snapData)))

	co.Write(head[8:])
	co.Write(hsData)
	co.Write(snapData)
	binary.LittleEndian.PutUint32(head[4:], co.Sum32())

	path := hf.dir + HsFileName(hf.shardId, hf.seqId+1)
	f, e := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0664)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(head); e == nil {
		if _, e = f.Write(hsData); e == nil {
			_, e = f.Write(snapData)
		}
	}
	if e == nil {
		hf.setMeta(hs, snap)
		hf.seqId++
	}
	return e
}
func (hf *HardStateFile) getHardState() (*proto.HardState, error) {
	if !hf.hsValue {
		return nil, Err_EmptyHardState
	}
	return copyHardState(&hf.hs, new(proto.HardState)), nil
}
func (hf *HardStateFile) getSnapshotMeta() (*proto.SnapshotMeta, error) {
	if !hf.snapValue {
		return nil, Err_EmptySnapshotMeta
	}
	return copySnapshotMeta(&hf.snap, new(proto.SnapshotMeta)), nil
}

func (hf *HardStateFile) setMeta(hs *proto.HardState, snap *proto.SnapshotMeta) {
	if hs != nil {
		copyHardState(hs, &hf.hs)
		hf.hsValue = true
	}
	if snap != nil {
		copySnapshotMeta(snap, &hf.snap)
		hf.snapValue = true
	}
}
func copyHardState(src, dst *proto.HardState) *proto.HardState {
	dst.Term = src.Term
	dst.Commit = src.Commit
	dst.Vote = src.Vote
	return dst
}
func copySnapshotMeta(src, dst *proto.SnapshotMeta) *proto.SnapshotMeta {
	dst.Index = src.Index
	dst.Term = src.Term
	dst.Peers = make([]proto.Peer, len(src.Peers))
	for i := range dst.Peers {
		dst.Peers[i] = src.Peers[i]
	}
	return dst
}
