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

package raftserver

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/log"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	raftMsgUrl  = "/raftMsgs"
	snapshotUrl = "/snapshot"
)

type Peer struct {
	Id              uint64 `json:"id"`
	Host            string `json:"host"`
	Match           uint64 `json:"match"`
	Next            uint64 `json:"next"`
	State           string `json:"state"`
	Paused          bool   `json:"paused"`
	PendingSnapshot uint64 `json:"pendingSnapshot"`
	RecentActive    bool   `json:"active"`
	IsLearner       bool   `json:"isLearner"`
	InflightFull    bool   `json:"isInflightFull"`
	InflightCount   int    `json:"inflightCount"`
}

type Status struct {
	Id             uint64 `json:"nodeId"`
	Term           uint64 `json:"term"`
	Vote           uint64 `json:"vote"`
	Commit         uint64 `json:"commit"`
	Leader         uint64 `json:"leader"`
	RaftState      string `json:"raftState"`
	Applied        uint64 `json:"applied"`
	RaftApplied    uint64 `json:"raftApplied"`
	LeadTransferee uint64 `json:"transferee"`
	ApplyingLength int    `json:"applyingLength"`
	Peers          []Peer `json:"peers"`
}

type Members struct {
	Mbs []Member `json:"members"`
}

type notifier chan error

func newNotifier() notifier {
	return make(chan error, 1)
}

func (nc notifier) notify(err error) {
	select {
	case nc <- err:
	default:
	}
}

func (nc notifier) wait(ctx context.Context, stopc <-chan struct{}) error {
	select {
	case err := <-nc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-stopc:
		return ErrStopped
	}
}

type readIndexNotifier struct {
	ch  chan struct{}
	err error
}

func newReadIndexNotifier() *readIndexNotifier {
	return &readIndexNotifier{
		ch: make(chan struct{}),
	}
}

func (nr *readIndexNotifier) Notify(err error) {
	nr.err = err
	close(nr.ch)
}

func (nr *readIndexNotifier) Wait(ctx context.Context, stopc <-chan struct{}) error {
	select {
	case <-nr.ch:
		return nr.err
	case <-ctx.Done():
		return ctx.Err()
	case <-stopc:
		return ErrStopped
	}
}

func normalEntryEncode(id uint64, data []byte) []byte {
	b := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(b[0:8], id)
	copy(b[8:], data)
	return b
}

func normalEntryDecode(data []byte) (uint64, []byte) {
	id := binary.BigEndian.Uint64(data[0:8])
	return id, data[8:]
}

type raftMsgs []pb.Message

func (msgs raftMsgs) Len() int {
	return len([]pb.Message(msgs))
}

// msgcnt   4 bytes
// len|recoder
// ...
// len|recoder
// crc      4 bytes
func (msgs raftMsgs) Encode(w io.Writer) error {
	crc := crc32.NewIEEE()
	mw := io.MultiWriter(w, crc)
	cnt := uint32(msgs.Len())
	b := make([]byte, 4)

	// write header
	binary.BigEndian.PutUint32(b, cnt)
	if _, err := w.Write(b); err != nil {
		return err
	}

	// write body
	for i := 0; i < msgs.Len(); i++ {
		buf := bytespool.Alloc(4 + msgs[i].Size())
		binary.BigEndian.PutUint32(buf, uint32(msgs[i].Size()))
		_, err := msgs[i].MarshalTo(buf[4:])
		if err != nil {
			bytespool.Free(buf)
			return err
		}
		if _, err = mw.Write(buf); err != nil {
			bytespool.Free(buf)
			return err
		}
		bytespool.Free(buf)
	}

	// write checksum
	binary.BigEndian.PutUint32(b, crc.Sum32())
	_, err := w.Write(b)
	return err
}

func (msgs raftMsgs) Decode(r io.Reader) (raftMsgs, error) {
	w := crc32.NewIEEE()
	tr := io.TeeReader(r, w)

	b := make([]byte, 4)
	// read msgcnt
	if _, err := io.ReadFull(r, b); err != nil {
		log.Errorf("read head[msgcnt] error: %v", err)
		return nil, err
	}
	cnt := binary.BigEndian.Uint32(b)
	msgs = make([]pb.Message, 0, cnt)
	for i := 0; i < int(cnt); i++ {
		// read recorder len
		if _, err := io.ReadFull(tr, b); err != nil {
			log.Errorf("read the %d's recorder len error: %v", i, err)
			return nil, err
		}
		msglen := binary.BigEndian.Uint32(b)
		data := bytespool.Alloc(int(msglen))
		// read recorder
		if _, err := io.ReadFull(tr, data); err != nil {
			log.Errorf("read the %d's recorder error: %v", i, err)
			bytespool.Free(data)
			return nil, err
		}
		var msg pb.Message
		if err := msg.Unmarshal(data); err != nil {
			bytespool.Free(data)
			return nil, err
		}
		bytespool.Free(data)
		msgs = append(msgs, msg)
	}
	// read checksum
	if _, err := io.ReadFull(r, b); err != nil {
		log.Errorf("read checksum error: %v", err)
		return nil, err
	}
	if binary.BigEndian.Uint32(b) != w.Sum32() {
		log.Error("checksum not match")
		return nil, ErrInvalidData
	}

	return msgs, nil
}

type propose struct {
	id        uint64
	nr        notifier
	entryType pb.EntryType
	b         []byte
}

type snapshot struct {
	st     Snapshot
	meta   SnapshotMeta
	expire time.Time
}

func (s *snapshot) Read() ([]byte, error) {
	return s.st.Read()
}

func (s *snapshot) Name() string {
	return s.st.Name()
}

func (s *snapshot) Index() uint64 {
	return s.st.Index()
}

func (s *snapshot) Close() {
	s.st.Close()
}

type applySnapshot struct {
	meta SnapshotMeta
	r    io.Reader
	nr   notifier
}

func newApplySnapshot(r io.Reader) Snapshot {
	return &applySnapshot{
		r:  r,
		nr: newNotifier(),
	}
}

func (s *applySnapshot) Read() ([]byte, error) {
	b := make([]byte, 4)
	crc := crc32.NewIEEE()
	tr := io.TeeReader(s.r, crc)

	// read msg header  4 bytes
	_, err := io.ReadFull(s.r, b)
	if err != nil {
		if err != io.EOF {
			log.Errorf("read header of snapshot error: %v", err)
		}
		return nil, err
	}

	// read msg body
	msgLen := int(binary.BigEndian.Uint32(b)) // recorder len
	body := make([]byte, msgLen)
	if _, err = io.ReadFull(tr, body); err != nil {
		log.Errorf("read recorder of snapshot error: %v len(%d)", err, msgLen)
		return nil, err
	}

	// read checksum and check
	if _, err = io.ReadFull(s.r, b); err != nil {
		log.Errorf("read checksum of snapshot error: %v", err)
		return nil, err
	}
	if binary.BigEndian.Uint32(b) != crc.Sum32() {
		log.Error("checksum not match")
		return nil, ErrInvalidData
	}
	return body, nil
}

func (s *applySnapshot) Name() string {
	return s.meta.Name
}

func (s *applySnapshot) Index() uint64 {
	return s.meta.Index
}

func (s *applySnapshot) Close() {
	// nothing to do
}
