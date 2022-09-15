// Copyright 2018 The tiglabs raft Authors.
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
package proto

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/util"
)

const (
	version1        byte   = 1
	peer_size       uint64 = 11
	entry_header    uint64 = 17
	snapmeta_header uint64 = 20
	message_header  uint64 = 68

	learner_size     uint64 = 10
	learner_len_size uint64 = 4

	snap_version_length uint64 = 4
)

// Peer codec
func (p *Peer) Encode(datas []byte) {
	datas[0] = byte(p.Type)
	binary.BigEndian.PutUint16(datas[1:], p.Priority)
	binary.BigEndian.PutUint64(datas[3:], p.ID)
}

func (p *Peer) Decode(datas []byte) {
	p.Type = PeerType(datas[0])
	p.Priority = binary.BigEndian.Uint16(datas[1:])
	p.ID = binary.BigEndian.Uint64(datas[3:])
}

// Learner codec
func (learner *Learner) Encode(datas []byte) {
	if learner.PromConfig.AutoPromote {
		datas[0] = 1
	} else {
		datas[0] = 0
	}
	datas[1] = learner.PromConfig.PromThreshold
	binary.BigEndian.PutUint64(datas[2:], learner.ID)
}

func (learner *Learner) Decode(datas []byte) {
	learner.PromConfig.AutoPromote = datas[0] == 1
	learner.PromConfig.PromThreshold = datas[1]
	learner.ID = binary.BigEndian.Uint64(datas[2:])
}

// HardState codec
func (c *HardState) Encode(datas []byte) {
	binary.BigEndian.PutUint64(datas[0:], c.Term)
	binary.BigEndian.PutUint64(datas[8:], c.Commit)
	binary.BigEndian.PutUint64(datas[16:], c.Vote)
}

func (c *HardState) Decode(datas []byte) {
	c.Term = binary.BigEndian.Uint64(datas[0:])
	c.Commit = binary.BigEndian.Uint64(datas[8:])
	c.Vote = binary.BigEndian.Uint64(datas[16:])
}

func (c *HardState) Size() uint64 {
	return 24
}

// ConfChange codec
func (c *ConfChange) Encode() []byte {
	datas := make([]byte, 1+peer_size+uint64(len(c.Context)))
	datas[0] = byte(c.Type)
	c.Peer.Encode(datas[1:])
	if len(c.Context) > 0 {
		copy(datas[peer_size+1:], c.Context)
	}
	return datas
}

func (c *ConfChange) Decode(datas []byte) {
	c.Type = ConfChangeType(datas[0])
	c.Peer.Decode(datas[1:])
	if uint64(len(datas)) > peer_size+1 {
		c.Context = append([]byte{}, datas[peer_size+1:]...)
	}
}

// SnapshotMeta codec
func (m *SnapshotMeta) Size() uint64 {
	return snapmeta_header + peer_size*uint64(len(m.Peers)) + learner_len_size + learner_size*uint64(len(m.Learners)) + snap_version_length
}

func (m *SnapshotMeta) Encode(w io.Writer) error {
	buf := getByteSlice()
	defer returnByteSlice(buf)

	binary.BigEndian.PutUint64(buf, m.Index)
	binary.BigEndian.PutUint64(buf[8:], m.Term)
	binary.BigEndian.PutUint32(buf[16:], uint32(len(m.Peers)))
	if _, err := w.Write(buf[0:snapmeta_header]); err != nil {
		return err
	}

	for _, p := range m.Peers {
		p.Encode(buf)
		if _, err := w.Write(buf[0:peer_size]); err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint32(buf[0:], uint32(len(m.Learners)))
	if _, err := w.Write(buf[0:learner_len_size]); err != nil {
		return err
	}
	for _, learner := range m.Learners {
		learner.Encode(buf)
		if _, err := w.Write(buf[0:learner_size]); err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint32(buf[0:], m.SnapV)
	if _, err := w.Write(buf[0:snap_version_length]); err != nil {
		return err
	}
	return nil
}

func (m *SnapshotMeta) Decode(datas []byte) {
	m.Index = binary.BigEndian.Uint64(datas)
	m.Term = binary.BigEndian.Uint64(datas[8:])
	size := binary.BigEndian.Uint32(datas[16:])
	m.Peers = make([]Peer, size)
	start := snapmeta_header
	for i := uint32(0); i < size; i++ {
		m.Peers[i].Decode(datas[start:])
		start = start + peer_size
	}

	if uint64(len(datas)) > start {
		learnerSize := binary.BigEndian.Uint32(datas[start:])
		m.Learners = make([]Learner, learnerSize)
		start = start + learner_len_size
		for i := uint32(0); i < learnerSize; i++ {
			m.Learners[i] = Learner{PromConfig: &PromoteConfig{}}
			m.Learners[i].Decode(datas[start:])
			start = start + learner_size
		}
	}

	m.SnapV = 0
	if uint64(len(datas)) > start {
		m.SnapV = binary.BigEndian.Uint32(datas[start:])
	}
}

// Entry codec
func (e *Entry) Size() uint64 {
	return entry_header + uint64(len(e.Data))
}

func (e *Entry) Encode(w io.Writer) error {
	buf := getByteSlice()
	defer func() {
		returnByteSlice(buf)
	}()

	buf[0] = byte(e.Type)
	binary.BigEndian.PutUint64(buf[1:], e.Term)
	binary.BigEndian.PutUint64(buf[9:], e.Index)
	if _, err := w.Write(buf[0:entry_header]); err != nil {
		return err
	}

	if len(e.Data) > 0 {
		if _, err := w.Write(e.Data); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entry) Decode(datas []byte) {
	e.Type = EntryType(datas[0])
	e.Term = binary.BigEndian.Uint64(datas[1:])
	e.Index = binary.BigEndian.Uint64(datas[9:])
	if uint64(len(datas)) > entry_header {
		e.Data = datas[entry_header:]
	}
}

// Message codec
func (m *Message) Size() uint64 {
	if m.Type == ReqMsgSnapShot {
		return message_header + m.SnapshotMeta.Size()
	}

	size := message_header + 4
	if len(m.Entries) > 0 {
		for _, e := range m.Entries {
			size = size + e.Size() + 4
		}
	}
	if len(m.Context) > 0 {
		size = size + uint64(len(m.Context))
	}
	return size
}

func (m *Message) Encode(w io.Writer) error {

	buf := getByteSlice()
	defer func() {
		returnByteSlice(buf)
	}()

	binary.BigEndian.PutUint32(buf, uint32(m.Size()))
	buf[4] = version1
	buf[5] = byte(m.Type)
	if m.ForceVote {
		buf[6] = 1
	} else {
		buf[6] = 0
	}
	if m.Reject {
		buf[7] = 1
	} else {
		buf[7] = 0
	}
	binary.BigEndian.PutUint64(buf[8:], m.RejectIndex)
	binary.BigEndian.PutUint64(buf[16:], m.ID)
	binary.BigEndian.PutUint64(buf[24:], m.From)
	binary.BigEndian.PutUint64(buf[32:], m.To)
	binary.BigEndian.PutUint64(buf[40:], m.Term)
	binary.BigEndian.PutUint64(buf[48:], m.LogTerm)
	binary.BigEndian.PutUint64(buf[56:], m.Index)
	binary.BigEndian.PutUint64(buf[64:], m.Commit)
	if _, err := w.Write(buf[0 : message_header+4]); err != nil {
		return err
	}

	if m.Type == ReqMsgSnapShot {
		return m.SnapshotMeta.Encode(w)
	}

	binary.BigEndian.PutUint32(buf, uint32(len(m.Entries)))
	if _, err := w.Write(buf[0:4]); err != nil {
		return err
	}
	if len(m.Entries) > 0 {
		for _, e := range m.Entries {
			binary.BigEndian.PutUint32(buf, uint32(e.Size()))
			if _, err := w.Write(buf[0:4]); err != nil {
				return err
			}
			if err := e.Encode(w); err != nil {
				return err
			}
			//PutEntryToPool(e)
		}
	}
	if len(m.Context) > 0 {
		if _, err := w.Write(m.Context); err != nil {
			return err
		}
	}
	return nil
}

func (m *Message) Decode(r *util.BufferReader) error {
	var (
		datas []byte
		err   error
	)
	if datas, err = r.ReadFull(4); err != nil {
		return err
	}
	dataLen := int(binary.BigEndian.Uint32(datas))
	if datas, err = r.ReadFull(dataLen); err != nil {
		return err
	}

	if len(datas) < int(message_header) {
		logger.Warn("message Decode: the length of data(%v) less than header length(%v) read length(%v)", len(datas), message_header, dataLen)
	}

	ver := datas[0]
	if ver == version1 {
		m.Type = MsgType(datas[1])
		m.ForceVote = datas[2] == 1
		m.Reject = datas[3] == 1
		m.RejectIndex = binary.BigEndian.Uint64(datas[4:])
		m.ID = binary.BigEndian.Uint64(datas[12:])
		m.From = binary.BigEndian.Uint64(datas[20:])
		m.To = binary.BigEndian.Uint64(datas[28:])
		m.Term = binary.BigEndian.Uint64(datas[36:])
		m.LogTerm = binary.BigEndian.Uint64(datas[44:])
		m.Index = binary.BigEndian.Uint64(datas[52:])
		m.Commit = binary.BigEndian.Uint64(datas[60:])
		if m.Type == ReqMsgSnapShot {
			m.SnapshotMeta.Decode(datas[message_header:])
		} else {
			size := binary.BigEndian.Uint32(datas[message_header:])
			start := message_header + 4
			if size > 0 {
				for i := uint32(0); i < size; i++ {
					esize := binary.BigEndian.Uint32(datas[start:])
					start = start + 4
					end := start + uint64(esize)
					entry := new(Entry)
					//entry:=GetEntryFromPoolWithFollower()
					entry.Decode(datas[start:end])
					m.Entries = append(m.Entries, entry)
					start = end
				}
			}
			if start < uint64(len(datas)) {
				m.Context = datas[start:]
			}
		}
	}
	return nil
}

func EncodeHBContext(ctx HeartbeatContext) (buf []byte) {
	sort.Slice(ctx, func(i, j int) bool {
		return ctx[i].ID < ctx[j].ID
	})

	scratch := make([]byte, binary.MaxVarintLen64)
	prev := uint64(0)
	for _, entry := range ctx {
		n := binary.PutUvarint(scratch, entry.ID-prev)
		buf = append(buf, scratch[:n]...)
		prev = entry.ID
		if entry.IsUnstable {
			buf = append(buf, 0)
		}
	}
	return
}

func DecodeHBContext(buf []byte) (ctx HeartbeatContext) {
	prev := uint64(0)
	for len(buf) > 0 {
		id, n := binary.Uvarint(buf)
		buf = buf[n:]
		ctxEnt := ContextInfo{
			ID: id + prev,
			IsUnstable: func() bool {
				if len(buf) > 0 && buf[0] == 0 {
					buf = buf[1:]
					return true
				}
				return false
			}(),
		}
		ctx = append(ctx, ctxEnt)
		prev = id + prev
	}
	return
}
