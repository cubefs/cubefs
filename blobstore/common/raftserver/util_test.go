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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestNotifier(t *testing.T) {
	nt := newNotifier()
	stopc := make(chan struct{})
	go func() {
		nt.notify(errors.New("error"))
	}()
	assert.NotNil(t, nt.wait(context.TODO(), stopc))

	nt = newNotifier()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	assert.NotNil(t, nt.wait(ctx, stopc))
	cancel()

	nt = newNotifier()
	go func() {
		nt.notify(nil)
	}()
	assert.Nil(t, nt.wait(context.TODO(), stopc))

	nt = newNotifier()
	go func() {
		close(stopc)
	}()
	assert.NotNil(t, nt.wait(context.TODO(), stopc))
}

func TestNormalEntryDecode(t *testing.T) {
	data := normalEntryEncode(100, []byte("123456"))
	id, b := normalEntryDecode(data)
	require.Equal(t, uint64(100), id)
	require.Equal(t, []byte("123456"), b)
}

func TestProto(t *testing.T) {
	var ms raftMsgs
	datas := [][]byte{
		[]byte("daweuiriooidaohnr"),
		[]byte("bnufdaiurekjhitu"),
		[]byte("yiuewqojitruiewouio"),
		[]byte("eqopiwerhnewhioruthor"),
		[]byte("yuweqj;eoritrowiojopi"),
	}
	for i := 0; i < len(datas); i++ {
		ms = append(ms, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: datas[i]}}})
	}

	buffer := &bytes.Buffer{}
	_ = ms.Encode(buffer)

	var (
		nms raftMsgs
		err error
	)
	nms, err = nms.Decode(buffer)
	assert.Nil(t, err)
	assert.Equal(t, nms, ms)

	buffer.Reset()
	buffer.Write([]byte("123"))
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)
	buffer.Reset()

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 4)
	buffer.Write(b)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, 10) // write msg len
	buffer.Write(b)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, 10) // write msg len
	buffer.Write(b)
	buffer.Write([]byte("0123456789"))
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	msg := &pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("njdaiuerjkteia")}}}
	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, uint32(msg.Size())) // write msg len
	buffer.Write(b)
	mdata, _ := msg.Marshal()
	buffer.Write(mdata)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, uint32(msg.Size())) // write msg len
	buffer.Write(b)
	buffer.Write(mdata)
	binary.BigEndian.PutUint32(b, 783287498) // write crc
	buffer.Write(b)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)
}
