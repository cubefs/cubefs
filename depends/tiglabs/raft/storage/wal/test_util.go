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
package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
)

func compapreEntry(le, re *proto.Entry) error {
	if le.Index != re.Index {
		return fmt.Errorf("unmatch index: %d != %d", le.Index, re.Index)
	}
	if le.Type != re.Type {
		return fmt.Errorf("unmatch type: %d != %d", le.Type, re.Type)
	}
	if le.Term != re.Term {
		return fmt.Errorf("unmatch term: %d != %d", le.Term, re.Term)
	}
	if !bytes.Equal(le.Data, re.Data) {
		return fmt.Errorf("unmatch data: %s != %s", string(le.Data), string(re.Data))
	}
	return nil
}

func compareEntries(lh, rh []*proto.Entry) error {
	if len(lh) != len(rh) {
		return fmt.Errorf("unmatch size: %d != %d", len(lh), len(rh))
	}

	for i := 0; i < len(lh); i++ {
		le := lh[i]
		re := rh[i]
		if err := compapreEntry(le, re); err != nil {
			return fmt.Errorf("%v at %d", err, i)
		}
	}
	return nil
}

func genLogEntry(rnd *rand.Rand, i uint64) *proto.Entry {
	randType := func() proto.EntryType {
		switch rnd.Int() % 2 {
		case 0:
			return proto.EntryNormal
		default:
			return proto.EntryConfChange
		}
	}
	randTerm := func() uint64 {
		return uint64(rnd.Uint32())
	}
	randData := func() []byte {
		const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		length := 10 + rnd.Int()%100
		buf := make([]byte, length)
		for i := 0; i < length; i++ {
			buf[i] = letters[rnd.Int()%len(letters)]
		}
		return buf
	}
	ent := &proto.Entry{
		Index: i,
		Type:  randType(),
		Term:  randTerm(),
		Data:  randData(),
	}
	return ent
}

func genLogEntries(lo, hi uint64) (ents []*proto.Entry) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := lo; i < hi; i++ {
		ents = append(ents, genLogEntry(rnd, i))
	}
	return
}
